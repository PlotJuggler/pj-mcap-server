"""Tests for the S3 backend: range-GET reader + S3Source.

A FakeS3 client serves bytes from an in-memory dict and records every range
requested, so the tests assert the cheap-read property (R2) — that reading a
summary never downloads the message body — without touching AWS or boto3.
"""

import io
import math
import threading
import time

import pytest
from mcap.writer import CompressionType, Writer

from mcap_catalog_builder.mcap_summary import read_file_summary, summary_from_stream
from mcap_catalog_builder.s3_storage import (
    S3RangeReader,
    S3Source,
    _is_missing,
    _is_permanent,
)
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap


class _FakeClientError(Exception):
    """Mimics botocore's ClientError shape for the missing-object path."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    """In-memory S3 stand-in recording the byte ranges (and LIST calls) it is
    asked for. ``list_objects_v2`` is delimiter-aware (mimics real S3's
    ``CommonPrefixes`` behavior) so it can serve both the top-level shard
    discovery call and (via the fallback paginator) a flat listing."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self._objects = objects
        self.ranges: list[tuple[int, int]] = []  # (start, end) inclusive
        self.fetched = 0
        self.list_calls: list[dict] = []

    def head_object(self, Bucket, Key):
        if Key not in self._objects:
            raise _FakeClientError("404")
        return {"ContentLength": len(self._objects[Key]), "ETag": f'"etag-{Key}"'}

    def get_object(self, Bucket, Key, Range):
        assert Range.startswith("bytes=")
        start_s, end_s = Range[len("bytes="):].split("-")
        start, end = int(start_s), int(end_s)
        self.ranges.append((start, end))
        chunk = self._objects[Key][start:end + 1]
        self.fetched += len(chunk)
        return {"Body": io.BytesIO(chunk)}

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, ContinuationToken=None):
        self.list_calls.append({"Prefix": Prefix, "Delimiter": Delimiter,
                                 "ContinuationToken": ContinuationToken})
        assert Delimiter in (None, "/")
        keys = sorted(k for k in self._objects if k.startswith(Prefix))
        if Delimiter is None:
            return {"Contents": [{"Key": k, "Size": len(self._objects[k]),
                                   "ETag": f'"etag-{k}"'} for k in keys],
                    "IsTruncated": False}
        prefixes, contents = [], []
        for k in keys:
            rest = k[len(Prefix):]
            if "/" in rest:
                p = Prefix + rest.split("/", 1)[0] + "/"
                if p not in prefixes:
                    prefixes.append(p)
            else:
                contents.append({"Key": k, "Size": len(self._objects[k]),
                                  "ETag": f'"etag-{k}"'})
        return {"CommonPrefixes": [{"Prefix": p} for p in prefixes],
                "Contents": contents, "IsTruncated": False}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self._objects
        list_calls = self.list_calls

        class _Paginator:
            def paginate(self, Bucket, Prefix=""):
                list_calls.append({"Prefix": Prefix, "Delimiter": None,
                                    "ContinuationToken": None, "shard_paginate": True})
                contents = [
                    {"Key": k, "Size": len(v), "ETag": f'"etag-{k}"'}
                    for k, v in objects.items()
                    if k.startswith(Prefix)
                ]
                yield {"Contents": contents}

        return _Paginator()


class LazyPaginatorS3(FakeS3):
    """A ``FakeS3`` whose shard paginator streams pages ONE AT A TIME (a real
    generator advanced lazily by each producer thread's ``for page in
    paginate():`` loop) instead of a pre-built list, and counts how many pages
    have been pulled out of that generator so far (thread-safe).

    A page only leaves the underlying per-shard generator when the produce()
    loop asks for the next one — which (in a correctly bounded implementation)
    only happens after the previous page was successfully queued. So
    ``pages_yielded`` is a direct proxy for "pages pulled off S3 but not yet
    retired by the consumer": if a broken implementation materializes a whole
    shard eagerly (e.g. ``list(paginator.paginate(...))``), this counter jumps
    by a whole shard's page count in one shot, long before the consumer has
    dequeued anything — which is exactly the regression these tests catch.

    ``pages_by_shard_prefix``: ``{prefix: [ [item-dict, ...], ... ]}`` — one
    list of pages per shard prefix (as produced by the ``Delimiter="/"``
    discovery call). ``fail_prefix`` (optional): a shard prefix whose
    paginator raises ``RuntimeError`` after its first page (for the
    error-propagation/teardown test).
    """

    def __init__(self, objects, pages_by_shard_prefix, fail_prefix=None):
        super().__init__(objects)
        self._pages_by_shard_prefix = pages_by_shard_prefix
        self._fail_prefix = fail_prefix
        self._lock = threading.Lock()
        self.pages_yielded = 0

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        outer = self

        class _Paginator:
            def paginate(self, Bucket, Prefix=""):
                pages = outer._pages_by_shard_prefix[Prefix]
                for i, page_items in enumerate(pages):
                    if outer._fail_prefix == Prefix and i == 1:
                        raise RuntimeError(f"simulated shard failure: {Prefix}")
                    with outer._lock:
                        outer.pages_yielded += 1
                    yield {"Contents": page_items}

        return _Paginator()


# --- S3RangeReader ---------------------------------------------------------

def test_range_reader_reads_exact_slice():
    data = bytes(range(256))
    r = S3RangeReader(FakeS3({"k": data}), "b", "k", len(data))
    assert r.seek(10) == 10
    assert r.read(5) == data[10:15]
    assert r.tell() == 15


def test_range_reader_seek_end_and_read_past_eof():
    data = bytes(range(256))
    client = FakeS3({"k": data})
    r = S3RangeReader(client, "b", "k", len(data))
    assert r.seek(-4, io.SEEK_END) == 252
    assert r.read(10) == data[252:256]   # clamped to EOF, no out-of-range request
    assert r.read(10) == b""             # nothing left
    assert max(end for _, end in client.ranges) <= 255


def test_range_reader_zero_length_readinto_issues_no_request():
    data = bytes(range(256))
    client = FakeS3({"k": data})
    r = S3RangeReader(client, "b", "k", len(data))
    assert r.readinto(bytearray(0)) == 0   # empty target: no bytes=N-(N-1) range
    assert client.ranges == []             # never hit the network


def test_range_reader_never_downloads_whole_object():
    size = 100_000
    client = FakeS3({"k": b"\x00" * size})
    r = S3RangeReader(client, "b", "k", size)
    r.seek(0)
    r.read(8)
    r.seek(size - 10)
    r.read(50)
    assert client.fetched == 18                      # 8 + 10 (tail clamped), body untouched
    assert all(end - start + 1 <= 50 for start, end in client.ranges)


# --- S3Source --------------------------------------------------------------

def test_stat_returns_size_and_unquoted_etag():
    src = S3Source(FakeS3({"k.mcap": b"abc"}), "bucket")
    st = src.stat("k.mcap")
    assert st.size == 3
    assert st.etag == "etag-k.mcap"            # surrounding quotes stripped


def test_stat_missing_returns_none():
    assert S3Source(FakeS3({}), "bucket").stat("gone.mcap") is None


def test_permanent_classifier_includes_auth_and_missing():
    # Auth/bad-request + missing are permanent (not retried); 5xx/throttle retry.
    for code in ("403", "AccessDenied", "Forbidden", "400", "404", "NoSuchKey"):
        assert _is_permanent(_FakeClientError(code)), code
    for code in ("500", "503", "SlowDown"):
        assert not _is_permanent(_FakeClientError(code)), code
    assert _is_missing(_FakeClientError("404")) and not _is_missing(_FakeClientError("403"))


def test_stat_auth_error_raises_not_retried():
    # A 403 must propagate (not be mapped to None, not retried 6x).
    class _Counting(FakeS3):
        def __init__(self):
            super().__init__({})
            self.head_calls = 0

        def head_object(self, Bucket, Key):
            self.head_calls += 1
            raise _FakeClientError("403")

    c = _Counting()
    with pytest.raises(_FakeClientError):
        S3Source(c, "bucket").stat("x.mcap")
    assert c.head_calls == 1  # permanent => no retry


def test_list_all_filters_non_mcap_and_unquotes_etag():
    client = FakeS3({"a/x.mcap": b"1", "a/notes.txt": b"2", "b/y.mcap": b"33"})
    listings = sorted(S3Source(client, "bucket").list_all(), key=lambda x: x.key)
    assert [x.key for x in listings] == ["a/x.mcap", "b/y.mcap"]
    assert listings[0].stat.etag == "etag-a/x.mcap"
    assert listings[1].stat.size == 2


# --- sharded list_all -------------------------------------------------------

def test_list_all_sharded_matches_flat_listing():
    objects = {
        f"pre/customer_site={s}/robot=r{i}/f{i}.mcap": b"x" * 10
        for s in ("a", "b", "c") for i in range(4)
    }
    objects["pre/root-level.mcap"] = b"y" * 3          # key at the prefix root
    objects["pre/customer_site=a/skip.txt"] = b"n"     # non-mcap filtered out
    fake = FakeS3(objects)
    src = S3Source(fake, "b", "pre/")
    got = {l.key: l.stat for l in src.list_all()}
    assert set(got) == {k for k in objects if k.endswith(".mcap")}
    for k, st in got.items():
        assert st.size == len(objects[k]) and st.etag == f"etag-{k}"
    # the sharding actually happened: one Delimiter discovery + per-shard pagination
    assert any(c["Delimiter"] == "/" for c in fake.list_calls)


def test_list_all_flat_bucket_no_shards():
    objects = {f"f{i}.mcap": b"z" for i in range(5)}   # no '/' below the prefix
    fake = FakeS3(objects)
    src = S3Source(fake, "b", "")
    assert {l.key for l in src.list_all()} == set(objects)
    # Even a flat bucket must go through the sharding discovery call (there are
    # simply zero shards to fan out to) — not just the old flat paginator.
    assert any(c["Delimiter"] == "/" for c in fake.list_calls)


# Shape used by the concurrency tests below: N shards, each with the same
# number of same-size pages, so "items consumed so far" maps exactly onto
# "pages dequeued so far" (ceil(items / _PAGE_SIZE)) regardless of how the
# shards interleave through the single shared queue.
_PAGE_SIZE = 20
_PAGES_PER_SHARD = 40  # > _LIST_QUEUE_PAGES (32): a per-shard materialization
                       # regression spikes pages_yielded past the bound in one shot.
_NUM_SHARDS = 4


def _make_lazy_fixture(fail_prefix=None):
    """Objects with one key per shard prefix (drives the Delimiter discovery
    call) + a matching ``pages_by_shard_prefix`` map of many same-size pages."""
    objects = {f"shard{i}/seed.mcap": b"" for i in range(_NUM_SHARDS)}
    pages_by_shard_prefix = {}
    for i in range(_NUM_SHARDS):
        prefix = f"shard{i}/"
        pages_by_shard_prefix[prefix] = [
            [
                {"Key": f"{prefix}f{p}_{j}.mcap", "Size": 1, "ETag": f'"e{p}_{j}"'}
                for j in range(_PAGE_SIZE)
            ]
            for p in range(_PAGES_PER_SHARD)
        ]
    fake = LazyPaginatorS3(objects, pages_by_shard_prefix, fail_prefix=fail_prefix)
    return fake


def _active_threads_baseline() -> int:
    return threading.active_count()


def _wait_for_thread_count(baseline: int, timeout: float = 10.0) -> int:
    """Poll ``threading.active_count()`` back to ``baseline``; return the last
    observed count (so a failing assertion shows the leaked-thread count)."""
    deadline = time.monotonic() + timeout
    count = threading.active_count()
    while count > baseline and time.monotonic() < deadline:
        time.sleep(0.05)
        count = threading.active_count()
    return count


def test_list_all_pages_stream_bounded():
    # Import here so the module-level constant is read fresh (not shadowed by
    # a local of the same name in this test file).
    from mcap_catalog_builder import s3_storage as s3_storage_mod

    fake = _make_lazy_fixture()
    src = S3Source(fake, "b", "")
    gen = src.list_all()

    bound = s3_storage_mod._LIST_QUEUE_PAGES + _NUM_SHARDS  # queue + one in-flight per producer
    max_observed_outstanding = 0
    consumed = 0
    total_items = _NUM_SHARDS * _PAGES_PER_SHARD * _PAGE_SIZE
    for _ in range(total_items):
        next(gen)  # consume ONE item at a time — deliberately slow
        consumed += 1
        dequeued_pages = math.ceil(consumed / _PAGE_SIZE)
        outstanding = fake.pages_yielded - dequeued_pages
        max_observed_outstanding = max(max_observed_outstanding, outstanding)
    with pytest.raises(StopIteration):
        next(gen)

    assert max_observed_outstanding <= bound, (
        f"observed {max_observed_outstanding} pages buffered/in-flight, "
        f"expected <= {bound} (_LIST_QUEUE_PAGES + producer threads) — "
        f"a per-shard materialization would blow well past this"
    )


def test_list_all_early_close_no_deadlock():
    baseline = _active_threads_baseline()
    fake = _make_lazy_fixture()
    src = S3Source(fake, "b", "")
    gen = src.list_all()
    next(gen)  # take exactly one listing

    start = time.monotonic()
    gen.close()
    elapsed = time.monotonic() - start
    assert elapsed < 10.0, f"gen.close() took {elapsed:.2f}s — looks deadlocked"

    final = _wait_for_thread_count(baseline)
    assert final == baseline, f"thread count did not return to baseline ({final} vs {baseline})"


def test_list_all_shard_error_propagates_and_tears_down():
    baseline = _active_threads_baseline()
    fake = _make_lazy_fixture(fail_prefix="shard0/")
    src = S3Source(fake, "b", "")

    with pytest.raises(RuntimeError, match="simulated shard failure"):
        list(src.list_all())

    final = _wait_for_thread_count(baseline)
    assert final == baseline, f"thread count did not return to baseline ({final} vs {baseline})"


def test_s3_event_translation_helpers():
    src = S3Source(FakeS3({}), "bucket")
    assert src.event_key("customer=a/.../x.mcap") == "customer=a/.../x.mcap"  # key is the key
    assert src.intended_key("anything") is None       # the object key is authoritative
    assert src.wait_for_stable("anything") is True     # S3 PUT is atomic, no poll


def test_open_summary_reads_real_mcap_without_downloading_body(tmp_path):
    # A real MCAP with a multi-MB, incompressible body so "body skipped" is
    # unmistakable: the summary section is tiny next to the message chunks.
    dest = str(tmp_path / "big.mcap")
    with open(dest, "wb") as f:
        w = Writer(f, compression=CompressionType.NONE)
        w.start(profile="ros2", library="test")
        sid = w.register_schema(name="S", encoding="ros2msg", data=b"x")
        cid = w.register_channel(topic="/a", message_encoding="cdr", schema_id=sid)
        payload = bytes(range(256)) * 8  # 2 KiB, incompressible-ish
        for t in range(1, 2001):
            w.add_message(channel_id=cid, log_time=t, data=payload, publish_time=t)
        w.finish()
    with open(dest, "rb") as f:
        raw = f.read()
    total = len(raw)
    assert total > 3_000_000  # body dominates

    client = FakeS3({"big.mcap": raw})
    src = S3Source(client, "bucket")
    with src.open_summary("big.mcap", src.stat("big.mcap").size) as stream:
        got = summary_from_stream(stream)

    assert got == read_file_summary(dest)        # identical to the local read
    assert client.fetched < 200_000              # only footer + summary fetched


def test_s3_read_summary_parity_and_single_get(tmp_path):
    p = tmp_path / "a.mcap"
    write_minimal_mcap(str(p))
    data = p.read_bytes()
    fake = FakeS3({"k.mcap": data})
    src = S3Source(fake, "b")
    summary, via = src.read_summary("k.mcap", len(data))
    assert summary == summary_from_stream(io.BytesIO(data)) and via == "targeted"
    assert len(fake.ranges) == 1  # small file: everything in the tail read


def test_s3_read_summary_falls_back_on_garbage(tmp_path):
    data = b"\x00" * 4096
    fake = FakeS3({"k.mcap": data})
    src = S3Source(fake, "b")
    try:
        src.read_summary("k.mcap", len(data))
        raised = None
    except Exception as e:  # noqa: BLE001
        raised = e
    assert raised is not None                      # streamed fallback's verdict
    assert getattr(raised, "summary_via", "") == "fallback-unavailable"
