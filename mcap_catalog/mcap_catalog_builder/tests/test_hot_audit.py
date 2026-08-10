"""Tier-2 hot-window scoped audit (design 2026-07-30 §4): scoped LIST,
registry derivation, fail-closed coverage, scoped sweep.

S3-ONLY by decision (Codex consult 2026-08-10): ``LocalSource.intended_key``
can override a file's identity via an embedded ``s3_key``, so raw-path prefix
scoping would mis-attribute local rows — the hot audit is wired for
``--source s3`` and these tests drive ``S3Source`` over injected fakes.
"""

import datetime as dt
import threading

import pytest

from mcap_catalog_builder.reconcile import ReconcileCancelled, full_reconcile
from mcap_catalog_builder.s3_storage import S3Source
from mcap_catalog_builder.tests.fixtures import InMemoryS3Client, write_minimal_mcap

CH = [("/a", "S", "ros2msg", 2)]
TODAY = dt.date(2026, 6, 2)
PA = "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-02/"
PB = "customer=beta/customer_site=hq/robot=r2/source=ros-bags/date=2026-06-02/"
KA = PA + "a.mcap"
KB = PB + "b.mcap"


def _raw(tmp_path):
    local = str(tmp_path / "src.mcap")
    write_minimal_mcap(local, channels=CH)
    with open(local, "rb") as f:
        return f.read()


class PagedClient(InMemoryS3Client):
    """Two-page pagination for list_objects_v2 (InMemoryS3Client is single-page)."""

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, ContinuationToken=None):
        keys = sorted(k for k in self._objects if k.startswith(Prefix))
        page = keys[:1] if ContinuationToken is None else keys[1:]
        return {
            "Contents": [{"Key": k, "Size": len(self._objects[k]),
                          "ETag": f'"etag-{k}"'} for k in page],
            "IsTruncated": ContinuationToken is None and len(keys) > 1,
            "NextContinuationToken": "page2",
        }


class Page2FailsClient(PagedClient):
    """Page 1 OK, page 2 raises for keys under fail_prefix — a PARTIAL listing."""

    def __init__(self, objects, fail_prefix):
        super().__init__(objects)
        self._fail_prefix = fail_prefix

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, ContinuationToken=None):
        if Prefix.startswith(self._fail_prefix) and ContinuationToken is not None:
            raise RuntimeError("page 2 boom")
        return super().list_objects_v2(Bucket, Prefix, Delimiter, ContinuationToken)


# -- list_prefix --------------------------------------------------------------

def test_s3_list_prefix_paginates_to_completion(tmp_path):
    raw = _raw(tmp_path)
    src = S3Source(PagedClient({KA: raw, PA + "a2.mcap": raw, KB: raw}), "bucket")
    got = src.list_prefix(PA)
    assert sorted(l.key for l in got) == [KA, PA + "a2.mcap"]  # complete, scoped
    for l in got:
        assert l.stat.etag  # fingerprints come from the LIST itself


def test_s3_list_prefix_raises_on_page_failure_never_partial(tmp_path):
    raw = _raw(tmp_path)
    src = S3Source(Page2FailsClient({KA: raw, PA + "a2.mcap": raw}, PA), "bucket")
    with pytest.raises(RuntimeError, match="page 2 boom"):
        src.list_prefix(PA)


def test_s3_list_prefix_stop_raises_cancelled(tmp_path):
    raw = _raw(tmp_path)
    src = S3Source(PagedClient({KA: raw}), "bucket")
    stop = threading.Event()
    stop.set()
    with pytest.raises(ReconcileCancelled):
        src.list_prefix(PA, stop_event=stop)


# -- registry derivation (§4.1) ----------------------------------------------

from mcap_catalog_builder.db import record_failure
from mcap_catalog_builder.hot_audit import hot_prefixes, registry_combos


def test_registry_combos_from_files_and_quarantine(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _raw(tmp_path)
    full_reconcile(conn, caches, S3Source(InMemoryS3Client({KA: raw}), "bucket"))
    # A quarantined key's combo must NOT be exiled from the registry (§4.1:
    # quarantine deletes the files row, so files alone would forget it).
    record_failure(conn, KB, "boom")
    conn.commit()
    combos = registry_combos(conn)
    assert ("acme", "hq", "r1", "ros-bags") in combos
    assert ("beta", "hq", "r2", "ros-bags") in combos
    # An unparseable failure key contributes nothing (and does not raise).
    record_failure(conn, "not-a-hive-key.mcap", "boom")
    conn.commit()
    assert len(registry_combos(conn)) == 2


def test_hot_prefixes_window_and_shape():
    combos = {("acme", "hq", "r1", "ros-bags")}
    targets = hot_prefixes(combos, TODAY, window_days=2)
    assert [t[0] for t in targets] == [
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-05-31/",
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-01/",
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-02/",
    ]
    prefix, combo, date = targets[-1]
    assert combo == ("acme", "hq", "r1", "ros-bags") and date == "2026-06-02"
