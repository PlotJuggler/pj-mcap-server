# Builder Scan Speedups Implementation Plan (v3 — two Codex passes folded in)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the catalog builder's cold scan ~1.6× faster and its warm reconcile ~1.7× faster, integrating the levers measured on the real staging bucket on 2026-07-29 — restructured after adversarial review into four independently-green stacked PRs.

**Architecture:** (PR1) a hardened **semantic catalog diff** tool — the only valid equivalence check, used as the gate for every later lever; (PR2) a **targeted MCAP summary read** using the footer's summary-offset section to fetch only the Schema/Channel/Statistics groups through a transport-agnostic `pread`, with an unconditional fallback to the streamed path as the semantics authority, **fallback-disposition telemetry** in the status sidecar, and an explicit `forkserver` mp context (fork-after-threads hazard on prod Python 3.12); (PR3) a **bounded sharded parallel S3 LIST** (streamed pages, never per-shard materialization); (PR4, **gated on in-region measurement**) a backend-aware `--extract-workers` default.

**Tech Stack:** Python 3.10+ (CI 3.11, prod 3.12, dev 3.14 — mp-context behavior differs, hence the explicit context), `mcap` 1.4.0 records/opcode/data_stream reused, `boto3` injected-never-imported, pytest. Test venv: `~/.venvs/pj-catalog/bin/python3`.

**Review record:** v1 of this plan was adversarially reviewed by Codex (session `019fad80-9bdc-72a3-bc5b-23e83eb38d8c`, 2026-07-29): verdict "revise before lock" with all three levers accepted in principle; the layout facts (footer = trailing 37 bytes; summary-offset section between `summary_offset_start` and the footer) were confirmed correct. A second Codex confirmation pass on v2 verified Q1/Q3/Q6/Q7/Q8 and all High defects addressed, and returned four remaining amendments — cancellation-safe sharded LIST, full disposition telemetry (incl. fallback-failure paths and targeted-success counts), corrected tail/body wording, and explicit PR4 pass thresholds — all folded into this v3.

---

## Evidence (2026-07-29 profile + experiments, `customer=dexory/`, 25,560 objects, healthy link)

| Measurement | Value |
|---|---|
| Cold build per-file cost | 3 sequential range-GETs, ~94 ms TTFB each; network 299 ms vs parse 11.6 ms vs parent apply 0.3 ms |
| Cold baseline (32 proc workers) | 380.8 s = 4,026 files/min; 10 GB moved (354 KiB/file) |
| `--extract-workers 64` (flag only) | 296.8 s (1.28×; link-ceiling-capped 275–330 Mbit/s) |
| Targeted fetch + 64 **threads** (prototype) | 241.4 s = 6,353 files/min (1.58×); 3→1.58 GETs/file, 354→214 KiB/file |
| Catalog equality | Semantically identical on all 25,550 files; same 10 quarantined; 0 fallbacks |
| Warm reconcile | 10.2 s total, ~8.6 s = the serial LIST |
| Sharded LIST (site-level) | 8.58 → 5.09 s (1.7×); depth-2 measured no better |

Caveats the review pinned: the 241.4 s run used **threads**, production uses S3 **processes** (PR2's A/B gate re-measures with processes); the LIST numbers were not preserved as artifacts (PR3 re-runs and saves them); catalogs are NOT byte-reproducible across runs (topic-id assignment order) — hence PR1.

Prototype + harnesses: `/tmp/claude-1000/-home-davide-ws-plotjuggler-mcap-server/15d30549-2b37-4d39-a3b4-f5948f34c2be/scratchpad/{fast_summary.py,bench_targeted.py,bench_e2e.py,compare_semantic.py,bench_list.py}`

## Non-goals (deliberate)

- **ETag-caching quarantined failures** — `catalog_failures` schema column → contract churn for ~1 s at current corpus.
- **O(bucket) `listings` list in `full_reconcile`** (~2 GB at 1.18M objects) — pre-existing; PR3's bounded streaming must not worsen it (review defect: v1's per-shard lists would have ~doubled it).
- **Deeper/recursive LIST sharding** — depth-2 measured no better; revisit at 1M-object scale.
- **GCS sharded LIST**; **GCS process pool** (GCS extraction is THREADS today — `SourceSpec` is built for s3 only, `__main__.py:266`; do not claim otherwise).
- **Parent-side batching** — measured irrelevant (0.3 ms/file, 12% CPU).

## Semantics contract for the targeted read (review-adjudicated)

- The fallback (streamed) path is the **semantics authority**: every quarantine verdict except the `summary_start == 0` no-summary ValueError comes from the fallback. Any structural surprise in the targeted path → `TargetedUnavailable` → fallback.
- **Accepted, documented relaxation (Q1):** the targeted path reads offset 0 only when the tail already covers it (`tail_start == 0` — then the leading magic IS checked at zero cost). A large file corrupt ONLY in its leading 8 bytes is cataloged where the old path quarantined it; the Go server's FormatCodec still rejects it at stream-open (`server/internal/format/format.go:92` builds `mcap.NewReader` over the fetched head). This is a permanent, tested, documented catalog-level false-positive — not silent drift.
- **Wording discipline (review, twice-corrected):** the targeted read is a *bounded speculative over-read*, not "never fetches the chunk index" and not "never touches body bytes": the fixed 64 KiB tail may include trailing DATA-section bytes (a small file arrives entirely inside the tail), and the ≤4 KiB coalescing gap may bridge unrelated summary records. The actual guarantee is **boundedness**: total reads never exceed `tail + needed-group sizes + 4 KiB × gaps`, and no *computed* range outside `[summary_start, summary_offset_start)` is ever requested — a corrupt footer/offset can therefore never trigger an unbounded or body-sized read.

---

# PR1 — `feat/catalog-semantic-diff`: the equivalence gate

### Task 1.1: Hardened semantic comparator

**Files:**
- Create: `scripts/catalog-semantic-diff.py`

- [ ] **Step 1: Write the tool** (read-only connections, snapshot transaction, closes handles; compares per-file identity/fingerprint fields, id-decoded topic maps, **tags (embedded + override) and failure error text** — the v1 draft omitted tags/error-text and could pass semantically different catalogs):

```python
#!/usr/bin/env python3
"""Semantic catalog diff: rowid/ordering-independent equivalence check.

Catalog builds are NOT byte-reproducible across runs (topic/schema dictionary
ids assign in file-completion order), so raw-row or byte comparison of two
builds is meaningless. This decodes each DB through its own id dictionaries.

Compared per file: dims+filename identity, etag, size, last_modified_ns,
start/end times, chunk_count, has_error, {(topic, schema, encoding): count},
and effective tags. Plus the full failure set as (s3_key, error_text).
Intentionally ignored: cataloged_at_ns, build_metadata, all rowids and
topic_sets fingerprints (run-order dependent by design).

Usage: catalog-semantic-diff.py A.db B.db   (exit 0 identical / 1 different)
Run against quiescent DBs (no live builder writing them).
"""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "mcap_catalog"))
from mcap_catalog_builder.varint import decode_counts_blob  # noqa: E402


def canon(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        c.execute("BEGIN")  # snapshot: one consistent read transaction
        topics = dict(c.execute("SELECT id, name FROM topic_names"))
        schemas = {r[0]: (r[1], r[2])
                   for r in c.execute("SELECT id, name, encoding FROM schemas")}
        members: dict = {}
        for sid, tid, scid in c.execute(
                "SELECT set_id, topic_id, schema_id FROM topic_set_members "
                "ORDER BY set_id, topic_id"):
            members.setdefault(sid, []).append((tid, scid))
        tags: dict = {}
        for table in ("tags_embedded", "tags_override"):
            for fid, k, v in c.execute(f"SELECT file_id, key, value FROM {table}"):
                tags.setdefault(fid, set()).add((table, k, v))
        out = {}
        for r in c.execute("""
            SELECT cu.name, si.name, ro.name, so.name, f.date, f.filename,
                   f.etag, f.size_bytes, f.last_modified_ns, f.start_time_ns,
                   f.end_time_ns, f.chunk_count, f.has_error, f.topic_set_id,
                   f.topic_counts, f.id
            FROM files f
            JOIN customers cu ON cu.id=f.customer_id JOIN sites si ON si.id=f.site_id
            JOIN robots ro ON ro.id=f.robot_id JOIN sources so ON so.id=f.source_id"""):
            counts = decode_counts_blob(r[14])
            mem = members.get(r[13], [])
            if len(counts) != len(mem):
                print(f"CORRUPT: member/count arity mismatch for {r[:6]}")
                sys.exit(2)
            topicmap = frozenset(
                (topics[tid], schemas[scid][0], schemas[scid][1], n)
                for (tid, scid), n in zip(mem, counts))
            out[tuple(r[:6])] = (*r[6:13], topicmap,
                                 frozenset(tags.get(r[15], set())))
        fails = {(r[0], r[1]) for r in
                 c.execute("SELECT s3_key, error_text FROM catalog_failures")}
        return out, fails
    finally:
        c.close()


fa, xa = canon(sys.argv[1])
fb, xb = canon(sys.argv[2])
diff = [k for k in set(fa) & set(fb) if fa[k] != fb[k]]
only_a, only_b = set(fa) - set(fb), set(fb) - set(fa)
print(f"files: A={len(fa)} B={len(fb)} only-A={len(only_a)} "
      f"only-B={len(only_b)} value-diffs={len(diff)} failures-equal={xa == xb}")
for k in [*only_a, *only_b, *diff][:5]:
    print(" differs:", k)
if xa != xb:
    for row in list(xa ^ xb)[:5]:
        print(" failure-differs:", row)
identical = not (only_a or only_b or diff) and xa == xb
print("VERDICT:", "SEMANTICALLY IDENTICAL" if identical else "DIFFERENT")
sys.exit(0 if identical else 1)
```

Then: `chmod +x scripts/catalog-semantic-diff.py`

- [ ] **Step 2: Verify column names before committing** (the tags tables' file-id/key/value column names must be read from `mcap_catalog_builder/schema.sql` and the SELECTs adjusted if they differ — run `grep -n 'CREATE TABLE tags' -A8 mcap_catalog/mcap_catalog_builder/schema.sql`; likewise `error_text` vs the actual `catalog_failures` column name).

- [ ] **Step 3: Prove it on the preserved benchmark DBs** (two identical-code baseline builds must compare IDENTICAL; they raw-row-differ in 24,878 rows):

```bash
S=/tmp/claude-1000/-home-davide-ws-plotjuggler-mcap-server/15d30549-2b37-4d39-a3b4-f5948f34c2be/scratchpad
./scripts/catalog-semantic-diff.py "$S/bench-catalog.db" "$S/bench2-catalog.db"
```
Expected: `VERDICT: SEMANTICALLY IDENTICAL`, exit 0.

- [ ] **Step 4: Docs** — add to `mcap_catalog/CLAUDE.md` "Gotchas":

```markdown
- **Catalogs are not byte-reproducible across runs** — topic/schema dictionary
  ids assign in file-completion order, so `topic_sets.fingerprint` and
  `topic_counts` blobs differ between two builds of the same bucket. Only
  `scripts/catalog-semantic-diff.py` (id-dictionary-decoded comparison) is a
  valid equivalence check between catalogs.
```

- [ ] **Step 5: Commit + PR**

```bash
git add scripts/catalog-semantic-diff.py mcap_catalog/CLAUDE.md
git commit -m "feat(scripts): semantic catalog diff (catalogs are not byte-reproducible)"
```

---

# PR2 — `feat/targeted-summary-read` (stacked on PR1)

### Task 2.1: The hardened targeted parser

**Files:**
- Create: `mcap_catalog/mcap_catalog_builder/targeted_summary.py`
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_targeted_summary.py`

- [ ] **Step 1: Write the failing tests.** Parity-style (targeted result/exception must equal the streamed baseline's), plus the review's adversarial set. Test file:

```python
"""Parity + adversarial tests for the targeted summary read."""
import struct
from pathlib import Path

import pytest

from mcap_catalog_builder.mcap_summary import summary_from_stream
from mcap_catalog_builder.targeted_summary import (
    DEFAULT_TAIL,
    TargetedUnavailable,
    read_summary_targeted,
    read_summary_with_fallback,
)
from .fixtures import write_minimal_mcap

MAGIC = b"\x89MCAP0\r\n"


class PreadRecorder:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.calls: list[tuple[int, int]] = []

    def __call__(self, lo: int, hi: int) -> bytes:
        self.calls.append((lo, hi))
        return self.data[lo:hi + 1]


def _mcap_bytes(tmp_path, **kw) -> bytes:
    p = tmp_path / "f.mcap"
    write_minimal_mcap(str(p), **kw)
    return p.read_bytes()


def _baseline(data: bytes):
    import io
    return summary_from_stream(io.BytesIO(data))


def _patch_footer(data: bytes, *, length=None, summary_start=None,
                  summary_offset_start=None) -> bytes:
    # trailing 37 bytes: op(1)+len(8)+summary_start(8)+summary_offset_start(8)+crc(4)+magic(8)
    assert data[-8:] == MAGIC and data[-37] == 0x02
    b = bytearray(data)
    if length is not None:
        b[-36:-28] = struct.pack("<Q", length)
    if summary_start is not None:
        b[-28:-20] = struct.pack("<Q", summary_start)
    if summary_offset_start is not None:
        b[-20:-12] = struct.pack("<Q", summary_offset_start)
    return bytes(b)


# --- parity -----------------------------------------------------------------

def test_parity_minimal(tmp_path):
    data = _mcap_bytes(tmp_path)
    got, via = read_summary_with_fallback(
        PreadRecorder(data), lambda: _open(data), "k", len(data))
    assert got == _baseline(data) and via == "targeted"


def test_parity_many_channels_zero_message(tmp_path):
    chans = [(f"/t{i}", "std_msgs/msg/String", "ros2msg", i % 5) for i in range(40)]
    data = _mcap_bytes(tmp_path, channels=chans)
    got = read_summary_targeted(PreadRecorder(data), len(data))
    assert got == _baseline(data)
    assert any(c.message_count == 0 for c in got.channels)


def test_single_read_when_file_fits_tail(tmp_path):
    data = _mcap_bytes(tmp_path)
    assert len(data) < DEFAULT_TAIL
    pread = PreadRecorder(data)
    read_summary_targeted(pread, len(data))
    assert len(pread.calls) == 1


def test_second_read_when_groups_before_tail(tmp_path):
    chans = [(f"/topic{i}", f"pkg/msg/T{i}", "ros2msg", 2) for i in range(30)]
    data = _mcap_bytes(tmp_path, channels=chans)
    tail = 2048
    assert len(data) > tail
    pread = PreadRecorder(data)
    assert read_summary_targeted(pread, len(data), tail=tail) == _baseline(data)
    assert len(pread.calls) >= 2


# --- adversarial: every corruption must be UNAVAILABLE (fallback decides),
# --- except the framed-and-valid no-summary case ----------------------------

def _open(data):
    import io
    return io.BytesIO(data)


def test_no_summary_error_parity(tmp_path):
    data = _patch_footer(_mcap_bytes(tmp_path), summary_start=0)
    with pytest.raises(ValueError, match="no summary/statistics"):
        read_summary_targeted(PreadRecorder(data), len(data))
    with pytest.raises(ValueError, match="no summary/statistics"):
        _baseline(data)


def test_truly_unsummarized_bytes(tmp_path):
    # Hand-built MCAP with no summary section at all: magic+header+data_end+
    # footer(0,0)+magic. Both paths must agree (via fallback for targeted).
    def rec(op, payload):
        return bytes([op]) + struct.pack("<Q", len(payload)) + payload
    header = rec(0x01, struct.pack("<I", 0) + struct.pack("<I", 0))  # empty strings
    data_end = rec(0x0F, struct.pack("<I", 0))
    footer = rec(0x02, struct.pack("<QQI", 0, 0, 0))
    data = MAGIC + header + data_end + footer + MAGIC
    with pytest.raises(ValueError, match="no summary/statistics"):
        read_summary_with_fallback(PreadRecorder(data), lambda: _open(data),
                                   "k", len(data))


@pytest.mark.parametrize("mutate", [
    lambda d: _patch_footer(d, length=21),                 # corrupt footer length
    lambda d: _patch_footer(d, summary_offset_start=5),    # offsets before summary
    lambda d: _patch_footer(d, summary_offset_start=len(d)),  # offsets past footer
    lambda d: d[:-8] + b"NOTMAGIC",                        # corrupt trailing magic
])
def test_corrupt_footer_is_unavailable(tmp_path, mutate):
    data = mutate(_mcap_bytes(tmp_path))
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(PreadRecorder(data), len(data))


def test_corrupt_leading_magic_small_file_unavailable(tmp_path):
    # File fits in the tail -> offset 0 is visible -> leading magic IS checked.
    data = _mcap_bytes(tmp_path)
    assert len(data) < DEFAULT_TAIL
    bad = b"XXXXXXXX" + data[8:]
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(PreadRecorder(bad), len(bad))


def test_corrupt_leading_magic_large_file_is_cataloged(tmp_path):
    # THE ACCEPTED Q1 RELAXATION, pinned: head outside the tail -> targeted
    # parses fine where the streamed path errors. The Go FormatCodec still
    # rejects such a file at stream-open (server/internal/format/format.go).
    chans = [(f"/t{i}", "pkg/msg/T", "ros2msg", 5) for i in range(60)]
    data = _mcap_bytes(tmp_path, channels=chans)
    tail = 1024
    assert len(data) > tail
    bad = b"XXXXXXXX" + data[8:]
    got = read_summary_targeted(PreadRecorder(bad), len(bad), tail=tail)
    assert got == _baseline(data)  # baseline on the UNCORRUPTED bytes
    with pytest.raises(Exception):
        _baseline(bad)  # streamed path rejects the corrupt head


def test_stale_size_short_tail_unavailable(tmp_path):
    data = _mcap_bytes(tmp_path)
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(PreadRecorder(data), len(data) + 512)


def test_tiny_input_unavailable():
    with pytest.raises(TargetedUnavailable):
        read_summary_targeted(PreadRecorder(b"xx"), 2)


def test_fallback_dispositions(tmp_path, caplog):
    import logging
    data = _patch_footer(_mcap_bytes(tmp_path), summary_offset_start=0)  # no offsets
    with caplog.at_level(logging.INFO):
        got, via = read_summary_with_fallback(
            PreadRecorder(data), lambda: _open(data), "k", len(data))
    assert via == "fallback-unavailable"
    assert got == _baseline(data)

    def boom(lo, hi):
        raise RuntimeError("targeted bug")
    good = _mcap_bytes(tmp_path)
    with caplog.at_level(logging.WARNING):
        got, via = read_summary_with_fallback(boom, lambda: _open(good), "k", len(good))
    assert via == "fallback-unexpected" and got == _baseline(good)


def test_garbage_fallback_error_parity(tmp_path):
    data = b"\x00" * 4096
    base_exc = None
    try:
        _baseline(data)
    except Exception as e:  # noqa: BLE001
        base_exc = e
    with pytest.raises(type(base_exc)):
        read_summary_with_fallback(PreadRecorder(data), lambda: _open(data),
                                   "k", len(data))
```

Note on `_patch_footer(summary_offset_start=5)`: 5 < summary_start for any real
fixture, violating `summary_start <= summary_offset_start` — the bounds check
the review demanded (a corrupt footer must never trigger a body-sized read).

- [ ] **Step 2: Run to verify failure**

Run: `~/.venvs/pj-catalog/bin/python3 -m pytest mcap_catalog/mcap_catalog_builder/tests/test_targeted_summary.py -q`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `targeted_summary.py`** — the v1 prototype logic plus every review hardening: footer length-field check, leading-magic-when-visible, offset-section and group bounds confined to `[summary_start, summary_offset_start)`, per-record consumed-vs-declared framing check, id-keyed schema/channel dicts (duplicate records overwrite, matching the baseline reader's dict semantics), and count validation against `Statistics.schema_count`/`channel_count`:

```python
"""Targeted MCAP summary read: fetch only the record groups the catalog needs.

The catalog consumes schemas, channels and statistics — never the ChunkIndex
group that dominates a chunked recording's summary section. The footer's
summary-offset section indexes the summary by group, so this reader fetches:

    read 1: the file's last ``tail`` bytes (footer + summary-offset section,
            usually also the Statistics group)
    read 2: (when not already inside the tail) the Schema+Channel groups,
            coalesced into one range.

This is a BOUNDED SPECULATIVE OVER-READ, not a byte-exact fetch: the fixed
tail may include trailing DATA-section (body) bytes — a small file arrives
entirely inside it — and the <=4 KiB coalescing gap may bridge unrelated
summary records. The guarantee is BOUNDEDNESS: total reads never exceed
``tail`` + the needed groups + 4 KiB per bridged gap, and every COMPUTED
range is validated against [summary_start, summary_offset_start), so a
corrupt footer/offset can never trigger an unbounded or body-sized read.

Measured 2026-07-29 (25,559-file staging corpus): 3 GETs/file -> 1.58,
354 -> 214 KiB/file, cold build 1.58x, catalog semantically identical.

Transport-agnostic: all reads go through ``pread(lo, hi_incl) -> bytes``.

STRICTLY AN OPTIMIZATION LAYER: any structural surprise raises
``TargetedUnavailable`` and ``read_summary_with_fallback`` re-reads via the
streamed baseline, which is the sole authority for quarantine verdicts. One
carve-out: a well-framed footer with ``summary_start == 0`` raises the
baseline's exact ``ValueError("no summary/statistics in MCAP")`` directly.
Accepted relaxation (2026-07-29 review Q1): offset 0 is only validated when
the tail already covers it, so a large file corrupt ONLY in its leading magic
is cataloged here; the Go FormatCodec still rejects it at stream-open.
"""

import io
import logging
import struct
from typing import BinaryIO, Callable

from mcap.data_stream import ReadDataStream
from mcap.opcode import Opcode
from mcap.records import Channel, Schema, Statistics, SummaryOffset

from .mcap_summary import ChannelInfo, FileSummary, summary_from_stream

logger = logging.getLogger(__name__)

MAGIC = b"\x89MCAP0\r\n"
DEFAULT_TAIL = 64 * 1024   # 42% of the staging corpus resolves in ONE read
_FOOTER_LEN = 37           # op(1) + len(8) + payload(20) + trailing magic(8)
_FOOTER_PAYLOAD = 20
_COALESCE_GAP = 4096       # merge needed groups separated by less than this
_MAX_OFFSETS_LEN = 64 * 1024  # sanity cap: ~7 SummaryOffset records expected


class TargetedUnavailable(Exception):
    """The targeted path cannot serve this file — use the streamed fallback."""


class _NoSummary(Exception):
    """Well-framed footer with summary_start == 0 (the baseline's verdict)."""


def _iter_records(buf: bytes):
    """Yield ``(opcode, record | None)`` for each record framed in ``buf``.

    Validates that each known reader consumes exactly its declared payload;
    any framing anomaly is TargetedUnavailable (never a wrong parse).
    """
    stream = ReadDataStream(io.BytesIO(buf))
    end = len(buf)
    readers = {Opcode.SCHEMA: Schema.read, Opcode.CHANNEL: Channel.read,
               Opcode.STATISTICS: Statistics.read,
               Opcode.SUMMARY_OFFSET: SummaryOffset.read}
    while stream.count < end:
        if stream.count + 9 > end:
            raise TargetedUnavailable("truncated record framing")
        opcode = stream.read1()
        length = stream.read8()
        payload_start = stream.count
        if payload_start + length > end:
            raise TargetedUnavailable("record overruns fetched range")
        reader = readers.get(opcode)
        if reader is None:
            stream.read(length)
            yield opcode, None
            continue
        rec = reader(stream)
        consumed = stream.count - payload_start
        if consumed > length:
            raise TargetedUnavailable("record reader overran declared length")
        if consumed < length:
            stream.read(length - consumed)  # spec-permitted trailing padding
        yield opcode, rec


def read_summary_targeted(
    pread: Callable[[int, int], bytes], size: int, tail: int = DEFAULT_TAIL
) -> FileSummary:
    if size < _FOOTER_LEN:
        raise TargetedUnavailable("too small for an MCAP footer")
    tail_start = max(0, size - tail)
    buf_tail = pread(tail_start, size - 1)
    if len(buf_tail) != size - tail_start:
        raise TargetedUnavailable("short tail read (stale listing size?)")
    if buf_tail[-8:] != MAGIC:
        raise TargetedUnavailable("bad trailing magic")
    if tail_start == 0 and buf_tail[:8] != MAGIC:
        raise TargetedUnavailable("bad leading magic")  # zero-cost when visible
    footer = buf_tail[-_FOOTER_LEN:-8]
    if footer[0] != Opcode.FOOTER:
        raise TargetedUnavailable("footer opcode mismatch")
    (footer_len,) = struct.unpack("<Q", footer[1:9])
    if footer_len != _FOOTER_PAYLOAD:
        raise TargetedUnavailable("footer length field mismatch")
    summary_start, summary_offset_start, _crc = struct.unpack("<QQI", footer[9:29])
    if summary_start == 0:
        raise _NoSummary
    if summary_offset_start == 0:
        raise TargetedUnavailable("no summary-offset section")

    footer_pos = size - _FOOTER_LEN
    if not summary_start <= summary_offset_start < footer_pos:
        raise TargetedUnavailable("summary-offset section out of bounds")
    if footer_pos - summary_offset_start > _MAX_OFFSETS_LEN:
        raise TargetedUnavailable("summary-offset section implausibly large")
    if summary_offset_start >= tail_start:
        so_buf = buf_tail[summary_offset_start - tail_start:footer_pos - tail_start]
    else:
        so_buf = pread(summary_offset_start, footer_pos - 1)

    groups: dict[int, tuple[int, int]] = {}
    for op, rec in _iter_records(so_buf):
        if op == Opcode.SUMMARY_OFFSET and rec is not None:
            groups[rec.group_opcode] = (rec.group_start, rec.group_length)
    if Opcode.STATISTICS not in groups:
        raise TargetedUnavailable("no statistics group in summary offsets")

    want = sorted(
        groups[op] for op in (Opcode.SCHEMA, Opcode.CHANNEL, Opcode.STATISTICS)
        if op in groups
    )
    merged: list[list[int]] = []
    for start, length in want:
        # Every needed group must lie wholly inside the summary section — a
        # corrupt footer/offset must never trigger a body read (R2).
        if not (summary_start <= start and start + length <= summary_offset_start):
            raise TargetedUnavailable("summary group outside summary section")
        if merged and start - merged[-1][1] <= _COALESCE_GAP:
            merged[-1][1] = max(merged[-1][1], start + length)
        else:
            merged.append([start, start + length])

    schemas: dict[int, Schema] = {}
    channels: dict[int, Channel] = {}   # id-keyed: duplicates overwrite (baseline dict semantics)
    stats: Statistics | None = None
    for lo, hi in merged:
        if lo >= tail_start:
            buf = buf_tail[lo - tail_start:hi - tail_start]
        else:
            buf = pread(lo, hi - 1)
            if len(buf) != hi - lo:
                raise TargetedUnavailable("short group read")
        for op, rec in _iter_records(buf):
            if op == Opcode.SCHEMA and rec is not None:
                schemas[rec.id] = rec
            elif op == Opcode.CHANNEL and rec is not None:
                channels[rec.id] = rec
            elif op == Opcode.STATISTICS and rec is not None:
                stats = rec
    if stats is None:
        raise TargetedUnavailable("statistics record missing from its group")
    if len(channels) != stats.channel_count or len(schemas) != stats.schema_count:
        raise TargetedUnavailable("schema/channel count mismatch vs statistics")

    counts = stats.channel_message_counts
    return FileSummary(
        start_time_ns=stats.message_start_time,
        end_time_ns=stats.message_end_time,
        message_count=stats.message_count,
        chunk_count=stats.chunk_count,
        channels=[
            ChannelInfo(
                channel_id=ch.id,
                topic=ch.topic,
                schema_name=schemas[ch.schema_id].name if ch.schema_id in schemas else "",
                schema_encoding=schemas[ch.schema_id].encoding
                if ch.schema_id in schemas else "",
                message_count=counts.get(ch.id, 0),  # zero-msg channels absent: .get MANDATORY
            )
            for ch in channels.values()
        ],
    )


def read_summary_with_fallback(
    pread: Callable[[int, int], bytes],
    open_stream: Callable[[], BinaryIO],
    key: str,
    size: int,
    tail: int = DEFAULT_TAIL,
) -> tuple[FileSummary, str]:
    """Targeted read with streamed fallback; returns ``(summary, via)``.

    ``via`` ∈ {"targeted", "fallback-unavailable", "fallback-unexpected"} feeds
    the reconcile disposition counters (status sidecar) — a systematic targeted
    bug must be VISIBLE, not a silent slow path. When the FALLBACK itself
    raises (the quarantine path), the disposition is preserved by stamping it
    onto the propagating exception as ``summary_via`` so failed files still
    count (review Q2: telemetry must not undercount on the error path).
    """
    try:
        return read_summary_targeted(pread, size, tail=tail), "targeted"
    except _NoSummary:
        raise ValueError("no summary/statistics in MCAP") from None
    except TargetedUnavailable as e:
        logger.info("targeted summary read unavailable for %s (%s); using streamed read",
                    key, e)
        via = "fallback-unavailable"
    except Exception as e:  # noqa: BLE001 — a targeted bug must never change verdicts
        logger.warning("targeted summary read failed for %s (%s: %s); using streamed read",
                       key, type(e).__name__, e)
        via = "fallback-unexpected"
    try:
        with open_stream() as stream:
            return summary_from_stream(stream), via
    except Exception as e:  # noqa: BLE001 — preserve disposition on the quarantine path
        try:
            e.summary_via = via
        except Exception:  # noqa: BLE001 — exotic exception without a __dict__
            pass
        raise
```

- [ ] **Step 4: Run the tests**

Run: `~/.venvs/pj-catalog/bin/python3 -m pytest mcap_catalog/mcap_catalog_builder/tests/test_targeted_summary.py -v`
Expected: all PASS. Contingency: if `write_minimal_mcap`'s writer emits no summary offsets, the parity tests will show `via == "fallback-unavailable"` — then extend `fixtures.py` to write with offsets enabled (check `mcap.writer.Writer` options) rather than weakening assertions.

- [ ] **Step 5: Commit**

```bash
git add mcap_catalog/mcap_catalog_builder/targeted_summary.py \
        mcap_catalog/mcap_catalog_builder/tests/test_targeted_summary.py
git commit -m "perf(builder): hardened targeted MCAP summary read via summary offsets"
```

### Task 2.2: Seam wiring + fallback telemetry + mp context

**Files:**
- Modify: `storage.py` (Protocol + LocalSource), `s3_storage.py`, `gcs_storage.py`, `builder.py`, `reconcile.py`, `status.py`, `__main__.py`
- Modify: `tests/{test_storage,test_s3_storage,test_gcs_storage,test_builder,test_reconcile,test_status}.py`

- [ ] **Step 1: Failing seam tests** — as v1 Task 2.1 (LocalSource/S3Source/GCSSource `read_summary` parity, single-GET assertion, garbage-error parity), adjusted for the tuple return: each parity assertion becomes `summary, via = src.read_summary(key, size)` with `via == "targeted"`.

- [ ] **Step 2: `Source.read_summary` returns `(FileSummary, via)`.** Protocol:

```python
    def read_summary(self, key: str, size: int) -> "tuple[FileSummary, str]":
        """Parse ``key``'s MCAP summary cheaply; returns (summary, via) where
        via is the targeted/fallback disposition for reconcile telemetry."""
```

`LocalSource` — ONE descriptor via `os.pread` (review: per-call `open()` can mix
file generations mid-parse):

```python
    def read_summary(self, key: str, size: int):
        with open(self._abs(key), "rb") as f:
            fd = f.fileno()

            def pread(lo: int, hi: int) -> bytes:
                return os.pread(fd, hi - lo + 1, lo)

            def open_stream():
                return open(self._abs(key), "rb")

            return read_summary_with_fallback(pread, open_stream, key, size)
```

`S3Source` (retry classification identical to `S3RangeReader.readinto`):

```python
    def read_summary(self, key: str, size: int):
        def pread(lo: int, hi: int) -> bytes:
            return retry_with(
                lambda: self._c.get_object(
                    Bucket=self._bucket, Key=key, Range=f"bytes={lo}-{hi}",
                )["Body"].read(),
                is_permanent=_is_permanent,
            )

        return read_summary_with_fallback(
            pread, lambda: self.open_summary(key, size), key, size)
```

`GCSSource` (same shape over `download_as_bytes(start, end)` under `retry_with`).

- [ ] **Step 3: Builder call sites + Extract disposition.** In `extract_summary` replace the `open_summary`+`summary_from_stream` block with:

```python
            summary, summary_via = source.read_summary(key, stat.size)
```

and return it on the Extract: add field `summary_via: str = ""` to the `Extract` dataclass, populate it in the `"ready"` return (`summary_via=summary_via`) **and on the `"error"` return** via `summary_via=getattr(e, "summary_via", "")` — the fallback wrapper stamps the disposition onto the exception precisely so quarantined files still count (review: telemetry must not undercount on the error path). **The `"vanished"` return keeps `summary_via=""`** (execution-review amendment: a benign TOCTOU deletion 404s both paths and would otherwise count as `fallback-unexpected`, firing the bug-suspected WARNING for lifecycle deletions — vanish is not a targeted-read outcome; pin with a test). In `catalog_object` replace its block with `summary, _via = source.read_summary(key, stat.size)` (the single-file event path deliberately drops the disposition — reconcile counters are the observability surface). **Do NOT remove the `summary_from_stream` import from `builder.py`** until Step 5 confirms no test monkeypatches remain against it.

- [ ] **Step 4: Reconcile counters + sidecar.** `ReconcileProgress.file_done` gains a `via: str = ""` kwarg; `full_reconcile._apply` passes `ex.summary_via` for EVERY extract — ready and failed alike. The progress object tallies all three dispositions and publishes all three (review: fallback-only counters undercount and hide the denominator): sidecar fields `summary_targeted_ok`, `summary_fallbacks_unavailable`, `summary_fallbacks_unexpected`, written via `_set_status(...)`. Counters live on the per-reconcile `ReconcileProgress` instance, so each reconcile's sidecar write starts from zero (no cross-reconcile accumulation); files whose `via` is empty (skip path, unparseable keys) are counted in none of the three. `finished()` logs ONE aggregate WARNING when `summary_fallbacks_unexpected > 0`. Additive sidecar fields are contract-safe (§12 explicitly permits unknown-field-tolerant readers; `server/internal/catalog/status_sidecar.go` ignores unknown JSON). Add a `test_status.py` case asserting all three fields appear and a `test_reconcile.py` case asserting the tallies — including one quarantined file whose disposition still lands in the counts.

- [ ] **Step 5: Update the existing test doubles the seam breaks** (review-enumerated):
  - `tests/test_reconcile.py` `_ReadCountingSource` / `_FlakySource`: override `read_summary` (count calls / raise for the chosen key) instead of `open_summary`.
  - `tests/test_builder.py:140,220`: replace `monkeypatch.setattr(builder, "summary_from_stream", ...)` with `monkeypatch.setattr(LocalSource, "read_summary", lambda self, k, s: (bad, "targeted"))`. After this, remove the now-unused `summary_from_stream` import from `builder.py`.
  - Run the FULL suite after this step, not just the new tests.

- [ ] **Step 6: Explicit mp context** (review: prod = Python 3.12 → `fork` default, and the status heartbeat thread starts BEFORE the pool → fork-after-threads hazard; CI = 3.11, dev = 3.14/forkserver). In `reconcile.py`:

```python
import multiprocessing

            pool = ProcessPoolExecutor(
                max_workers=n, initializer=_init_worker, initargs=(source_spec,),
                mp_context=multiprocessing.get_context("forkserver"),
            )
```

with a comment citing the hazard. (Behavior identical on dev 3.14; new on 3.11/3.12.)

- [ ] **Step 7: Docs** — update `mcap_catalog/CLAUDE.md` "R2 / R4 cheap path" to describe the targeted read as a *bounded speculative over-read* with the fallback as semantics authority and the leading-magic carve-out; update `mcap_catalog_builder/README.md` similarly.

- [ ] **Step 8: Gates**

```bash
~/.venvs/pj-catalog/bin/python3 -m pytest mcap_catalog/mcap_catalog_builder/tests/ -q
~/.venvs/pj-catalog/bin/python3 -m compileall mcap_catalog/mcap_catalog_builder
make smoke                                   # SMOKE PASS
PJ_CI_BUILDER_PYTHON=~/.venvs/pj-catalog/bin/python3 scripts/ci-integration.sh  # exercises GCS read_summary vs fake-gcs
```

Contract gate (replaces v1's function-name grep, per review): `git diff --stat mcap_catalog/CATALOG_CONTRACT.md docs/CATALOG_CONTRACT.md mcap_catalog/mcap_catalog_builder/schema.sql` must be EMPTY, and `cmp mcap_catalog/CATALOG_CONTRACT.md docs/CATALOG_CONTRACT.md` must still report byte-identical copies.

- [ ] **Step 9: Real-bucket A/B parity + performance gate** (manual; check the link first — `iw dev wlo1 link` better than −60 dBm): fresh build with THIS branch (S3 **process** pool — this re-validates the thread-measured 1.58× under the production pool shape) vs a fresh build with `main`, then `./scripts/catalog-semantic-diff.py ref.db new.db` → `SEMANTICALLY IDENTICAL`; sidecar shows `targeted_fallbacks_unexpected == 0`; wall time ≤ ~70% of ref.

- [ ] **Step 10: Commit + stacked PR onto PR1.**

---

# PR3 — `feat/sharded-s3-list` (stacked on PR2)

### Task 3.1: Bounded sharded LIST

**Files:**
- Modify: `s3_storage.py` (`list_all` + `_listing` helper), `__main__.py` (parent client pool)
- Modify: `tests/test_s3_storage.py` (FakeS3 + tests), `tests/fixtures.py` (`InMemoryS3Client`), `tests/test_cli.py` (`_EmptyS3Client` + botocore fake)

- [ ] **Step 1: Failing tests that actually fail** (review: v1's key-set assertions passed on the old code). FakeS3 gains a delimiter-aware `list_objects_v2` (v1 Task 4.1 code) **plus call recording** (`self.list_calls.append(dict(kw))`), and a **lazy paginator** that counts un-consumed buffered pages. Tests assert: (a) a `Delimiter="/"` discovery call happened; (b) each discovered shard was paginated; (c) key-set + Stat parity with the flat listing; (d) root-level keys and flat buckets work; (e) with a fake of 4 shards × 3 lazy pages, the max simultaneously-buffered page count stays ≤ the documented bound (catches per-shard materialization regressions — the review's O(bucket) defect); (f) **early abandonment**: take one listing from the generator, `gen.close()`, and assert it returns promptly (wrap in a watchdog `threading.Timer`-based timeout or `pytest-timeout`-style alarm) with the fake recording that shard pagination stopped — this pins the confirmation-pass deadlock defect; (g) a mid-shard error from the fake propagates out of `list_all` AND the generator still tears down cleanly (no hung threads — assert via `threading.active_count()` returning to baseline).

- [ ] **Step 2: Implement bounded, CANCELLATION-SAFE streaming** — no per-shard `list[Listing]` (review High defect), and no deadlock when the consumer abandons the generator or an error aborts the listing (confirmation-pass defect: a plain `q.put` blocks forever on a full queue while executor shutdown waits for the producer). Every producer put is cancel-aware; the generator's `finally` cancels, drains, then joins:

```python
_LIST_SHARD_THREADS = 16
_LIST_QUEUE_PAGES = 32  # max buffered pages (~32k listings) — bounds listing memory


    def _listing(self, o) -> Listing:
        return Listing(
            key=o["Key"],
            stat=Stat(size=o["Size"], etag=o["ETag"].strip('"'),
                      mtime_ns=_last_modified_ns(o.get("LastModified"))),
        )

    def list_all(self) -> Iterator[Listing]:
        # S3 pagination is SERIAL within one prefix (continuation tokens), so a
        # large listing is parallelized by sharding the key space on the first
        # '/'-delimited level and paginating shards concurrently. Yield order
        # is NOT lexicographic — reconcile is dict/set-keyed throughout.
        # Memory: the shard list + submitted futures are O(first-level
        # prefixes) (small: sites/customers), and BUFFERED LISTINGS are
        # bounded by the queue (_LIST_QUEUE_PAGES pages) — the full-bucket
        # O(objects) accumulation lives (as before) in the reconcile caller,
        # not here.
        shards: list[str] = []
        kw: dict = {"Bucket": self._bucket, "Prefix": self._prefix, "Delimiter": "/"}
        while True:
            resp = self._c.list_objects_v2(**kw)
            shards += [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
            for o in resp.get("Contents", []):  # keys at the prefix root itself
                if o["Key"].endswith(".mcap"):
                    yield self._listing(o)
            if not resp.get("IsTruncated"):
                break
            kw["ContinuationToken"] = resp["NextContinuationToken"]
        if not shards:
            return

        import queue as _queue
        import threading
        from concurrent.futures import ThreadPoolExecutor

        q: "_queue.Queue" = _queue.Queue(maxsize=_LIST_QUEUE_PAGES)
        cancel = threading.Event()

        def put_or_cancel(item) -> bool:
            """Cancel-aware put: never blocks past teardown."""
            while not cancel.is_set():
                try:
                    q.put(item, timeout=0.25)
                    return True
                except _queue.Full:
                    continue
            return False

        def produce(prefix: str) -> None:
            try:
                for page in self._c.get_paginator("list_objects_v2").paginate(
                        Bucket=self._bucket, Prefix=prefix):
                    if not put_or_cancel(("page", page.get("Contents", []))):
                        return  # consumer is gone — stop paginating
            except Exception as e:  # noqa: BLE001 — surfaced on the consumer side
                put_or_cancel(("err", e))
            else:
                put_or_cancel(("done", None))

        pool = ThreadPoolExecutor(max_workers=min(_LIST_SHARD_THREADS, len(shards)))
        try:
            for prefix in shards:
                pool.submit(produce, prefix)
            finished = 0
            while finished < len(shards):
                kind, payload = q.get()
                if kind == "done":
                    finished += 1
                elif kind == "err":
                    raise payload  # abort-the-reconcile, same as the old paginator
                else:
                    for o in payload:
                        if o["Key"].endswith(".mcap"):
                            yield self._listing(o)
        finally:
            cancel.set()          # unblock any producer stuck in put_or_cancel
            while True:           # drain so in-flight puts find space
                try:
                    q.get_nowait()
                except _queue.Empty:
                    break
            pool.shutdown(wait=True)  # producers exit promptly via cancel
```

Teardown invariant (make this a test): `cancel.set()` + drain + `shutdown(wait=True)`
runs on EVERY exit — normal completion, an `err` raise, or the consumer
closing/abandoning the generator (GeneratorExit) — and `put_or_cancel` means no
producer can outlive it blocked on a full queue.

- [ ] **Step 3: Parent client pool + no-SDK-CI safety.** In `__main__.py`'s s3 branch (imports stay INSIDE the branch — the CI unit job installs no cloud SDKs):

```python
        import boto3  # imported lazily so local mode has no boto3 dependency
        from botocore.config import Config

        source = S3Source(
            boto3.client("s3", config=Config(max_pool_connections=24)),
            args.s3_bucket, args.s3_prefix,
        )
```

Update `tests/test_cli.py`'s fake-boto3 injection to also install a fake
`botocore.config` module (`Config = lambda **kw: None`) in `sys.modules`, and
give `_EmptyS3Client` a `list_objects_v2` returning `{"Contents": [], "IsTruncated": False}`. Give `tests/fixtures.py`'s `InMemoryS3Client` the same delimiter-aware `list_objects_v2` as FakeS3.

- [ ] **Step 4: Full suite + smoke + ci-integration** (same commands as PR2 Step 8).

- [ ] **Step 5: Re-run the LIST A/B and SAVE the output** (review: v1's 8.58→5.09 s numbers were not preserved): run the sequential-vs-sharded timing against the staging bucket 3× interleaved, save the transcript under `docs/plans/artifacts/2026-XX-XX-sharded-list-ab.txt`, and quote the refreshed numbers in the PR body.

- [ ] **Step 6: Commit + stacked PR onto PR2.**

---

# PR4 — `feat/extract-workers-default` (GATED — do not start until the gate data exists)

**Gate (review Q3 + confirmation pass — explicit pass thresholds, not just "measurements exist"):** the 1.28× evidence is WAN+thread/process-mixed; production is in-region where TTFB collapses and the optimum shifts toward CPU count, and one initialized S3 worker ≈ 57 MiB RSS. Collect on a real in-region box (GCE/EC2 per `docs/gce-deploy-smoke.md` / `docs/ec2-deploy.md`): (a) cold-build A/B at 2×CPU vs 4×CPU workers with the PR2 targeted read active, (b) peak RSS/PSS of the whole extraction fleet, (c) the instance's actual memory budget. **Adopt the new default ONLY IF (a) shows ≥ 1.15× end-to-end speedup with a semantically-identical catalog AND (b) peak fleet PSS ≤ 40% of instance memory at the new worker count.** If either threshold fails, PR4 becomes documentation-only (record the measured numbers + the "use `--extract-workers` to override on WAN" guidance in the README) and the default stays `min(2×CPU, 32)`.

- [ ] **Step 1 (only if the gate PASSES):** failing test for `default_extract_workers` (v1 Task 3.1 code) — **s3 only** gets `min(4 × ncpu, 64)`; `gcs` keeps the thread default `min(2 × ncpu, 32)` (GCS extraction is THREADS — 64 GIL-sharing threads is not obviously right and is unmeasured; the v1 help text claiming "s3/gcs processes" was wrong).
- [ ] **Step 2:** implement + resolve `None` default post-parse (v1 Task 3.3 code, s3-only); update `tests/test_cli.py:37` (asserts the old concrete default) and the `--extract-workers` help text; note the bounded window doubles to `2×workers = 128` in-flight Extracts (~few MB — fine).
- [ ] **Step 3:** suite + smoke; document the measured in-region numbers in the PR body; stacked PR onto PR3.

Until PR4 lands, the WAN guidance is documentation-only: "`--extract-workers 64` measured 1.28× on WAN cold scans" in `mcap_catalog_builder/README.md` (add in PR2 Step 7).

---

## Self-review notes

- Review Q1→PR2 (leading-magic-when-visible + pinned relaxation tests + docs); Q2→PR2 Task 2.2 telemetry; Q3→PR4 gate + s3-only; Q4→PR3 bounded streaming + real failing tests; Q5→accepted (fake-gcs leg + fallback safety); Q6→hardened footer gate before `_NoSummary`; Q7→contract byte-compare gate; Q8→stacked PRs, mp context, test-double updates, no-SDK CI safety.
- All High defects addressed: bounded group ranges (`[summary_start, summary_offset_start)` + offsets cap), statistics count validation, consumed-vs-declared framing, id-keyed dicts, single-fd local pread, bounded LIST streaming, test-double enumeration, genuinely-failing LIST tests, botocore fake, GCS wording, forkserver context. Medium: comparator hardening (ro/snapshot/tags/error-text), over-read wording.
- Confirmation-pass (v2→v3) amendments: cancellation-safe LIST generator (`put_or_cancel` + cancel/drain/join in `finally`, early-close + mid-error teardown tests); disposition preserved across fallback failure (exception-stamped `summary_via`) and all three counters published with per-reconcile reset; tail/body wording corrected to a boundedness guarantee; PR4 gate given explicit pass thresholds (≥1.15× A/B AND fleet PSS ≤40% of instance memory) with a documentation-only fallback outcome.
