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
from mcap_catalog_builder.tests.fixtures import InMemoryS3Client, minimal_mcap_bytes

CH = [("/a", "S", "ros2msg", 2)]
TODAY = dt.date(2026, 6, 2)
PA = "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-02/"
PB = "customer=beta/customer_site=hq/robot=r2/source=ros-bags/date=2026-06-02/"
KA = PA + "a.mcap"
KB = PB + "b.mcap"


def _raw(tmp_path):
    return minimal_mcap_bytes(tmp_path, CH)


class Page2FailsClient(InMemoryS3Client):
    """Page 1 OK, page 2 raises for keys under fail_prefix — a PARTIAL listing."""

    def __init__(self, objects, fail_prefix):
        super().__init__(objects, page_size=1)
        self._fail_prefix = fail_prefix

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, ContinuationToken=None):
        if Prefix.startswith(self._fail_prefix) and ContinuationToken is not None:
            raise RuntimeError("page 2 boom")
        return super().list_objects_v2(Bucket, Prefix, Delimiter, ContinuationToken)


# -- list_prefix --------------------------------------------------------------

def test_s3_list_prefix_paginates_to_completion(tmp_path):
    raw = _raw(tmp_path)
    src = S3Source(InMemoryS3Client({KA: raw, PA + "a2.mcap": raw, KB: raw},
                                page_size=1), "bucket")
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
    src = S3Source(InMemoryS3Client({KA: raw}, page_size=1), "bucket")
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


# -- hot_audit (§4.2): scoped catalog + fail-closed scoped sweep --------------

from mcap_catalog_builder.hot_audit import hot_audit


def _seed(conn, caches, tmp_path, objects):
    """Full-reconcile the given object keys in, returning the raw bytes used."""
    raw = _raw(tmp_path)
    full_reconcile(conn, caches, S3Source(InMemoryS3Client(
        {k: raw for k in objects}), "bucket"))
    return raw


def _count(conn, filename):
    return conn.execute(
        "SELECT COUNT(*) FROM files WHERE filename=?", (filename,)
    ).fetchone()[0]


def test_hot_audit_catalogs_new_skips_unchanged(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _seed(conn, caches, tmp_path, [KA])
    src = S3Source(InMemoryS3Client({KA: raw, PA + "new.mcap": raw}), "bucket")
    tally = hot_audit(conn, caches, src, today=TODAY)
    assert tally["cataloged"] == 1 and tally["skipped"] == 1
    assert _count(conn, "new.mcap") == 1
    assert tally["covered_prefixes"] >= 1 and tally["skipped_prefixes"] == 0


def test_hot_audit_deletes_only_inside_covered_prefixes(tmp_db, tmp_path):
    """One failed prefix among many (§9): the failed prefix's stale row
    SURVIVES; the covered prefix's stale row is deleted."""
    conn, caches = tmp_db
    raw = _seed(conn, caches, tmp_path, [KA, KB])
    # Both objects vanish; prefix A's LIST breaks on page 2 -> A uncovered.
    src = S3Source(Page2FailsClient({PA + "x1.mcap": raw, PA + "x2.mcap": raw}, PA),
                   "bucket")
    tally = hot_audit(conn, caches, src, today=TODAY)
    assert tally["skipped_prefixes"] >= 1
    assert _count(conn, "a.mcap") == 1      # uncovered prefix: NO deletion
    assert _count(conn, "b.mcap") == 0      # covered empty prefix: deleted
    assert tally["deleted"] == 1


def test_hot_audit_covered_empty_prefix_is_authoritative(tmp_db, tmp_path):
    conn, caches = tmp_db
    _seed(conn, caches, tmp_path, [KA])
    src = S3Source(InMemoryS3Client({}), "bucket")   # everything gone, LISTs fine
    tally = hot_audit(conn, caches, src, today=TODAY)
    assert tally["deleted"] == 1 and _count(conn, "a.mcap") == 0


def test_hot_audit_ignores_dates_outside_window(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _raw(tmp_path)
    old = "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-05-01/old.mcap"
    full_reconcile(conn, caches, S3Source(InMemoryS3Client({old: raw}), "bucket"))
    # Object vanishes, but its date is outside [TODAY-2, TODAY]: tier 3's job.
    tally = hot_audit(conn, caches, S3Source(InMemoryS3Client({}), "bucket"),
                      today=TODAY)
    assert _count(conn, "old.mcap") == 1 and tally["deleted"] == 0


def test_hot_audit_never_stamps_build_metadata(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _seed(conn, caches, tmp_path, [KA])
    before = conn.execute("SELECT build_id FROM build_metadata").fetchone()[0]
    hot_audit(conn, caches,
              S3Source(InMemoryS3Client({KA: raw, PA + "n.mcap": raw}), "bucket"),
              today=TODAY)
    after = conn.execute("SELECT build_id FROM build_metadata").fetchone()[0]
    assert after == before  # §4.2: a subset scan stamping freshness would lie


def test_hot_audit_quarantined_combo_is_scanned_and_repaired(tmp_db, tmp_path):
    conn, caches = tmp_db
    record_failure(conn, KB, "boom")   # combo known ONLY via quarantine
    conn.commit()
    raw = _raw(tmp_path)
    tally = hot_audit(conn, caches,
                      S3Source(InMemoryS3Client({KB: raw}), "bucket"), today=TODAY)
    assert tally["cataloged"] == 1 and _count(conn, "b.mcap") == 1


def test_hot_audit_stop_raises_cancelled_and_changes_nothing(tmp_db, tmp_path):
    conn, caches = tmp_db
    _seed(conn, caches, tmp_path, [KA])
    stop = threading.Event()
    stop.set()
    with pytest.raises(ReconcileCancelled):
        hot_audit(conn, caches, S3Source(InMemoryS3Client({}), "bucket"),
                  today=TODAY, stop_event=stop)
    assert _count(conn, "a.mcap") == 1


def test_hot_audit_sweep_head_guards_deletion_candidates(tmp_db, tmp_path):
    """TOCTOU (Codex branch review 2026-08-10): an object re-uploaded AFTER
    the prefix LIST but before the sweep is absent from the listing yet LIVE —
    the sweep must HEAD-confirm the 404 before deleting, or a live object's
    row (and its cascading tags_override) would be destroyed."""
    conn, caches = tmp_db
    raw = _seed(conn, caches, tmp_path, [KA])

    class ReappearingClient(InMemoryS3Client):
        """LISTs report the object gone, but HEAD finds it live (re-upload
        landed between the LIST and the sweep)."""

        def list_objects_v2(self, Bucket, Prefix="", Delimiter=None,
                            ContinuationToken=None):
            return {"Contents": [], "IsTruncated": False}

    src = S3Source(ReappearingClient({KA: raw}), "bucket")
    tally = hot_audit(conn, caches, src, today=TODAY)
    assert tally["deleted"] == 0
    assert _count(conn, "a.mcap") == 1   # live object's row survives


def test_hot_audit_zero_coverage_raises(tmp_db, tmp_path):
    """All prefix LISTs failed => the pass did nothing; it must FAIL (so the
    coordinator backs off), never report a healthy ok with zero coverage."""
    conn, caches = tmp_db
    _seed(conn, caches, tmp_path, [KA])

    class AlwaysFailsClient(InMemoryS3Client):
        def list_objects_v2(self, *_a, **_kw):
            raise RuntimeError("LIST always fails")

    with pytest.raises(RuntimeError, match="zero coverage"):
        hot_audit(conn, caches, S3Source(AlwaysFailsClient({}), "bucket"),
                  today=TODAY)
    assert _count(conn, "a.mcap") == 1
