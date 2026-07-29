"""Tests for the full reconcile scan: catalog, dedup, warm no-op, deletion sweep."""

import os

from mcap_catalog_builder.db import load_caches, open_db
from mcap_catalog_builder.reconcile import full_reconcile, scan_disk
from mcap_catalog_builder.storage import LocalSource
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap
from mcap_catalog_builder.varint import decode_counts_blob

CH = [("/a", "S", "ros2msg", 2), ("/b", "S", "ros2msg", 1), ("/zero", "S", "ros2msg", 0)]


def _hive(root, filename="x.mcap", channels=None, s3_key=None):
    dest = os.path.join(
        root,
        "customer=globex",
        "customer_site=london",
        "robot=rob01",
        "source=ros-bags",
        "date=2026-06-01",
        filename,
    )
    write_minimal_mcap(dest, s3_key=s3_key, channels=channels or CH)
    return dest


def test_scan_disk_filters_temp_and_hidden(tmp_path):
    root = str(tmp_path)
    write_minimal_mcap(os.path.join(root, "good.mcap"))
    open(os.path.join(root, "partial.mcap.tmp"), "w").close()
    open(os.path.join(root, "x.part"), "w").close()
    open(os.path.join(root, ".hidden.mcap"), "w").close()
    open(os.path.join(root, "notes.txt"), "w").close()
    os.makedirs(os.path.join(root, ".git"))
    write_minimal_mcap(os.path.join(root, ".git", "buried.mcap"))
    assert [os.path.basename(p) for p in scan_disk(root)] == ["good.mcap"]


def test_reconcile_catalogs_and_dedups(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="a.mcap")
    _hive(root, filename="b.mcap")  # same channel layout → one topic set
    tally = full_reconcile(conn, caches, root)
    assert tally["cataloged"] == 2
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0] == 1


def test_reconcile_warm_noop(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="a.mcap")
    full_reconcile(conn, caches, root)
    tally = full_reconcile(conn, caches, root)
    assert tally["cataloged"] == 0 and tally["skipped"] == 1
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1


def test_full_reconcile_process_mode_matches_thread_mode(tmp_path):
    """The process-pool extraction path must produce a catalog IDENTICAL to the
    thread path — same file rows AND same per-file quarantine. This proves the GIL
    bypass changes only WHERE the parse runs, never WHAT gets written."""
    from mcap_catalog_builder.reconcile import SourceSpec

    root = str(tmp_path / "lake")
    _hive(root, "a.mcap")
    _hive(root, "b.mcap", channels=[("/x", "S", "ros2msg", 3)])
    _hive(root, "c.mcap")
    # A flat (non-Hive) key -> must quarantine into catalog_failures in BOTH modes.
    write_minimal_mcap(os.path.join(root, "flat_bad.mcap"))

    def run(spec):
        conn = open_db(str(tmp_path / ("proc.db" if spec else "thr.db")))
        caches = load_caches(conn)
        tally = full_reconcile(conn, caches, LocalSource(root), workers=4, source_spec=spec)
        rows = conn.execute(
            """SELECT c.name, si.name, r.name, so.name, f.date, f.filename,
                      f.chunk_count, f.start_time_ns, f.end_time_ns, hex(f.topic_counts)
               FROM files f
               JOIN customers c ON c.id = f.customer_id
               JOIN sites si   ON si.id = f.site_id
               JOIN robots r   ON r.id  = f.robot_id
               JOIN sources so ON so.id = f.source_id
               ORDER BY f.filename"""
        ).fetchall()
        fails = [r[0] for r in conn.execute("SELECT s3_key FROM catalog_failures ORDER BY s3_key")]
        conn.close()
        return tally, rows, fails

    t_tally, t_rows, t_fails = run(None)                                   # thread pool
    p_tally, p_rows, p_fails = run(SourceSpec(kind="local", root=root))    # PROCESS pool

    assert p_rows == t_rows, "process-mode file rows differ from thread-mode"
    assert p_fails == t_fails, "process-mode quarantine differs from thread-mode"
    assert t_tally["cataloged"] == p_tally["cataloged"] == 3
    assert t_tally["failed"] == p_tally["failed"] >= 1
    assert len(t_rows) == 3


def _catalog_snapshot(conn) -> dict:
    """An order-independent view of the catalog: per file, its dims + fingerprint +
    ``{topic_name: count}``. Independent of lookup-id assignment order, so a parallel
    build and a sequential build of the same corpus must produce EQUAL snapshots
    even though their internal ids may be numbered differently."""
    snap: dict = {}
    for f in conn.execute(
        "SELECT cu.name c, si.name s, ro.name r, f.date d, f.filename fn, "
        "f.start_time_ns st, f.end_time_ns et, f.chunk_count ch, "
        "f.topic_set_id tsid, f.topic_counts tc "
        "FROM files f JOIN customers cu ON cu.id=f.customer_id "
        "JOIN sites si ON si.id=f.site_id JOIN robots ro ON ro.id=f.robot_id"
    ).fetchall():
        member_ids = [
            row[0] for row in conn.execute(
                "SELECT topic_id FROM topic_set_members WHERE set_id=? ORDER BY topic_id",
                (f["tsid"],),
            ).fetchall()
        ]
        counts = decode_counts_blob(f["tc"])
        topics = {}
        for tid, cnt in zip(member_ids, counts):
            name = conn.execute("SELECT name FROM topic_names WHERE id=?", (tid,)).fetchone()[0]
            topics[name] = cnt
        snap[(f["c"], f["s"], f["r"], f["d"], f["fn"])] = (f["st"], f["et"], f["ch"], topics)
    return snap


def test_reconcile_parallel_matches_sequential(tmp_path):
    """workers>1 parallelizes only the summary reads; the resulting catalog must be
    byte-for-byte equivalent (content-wise) to the sequential build."""
    layouts = [
        [("/a", "S", "ros2msg", 2), ("/b", "S", "ros2msg", 1)],
        [("/a", "S", "ros2msg", 2), ("/b", "S", "ros2msg", 1)],  # dup layout -> shared set
        [("/x", "T", "ros2msg", 3)],
        [("/a", "S", "ros2msg", 1), ("/c", "U", "ros2msg", 4), ("/z", "S", "ros2msg", 0)],
    ]

    def build(root):
        for i, ch in enumerate(layouts):
            _hive(root, filename=f"f{i}.mcap", channels=ch)

    build(str(tmp_path / "seq"))
    build(str(tmp_path / "par"))
    cs = open_db(str(tmp_path / "seq.db"))
    cp = open_db(str(tmp_path / "par.db"))
    try:
        t_seq = full_reconcile(cs, load_caches(cs), str(tmp_path / "seq"), workers=1)
        t_par = full_reconcile(cp, load_caches(cp), str(tmp_path / "par"), workers=4)
        assert t_seq["cataloged"] == t_par["cataloged"] == len(layouts)
        assert _catalog_snapshot(cs) == _catalog_snapshot(cp)
        assert (
            cs.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0]
            == cp.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0]
        )
    finally:
        cs.close()
        cp.close()


class _ReadCountingSource(LocalSource):
    """A LocalSource that records every ``read_summary`` (the body-region read)."""

    def __init__(self, root: str) -> None:
        super().__init__(root)
        self.reads: list[str] = []

    def read_summary(self, key: str, size: int):
        self.reads.append(key)
        return super().read_summary(key, size)


def test_reconcile_skip_touches_no_summary(tmp_db, tmp_path):
    """R4 + lever B: an unchanged file is skipped from the listing fingerprint alone,
    so a warm re-scan opens NO summary (over S3 that is zero network)."""
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="a.mcap")
    src = _ReadCountingSource(root)

    full_reconcile(conn, caches, src, workers=1)
    assert len(src.reads) == 1  # cold pass reads the one file's summary

    src.reads.clear()
    tally = full_reconcile(conn, caches, src, workers=4)
    assert tally["skipped"] == 1 and tally["cataloged"] == 0
    assert src.reads == []  # warm pass: summary never opened for the skipped file


class _FlakySource(LocalSource):
    """A LocalSource whose read_summary raises for one chosen key — simulates a
    transient/broken read to prove a worker error quarantines only that file and
    can never abort the parallel reconcile pool."""

    def __init__(self, root: str, bad_substr: str) -> None:
        super().__init__(root)
        self._bad = bad_substr

    def read_summary(self, key: str, size: int):
        if self._bad in key:
            raise RuntimeError("boom")
        return super().read_summary(key, size)


def test_reconcile_worker_error_quarantines_and_continues(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="good1.mcap")
    _hive(root, filename="bad.mcap")
    _hive(root, filename="good2.mcap")
    src = _FlakySource(root, bad_substr="bad.mcap")

    tally = full_reconcile(conn, caches, src, workers=4)
    assert tally["cataloged"] == 2 and tally["failed"] == 1  # pool did not abort
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 2
    assert conn.execute(
        "SELECT COUNT(*) FROM catalog_failures WHERE s3_key LIKE '%bad.mcap'"
    ).fetchone()[0] == 1


def test_reconcile_extraction_results_release_incrementally(tmp_db, tmp_path, monkeypatch):
    """The parallel read phase must not retain every completed Extract until the
    pool closes: each Extract carries a full FileSummary (one ChannelInfo per
    channel), so on a million-object cold build that retention is gigabytes of
    parsed summaries held for hours (the 2026-07-28 production kill). Peak
    simultaneously-live Extracts must stay bounded by the in-flight window,
    far below the total file count."""
    import threading
    import weakref

    from mcap_catalog_builder import reconcile

    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    n_files = 48
    for i in range(n_files):
        _hive(root, filename=f"f{i:03d}.mcap")

    lock = threading.Lock()
    state = {"live": 0, "peak": 0}

    def _released():
        with lock:
            state["live"] -= 1

    real = reconcile.extract_summary

    def tracking(source, key, stat, dims, eff_key):
        ex = real(source, key, stat, dims, eff_key)
        with lock:
            state["live"] += 1
            state["peak"] = max(state["peak"], state["live"])
        weakref.finalize(ex, _released)
        return ex

    monkeypatch.setattr(reconcile, "extract_summary", tracking)
    workers = 4
    tally = full_reconcile(conn, caches, LocalSource(root), workers=workers)
    assert tally["cataloged"] == n_files
    assert state["peak"] <= 4 * workers, (
        f"peak live Extracts {state['peak']} ~ total files {n_files}: completed "
        "results are being retained until pool close instead of released as applied"
    )


def _corrupt_post_write(path: str, size: int = 4096) -> None:
    """Overwrite a written file with bytes that are simultaneously
    TargetedUnavailable (bad trailing magic -> no valid MCAP framing) AND
    rejected by the streamed baseline (``mcap.exceptions.InvalidMagic`` — the
    library DOES validate magic once it actually parses the stream), so it
    exercises the full targeted -> unavailable -> fallback-also-fails ->
    quarantine path.

    NOTE (adaptation from the reviewed spec, which said "trailing magic
    corrupted"): corrupting ONLY the trailing 8 magic bytes is not enough —
    verified empirically that ``summary_from_stream``'s ``get_summary()``
    never itself re-checks the trailing magic (it seeks to the footer via the
    offsets already read out of it and does not care that the last 8 bytes
    happen to differ), so the streamed fallback would still succeed and this
    file would never reach quarantine. Overwriting the whole file with
    non-MCAP bytes makes both the targeted path AND the streamed fallback
    reject it, matching the intended fallback-unavailable disposition.
    """
    with open(path, "wb") as f:
        f.write(b"\x00" * size)


def test_reconcile_reports_summary_via_dispositions(tmp_db, tmp_path):
    """Both a healthy file (targeted read succeeds) and a quarantined file (targeted
    unavailable, streamed fallback also fails) must land in the disposition tallies —
    the reconcile telemetry must not undercount on the error/quarantine path."""
    from mcap_catalog_builder.status import ReconcileProgress

    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    _hive(root, filename="good.mcap")
    bad = _hive(root, filename="bad.mcap")
    _corrupt_post_write(bad)

    progress = ReconcileProgress()
    tally = full_reconcile(conn, caches, root, progress=progress)
    assert tally["cataloged"] == 1 and tally["failed"] == 1
    assert progress.summary_via_counts["targeted"] == 1
    assert progress.summary_via_counts["fallback-unavailable"] == 1
    assert progress.summary_via_counts["fallback-unexpected"] == 0


def test_reconcile_deletes_removed(tmp_db, tmp_path):
    conn, caches = tmp_db
    root = str(tmp_path / "watch")
    a = _hive(root, filename="a.mcap")
    _hive(root, filename="b.mcap")
    full_reconcile(conn, caches, root)
    os.remove(a)
    tally = full_reconcile(conn, caches, root)
    assert tally["deleted"] == 1
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM topic_sets").fetchone()[0] == 1  # set survives
