"""Tests for the builder-status sidecar (<db>.status.json) and reconcile progress.

The sidecar is the machine-readable "is the builder alive and what is it doing"
signal consumed by the container healthcheck and (best-effort) by the Go server's
degraded-mode /health. Its contract (CATALOG_CONTRACT.md §12): atomic publish via
tmp+rename, first write deferred until a phase exists, phase="error" persisted on
fatal failures, and NO writes at all from a process that failed to take the
single-writer flock.
"""

import json
import os
import threading
import time

import pytest

from mcap_catalog_builder.__main__ import main
from mcap_catalog_builder.db import load_caches, open_db
from mcap_catalog_builder.reconcile import full_reconcile
from mcap_catalog_builder.status import ReconcileProgress, StatusWriter
from mcap_catalog_builder.tests.fixtures import write_minimal_mcap
from mcap_catalog_builder.writer_lock import acquire_writer_lock


def _read_status(db: str) -> dict:
    with open(db + ".status.json") as f:
        return json.load(f)


class _FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


# ---------------------------------------------------------------- StatusWriter


def test_first_write_deferred_until_phase(tmp_path):
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db)
    w.update(extract_done=3)
    assert not os.path.exists(db + ".status.json")  # no phase yet -> no file
    w.update(phase="listing")
    doc = _read_status(db)
    assert doc["phase"] == "listing"
    assert doc["version"] == 1
    assert doc["pid"] == os.getpid()
    assert abs(doc["updated_at_unix"] - time.time()) < 60
    assert not os.path.exists(db + ".status.json.tmp")  # atomic: tmp renamed away


def test_update_merges_fields_and_replaces_atomically(tmp_path):
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    w.update(phase="extracting", extract_total=10)
    w.update(extract_done=4)
    doc = _read_status(db)
    assert doc["phase"] == "extracting"
    assert doc["extract_total"] == 10
    assert doc["extract_done"] == 4
    assert not os.path.exists(db + ".status.json.tmp")


def test_throttle_coalesces_writes_but_phase_change_bypasses(tmp_path):
    db = str(tmp_path / "catalog.db")
    clk = _FakeClock()
    w = StatusWriter(db, min_interval=10.0, clock=clk)
    w.update(phase="extracting", extract_done=0)
    w.update(extract_done=1)  # inside throttle window -> not written
    assert "extract_done" not in _read_status(db) or _read_status(db)["extract_done"] == 0
    w.update(phase="idle")  # phase change writes despite the window
    assert _read_status(db)["phase"] == "idle"
    assert _read_status(db)["extract_done"] == 1  # merged state rides along
    clk.now += 11
    w.update(extract_done=2)  # window elapsed -> written
    assert _read_status(db)["extract_done"] == 2


def test_force_write_bypasses_throttle(tmp_path):
    db = str(tmp_path / "catalog.db")
    clk = _FakeClock()
    w = StatusWriter(db, min_interval=10.0, clock=clk)
    w.update(phase="extracting")
    w.update(extract_done=7, force=True)
    assert _read_status(db)["extract_done"] == 7


def test_fatal_writes_error_phase_immediately(tmp_path):
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=100.0)
    w.fatal("NoCredentialsError: Unable to locate credentials")
    doc = _read_status(db)
    assert doc["phase"] == "error"
    assert "NoCredentialsError" in doc["last_error"]


def test_full_audit_outcome_and_duration_are_additive(tmp_path):
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    w.update(phase="idle", existing_field="preserved")
    w.full_audit_finished("failed", 12.5)

    doc = _read_status(db)
    assert doc["phase"] == "idle"
    assert doc["existing_field"] == "preserved"
    assert doc["full_audit_outcome"] == "failed"
    assert doc["full_audit_duration"] == 12.5
    assert doc["full_audit_last"].endswith("Z")


def test_heartbeat_refreshes_updated_at(tmp_path):
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    w.update(phase="idle")
    first = _read_status(db)["updated_at_unix"]
    stop = threading.Event()
    w.heartbeat_start(stop, interval=0.05)
    try:
        deadline = time.time() + 2.0
        while time.time() < deadline:
            if _read_status(db)["updated_at_unix"] > first:
                break
            time.sleep(0.02)
        assert _read_status(db)["updated_at_unix"] > first
    finally:
        stop.set()
        w.heartbeat_stop()


def test_heartbeat_never_creates_file_before_first_phase(tmp_path):
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    stop = threading.Event()
    w.heartbeat_start(stop, interval=0.01)
    try:
        time.sleep(0.1)
        assert not os.path.exists(db + ".status.json")
    finally:
        stop.set()
        w.heartbeat_stop()


# ---------------------------------------------------------- ReconcileProgress


def test_progress_drives_status_and_logs(tmp_path, caplog):
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    p = ReconcileProgress(status=w, log_interval=0.0)
    with caplog.at_level("INFO"):
        p.listed(total=10)
        p.extract_start(to_extract=4, skipped=5, failed=1)
        for st in ("cataloged", "cataloged", "failed", "cataloged"):
            p.file_done(st)
        p.finished({"cataloged": 3, "skipped": 5, "failed": 2, "deleted": 0})
    doc = _read_status(db)
    assert doc["phase"] == "extracting"
    assert doc["listed_total"] == 10
    assert doc["extract_total"] == 4
    assert doc["extract_done"] == 4
    assert doc["cataloged"] == 3
    assert doc["failed"] == 2  # 1 unparseable at classify + 1 quarantined at apply
    assert "progress" in caplog.text  # per-file throttled progress lines
    assert "4/4" in caplog.text


def test_progress_tracks_summary_via_dispositions_in_sidecar(tmp_path):
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    p = ReconcileProgress(status=w, log_interval=0.0)
    p.extract_start(to_extract=4, skipped=0, failed=0)
    p.file_done("cataloged", via="targeted")
    p.file_done("cataloged", via="targeted")
    p.file_done("failed", via="fallback-unavailable")
    p.file_done("cataloged", via="fallback-unexpected")
    doc = _read_status(db)
    assert doc["summary_targeted_ok"] == 2
    assert doc["summary_fallbacks_unavailable"] == 1
    assert doc["summary_fallbacks_unexpected"] == 1


def test_progress_without_status_writer_only_logs(caplog):
    p = ReconcileProgress(status=None, log_interval=0.0)
    with caplog.at_level("INFO"):
        p.listed(total=2)
        p.extract_start(to_extract=2, skipped=0, failed=0)
        p.file_done("cataloged")
        p.file_done("cataloged")
    assert "2/2" in caplog.text


# ------------------------------------------------- full_reconcile integration


def _hive(root, filename="a.mcap"):
    dest = os.path.join(
        root,
        "customer=globex", "customer_site=london", "robot=rob01",
        "source=ros-bags", "date=2026-06-01", filename,
    )
    write_minimal_mcap(dest)


def test_full_reconcile_reports_progress(tmp_path, caplog):
    root = str(tmp_path / "watch")
    for name in ("a.mcap", "b.mcap", "c.mcap"):
        _hive(root, name)
    db = str(tmp_path / "catalog.db")
    conn = open_db(db)
    caches = load_caches(conn)
    w = StatusWriter(db, min_interval=0.0)
    p = ReconcileProgress(status=w, log_interval=0.0)
    with caplog.at_level("INFO"):
        tally = full_reconcile(conn, caches, root, workers=4, progress=p)
    assert tally["cataloged"] == 3
    doc = _read_status(db)
    assert doc["listed_total"] == 3
    assert doc["extract_total"] == 3
    assert doc["extract_done"] == 3
    assert "3/3" in caplog.text
    conn.close()


def test_full_reconcile_all_skipped_reports_zero_extract(tmp_path):
    root = str(tmp_path / "watch")
    _hive(root)
    db = str(tmp_path / "catalog.db")
    conn = open_db(db)
    caches = load_caches(conn)
    full_reconcile(conn, caches, root)  # first pass catalogs
    w = StatusWriter(db, min_interval=0.0)
    p = ReconcileProgress(status=w, log_interval=0.0)
    full_reconcile(conn, caches, root, progress=p)  # second pass: all skipped
    doc = _read_status(db)
    assert doc["extract_total"] == 0
    assert doc["skipped"] == 1
    conn.close()


# ------------------------------------------------------------- CLI / __main__


def test_once_writes_status_sidecar_idle(tmp_path):
    root = str(tmp_path / "watch")
    _hive(root)
    db = str(tmp_path / "catalog.db")
    assert main(["--once", root, "--db", db]) == 0
    doc = _read_status(db)
    assert doc["phase"] == "idle"
    assert doc["extract_done"] == 1
    assert not os.path.exists(db + ".status.json.tmp")


def test_fatal_reconcile_error_writes_error_status(tmp_path, monkeypatch):
    import mcap_catalog_builder.__main__ as m

    root = str(tmp_path / "watch")
    _hive(root)
    db = str(tmp_path / "catalog.db")

    def boom(*a, **k):
        raise RuntimeError("kaboom during reconcile")

    monkeypatch.setattr(m, "full_reconcile", boom)
    with pytest.raises(RuntimeError, match="kaboom"):
        main(["--once", root, "--db", db])
    doc = _read_status(db)
    assert doc["phase"] == "error"
    assert "kaboom" in doc["last_error"]


def test_lock_conflict_never_touches_status(tmp_path):
    root = str(tmp_path / "watch")
    _hive(root)
    db = str(tmp_path / "catalog.db")
    sentinel = {"phase": "extracting", "sentinel": True}
    with open(db + ".status.json", "w") as f:
        json.dump(sentinel, f)
    lock = acquire_writer_lock(db)  # simulate the real builder holding the flock
    try:
        assert main(["--once", root, "--db", db]) == 3
    finally:
        lock.release()
    assert _read_status(db) == sentinel  # refused builder wrote NOTHING


# --------------------------------------------------- Codex review follow-ups


def test_listing_writes_status_on_first_object(tmp_path):
    # The sidecar's first write must land as soon as the LIST provably began
    # (first object yielded — S3 access works), NOT after 5000 objects: a slow
    # listing must not leave the healthcheck staring at an absent sidecar.
    root = str(tmp_path / "watch")
    for name in ("a.mcap", "b.mcap", "c.mcap"):
        _hive(root, name)
    db = str(tmp_path / "catalog.db")
    conn = open_db(db)
    caches = load_caches(conn)
    w = StatusWriter(db, min_interval=0.0)
    p = ReconcileProgress(status=w, log_interval=0.0)

    from mcap_catalog_builder.storage import LocalSource

    seen_at_second_yield = []

    class _AssertingSource(LocalSource):
        def list_all(self):
            for i, lst in enumerate(super().list_all()):
                if i == 1:  # by the SECOND object, the first must have written
                    seen_at_second_yield.append(os.path.exists(db + ".status.json"))
                yield lst

    full_reconcile(conn, caches, _AssertingSource(root), progress=p)
    assert seen_at_second_yield == [True]
    conn.close()


def test_rescan_resets_progress_counters(tmp_path):
    # ONE ReconcileProgress instance is reused across every daemon rescan;
    # per-reconcile counters must reset, not accumulate.
    root = str(tmp_path / "watch")
    _hive(root)
    db = str(tmp_path / "catalog.db")
    conn = open_db(db)
    caches = load_caches(conn)
    w = StatusWriter(db, min_interval=0.0)
    p = ReconcileProgress(status=w, log_interval=0.0)

    full_reconcile(conn, caches, root, progress=p)   # rescan 1: catalogs 1
    full_reconcile(conn, caches, root, progress=p)   # rescan 2: all skipped
    doc = _read_status(db)
    assert doc["extract_total"] == 0
    assert doc["extract_done"] == 0    # NOT 1 carried over from rescan 1
    assert doc["cataloged"] == 0       # NOT accumulated across rescans
    assert doc["skipped"] == 1
    conn.close()


def test_status_write_failure_is_swallowed_and_recovers(tmp_path, monkeypatch, caplog):
    # The sidecar is ADVISORY: a failed write must never raise into the
    # reconcile/daemon (or kill the heartbeat thread) — log, then recover on
    # the next update.
    import mcap_catalog_builder.status as status_mod

    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    real_replace = os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError(28, "No space left on device")
        return real_replace(src, dst)

    monkeypatch.setattr(status_mod.os, "replace", flaky_replace)
    with caplog.at_level("WARNING"):
        w.update(phase="listing")  # first write fails -> swallowed + logged
    assert "status sidecar" in caplog.text
    assert not os.path.exists(db + ".status.json")
    w.update(listed_total=5)       # next write recovers
    doc = _read_status(db)
    assert doc["phase"] == "listing"
    assert doc["listed_total"] == 5


def test_hot_audit_and_maintenance_fields(tmp_path):
    """§6 additive fields for the tier-2 audit + the §5.2 declared window."""
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    w.update(phase="idle")
    w.hot_audit_finished("ok", 1.5, covered=6, skipped=1)
    w.maintenance_window(True)
    doc = _read_status(db)
    assert doc["hot_audit_outcome"] == "ok"
    assert doc["hot_audit_duration"] == 1.5
    assert doc["hot_audit_covered_prefixes"] == 6
    assert doc["hot_audit_skipped_prefixes"] == 1
    assert doc["hot_audit_last"].endswith("Z")
    assert doc["maintenance_window_active"] is True
    w.maintenance_window(False)
    assert _read_status(db)["maintenance_window_active"] is False


def test_tag_edit_counters(tmp_path):
    """§6 counters, design names: tag_edit_expired / tag_edit_failed —
    keyed off the TagEditItem's terminal status; ok/not_found don't count."""
    db = str(tmp_path / "catalog.db")
    w = StatusWriter(db, min_interval=0.0)
    w.update(phase="idle")
    w.tag_edit_result("expired")
    w.tag_edit_result("error")
    w.tag_edit_result("expired")
    w.tag_edit_result("ok")
    w.tag_edit_result("not_found")
    doc = _read_status(db)
    assert doc["tag_edit_expired"] == 2 and doc["tag_edit_failed"] == 1
