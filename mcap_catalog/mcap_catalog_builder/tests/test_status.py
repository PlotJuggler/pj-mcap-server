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
