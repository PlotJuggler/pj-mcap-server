"""Phase-1 audit coordinator tests: coalescing, timing, backoff, and outcomes."""

import queue
import threading
import time

from mcap_catalog_builder.__main__ import (
    AuditCoordinator,
    AuditItem,
    AuditResult,
    worker_loop,
)
from mcap_catalog_builder.watcher import WatchEvent


def test_slow_audit_coalesces_and_next_due_is_completion_relative():
    work_q = queue.Queue()
    stop = threading.Event()
    interval = 0.12
    coordinator = AuditCoordinator(
        work_q, stop, interval, backoff_initial=0.02
    )
    second = None
    coordinator.start(immediate=True)
    try:
        first = work_q.get(timeout=1.0)
        assert isinstance(first, AuditItem)
        assert coordinator.queued_or_running

        # Several nominal intervals may pass while the audit is slow, but no
        # second item is queued and an explicit due tick is coalesced.
        time.sleep(interval * 1.5)
        assert work_q.empty()
        assert coordinator.request_due() is False

        completed_at = time.monotonic()
        first.finish(AuditResult("ok", 1.0))
        second = work_q.get(timeout=1.0)
        queued_at = time.monotonic()
        assert queued_at - completed_at >= interval * 0.75
        assert isinstance(second, AuditItem)
    finally:
        stop.set()
        coordinator.join()

    assert not coordinator.queued_or_running
    assert second is not None
    assert second.start() is False  # pending audit was dropped on stop


def test_failing_audits_retry_with_exponential_backoff_capped_at_interval():
    work_q = queue.Queue()
    stop = threading.Event()
    interval = 0.20
    base = 0.03
    coordinator = AuditCoordinator(
        work_q, stop, interval, backoff_initial=base
    )
    coordinator.start(immediate=True)
    try:
        first = work_q.get(timeout=1.0)
        t1 = time.monotonic()
        first.finish(AuditResult("failed", 0.01, "first"))

        second = work_q.get(timeout=1.0)
        t2 = time.monotonic()
        second.finish(AuditResult("failed", 0.01, "second"))

        third = work_q.get(timeout=1.0)
        t3 = time.monotonic()
        assert base * 0.70 <= t2 - t1 < interval
        assert (base * 2) * 0.70 <= t3 - t2 < interval
        third.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()  # also proves the completion/backoff wait is interruptible
        coordinator.join()

    assert not coordinator.queued_or_running
    assert work_q.empty()


def test_stop_interrupts_audit_backoff_wait_promptly():
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=60.0, backoff_initial=30.0
    )
    coordinator.start(immediate=True)
    item = work_q.get(timeout=1.0)
    item.finish(AuditResult("failed", 0.01, "boom"))

    deadline = time.monotonic() + 1.0
    while coordinator.queued_or_running and time.monotonic() < deadline:
        time.sleep(0.005)
    assert not coordinator.queued_or_running

    started = time.monotonic()
    stop.set()
    coordinator.join()
    assert time.monotonic() - started < 0.5
    assert work_q.empty()


def test_worker_routes_failed_audit_result_and_keeps_processing(tmp_db, monkeypatch):
    import mcap_catalog_builder.__main__ as main_mod

    conn, caches = tmp_db
    handled = []

    def fail_audit(*_args, **_kwargs):
        raise RuntimeError("audit boom")

    monkeypatch.setattr(main_mod, "full_reconcile", fail_audit)
    monkeypatch.setattr(
        main_mod,
        "catalog_object",
        lambda _conn, _caches, key, _source: handled.append(key),
    )

    class Source:
        def wait_for_stable(self, _path):
            return True

        def event_key(self, path):
            return path

    item = AuditItem()
    work_q = queue.Queue()
    work_q.put(item)
    work_q.put(WatchEvent("catalog", "after-failure.mcap"))
    work_q.put(WatchEvent("stop"))

    worker_loop(conn, caches, Source(), work_q)

    result = item.wait(threading.Event())
    assert result is not None
    assert result.outcome == "failed"
    assert "audit boom" in (result.error or "")
    assert handled == ["after-failure.mcap"]


def test_pending_unstarted_audit_is_dropped_on_stop():
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(work_q, stop, interval=60.0)
    coordinator.start(immediate=True)
    item = work_q.get(timeout=1.0)

    stop.set()
    coordinator.join()

    assert item.start() is False
    assert not coordinator.queued_or_running
