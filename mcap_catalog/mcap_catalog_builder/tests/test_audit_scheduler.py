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


def test_intake_gate_pauses_and_drains_before_audit_then_resumes():
    """§3.5 handshake: pause intake -> wait for received batches to ack ->
    only then enqueue the audit; resume on completion."""
    from mcap_catalog_builder.s3_producer import IntakeGate

    work_q = queue.Queue()
    stop = threading.Event()
    gate = IntakeGate()
    gate.batch_received(2)  # a message was received and is still un-acked

    coordinator = AuditCoordinator(
        work_q, stop, 0.05, backoff_initial=0.02, intake_gate=gate
    )
    coordinator.start(immediate=True)
    try:
        time.sleep(0.3)
        assert gate.paused  # intake paused for the due audit...
        assert work_q.empty()  # ...but the audit waits for the drain

        gate.batch_acked()  # the producer acked the outstanding batch
        item = work_q.get(timeout=1.0)
        assert isinstance(item, AuditItem)
        assert gate.paused  # still paused while the audit runs
        assert item.start()
        item.finish(AuditResult("ok", 0.01))

        deadline = time.monotonic() + 1.0
        while gate.paused and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not gate.paused  # resumed after the audit completed
    finally:
        stop.set()
        coordinator.join()
    assert not gate.paused  # resume also holds on the stop path


# -- dual-cadence scheduling (design §5.2: one arbiter, full supersedes hot) --

def test_hot_audits_run_completion_relative_alongside_full():
    """hot_interval small, full interval large: hot items flow with
    audit_kind='hot', completion-relative, through the SAME single arbiter."""
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=60.0, backoff_initial=0.02, hot_interval=0.05
    )
    coordinator.start(immediate=False)
    try:
        first = work_q.get(timeout=1.0)
        assert first.audit_kind == "hot"
        assert coordinator.queued_or_running
        assert first.start()
        completed = time.monotonic()
        first.finish(AuditResult("ok", 0.01))
        second = work_q.get(timeout=1.0)
        assert second.audit_kind == "hot"
        assert time.monotonic() - completed >= 0.05 * 0.70  # completion-relative
        second.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()
        coordinator.join()


def test_hot_failures_back_off_capped_at_hot_interval():
    """A failing hot audit retries with capped exponential backoff (§5.2's
    result-bearing rule applies to BOTH tiers — Codex consult 2026-08-10)."""
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=60.0, backoff_initial=0.03, hot_interval=0.20
    )
    coordinator.start(immediate=False)
    try:
        first = work_q.get(timeout=1.0)
        t1 = time.monotonic()
        first.finish(AuditResult("failed", 0.01, "first"))
        second = work_q.get(timeout=1.0)
        t2 = time.monotonic()
        assert 0.03 * 0.70 <= t2 - t1 < 0.20  # backoff, not the full cadence
        second.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()
        coordinator.join()


def test_due_full_supersedes_due_hot():
    """Both due when the scheduler wakes: full wins (§5.2), and the arbiter
    never queues a hot item while an audit is queued/running."""
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=0.05, backoff_initial=0.02, hot_interval=0.05
    )
    coordinator.start(immediate=True)   # full due NOW; hot due at 0.05
    try:
        first = work_q.get(timeout=1.0)
        assert first.audit_kind == "full"
        time.sleep(0.12)                # hot came due while full still runs
        assert work_q.empty()
        first.finish(AuditResult("ok", 0.01))
        second = work_q.get(timeout=1.0)
        assert second.audit_kind in ("hot", "full")
        second.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()
        coordinator.join()


def test_worker_dispatches_hot_item_to_hot_audit(tmp_db, monkeypatch):
    import mcap_catalog_builder.__main__ as main_mod

    conn, caches = tmp_db
    calls = []

    def fake_hot(*_a, **kw):
        calls.append(("hot", kw.get("window_days")))
        return {"cataloged": 0, "skipped": 0, "failed": 0, "deleted": 0,
                "covered_prefixes": 3, "skipped_prefixes": 1}

    monkeypatch.setattr(main_mod, "hot_audit", fake_hot)
    monkeypatch.setattr(
        main_mod, "full_reconcile",
        lambda *_a, **_kw: calls.append(("full", None)),
    )
    hot = AuditItem(audit_kind="hot")
    full = AuditItem()
    work_q = queue.Queue()
    work_q.put(hot)
    work_q.put(full)
    work_q.put(WatchEvent("stop"))
    worker_loop(conn, caches, object(), work_q, hot_window_days=5)
    assert calls == [("hot", 5), ("full", None)]
    assert hot.wait(threading.Event()).outcome == "ok"
    assert full.wait(threading.Event()).outcome == "ok"


# -- fixed-hour full audits (§5.2/§5.3, Phase 6) ------------------------------

def test_next_fixed_hour_delay_math():
    import calendar
    from mcap_catalog_builder.__main__ import _next_fixed_hour_delay

    # 2026-08-10 10:30:00 UTC
    now = calendar.timegm((2026, 8, 10, 10, 30, 0, 0, 0, 0))
    assert _next_fixed_hour_delay(now, 11) == 1800.0          # same day, 30 min out
    assert _next_fixed_hour_delay(now, 2) == 15.5 * 3600.0    # tomorrow 02:00
    at_slot = calendar.timegm((2026, 8, 10, 11, 0, 0, 0, 0, 0))
    assert _next_fixed_hour_delay(at_slot, 11) == 86400.0     # STRICTLY after now


def test_fixed_hour_schedules_from_completion_skip_missed(monkeypatch):
    """After a full audit completes, the next one lands one (patched)
    fixed-hour delay later — computed from NOW, never from a nominal missed
    schedule, and never from --rescan-interval."""
    import mcap_catalog_builder.__main__ as main_mod

    monkeypatch.setattr(main_mod, "_next_fixed_hour_delay",
                        lambda _now, _hour: 0.06)
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=999.0, backoff_initial=0.02, full_audit_hour=2
    )
    coordinator.start(immediate=True)
    try:
        first = work_q.get(timeout=1.0)
        assert first.audit_kind == "full"
        done = time.monotonic()
        first.finish(AuditResult("ok", 0.01))
        second = work_q.get(timeout=1.0)
        assert 0.04 <= time.monotonic() - done < 1.0   # the patched slot, not 999s
        second.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()
        coordinator.join()


def test_fixed_hour_honored_for_non_immediate_start(monkeypatch):
    """A fresh build (immediate=False) must schedule its FIRST full audit at
    the fixed hour, not one --rescan-interval out (Codex consult 2026-08-10)."""
    import mcap_catalog_builder.__main__ as main_mod

    monkeypatch.setattr(main_mod, "_next_fixed_hour_delay",
                        lambda _now, _hour: 0.06)
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=999.0, backoff_initial=0.02, full_audit_hour=2
    )
    started = time.monotonic()
    coordinator.start(immediate=False)
    try:
        first = work_q.get(timeout=1.0)
        assert first.audit_kind == "full"
        assert time.monotonic() - started < 1.0   # not 999s
        first.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()
        coordinator.join()
