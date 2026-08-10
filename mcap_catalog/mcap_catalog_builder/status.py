"""Builder observability: the ``<db>.status.json`` sidecar + reconcile progress.

Two consumers depend on this file (CATALOG_CONTRACT.md §12):

- the deploy container healthcheck: *"is the builder alive and not fatally
  failed"* — ``phase != "error"`` and ``updated_at_unix`` fresh;
- the Go server's degraded-mode ``/health``: best-effort detail about what the
  first catalog build is currently doing.

Rules the tests pin:

- **Atomic publish**: every write goes to ``<db>.status.json.tmp`` then
  ``os.replace`` — a reader never sees a torn document.
- **First write deferred until a phase exists.** A process that never reaches a
  real milestone (e.g. it crash-loops before its first S3 LIST) must not
  overwrite the previous run's ``phase="error"`` verdict with an empty state.
- **Ownership = the writer flock.** Callers construct a ``StatusWriter`` only
  after ``acquire_writer_lock`` succeeds, so a refused second builder never
  touches the incumbent's sidecar.
"""

import json
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

STATUS_SUFFIX = ".status.json"

# Fatal messages are truncated so a pathological traceback cannot balloon the
# sidecar (readers cap what they display anyway).
_MAX_ERROR_LEN = 2000


def _utc_iso(wall: float | None = None) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() if wall is None else wall))


class StatusWriter:
    """Atomically maintains ``<db>.status.json`` (thread-safe, throttled)."""

    def __init__(self, db_path: str, *, min_interval: float = 1.0, clock=time.monotonic) -> None:
        self.path = db_path + STATUS_SUFFIX
        # pid-suffixed temp: a heartbeat thread stuck >join-timeout in a write
        # during shutdown could otherwise share a temp file with the NEXT
        # builder process and hand it a torn document to rename. Distinct temp
        # paths make the worst case a stale-but-well-formed sidecar, which the
        # new builder's next write (≤10 s later) self-heals.
        self._tmp = f"{self.path}.tmp.{os.getpid()}"
        self._lock = threading.Lock()
        self._state: dict = {}
        self._min_interval = min_interval
        self._clock = clock
        self._last_write: float | None = None
        self._hb_thread: threading.Thread | None = None
        # Event-tier counters (§6, additive advisory fields): guarded by their
        # own lock — increments come from both producer and worker threads.
        self._counters_lock = threading.Lock()
        self._events_applied = 0
        self._events_unknown = 0
        self._events_acked = 0
        self._tag_edits_expired = 0
        self._tag_edits_failed = 0

    def update(self, *, force: bool = False, **fields) -> None:
        """Merge ``fields`` into the state and write, subject to throttling.

        A phase transition or ``force=True`` always writes; other updates are
        coalesced to at most one write per ``min_interval``. Nothing is written
        until the state carries a phase (see module docstring).
        """
        with self._lock:
            phase_change = "phase" in fields and fields["phase"] != self._state.get("phase")
            self._state.update(fields)
            if not self._state.get("phase"):
                return
            now = self._clock()
            if (
                not (force or phase_change)
                and self._last_write is not None
                and now - self._last_write < self._min_interval
            ):
                return
            self._write_locked(now)

    def fatal(self, message: str) -> None:
        """Record a fatal failure (``phase="error"``) immediately."""
        self.update(phase="error", last_error=str(message)[:_MAX_ERROR_LEN], force=True)

    def producer_poll_ok(self) -> None:
        """Record a successful SQS poll (§3.5: distinguishes a dead producer
        from a quiet bucket — ``last_event_at`` alone cannot). Throttled by the
        normal ``min_interval`` coalescing; polls recur every ~20 s anyway."""
        self.update(producer_last_poll_ok_at=_utc_iso())

    def producer_acked(self) -> None:
        with self._counters_lock:
            self._events_acked += 1
            n = self._events_acked
        self.update(producer_last_ack_at=_utc_iso(), events_acked=n)

    def event_unknown(self, name: str) -> None:
        """Count an S3 event name the translator does not support (§3.2) — a
        visible counter, never a silently acked-and-dropped message."""
        with self._counters_lock:
            self._events_unknown += 1
            n = self._events_unknown
        self.update(events_unknown_name=n)

    def event_applied(self) -> None:
        with self._counters_lock:
            self._events_applied += 1
            n = self._events_applied
        self.update(events_applied=n)

    def full_audit_finished(self, outcome: str, duration: float) -> None:
        """Record the last terminal full-audit result.

        These fields are additive advisory telemetry; existing sidecar readers
        continue to key only on ``phase``/``updated_at``.
        """
        self.update(
            full_audit_last=_utc_iso(),
            full_audit_outcome=outcome,
            full_audit_duration=max(0.0, duration),
            force=True,
        )

    def hot_audit_finished(self, outcome: str, duration: float,
                           covered: int, skipped: int) -> None:
        """Tier-2 result (design §4.2: hot-audit status goes to the sidecar
        ONLY — never ``build_metadata``, which is whole-catalog freshness)."""
        self.update(
            hot_audit_last=_utc_iso(),
            hot_audit_outcome=outcome,
            hot_audit_duration=max(0.0, duration),
            hot_audit_covered_prefixes=covered,
            hot_audit_skipped_prefixes=skipped,
            force=True,
        )

    def maintenance_window(self, active: bool) -> None:
        """§5.2: the declared window while a full audit holds the writer —
        events pause and tag edits can expire; make it sidecar-visible."""
        self.update(maintenance_window_active=bool(active), force=True)

    def tag_edit_expired(self) -> None:
        """§6 counter (design name ``tag_edit_expired``): an edit whose 5 s
        deadline passed before the worker reached it — the visible cost of a
        busy writer (e.g. the nightly maintenance window)."""
        with self._counters_lock:
            self._tag_edits_expired += 1
            n = self._tag_edits_expired
        self.update(tag_edit_expired=n)

    def tag_edit_failed(self) -> None:
        with self._counters_lock:
            self._tag_edits_failed += 1
            n = self._tag_edits_failed
        self.update(tag_edit_failed=n)

    def heartbeat_start(self, stop_event: threading.Event, interval: float = 10.0) -> None:
        """Refresh ``updated_at`` every ``interval`` seconds on a daemon thread,
        so sidecar freshness means "the builder process is alive" even during
        phases that produce no per-file progress (long LISTs, idle daemon)."""

        def _beat() -> None:
            while not stop_event.wait(interval):
                if stop_event.is_set():  # narrows the stop→write race window
                    return
                self.update(force=True)  # write failures are swallowed+logged inside

        self._hb_thread = threading.Thread(target=_beat, name="status-heartbeat", daemon=True)
        self._hb_thread.start()

    def heartbeat_stop(self) -> None:
        """Best-effort join (the caller sets the stop event first). A thread
        stuck >5 s inside a single tiny-file write is pathological; if it ever
        loses this race against a successor builder, the pid-suffixed temp
        (see __init__) bounds the damage to one stale-but-well-formed sidecar
        that the successor overwrites within a heartbeat interval."""
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=5.0)
            self._hb_thread = None

    def _write_locked(self, now: float) -> None:
        doc = dict(self._state)
        doc["version"] = 1
        doc["pid"] = os.getpid()
        wall = time.time()
        doc["updated_at_unix"] = wall
        doc["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(wall))
        # The sidecar is ADVISORY (§12): a write failure must never propagate
        # into the reconcile or kill the heartbeat thread. Log and retry on the
        # next update — _last_write is only advanced on success, so the throttle
        # cannot swallow the retry.
        try:
            with open(self._tmp, "w") as f:
                json.dump(doc, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(self._tmp, self.path)
        except Exception as e:  # noqa: BLE001 - advisory signal, never fatal
            logger.warning("status sidecar write failed (advisory, retried on next update): %s", e)
            try:
                os.unlink(self._tmp)
            except OSError:
                pass
            return
        self._last_write = now


class ReconcileProgress:
    """Parent-side reconcile progress: throttled INFO log lines + sidecar updates.

    Lives entirely on the single writer thread (worker processes never see it);
    ``full_reconcile`` calls the milestone methods, this class does the pacing.
    """

    def __init__(self, status: StatusWriter | None = None, *,
                 log_interval: float = 30.0, clock=time.monotonic) -> None:
        self._status = status
        self._log_interval = log_interval
        self._clock = clock
        self._last_log: float | None = None
        self._t0: float | None = None
        self._total = 0
        self._done = 0
        self._counts = {"cataloged": 0, "skipped": 0, "failed": 0}
        # Per-file summary-read disposition tallies (targeted_summary.py's
        # ``via``) — the aggregate observability surface for the targeted-read
        # optimization: a systematic targeted-path bug must show up here, not
        # just as a per-file DEBUG/WARNING line at million-object scale.
        self._via_counts = {"targeted": 0, "fallback-unavailable": 0, "fallback-unexpected": 0}

    # -- milestones -------------------------------------------------------

    def listing(self, count_so_far: int) -> None:
        """Called periodically while the LIST is being drained."""
        self._set_status(phase="listing", listed_total=count_so_far)
        if self._log_due():
            logger.info("reconcile progress: listing... %d objects so far", count_so_far)

    def listed(self, total: int) -> None:
        self._set_status(phase="listing", listed_total=total, force=True)
        logger.info("reconcile: listed %d objects", total)

    def extract_start(self, to_extract: int, skipped: int, failed: int) -> None:
        # Full per-reconcile reset: ONE ReconcileProgress instance is reused
        # across every daemon rescan, so carried-over counters would
        # double-count (extract_done > extract_total on the second rescan).
        self._total = to_extract
        self._done = 0
        self._counts = {"cataloged": 0, "skipped": skipped, "failed": failed}
        self._via_counts = {"targeted": 0, "fallback-unavailable": 0, "fallback-unexpected": 0}
        self._t0 = self._clock()
        self._last_log = None
        self._set_status(
            phase="extracting", extract_total=to_extract, extract_done=0,
            cataloged=0, skipped=skipped, failed=failed,
            summary_targeted_ok=0, summary_fallbacks_unavailable=0,
            summary_fallbacks_unexpected=0,
            force=True,
        )
        logger.info(
            "reconcile: extracting %d files (%d unchanged skipped, %d unparseable)",
            to_extract, skipped, failed,
        )

    def file_done(self, status: str, via: str = "") -> None:
        self._done += 1
        self._counts[status] = self._counts.get(status, 0) + 1
        if via in self._via_counts:  # empty/unknown via counts in no disposition bucket
            self._via_counts[via] += 1
        self._set_status(
            extract_done=self._done,
            cataloged=self._counts["cataloged"],
            skipped=self._counts["skipped"],
            failed=self._counts["failed"],
            summary_targeted_ok=self._via_counts["targeted"],
            summary_fallbacks_unavailable=self._via_counts["fallback-unavailable"],
            summary_fallbacks_unexpected=self._via_counts["fallback-unexpected"],
        )
        if self._log_due():
            elapsed = max(self._clock() - (self._t0 or self._clock()), 1e-9)
            rate = self._done / elapsed
            remaining = self._total - self._done
            eta = remaining / rate if rate > 0 else 0.0
            logger.info(
                "reconcile progress: %d/%d files (%.0f%%), %.0f files/min, ETA %s, quarantined %d",
                self._done, self._total,
                100.0 * self._done / self._total if self._total else 100.0,
                rate * 60.0, _fmt_duration(eta), self._counts["failed"],
            )

    def idle(self) -> None:
        """The build (incl. any publish) is done and the catalog is served —
        daemon steady state between rescans, or a completed --once run."""
        self._set_status(phase="idle", force=True)

    def finished(self, tally: dict) -> None:
        self._set_status(
            extract_done=self._done,
            cataloged=tally.get("cataloged", 0),
            skipped=tally.get("skipped", 0),
            failed=tally.get("failed", 0),
            deleted=tally.get("deleted", 0),
            failures_pruned=tally.get("failures_pruned", 0),
            summary_targeted_ok=self._via_counts["targeted"],
            summary_fallbacks_unavailable=self._via_counts["fallback-unavailable"],
            summary_fallbacks_unexpected=self._via_counts["fallback-unexpected"],
            force=True,
        )
        if self._via_counts["fallback-unexpected"] > 0:
            # ONE aggregate WARNING per reconcile (the per-file WARNING already
            # fired in targeted_summary.py for each occurrence) — a summary a
            # human skimming logs can't miss even if the per-file lines scrolled by.
            logger.warning(
                "reconcile: %d file(s) hit an UNEXPECTED targeted-summary-read "
                "failure this build (see per-file WARNING lines above) — a "
                "targeted-read bug is suspected",
                self._via_counts["fallback-unexpected"],
            )

    def audit_finished(self, outcome: str, duration: float) -> None:
        """Publish the result returned by the worker to the audit coordinator."""
        if self._status is not None:
            self._status.full_audit_finished(outcome, duration)

    def event_applied(self) -> None:
        """Count one committed event-tier record (create/delete via SQS)."""
        if self._status is not None:
            self._status.event_applied()

    def hot_audit_finished(self, outcome: str, duration: float,
                           covered: int, skipped: int) -> None:
        if self._status is not None:
            self._status.hot_audit_finished(outcome, duration, covered, skipped)

    def maintenance_window(self, active: bool) -> None:
        if self._status is not None:
            self._status.maintenance_window(active)

    def tag_edit_expired(self) -> None:
        if self._status is not None:
            self._status.tag_edit_expired()

    def tag_edit_failed(self) -> None:
        if self._status is not None:
            self._status.tag_edit_failed()

    @property
    def summary_via_counts(self) -> dict:
        """A snapshot of this reconcile's summary-read disposition tallies
        (``targeted`` / ``fallback-unavailable`` / ``fallback-unexpected``)."""
        return dict(self._via_counts)

    # -- helpers ----------------------------------------------------------

    def _set_status(self, *, force: bool = False, **fields) -> None:
        if self._status is not None:
            self._status.update(force=force, **fields)

    def _log_due(self) -> bool:
        now = self._clock()
        if self._last_log is not None and now - self._last_log < self._log_interval:
            return False
        self._last_log = now
        return True


def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    if s >= 3600:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    if s >= 60:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s}s"
