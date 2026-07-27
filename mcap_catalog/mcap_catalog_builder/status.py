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


class StatusWriter:
    """Atomically maintains ``<db>.status.json`` (thread-safe, throttled)."""

    def __init__(self, db_path: str, *, min_interval: float = 1.0, clock=time.monotonic) -> None:
        self.path = db_path + STATUS_SUFFIX
        self._tmp = self.path + ".tmp"
        self._lock = threading.Lock()
        self._state: dict = {}
        self._min_interval = min_interval
        self._clock = clock
        self._last_write: float | None = None
        self._hb_thread: threading.Thread | None = None

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

    def heartbeat_start(self, stop_event: threading.Event, interval: float = 10.0) -> None:
        """Refresh ``updated_at`` every ``interval`` seconds on a daemon thread,
        so sidecar freshness means "the builder process is alive" even during
        phases that produce no per-file progress (long LISTs, idle daemon)."""

        def _beat() -> None:
            while not stop_event.wait(interval):
                self.update(force=True)

        self._hb_thread = threading.Thread(target=_beat, name="status-heartbeat", daemon=True)
        self._hb_thread.start()

    def heartbeat_stop(self) -> None:
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
        with open(self._tmp, "w") as f:
            json.dump(doc, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(self._tmp, self.path)
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
        self._total = to_extract
        self._counts["skipped"] = skipped
        self._counts["failed"] = failed
        self._t0 = self._clock()
        self._last_log = None
        self._set_status(
            phase="extracting", extract_total=to_extract, extract_done=0,
            skipped=skipped, failed=failed, force=True,
        )
        logger.info(
            "reconcile: extracting %d files (%d unchanged skipped, %d unparseable)",
            to_extract, skipped, failed,
        )

    def file_done(self, status: str) -> None:
        self._done += 1
        self._counts[status] = self._counts.get(status, 0) + 1
        self._set_status(
            extract_done=self._done,
            cataloged=self._counts["cataloged"],
            skipped=self._counts["skipped"],
            failed=self._counts["failed"],
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
            force=True,
        )

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
