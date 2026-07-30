"""CLI entry point: ``python3 -m mcap_catalog_builder <watch_root> [options]``.

Architecture: the live producers (the watchdog observer + debounce Timers for
local, or the SQS event drainer for S3) only enqueue WatchEvents. The audit
coordinator likewise only enqueues one coalesced, result-bearing AuditItem.
``worker_loop`` (run on the main thread) is the single CONSUMER and the only DB
writer. It is driven by a storage ``Source`` and is identical for all backends.
"""

import argparse
import logging
import os
import queue
import signal
import sqlite3
import threading
import time
from dataclasses import dataclass

from .db import Caches, load_caches, open_db
from .builder import catalog_object, delete_by_key
from .publish import build_and_publish
from .reconcile import ReconcileCancelled, SourceSpec, full_reconcile
from .s3_producer import EventRecord, IntakeGate
from .status import ReconcileProgress, StatusWriter
from .storage import LocalSource
from .tag_ipc import TagEditItem, TagEditServer, handle_tag_edit
from .watcher import McapEventHandler, WatchEvent, start_observer
from .writer_lock import WriterLockError, acquire_writer_lock

logger = logging.getLogger(__name__)

DEFAULT_DB = "/tmp/pj-cloud-catalog.db"
_AUDIT_BACKOFF_INITIAL = 1.0


@dataclass(frozen=True)
class AuditResult:
    """The explicit worker-to-coordinator result for one full audit."""

    outcome: str  # "ok" | "failed" | "cancelled"
    duration: float
    error: str | None = None


class AuditItem:
    """A coalesced, result-bearing full-audit work item.

    The coordinator creates it, the single writer calls ``start`` immediately
    before entering ``full_reconcile``, and the worker always calls ``finish``.
    ``cancel_if_pending`` lets shutdown drop an audit that never started without
    removing arbitrary items from ``queue.Queue``.
    """

    kind = "audit"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._state = "pending"  # pending | running | finished
        self._result: AuditResult | None = None

    def start(self) -> bool:
        """Mark the item running, or return False if shutdown already dropped it."""
        with self._lock:
            if self._state != "pending":
                return False
            self._state = "running"
            return True

    def finish(self, result: AuditResult) -> None:
        """Publish exactly one terminal result to the coordinator."""
        with self._lock:
            if self._state == "finished":
                return
            self._state = "finished"
            self._result = result
            self._done.set()

    def cancel_if_pending(self) -> bool:
        """Drop an unstarted audit. A running audit cancels via ``stop_event``."""
        with self._lock:
            if self._state != "pending":
                return False
            self._state = "finished"
            self._result = AuditResult("cancelled", 0.0)
            self._done.set()
            return True

    def wait(self, stop_event: threading.Event) -> AuditResult | None:
        """Wait interruptibly for the worker result.

        On stop, a pending item is made terminal; a running item is left to the
        worker's cancellation checks and the coordinator may exit immediately.
        """
        while not self._done.wait(0.1):
            if stop_event.is_set():
                self.cancel_if_pending()
                if not self._done.is_set():
                    return None
        return self._result


class AuditCoordinator:
    """Single arbiter for completion-relative full-audit scheduling.

    Exactly one item may be queued or running. Successful audits schedule the
    next audit one full interval after completion; failures retry with
    exponential backoff capped at that same interval.
    """

    def __init__(
        self,
        work_q,
        stop_event: threading.Event,
        interval: float,
        *,
        backoff_initial: float = _AUDIT_BACKOFF_INITIAL,
        intake_gate=None,
    ) -> None:
        self._work_q = work_q
        self._stop_event = stop_event
        self._interval = max(0.0, interval)
        self._backoff_initial = max(0.0, backoff_initial)
        # §3.5 handshake: pause SQS intake and wait until every received batch
        # is terminal AND acked before an audit runs (None = no event tier).
        self._gate = intake_gate
        self._lock = threading.Lock()
        self._queued_or_running = False
        self._current: AuditItem | None = None
        self._thread: threading.Thread | None = None

    @property
    def queued_or_running(self) -> bool:
        with self._lock:
            return self._queued_or_running

    def request_due(self) -> bool:
        """Enqueue one due audit, or coalesce it into the active audit."""
        item = self._enqueue_if_idle()
        return item is not None

    def start(self, *, immediate: bool) -> None:
        """Start scheduling; ``immediate`` is used for an existing served DB."""
        initial_delay = 0.0 if immediate else self._interval
        self._thread = threading.Thread(
            target=self._run,
            args=(initial_delay,),
            name="audit-coordinator",
            daemon=True,
        )
        self._thread.start()

    def join(self, timeout: float = 5.0) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def _enqueue_if_idle(self) -> AuditItem | None:
        with self._lock:
            if self._stop_event.is_set() or self._queued_or_running:
                return None
            item = AuditItem()
            self._queued_or_running = True
            self._current = item
        try:
            self._work_q.put_nowait(item)
        except Exception:
            with self._lock:
                self._queued_or_running = False
                self._current = None
            raise
        return item

    def _clear_active(self, item: AuditItem) -> None:
        with self._lock:
            if self._current is item:
                self._current = None
                self._queued_or_running = False

    def _run(self, initial_delay: float) -> None:
        delay = initial_delay
        failures = 0
        while not self._stop_event.wait(delay):
            try:
                if self._gate is not None:
                    # Pause intake BEFORE enqueueing the audit, then wait for
                    # every already-received batch to be committed and acked —
                    # a message must never sit un-acked behind a long audit
                    # blowing its visibility timeout (§3.5). resume() runs in
                    # the outer finally on every exit path.
                    self._gate.pause()
                    if not self._gate.wait_drained(self._stop_event):
                        return  # stop arrived during the drain
                item = self._enqueue_if_idle()
                if item is None:
                    # A due tick while an audit is queued/running is deliberately
                    # coalesced. If another caller requested the active audit, this
                    # scheduler observes that same result instead of creating work.
                    with self._lock:
                        item = self._current
                    if item is None:
                        return  # stop raced the due tick
                result: AuditResult | None = None
                try:
                    result = item.wait(self._stop_event)
                    if result is None or result.outcome == "cancelled":
                        return
                    if result.outcome == "ok":
                        failures = 0
                        delay = self._interval
                    else:
                        failures += 1
                        delay = min(
                            self._interval,
                            self._backoff_initial * (2 ** (failures - 1)),
                        )
                        logger.warning(
                            "full audit failed; retrying in %.1fs (failure %d): %s",
                            delay,
                            failures,
                            result.error or "unknown error",
                        )
                finally:
                    # Never leave the arbiter stuck after failure, cancellation,
                    # or an unexpected result-handling exception.
                    self._clear_active(item)
            finally:
                if self._gate is not None:
                    self._gate.resume()

        # Stop may arrive while the first/due timer is waiting. If an external
        # due request queued an item, make it a dropped terminal item.
        with self._lock:
            current = self._current
        if current is not None:
            try:
                current.cancel_if_pending()
            finally:
                self._clear_active(current)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    p = argparse.ArgumentParser(
        prog="mcap_catalog_builder",
        description="Watch a folder (or S3 bucket) of .mcap files and keep the SQLite catalog in sync.",
    )
    p.add_argument("watch_root", nargs="?", default=".",
                   help="folder of .mcap recordings to watch (local source)")
    p.add_argument("--source", choices=["local", "s3", "gcs"], default="local",
                   help="storage backend (default: local)")
    p.add_argument("--s3-bucket", default=None, help="[s3] bucket name")
    p.add_argument("--s3-prefix", default="", help="[s3] key prefix to scope listing")
    p.add_argument("--sqs-url", default=None, help="[s3] SQS queue URL for S3 event notifications")
    p.add_argument("--gcs-bucket", default=None, help="[gcs] bucket name")
    p.add_argument("--gcs-prefix", default="", help="[gcs] object-name prefix to scope listing")
    p.add_argument("--db", default=DEFAULT_DB, help=f"catalog DB path (default: {DEFAULT_DB})")
    p.add_argument("--tag-socket", default=None,
                   help="path for the tag-edit IPC unix socket (default: off). Daemon "
                        "mode only — started after the served DB opens (or the initial "
                        "build publishes); ignored with --once. See CATALOG_CONTRACT.md's "
                        "'Tag-edit IPC' section (catalog-migration DECISION D2(a)).")
    p.add_argument("--once", action="store_true",
                   help="run one synchronous full reconcile, then exit (no watching). "
                        "Used by the cross-language e2e / CI to build a DB and hand it "
                        "to the Go read-only server.")
    p.add_argument("--rebuild", action="store_true",
                   help="force a from-scratch rebuild published atomically (build to a "
                        "temp DB, checkpoint-gate any existing served DB, then rename "
                        "into place — catalog-migration §6.2a), instead of mutating the "
                        "served DB in place. Implied automatically when --db does not "
                        "exist yet. Valid with --once (the primary use) or in daemon "
                        "mode (rebuilds first, then watches the published path in place).")
    p.add_argument("--rescan-interval", type=float, default=300.0,
                   help="seconds between safety re-scans (default: 300)")
    p.add_argument("--no-watch", action="store_true",
                   help="daemon mode: start no live event producer at all — no "
                        "local watchdog/inotify observer, no S3 SQS long-poll "
                        "thread. Discovery is then rescan-only, driven purely by "
                        "--rescan-interval (useful on hosts where inotify is "
                        "unavailable/exhausted, or where SQS is not wired up). "
                        "With --source s3 in daemon mode, also relaxes the "
                        "--sqs-url requirement. GCS daemon mode is already "
                        "rescan-only, so this is a no-op there. No-op with "
                        "--once (which never starts a producer anyway).")
    p.add_argument("--debounce", type=float, default=2.0,
                   help="[local] seconds to debounce file events (default: 2)")
    p.add_argument("--stability-checks", type=int, default=3,
                   help="[local] size-stability poll count before cataloging (default: 3)")
    p.add_argument("--stability-interval", type=float, default=0.5,
                   help="[local] seconds between size-stability polls (default: 0.5)")
    p.add_argument("--extract-workers", type=int, default=min(2 * (os.cpu_count() or 1), 32),
                   help="concurrency for the full-reconcile read phase (fetch+parse "
                        "summaries). For a remote bucket (--source s3) these are worker "
                        "PROCESSES — each with its own client and its own GIL, so the "
                        "pure-Python MCAP parse scales across cores; for a local watch "
                        "root they are threads. The DB apply stays serial either way. "
                        "Default: 2x CPU cores, capped at 32 — rarely needs tuning.")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def worker_loop(
    conn: sqlite3.Connection,
    caches: Caches,
    source,
    work_q: "queue.Queue[WatchEvent | TagEditItem | AuditItem]",
    workers: int = 1,
    source_spec: "SourceSpec | None" = None,
    progress=None,
    stop_event: threading.Event | None = None,
) -> None:
    """Drain the work queue and perform all DB writes (the single writer).

    Backend-agnostic: each event's payload is mapped to a key via the source,
    stability is gated by the source (local polls; S3 is atomic), and every event
    is handled under a try/except so the worker never dies. Alongside
    ``WatchEvent`` (file-system/S3-driven), the queue also carries
    ``TagEditItem`` — a client tag edit forwarded here from the tag-edit IPC
    server (D2(a)); it is dispatched to ``handle_tag_edit`` instead of the
    ``ev.kind`` chain below (it needs to reply on its own ``event``, which
    ``WatchEvent`` never carries).
    """
    while True:
        if stop_event is not None and stop_event.is_set():
            break
        try:
            ev = work_q.get(timeout=0.1) if stop_event is not None else work_q.get()
        except queue.Empty:
            continue

        if stop_event is not None and stop_event.is_set():
            if isinstance(ev, AuditItem):
                ev.cancel_if_pending()
            # An EventRecord in hand is simply dropped un-terminal: the SQS
            # message stays unacked and redelivers after restart (§3.3).
            break

        if isinstance(ev, AuditItem):
            if not ev.start():
                continue  # pending audit was dropped during shutdown
            started = time.monotonic()
            outcome = "ok"
            error = None
            try:
                full_reconcile(
                    conn,
                    caches,
                    source,
                    workers=workers,
                    source_spec=source_spec,
                    progress=progress,
                    stop_event=stop_event,
                )
            except ReconcileCancelled:
                outcome = "cancelled"
                logger.info("full audit cancelled")
            except Exception as e:  # noqa: BLE001 - result is explicit; worker survives
                outcome = "failed"
                error = f"{type(e).__name__}: {e}"
                logger.exception("full audit failed")
            finally:
                result = AuditResult(outcome, time.monotonic() - started, error)
                try:
                    if progress is not None:
                        progress.audit_finished(result.outcome, result.duration)
                        progress.idle()
                except Exception:  # noqa: BLE001 - observability cannot kill the worker
                    logger.exception("failed to record full-audit status")
                finally:
                    ev.finish(result)
            continue

        if isinstance(ev, EventRecord):
            # SQS event tier (§3.3/§3.4). ``terminal`` = the DB outcome
            # COMMITTED (cataloged / ETag-skipped / committed quarantine /
            # confirmed delete / safe no-op). A raised exception is NOT
            # terminal: the message stays unacked and SQS redelivers it.
            terminal = False
            try:
                if ev.kind == "catalog":
                    catalog_object(conn, caches, ev.key, source)
                    terminal = True
                elif ev.kind == "delete":
                    # HEAD-guard (§3.4): a stale/out-of-order delete for a
                    # re-uploaded object re-catalogs from the live Stat (passed
                    # through — no second HEAD) instead of deleting the row.
                    st = source.stat(ev.key)
                    if st is not None:
                        catalog_object(conn, caches, ev.key, source, stat=st)
                    else:
                        delete_by_key(conn, caches, ev.key)
                    terminal = True
                else:
                    logger.warning("unknown event record kind: %r", ev.kind)
                    terminal = True  # redelivery cannot help an unknown kind
            except Exception:  # noqa: BLE001 - the worker must never die
                logger.exception(
                    "event record failed; left unacked for redelivery: %s", ev.key
                )
            finally:
                if terminal:
                    ev.batch.record_terminal()
                    if progress is not None:
                        progress.event_applied()
            continue

        try:
            if isinstance(ev, TagEditItem):
                handle_tag_edit(conn, caches, ev)
                continue
            if ev.kind == "stop":
                break
            if ev.kind == "catalog":
                if source.wait_for_stable(ev.path):
                    catalog_object(conn, caches, source.event_key(ev.path), source)
                else:
                    logger.warning("file not stable, dropping (retries on rescan): %s", ev.path)
            elif ev.kind == "delete":
                delete_by_key(conn, caches, source.event_key(ev.path))
            else:
                logger.warning("unknown event: %r", ev)
        except Exception:  # noqa: BLE001 - the worker must never die
            logger.exception("worker error handling %r", ev)


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns a process exit code."""
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    work_q: "queue.Queue[WatchEvent | TagEditItem | AuditItem]" = queue.Queue()
    stop_event = threading.Event()
    observer = None
    handler = None
    tag_server = None
    start_producer = None  # deferred until the served DB is ready
    intake_gate = None  # §3.5 audit handshake; set only for the SQS event tier

    # --- build + validate the source (producers are started later) -----------
    if args.source == "s3":
        # --once does a single full_reconcile (a LIST + catalog sweep) and exits, so
        # it needs only the bucket — no SQS event queue. The watch daemon still
        # requires --sqs-url to drain live S3 events, unless --no-watch says to
        # skip live event producers altogether (rescan-only daemon).
        if not args.s3_bucket:
            logger.error("--source s3 requires --s3-bucket")
            return 2
        if not args.once and not args.no_watch and not args.sqs_url:
            logger.error(
                "--source s3 requires --sqs-url (or pass --once for a one-shot "
                "reconcile, or --no-watch for a rescan-only daemon)"
            )
            return 2
        import boto3  # imported lazily so local mode has no boto3 dependency
        from botocore.config import Config
        from .s3_storage import S3Source, _LIST_SHARD_THREADS
        from .s3_producer import s3_event_producer

        # The sharded LIST (s3_storage.list_all) now pipelines up to
        # _LIST_SHARD_THREADS concurrent per-shard paginations on this client,
        # so its connection pool must be sized to match (+ slack for the
        # serial per-event single-file catalogs sharing the same client) — the
        # boto3 default (10) would otherwise serialize shards behind pool
        # waits. The parallel range-GET extraction runs in worker PROCESSES
        # with their own clients (see reconcile.SourceSpec), so this pool only
        # needs to cover the LIST fan-out + serial per-event catalogs.
        source = S3Source(
            boto3.client("s3", config=Config(max_pool_connections=_LIST_SHARD_THREADS + 8)),
            args.s3_bucket, args.s3_prefix,
        )

        if args.sqs_url and not args.once and not args.no_watch:
            intake_gate = IntakeGate()

        def start_producer() -> None:
            # `status` is main()'s StatusWriter, assigned after the writer lock
            # and before _locked_main ever calls this closure (late binding).
            threading.Thread(
                target=s3_event_producer,
                args=(boto3.client("sqs"), args.sqs_url, work_q, stop_event),
                kwargs={"gate": intake_gate, "status": status},
                daemon=True,
                name="sqs-intake",
            ).start()
            logger.info("watching s3://%s/%s via %s", args.s3_bucket, args.s3_prefix, args.sqs_url)
    elif args.source == "gcs":
        if not args.gcs_bucket:
            logger.error("--source gcs requires --gcs-bucket")
            return 2
        from google.cloud import storage as gcs_storage_lib  # lazy: no google dep for local/s3
        from .gcs_storage import GCSSource

        # STORAGE_EMULATOR_HOST is auto-handled by the SDK (the fake-gcs leg).
        source = GCSSource(gcs_storage_lib.Client(), args.gcs_bucket, args.gcs_prefix)

        def start_producer() -> None:
            # GCS has no live-event producer wired today (Pub/Sub is future work,
            # mirroring S3's SQS); scheduled full audits keep the catalog in sync.
            logger.info("watching gcs://%s/%s (rescan-only; --once for a one-shot build)",
                        args.gcs_bucket, args.gcs_prefix)
    else:
        if not os.path.isdir(args.watch_root):
            logger.error("watch_root is not a directory: %s", args.watch_root)
            return 2
        source = LocalSource(args.watch_root, args.stability_checks, args.stability_interval)

        def start_producer() -> None:
            nonlocal observer, handler
            handler = McapEventHandler(work_q, args.debounce)
            observer = start_observer(args.watch_root, handler)
            logger.info("watching %s", args.watch_root)

    # Single-writer enforcement (CATALOG_CONTRACT.md §11): acquire the per-DB
    # writer lock BEFORE any DB write or tag-socket bind, in BOTH daemon and
    # --once modes. A second builder on the same --db (e.g. a --once --rebuild
    # racing a live daemon, or a double-started deploy) fails fast here with
    # exit code 3 instead of interleaving writes / stealing the tag socket.
    # Held for the process lifetime (released in the finally below for
    # in-process callers like the tests; the kernel also auto-releases it on
    # any process death, so a crash never leaves a stale lock).
    try:
        writer_lock = acquire_writer_lock(args.db)
    except WriterLockError as e:
        logger.error("%s", e)
        return 3
    # The status sidecar is constructed only AFTER the flock succeeds — holding
    # the writer lock is what makes this process the owner of <db>.status.json
    # (a refused second builder must never clobber the incumbent's status). The
    # heartbeat keeps updated_at fresh for the container healthcheck through
    # phases with no per-file progress (long LISTs, idle daemon).
    status = StatusWriter(args.db)
    status.heartbeat_start(stop_event)
    try:
        return _locked_main(args, source, start_producer, work_q, stop_event,
                            lambda: observer, lambda: handler, status, intake_gate)
    except Exception as e:
        # Funnel any fatal error (missing credentials surfacing at the first
        # LIST, a failed publish, ...) into the sidecar so "unhealthy" comes
        # with a reason, then re-raise for the traceback + nonzero exit.
        status.fatal(f"{type(e).__name__}: {e}")
        raise
    finally:
        stop_event.set()
        status.heartbeat_stop()
        writer_lock.release()


def _locked_main(args, source, start_producer, work_q, stop_event,
                 get_observer, get_handler, status, intake_gate=None) -> int:
    """The post-lock body of main(): everything that reads or writes the served
    DB / binds the tag socket runs under the single-writer lock. The observer/
    handler are read through GETTERS because the local-source start_producer
    closure assigns them in main()'s scope (nonlocal) after this function has
    already been entered."""
    tag_server = None
    audit_coordinator = None

    # Sensible default, no knob: a remote bucket extracts in a PROCESS pool so the
    # GIL-bound MCAP parse scales across cores (the reason the read phase is slow on a
    # big first scan). A local watch root stays on threads — dev/tests/small file
    # counts, where process overhead isn't worth it. (--source gcs is not yet wired
    # for processes, so it stays on threads too.) The picklable SourceSpec lets each
    # worker rebuild the read-only Source, whose live boto3 client can't be pickled.
    extract_spec = (
        SourceSpec(kind="s3", bucket=args.s3_bucket, prefix=args.s3_prefix)
        if args.source == "s3"
        else None
    )

    # Create/rebuild path: the served DB does not exist yet, or --rebuild forces a
    # from-scratch build. Either way, a reader must never observe a half-built
    # catalog at the served path — build to a temp DB and publish atomically
    # (catalog-migration §6.2a), instead of creating/mutating args.db directly.
    use_publish = args.rebuild or not os.path.exists(args.db)
    startup_audit = False

    progress = ReconcileProgress(status=status)

    if use_publish:
        logger.info("rebuild-publish (db=%s, rebuild=%s)", args.db, args.rebuild)
        build_and_publish(
            args.db,
            lambda c, ca: full_reconcile(
                c, ca, source, workers=args.extract_workers, source_spec=extract_spec,
                progress=progress,
            ),
        )
        # One-shot mode: the publish above already built + published the full
        # catalog. Exit cleanly without starting any producer/rescan thread — the
        # caller (the Go read-only server, the cross-language e2e, CI) takes over
        # the DB from here.
        if args.once:
            progress.idle()
            logger.info("--once --rebuild: publish complete, exiting")
            return 0
        # Daemon mode: continue watching the just-PUBLISHED path in place (the
        # normal in-place-mutation norm resumes from here on).
        conn = open_db(args.db)
        caches = load_caches(conn)
    else:
        conn = open_db(args.db)
        caches = load_caches(conn)

        if args.once:
            # Preserve one-shot behavior exactly: an existing DB is reconciled
            # synchronously and no producer, IPC server, or coordinator starts.
            logger.info("startup reconcile (db=%s)", args.db)
            full_reconcile(conn, caches, source, workers=args.extract_workers,
                           source_spec=extract_spec, progress=progress)
            conn.close()
            progress.idle()
            logger.info("--once: reconcile complete, exiting")
            return 0
        # Existing, cleanly-opened served DB: service starts first and the
        # startup audit is an ordinary coalesced worker item.
        startup_audit = True
        logger.info("startup full audit scheduled (db=%s)", args.db)

    progress.idle()  # daemon steady state: first build done, catalog published

    def _on_signal(_signum, _frame) -> None:
        # Set the cancellation signal immediately: a stop item alone would sit
        # behind a long audit and could not interrupt LIST/apply/backoff work.
        stop_event.set()
        work_q.put(WatchEvent("stop"))

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        # Tag IPC binds before the startup audit is even queued. Its server
        # thread remains enqueue-only; edits still execute on this worker.
        if args.tag_socket:
            tag_server = TagEditServer(args.tag_socket, work_q)
            threading.Thread(target=tag_server.serve_forever, daemon=True).start()
            logger.info("tag-edit IPC listening on %s", args.tag_socket)

        if args.no_watch:
            logger.info(
                "discovery is rescan-only (--no-watch): interval=%ss",
                args.rescan_interval,
            )
        else:
            start_producer()

        audit_coordinator = AuditCoordinator(
            work_q, stop_event, args.rescan_interval, intake_gate=intake_gate
        )
        audit_coordinator.start(immediate=startup_audit)

        worker_loop(conn, caches, source, work_q, workers=args.extract_workers,
                    source_spec=extract_spec, progress=progress,
                    stop_event=stop_event)
    finally:
        stop_event.set()
        if audit_coordinator is not None:
            audit_coordinator.join()
        if tag_server is not None:
            tag_server.shutdown()
            tag_server.server_close()  # also unlinks the socket file
        handler = get_handler()
        if handler is not None:
            handler.cancel_timers()
        observer = get_observer()
        if observer is not None:
            observer.stop()
            observer.join()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
