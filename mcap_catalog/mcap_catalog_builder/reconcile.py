"""Full reconcile scan: catalog every object in the source, then hard-delete
vanished rows.

This is the authoritative path for removals (live ``on_deleted`` / SQS-delete
events are best-effort). It is **backend-agnostic**: it iterates a storage
``Source.list_all()``, so it works over the local filesystem or S3. It runs on
the single writer thread like everything else.
"""

import logging
import multiprocessing
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path

import sqlite3

from .db import Caches, record_build, record_failure
from .builder import apply_extract, extract_summary, resolve_key_dims
from .storage import LocalSource

logger = logging.getLogger(__name__)


class ReconcileCancelled(Exception):
    """A daemon audit stopped before it could authoritatively finish."""


def _raise_if_stopped(stop_event) -> None:
    if stop_event is not None and stop_event.is_set():
        raise ReconcileCancelled("full audit cancelled")


@dataclass(frozen=True)
class SourceSpec:
    """A **picklable** recipe for rebuilding a read-only ``Source`` inside a worker
    PROCESS. The live ``Source`` holds an unpicklable boto3 client, so it cannot
    cross a process boundary — each worker builds its own from this spec instead.
    Used only by the process-pool extraction path (below)."""

    kind: str  # "s3" | "local"
    bucket: str = ""   # s3
    prefix: str = ""   # s3
    max_pool: int = 4  # s3: per-PROCESS boto3 pool (each process extracts one file at a time)
    root: str = ""     # local


# Per-process singleton, built once by the ProcessPoolExecutor initializer so the
# boto3 client / open fds are created ONCE per worker, not per task.
_worker_source = None


def _init_worker(spec: SourceSpec) -> None:
    """ProcessPoolExecutor initializer: construct one ``Source`` for this worker."""
    global _worker_source
    if spec.kind == "s3":
        import boto3
        from botocore.config import Config

        from .s3_storage import S3Source

        _worker_source = S3Source(
            boto3.client("s3", config=Config(max_pool_connections=spec.max_pool)),
            spec.bucket,
            spec.prefix,
        )
    elif spec.kind == "local":
        _worker_source = LocalSource(spec.root)
    else:  # pragma: no cover - guarded by callers
        raise ValueError(f"unsupported SourceSpec.kind: {spec.kind!r}")


def _extract_task(item):
    """Worker-process task: fetch+parse one object's summary via the per-process
    ``Source``. Returns a picklable ``Extract`` (``extract_summary`` never raises)."""
    key, stat, dims, eff_key = item
    return extract_summary(_worker_source, key, stat, dims, eff_key)


def _bounded_completions(submit, items, window: int, stop_event=None):
    """Submit ``items`` keeping at most ``window`` futures in flight; yield each
    result as it completes.

    NEVER submit-all + ``as_completed``: a list of every future pins every
    completed ``Extract`` (its whole FileSummary graph) until the pool closes —
    O(bucket) memory, multi-GB on a million-object cold build (the 2026-07-28
    production kill). Here a completed future is dropped the moment its result
    is yielded, so retained results stay O(window) regardless of bucket size.
    """
    in_flight: set = set()
    try:
        for item in items:
            _raise_if_stopped(stop_event)
            in_flight.add(submit(item))
            if len(in_flight) >= window:
                while True:
                    done, in_flight = wait(
                        in_flight, timeout=0.1, return_when=FIRST_COMPLETED
                    )
                    _raise_if_stopped(stop_event)
                    if done:
                        break
                for fut in done:
                    _raise_if_stopped(stop_event)
                    yield fut.result()
        while in_flight:
            done, in_flight = wait(
                in_flight, timeout=0.1, return_when=FIRST_COMPLETED
            )
            _raise_if_stopped(stop_event)
            for fut in done:
                _raise_if_stopped(stop_event)
                yield fut.result()
    finally:
        if stop_event is not None and stop_event.is_set():
            for fut in in_flight:
                fut.cancel()


def _is_catalogable_name(name: str) -> bool:
    return (
        name.endswith(".mcap")
        and not name.startswith(".")
        and not name.endswith(".mcap.tmp")
        and not name.endswith(".part")
    )


def scan_disk(watched_root: str) -> list[str]:
    """Return sorted absolute paths of catalogable ``.mcap`` files under ``watched_root``.

    Skips dotfiles, any path with a hidden directory component, and ``*.mcap.tmp`` /
    ``*.part`` temp files.
    """
    out: list[str] = []
    root = Path(watched_root)
    for p in root.rglob("*.mcap"):
        rel_parts = p.relative_to(root).parts
        if any(part.startswith(".") for part in rel_parts):
            continue
        if _is_catalogable_name(p.name):
            out.append(str(p))
    return sorted(out)


def _composite_ids(caches: Caches, dims) -> tuple | None:
    """The 6-tuple (dim ids + date + filename) for a file, or ``None`` if any
    dimension is not yet in the caches (i.e. the file cannot already be catalogued)."""
    cid = caches.customer.get(dims["customer"])
    sid = caches.site.get((cid, dims["site"])) if cid is not None else None
    rid = caches.robot.get((sid, dims["robot"])) if sid is not None else None
    srcid = caches.source.get(dims["source"])
    if None in (cid, sid, rid, srcid):
        return None
    return (cid, sid, rid, srcid, dims["date"], dims["filename"])


def full_reconcile(
    conn: sqlite3.Connection, caches: Caches, source, workers: int = 1,
    source_spec: "SourceSpec | None" = None, progress=None, stop_event=None,
) -> dict[str, int]:
    """Catalog all objects in ``source``, then delete catalog rows with no object.

    ``source`` is a storage ``Source``; a ``str`` is accepted as shorthand for a
    local watch root. ``workers`` > 1 fetches summaries on a thread pool (the slow,
    network-bound, out-of-transaction read); DB writes stay on this single thread.
    When ``source_spec`` is given (a picklable recipe for the ``Source``), that read
    phase runs in a PROCESS pool instead, so the GIL-bound pure-Python MCAP parse
    scales across cores; the DB apply stays serial on this thread either way.
    ``progress`` is an optional ``status.ReconcileProgress`` — milestone/per-file
    callbacks stay on THIS thread (worker processes never see it).
    Returns a tally ``{"cataloged", "skipped", "failed", "deleted"}``.
    """
    if isinstance(source, str):
        source = LocalSource(source)

    _raise_if_stopped(stop_event)
    tally = {"cataloged": 0, "skipped": 0, "failed": 0, "deleted": 0}
    listings = []
    listing_iter = (
        source.list_all(stop_event=stop_event)
        if stop_event is not None
        else source.list_all()
    )
    for lst in listing_iter:
        _raise_if_stopped(stop_event)
        listings.append(lst)
        # First object = the LIST provably began (credentials/bucket access
        # work) — that is the sidecar's first-milestone write (§12), so the
        # healthcheck sees a fresh phase=listing within seconds, not after
        # 5000 objects. Then periodic count updates.
        if progress is not None and (len(listings) == 1 or len(listings) % 5000 == 0):
            progress.listing(len(listings))
    _raise_if_stopped(stop_event)
    if progress is not None:
        progress.listed(len(listings))

    # Fingerprints already catalogued, keyed by the composite id-tuple. An unchanged
    # file is then skipped in classification below with NO network at all (R4) —
    # neither a HEAD nor a summary read — since the listing already carries the etag.
    stored: dict[tuple, str] = {}
    for r in conn.execute(
        "SELECT customer_id, site_id, robot_id, source_id, date, filename, etag FROM files"
    ).fetchall():
        _raise_if_stopped(stop_event)
        stored[(
            r["customer_id"], r["site_id"], r["robot_id"], r["source_id"],
            r["date"], r["filename"],
        )] = r["etag"]

    # Classify every listing (no DB writes beyond recording unparseable keys, no
    # summary reads): resolve dims ONCE here, skip unchanged files, queue the rest
    # for the read phase carrying those same dims (one identity per file — see
    # extract_summary). dims_by_key drives the deletion sweep below.
    dims_by_key: dict[str, dict] = {}
    to_extract: list = []  # (key, stat, dims, eff_key)
    for lst in listings:
        _raise_if_stopped(stop_event)
        res = resolve_key_dims(lst.key, source)
        if res is None:
            record_failure(conn, lst.key, "unparseable key")
            conn.commit()
            tally["failed"] += 1
            continue
        dims, eff_key = res
        dims_by_key[lst.key] = dims
        comp = _composite_ids(caches, dims)
        if comp is not None and stored.get(comp) == lst.stat.etag:
            tally["skipped"] += 1
            continue
        to_extract.append((lst.key, lst.stat, dims, eff_key))

    if progress is not None:
        progress.extract_start(len(to_extract), tally["skipped"], tally["failed"])

    def _apply(ex) -> None:
        _raise_if_stopped(stop_event)
        st = apply_extract(conn, caches, ex).status
        tally[st] += 1
        _raise_if_stopped(stop_event)
        if progress is not None:
            progress.file_done(st, getattr(ex, "summary_via", ""))

    # Read phase (parallel, network-bound, NO DB) -> apply phase (serial, DB writes
    # on this thread only). Each summary applies as soon as it lands while other
    # fetches are still in flight, through a BOUNDED submission window (see
    # _bounded_completions) so completed results never accumulate.
    if workers > 1 and len(to_extract) > 1:
        # Parallelize fetch+parse. For a remote bucket (source_spec given) use a
        # PROCESS pool so the pure-Python MCAP parse isn't GIL-serialized — each worker
        # has its own client + GIL; otherwise a thread pool. Size to the actual work so
        # a small rescan doesn't spawn a full-width pool. The DB apply stays serial on
        # THIS thread either way (per-file quarantine + count-check unchanged).
        n = min(workers, len(to_extract))
        if source_spec is not None:
            # Explicit context, not the platform default: production runs Python
            # 3.12 (CI runs 3.11 — same default), whose default start method is
            # fork, and the daemon starts the sidecar heartbeat THREAD
            # (status.StatusWriter.heartbeat_start) before this pool — a bare
            # fork() would fork-after-threads, a known hazard (any lock held by
            # a thread other than the forking one at fork time stays locked
            # forever in the child, since only the forking thread survives the
            # fork). forkserver forks from a clean, thread-free helper process
            # instead, sidestepping the hazard entirely. Dev's 3.14 already
            # defaults to forkserver, so this is a behavior change on 3.11/3.12
            # only. Side effect: worker-process logging (targeted_summary.py's
            # per-file DEBUG/WARNING lines) no longer inherits the parent's
            # logging.basicConfig under forkserver the way it would under fork
            # — the summary_* sidecar counters are parent-side (file_done runs
            # on THIS thread) and are unaffected either way.
            pool = ProcessPoolExecutor(
                max_workers=n, initializer=_init_worker, initargs=(source_spec,),
                mp_context=multiprocessing.get_context("forkserver"),
            )
            submit = lambda item: pool.submit(_extract_task, item)  # noqa: E731
        else:
            pool = ThreadPoolExecutor(max_workers=n)
            submit = lambda item: pool.submit(extract_summary, source, *item)  # noqa: E731
        try:
            # window = 2n: every worker stays fed (one running + one queued each)
            # while at most ~2n results are resident awaiting the serial apply.
            for ex in _bounded_completions(
                submit, to_extract, window=2 * n, stop_event=stop_event
            ):
                _apply(ex)
        finally:
            cancelled = stop_event is not None and stop_event.is_set()
            pool.shutdown(wait=not cancelled, cancel_futures=cancelled)
    else:
        for item in to_extract:
            _raise_if_stopped(stop_event)
            ex = extract_summary(source, *item)
            _raise_if_stopped(stop_event)
            _apply(ex)

    # Deletion sweep: composite keys present in the source (parseable + cached ids).
    # Reuses the dims parsed above; caches are now fully populated post-apply.
    present: set[tuple] = set()
    for lst in listings:
        _raise_if_stopped(stop_event)
        dims = dims_by_key.get(lst.key)
        if dims is None:
            continue
        comp = _composite_ids(caches, dims)
        if comp is None:
            continue
        present.add(comp)

    try:
        for r in conn.execute(
            "SELECT id, customer_id, site_id, robot_id, source_id, date, filename FROM files"
        ).fetchall():
            _raise_if_stopped(stop_event)
            comp = (
                r["customer_id"], r["site_id"], r["robot_id"], r["source_id"],
                r["date"], r["filename"],
            )
            if comp not in present:
                conn.execute("DELETE FROM files WHERE id=?", (r["id"],))
                tally["deleted"] += 1
        _raise_if_stopped(stop_event)
        conn.commit()
    except ReconcileCancelled:
        # The sweep is one transaction; never leave a partial set of deletes
        # pending for an unrelated later write to commit.
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise

    # Stamp build_metadata so the read-only Go server can report catalog freshness
    # (§6.5). files_scanned = objects seen (cataloged + skipped); outcome is
    # 'partial' if any file quarantined this build.
    _raise_if_stopped(stop_event)
    record_build(
        conn,
        files_scanned=tally["cataloged"] + tally["skipped"],
        files_failed=tally["failed"],
        outcome="partial" if tally["failed"] else "ok",
    )

    if progress is not None:
        progress.finished(tally)
    logger.info(
        "reconcile: cataloged=%d skipped=%d failed=%d deleted=%d",
        tally["cataloged"], tally["skipped"], tally["failed"], tally["deleted"],
    )
    return tally
