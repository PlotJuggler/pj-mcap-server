"""Full reconcile scan: catalog every object in the source, then hard-delete
vanished rows.

This is the authoritative path for removals (live ``on_deleted`` / SQS-delete
events are best-effort). It is **backend-agnostic**: it iterates a storage
``Source.list_all()``, so it works over the local filesystem or S3. It runs on
the single writer thread like everything else.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import sqlite3

from .db import Caches, record_build, record_failure
from .builder import apply_extract, extract_summary, resolve_key_dims
from .storage import LocalSource

logger = logging.getLogger(__name__)


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
    source_spec: "SourceSpec | None" = None,
) -> dict[str, int]:
    """Catalog all objects in ``source``, then delete catalog rows with no object.

    ``source`` is a storage ``Source``; a ``str`` is accepted as shorthand for a
    local watch root. ``workers`` > 1 fetches summaries on a thread pool (the slow,
    network-bound, out-of-transaction read); DB writes stay on this single thread.
    When ``source_spec`` is given (a picklable recipe for the ``Source``), that read
    phase runs in a PROCESS pool instead, so the GIL-bound pure-Python MCAP parse
    scales across cores; the DB apply stays serial on this thread either way.
    Returns a tally ``{"cataloged", "skipped", "failed", "deleted"}``.
    """
    if isinstance(source, str):
        source = LocalSource(source)

    tally = {"cataloged": 0, "skipped": 0, "failed": 0, "deleted": 0}
    listings = list(source.list_all())

    # Fingerprints already catalogued, keyed by the composite id-tuple. An unchanged
    # file is then skipped in classification below with NO network at all (R4) —
    # neither a HEAD nor a summary read — since the listing already carries the etag.
    stored: dict[tuple, str] = {}
    for r in conn.execute(
        "SELECT customer_id, site_id, robot_id, source_id, date, filename, etag FROM files"
    ).fetchall():
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

    # Read phase (parallel, network-bound, NO DB) -> apply phase (serial, DB writes
    # on this thread only). as_completed lets each summary apply as soon as it lands
    # while other fetches are still in flight.
    if workers > 1 and len(to_extract) > 1:
        # Parallelize fetch+parse. For a remote bucket (source_spec given) use a
        # PROCESS pool so the pure-Python MCAP parse isn't GIL-serialized — each worker
        # has its own client + GIL; otherwise a thread pool. Size to the actual work so
        # a small rescan doesn't spawn a full-width pool. The DB apply stays serial on
        # THIS thread either way (per-file quarantine + count-check unchanged).
        n = min(workers, len(to_extract))
        if source_spec is not None:
            pool = ProcessPoolExecutor(
                max_workers=n, initializer=_init_worker, initargs=(source_spec,)
            )
        else:
            pool = ThreadPoolExecutor(max_workers=n)
        with pool:
            if source_spec is not None:
                futures = [pool.submit(_extract_task, item) for item in to_extract]
            else:
                futures = [pool.submit(extract_summary, source, *item) for item in to_extract]
            for fut in as_completed(futures):
                tally[apply_extract(conn, caches, fut.result()).status] += 1
    else:
        for item in to_extract:
            tally[apply_extract(conn, caches, extract_summary(source, *item)).status] += 1

    # Deletion sweep: composite keys present in the source (parseable + cached ids).
    # Reuses the dims parsed above; caches are now fully populated post-apply.
    present: set[tuple] = set()
    for lst in listings:
        dims = dims_by_key.get(lst.key)
        if dims is None:
            continue
        comp = _composite_ids(caches, dims)
        if comp is None:
            continue
        present.add(comp)

    for r in conn.execute(
        "SELECT id, customer_id, site_id, robot_id, source_id, date, filename FROM files"
    ).fetchall():
        comp = (
            r["customer_id"], r["site_id"], r["robot_id"], r["source_id"],
            r["date"], r["filename"],
        )
        if comp not in present:
            conn.execute("DELETE FROM files WHERE id=?", (r["id"],))
            tally["deleted"] += 1
    conn.commit()

    # Stamp build_metadata so the read-only Go server can report catalog freshness
    # (§6.5). files_scanned = objects seen (cataloged + skipped); outcome is
    # 'partial' if any file quarantined this build.
    record_build(
        conn,
        files_scanned=tally["cataloged"] + tally["skipped"],
        files_failed=tally["failed"],
        outcome="partial" if tally["failed"] else "ok",
    )

    logger.info(
        "reconcile: cataloged=%d skipped=%d failed=%d deleted=%d",
        tally["cataloged"], tally["skipped"], tally["failed"], tally["deleted"],
    )
    return tally
