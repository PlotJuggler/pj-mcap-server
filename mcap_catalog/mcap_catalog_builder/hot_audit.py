"""Tier-2 hot-window scoped audit (event-discovery design 2026-07-30 §4).

A frequent, cheap, SCOPED audit of the prefixes where change is expected: the
registry of dimension combos known to the catalog (files ∪ parseable
``catalog_failures`` keys) × the last ``window_days`` of ``date=`` partitions.
Fail-closed by construction (§4.2):

- a prefix counts as *covered* only if its pagination COMPLETED (list_prefix
  raises rather than returning a partial result);
- the deletion sweep runs per covered prefix ONLY — global deletion is
  structurally impossible from a scoped feed;
- a hot audit NEVER stamps ``build_metadata`` (that is whole-catalog freshness,
  owned by the tier-3 full audit). Its telemetry goes to the sidecar only.

S3-only: ``LocalSource.intended_key`` can override a file's identity via an
embedded ``s3_key``, so raw-path prefix scoping would mis-attribute local
rows (a flat file cataloged under a Hive identity would look like a
covered-empty prefix and be falsely swept). ``__main__`` therefore accepts
``--hot-audit-interval`` only with ``--source s3``.

Runs on the single writer thread like everything else (an ``AuditItem`` with
``audit_kind="hot"`` dispatched by ``worker_loop``).
"""

import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor

from .builder import resolve_key_dims
from .db import record_failure
from .keyparse import parse_hive_key
from .reconcile import ReconcileCancelled, _raise_if_stopped, _run_extract_apply

logger = logging.getLogger(__name__)

# Hot prefixes are small (one robot-day each); a modest pool keeps the LIST
# phase snappy without the full sharded-LIST machinery.
_LIST_PREFIX_THREADS = 8

Combo = tuple  # (customer, site, robot, source) — names, not ids


def registry_combos(conn) -> "set[Combo]":
    """§4.1: dimension combos present in ``files`` ∪ parseable keys in
    ``catalog_failures`` (a quarantined file must not exile its combo).

    Cost note (Codex consult 2026-08-10): the DISTINCT runs index-only over
    the UNIQUE composite ``(customer_id, site_id, robot_id, source_id, date,
    filename)`` — no row fetches — and names resolve via the four small
    lookup tables. O(index entries) per cadence, measured well under a second
    at 1M rows; an incrementally-maintained registry was rejected as
    staleness-prone for that price."""
    id_combos = conn.execute(
        "SELECT DISTINCT customer_id, site_id, robot_id, source_id FROM files"
    ).fetchall()
    names = {
        "customers": dict(conn.execute("SELECT id, name FROM customers").fetchall()),
        "sites": dict(conn.execute("SELECT id, name FROM sites").fetchall()),
        "robots": dict(conn.execute("SELECT id, name FROM robots").fetchall()),
        "sources": dict(conn.execute("SELECT id, name FROM sources").fetchall()),
    }
    combos: set[Combo] = set()
    for r in id_combos:
        combos.add((
            names["customers"][r["customer_id"]],
            names["sites"][r["site_id"]],
            names["robots"][r["robot_id"]],
            names["sources"][r["source_id"]],
        ))
    for r in conn.execute("SELECT s3_key FROM catalog_failures").fetchall():
        dims = parse_hive_key(r["s3_key"])
        if dims is not None:
            combos.add((dims["customer"], dims["site"], dims["robot"], dims["source"]))
    return combos


def hot_prefixes(
    combos: "set[Combo]", today: dt.date, window_days: int
) -> "list[tuple[str, Combo, str]]":
    """Hive prefixes for ``date ∈ [today − window_days, today]`` per combo,
    oldest-first, as ``(prefix, combo, date)`` — the combo+date ride along so
    the scoped sweep never has to re-parse a prefix string. Dates are ISO
    (matching the builder's own key convention); a bucket using non-ISO
    ``date=`` values is repaired by tier 3 only (§4.3's honesty list)."""
    dates = [
        (today - dt.timedelta(days=n)).isoformat()
        for n in range(window_days, -1, -1)
    ]
    out: list[tuple[str, Combo, str]] = []
    for combo in sorted(combos):
        customer, site, robot, source = combo
        for d in dates:
            out.append((
                f"customer={customer}/customer_site={site}/robot={robot}/"
                f"source={source}/date={d}/",
                combo, d,
            ))
    return out


def _stored_for(conn, caches, combo: Combo, date: str) -> "dict[str, tuple[int, str]]":
    """{filename: (files.id, etag)} for one combo+date, via the UNIQUE
    composite index — O(scope) on the SQLite side (§4.2), never a full-table
    fingerprint load like full_reconcile's."""
    customer, site, robot, source = combo
    cid = caches.customer.get(customer)
    sid = caches.site.get((cid, site)) if cid is not None else None
    rid = caches.robot.get((sid, robot)) if sid is not None else None
    srcid = caches.source.get(source)
    if None in (cid, sid, rid, srcid):
        return {}   # no ids cached => no rows can exist for this combo
    return {
        r["filename"]: (r["id"], r["etag"])
        for r in conn.execute(
            "SELECT id, filename, etag FROM files WHERE customer_id=? AND "
            "site_id=? AND robot_id=? AND source_id=? AND date=?",
            (cid, sid, rid, srcid, date),
        ).fetchall()
    }


def hot_audit(
    conn, caches, source, *, window_days: int = 2, workers: int = 1,
    source_spec=None, stop_event=None, today: "dt.date | None" = None,
) -> "dict[str, int]":
    """One tier-2 pass: scoped LIST -> catalog changes -> scoped sweep.

    Returns the tally ``{cataloged, skipped, failed, deleted,
    covered_prefixes, skipped_prefixes}``. Raises ``ReconcileCancelled`` on
    stop (a partial pass changes nothing the next pass can't redo). Raises
    ``RuntimeError`` when EVERY targeted prefix failed to LIST (zero
    coverage = the pass did nothing; the coordinator's backoff should see a
    failure, not a healthy "ok" — Codex consult 2026-08-10)."""
    tally = {"cataloged": 0, "skipped": 0, "failed": 0, "deleted": 0,
             "covered_prefixes": 0, "skipped_prefixes": 0}
    _raise_if_stopped(stop_event)
    targets = hot_prefixes(
        registry_combos(conn),
        today if today is not None else dt.datetime.now(dt.timezone.utc).date(),
        window_days,
    )
    if not targets:
        return tally

    # LIST phase: each prefix independently. Covered = pagination COMPLETED
    # (list_prefix raises otherwise); a failed prefix is excluded from BOTH
    # the catalog and the sweep below — fail-closed per prefix (§4.2).
    covered: list = []  # (prefix, combo, date, listings)

    def _list_one(target):
        prefix, combo, date = target
        return prefix, combo, date, source.list_prefix(prefix, stop_event=stop_event)

    with ThreadPoolExecutor(
        max_workers=min(_LIST_PREFIX_THREADS, len(targets))
    ) as pool:
        for fut in [pool.submit(_list_one, t) for t in targets]:
            try:
                covered.append(fut.result())
            except ReconcileCancelled:
                raise
            except Exception as e:  # noqa: BLE001 — excluded, never partial
                tally["skipped_prefixes"] += 1
                logger.warning(
                    "hot audit: prefix excluded from coverage (LIST failed): %s", e
                )
    _raise_if_stopped(stop_event)
    tally["covered_prefixes"] = len(covered)
    if not covered:
        raise RuntimeError(
            f"hot audit: zero coverage — all {tally['skipped_prefixes']} "
            "prefix LISTs failed"
        )

    # Classify: scoped fingerprint lookup per covered prefix; unchanged files
    # skip with zero network (the listing carries the etag), like tier 3.
    to_extract: list = []
    for _prefix, combo, date, listings in covered:
        stored = _stored_for(conn, caches, combo, date)
        for lst in listings:
            _raise_if_stopped(stop_event)
            res = resolve_key_dims(lst.key, source)
            if res is None:
                record_failure(conn, lst.key, "unparseable key")
                conn.commit()
                tally["failed"] += 1
                continue
            dims, eff_key = res
            # S3 keys are authoritative (intended_key is None), so dims always
            # match this prefix's combo/date and the filename lookup is exact.
            row = stored.get(dims["filename"])
            if row is not None and row[1] == lst.stat.etag:
                tally["skipped"] += 1
                continue
            to_extract.append((lst.key, lst.stat, dims, eff_key))

    _run_extract_apply(
        conn, caches, source, to_extract, workers=workers,
        source_spec=source_spec, tally=tally, progress=None,
        stop_event=stop_event,
    )

    # Scoped sweep: deletions ONLY for rows inside covered prefixes. The
    # per-prefix row set is re-read here, AFTER apply, so rows just written
    # are present and can never be swept. One transaction, rolled back on
    # cancellation (mirrors full_reconcile's sweep discipline).
    try:
        for _prefix, combo, date, listings in covered:
            _raise_if_stopped(stop_event)
            listed = {lst.key.rsplit("/", 1)[-1] for lst in listings}
            for filename, (row_id, _etag) in _stored_for(conn, caches, combo, date).items():
                if filename not in listed:
                    conn.execute("DELETE FROM files WHERE id=?", (row_id,))
                    tally["deleted"] += 1
        _raise_if_stopped(stop_event)
        conn.commit()
    except ReconcileCancelled:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise

    # NO record_build here, ever (§4.2) — build_metadata is whole-catalog
    # freshness owned by the tier-3 full audit; a subset stamp would lie.
    logger.info(
        "hot audit: cataloged=%d skipped=%d failed=%d deleted=%d "
        "covered=%d excluded=%d",
        tally["cataloged"], tally["skipped"], tally["failed"], tally["deleted"],
        tally["covered_prefixes"], tally["skipped_prefixes"],
    )
    return tally
