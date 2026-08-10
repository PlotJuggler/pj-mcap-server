"""Tier-2 hot-window scoped audit (event-discovery design 2026-07-30 §4).

A frequent, cheap, SCOPED audit of the prefixes where change is expected: the
registry of dimension combos known to the catalog (files ∪ parseable
``catalog_failures`` keys) × the last ``window_days`` of ``date=`` partitions.
Fail-closed by construction (§4.2):

- a prefix counts as *covered* only if its pagination COMPLETED (list_prefix
  raises rather than returning a partial result);
- the deletion sweep runs per covered prefix ONLY — global deletion is
  structurally impossible from a scoped feed — and every candidate is
  HEAD-confirmed (live/ambiguous ⇒ skip);
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

from .keyparse import parse_hive_key, rebuild_hive_key
from .reconcile import (
    ReconcileCancelled,
    _bounded_completions,
    _classify_listing,
    _composite_ids,
    _raise_if_stopped,
    _run_extract_apply,
)

logger = logging.getLogger(__name__)

# Hot prefixes are small (one robot-day each); a modest pool keeps the LIST
# phase (and the sweep's HEAD-confirm phase) snappy without the full
# sharded-LIST machinery.
_LIST_PREFIX_THREADS = 8

Combo = tuple[str, str, str, str]  # (customer, site, robot, source) names


def _combo_dims(combo: Combo, date: str, filename: str = "") -> dict[str, str]:
    """The keyparse/caches ``dims`` dict for a combo — the one shape every
    shared helper (rebuild_hive_key, _composite_ids) speaks."""
    customer, site, robot, source = combo
    return {"customer": customer, "site": site, "robot": robot,
            "source": source, "date": date, "filename": filename}


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
    the scoped sweep never has to re-parse a prefix string. The layout comes
    from ``rebuild_hive_key`` (an empty filename yields the ``date=…/``
    prefix), so keyparse stays the sole owner of the partition template.
    Dates are ISO (matching the builder's own key convention); a bucket using
    non-ISO ``date=`` values is repaired by tier 3 only (§4.3's honesty
    list)."""
    dates = [
        (today - dt.timedelta(days=n)).isoformat()
        for n in range(window_days, -1, -1)
    ]
    return [
        (rebuild_hive_key(_combo_dims(combo, d)), combo, d)
        for combo in sorted(combos)
        for d in dates
    ]


def _stored_for(conn, caches, combo: Combo, date: str) -> "dict[str, tuple[int, str]]":
    """{filename: (files.id, etag)} for one combo+date, via the UNIQUE
    composite index — O(scope) on the SQLite side (§4.2), never a full-table
    fingerprint load like full_reconcile's. Id resolution goes through
    reconcile._composite_ids so the caches' key shapes live in one place."""
    comp = _composite_ids(caches, _combo_dims(combo, date))
    if comp is None:
        return {}   # no ids cached => no rows can exist for this combo
    cid, sid, rid, srcid = comp[:4]
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
    tally: "dict[str, int] | None" = None,
) -> "dict[str, int]":
    """One tier-2 pass: scoped LIST -> catalog changes -> scoped sweep.

    Returns the tally ``{cataloged, skipped, failed, deleted,
    covered_prefixes, skipped_prefixes}``. Raises ``ReconcileCancelled`` on
    stop (a partial pass changes nothing the next pass can't redo). Raises
    ``RuntimeError`` when EVERY targeted prefix failed to LIST (zero
    coverage = the pass did nothing; the coordinator's backoff should see a
    failure, not a healthy "ok" — Codex consult 2026-08-10)."""
    # ``tally`` may be caller-owned so partial counts (covered/skipped — the
    # diagnostics) survive a raise; a fresh dict otherwise.
    if tally is None:
        tally = {}
    for k in ("cataloged", "skipped", "failed", "deleted",
              "covered_prefixes", "skipped_prefixes"):
        tally.setdefault(k, 0)
    _raise_if_stopped(stop_event)
    targets = hot_prefixes(
        registry_combos(conn),
        today if today is not None else dt.datetime.now(dt.timezone.utc).date(),
        window_days,
    )
    # Intersect with the deployment's --s3-prefix scope (merge-gate review
    # 2026-08-10): the registry can name combos/dates outside a scoped
    # deployment (e.g. a date-scoped prefix), and cataloging out-of-scope
    # keys would make the prefix-scoped full audit sweep them forever. Only a
    # target wholly inside the scope is audited; anything else is tier 3's
    # job (or nobody's, if it is genuinely out of scope).
    scope = getattr(source, "scope_prefix", "")
    if scope:
        targets = [t for t in targets if t[0].startswith(scope)]
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

    # Classify (shared rules — reconcile._classify_listing): unchanged files
    # skip with zero network, unparseable keys quarantine. The fingerprint
    # lookup is the per-prefix filename map; S3 keys are authoritative
    # (intended_key is None), so dims always match this prefix's combo/date.
    to_extract: list = []
    for _prefix, combo, date, listings in covered:
        stored = _stored_for(conn, caches, combo, date)
        for lst in listings:
            _raise_if_stopped(stop_event)
            _dims, _eff, item = _classify_listing(
                conn, source, lst,
                lambda d: (stored.get(d["filename"]) or (None, None))[1], tally)
            if item is not None:
                to_extract.append(item)
    conn.commit()  # the classify loops' record_failure upserts, one fsync

    _run_extract_apply(
        conn, caches, source, to_extract, workers=workers,
        source_spec=source_spec, tally=tally, progress=None,
        stop_event=stop_event,
    )

    # Scoped sweep, three phases so no network call ever runs inside an open
    # write transaction (the single writer's lock would make SQS events and
    # 5s-deadline tag edits queue behind per-candidate HEADs).
    #
    # Phase 1 — candidates: rows in covered prefixes absent from their
    # listing. Rows are re-read AFTER apply because apply can re-insert a
    # row under a NEW id — deleting by a pre-apply id could hit a reused
    # rowid; the re-read is an index seek per prefix, sub-ms.
    candidates: list = []  # (prefix, filename, row_id)
    for prefix, combo, date, listings in covered:
        _raise_if_stopped(stop_event)
        listed = {lst.key.rsplit("/", 1)[-1] for lst in listings}
        for filename, (row_id, _etag) in _stored_for(conn, caches, combo, date).items():
            if filename not in listed:
                candidates.append((prefix, filename, row_id))

    # Phase 2 — HEAD-guard (Codex branch review 2026-08-10), concurrent: the
    # LIST completed EARLIER, so an object (re-)uploaded since is absent from
    # the listing yet LIVE, and deleting its row would cascade tags_override
    # (user data no rebuild reconstructs). One stat() per candidate confirms
    # the 404; live or ambiguous (stat raised) ⇒ skip, fail-closed — the next
    # pass or the object's own create event self-heals. (Tier 3's full sweep
    # keeps the design's live-LIST-authority semantics unchanged; this guard
    # exists because the hot tier runs ~50x more windows per day.)
    confirmed: list = []
    if candidates:
        def _gone_row_id(cand):
            prefix, filename, row_id = cand
            try:
                return row_id if source.stat(prefix + filename) is None else None
            except Exception as e:  # noqa: BLE001 — ambiguous ⇒ no delete
                logger.warning(
                    "hot audit: HEAD-guard inconclusive for %s%s, "
                    "skipping delete: %s", prefix, filename, e,
                )
                return None

        # The shared bounded-window engine (reconcile._bounded_completions):
        # at most 2x pool-width HEADs in flight, results dropped as consumed,
        # stop polled every 0.1s inside the wait loop — SIGTERM during a
        # bulk-deletion sweep aborts within one in-flight stat, never waiting
        # out every submitted candidate.
        n = min(_LIST_PREFIX_THREADS, len(candidates))
        pool = ThreadPoolExecutor(max_workers=n)
        try:
            for row_id in _bounded_completions(
                lambda c: pool.submit(_gone_row_id, c), candidates,
                window=2 * n, stop_event=stop_event,
            ):
                if row_id is not None:
                    confirmed.append(row_id)
        finally:
            cancelled = stop_event is not None and stop_event.is_set()
            pool.shutdown(wait=not cancelled, cancel_futures=cancelled)

    # Pass-level bucket confirmation (merge-gate review 2026-08-10): a
    # transient bucket-level 404 makes EVERY stat above look like a confirmed
    # object deletion. One probe vetoes the whole batch when the bucket
    # cannot be confirmed — fail-closed; the next pass self-heals.
    if confirmed and not source.bucket_exists():
        logger.warning(
            "hot audit: bucket existence unconfirmed — skipping %d deletion(s)",
            len(confirmed),
        )
        confirmed = []

    # Phase 3 — one short transaction for the confirmed deletions.
    try:
        for row_id in confirmed:
            _raise_if_stopped(stop_event)
            conn.execute("DELETE FROM files WHERE id=?", (row_id,))
            tally["deleted"] += 1
        _raise_if_stopped(stop_event)
        conn.commit()
    except Exception:  # incl. ReconcileCancelled — never leave partial deletes
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
