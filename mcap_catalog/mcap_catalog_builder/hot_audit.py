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
