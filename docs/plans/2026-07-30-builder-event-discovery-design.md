# Builder discovery at scale: event-primary + hybrid audit (design)

**Date:** 2026-07-30 · **Status:** PROPOSED (design/spec — implementation plan to follow)
**Scope:** `mcap_catalog/` builder discovery + `server/deploy/` wiring. No Go-server,
wire-protocol, or catalog-schema changes.

## 1. Problem

Production runs the builder rescan-only (`--no-watch --rescan-interval=300` in
`server/deploy/docker-compose*.yml` and `pj-cloud-builder.service`), so the **only**
discovery mechanism is `full_reconcile`: a full-bucket LIST + classify + deletion
sweep every 5 minutes. At the current bucket size (millions of objects) one
reconcile takes ~30 minutes, **25+ of which is the LIST** — by the time a reconcile
finishes, several more are due.

This is worse than an aggressive interval; it is an **unstable queue**:

- `rescan_loop` (`__main__.py`) enqueues a rescan every 300 s into an **unbounded**
  queue, regardless of whether one is already queued or running.
- The single worker needs ~30 min per rescan, so ~6 rescans queue up during each
  one. The backlog grows without bound; the builder reconciles back-to-back forever.
- Everything else sharing that queue — tag-edit IPC requests, the `stop` event from
  SIGTERM — waits behind the backlog. Shutdown can exceed the systemd kill timeout.

Cost scaling (per Codex's independent analysis, confirmed against the code): the
LIST phase is Θ(all objects under the prefix) — the ETag skip eliminates per-file
reads for unchanged files but **cannot** eliminate enumeration, because fingerprints
and deletions are learned *from* the listing. No worker-count tuning changes the
25-minute phase. The sharded LIST (PR #13) parallelizes wall-time by first-level
prefix but cannot beat a skewed shard's serial page chain, and never reduces the
request count (~N/1000 LIST calls per sweep, 288 sweeps/day today).

### Requirements (from 2026-07-30 discussion)

| Question | Answer | Consequence |
|---|---|---|
| Backend / scale | S3, in-region, millions of objects, latest builder | Sharding already deployed; LIST is inherently Θ(N) |
| Freshness for new uploads | **Minutes** (near-real-time) | Polling can never satisfy this; events are mandatory |
| Can we configure the bucket? | Yes — we control it | S3→SQS notifications are available |
| Deletion semantics | Effectively **append-only** + lifecycle expiry | Authoritative sweep may be slow (daily+); delete events exist (incl. lifecycle) |

## 2. Design overview

Three cooperating mechanisms, replacing the single treadmill:

```
                       ┌───────────────────────────────────────────────┐
   S3 bucket           │                 builder daemon                │
   notifications ──► SQS ──► s3_event_producer ──► work_q ──► worker   │  minutes
   (.mcap suffix)      │            (ack AFTER commit)        (single  │  (tier 1)
                       │                                       writer) │
   catalog-derived     │   hot-window scoped LIST ──► scoped reconcile │  ~15–60 min
   recent prefixes ────┼──────────────────────────────────────►        │  (tier 2)
                       │                                               │
   full enumeration ───┼── AuditFeed: live LIST  ──► full reconcile    │  daily+
   (substrate seam)    │           or S3 Inventory   (+ snapshot guard)│  (tier 3)
                       └───────────────────────────────────────────────┘
```

- **Tier 1 — events (freshness):** S3 → SQS → the *existing* `s3_producer.py`
  drain path. Steady-state work becomes Θ(changes), independent of bucket size.
- **Tier 2 — hot-window audit (self-healing):** a frequent, cheap, scoped LIST of
  only the recent `date=` partitions, generated from dimension combos the catalog
  already knows. Catches any missed/lost event within its cadence. Point-in-time
  truth exactly where change happens.
- **Tier 3 — full audit (authority):** today's `full_reconcile`, demoted to
  daily+, remains the sole authority for deletions and drift. Its enumeration
  source becomes a seam (`AuditFeed`): live LIST now; **S3 Inventory** later,
  made safe by a snapshot-staleness guard. This is the A/C hybrid: the two
  approaches differ only in this substrate, so both fit behind one seam.

Rationale for the hybrid split: the bucket has two regions with opposite
characteristics. The **hot region** (recent dates) is small, mutable, and
recency-critical — LIST it live and often. The **cold region** (everything else)
is immutable except lifecycle expiry, huge, and staleness-tolerant — enumerate it
rarely, eventually via Inventory where AWS does the Θ(N) work off-path. Split by
*mutability*, using the physical `date=` boundary the Hive layout already provides.

### Non-goals

- GCS event parity (Pub/Sub producer) and a GCS Storage-Insights feed: future work,
  mirrored behind the same seams; explicitly out of scope here (prod is S3).
- Fixing the O(N)-listings reconcile *memory* baseline (known follow-up): unchanged
  by this design — all tiers feed the same reconcile; streaming it is orthogonal.
- Multi-consumer fan-out (SNS/EventBridge): one consumer today → S3→SQS direct.
  If a second consumer appears, switch the bucket config to S3→SNS→SQS-per-consumer
  without touching the builder.
- `file_metrics`, `derive_tags()`: untouched.

## 3. Tier 1 — S3 event notifications (the primary discovery path)

### 3.1 Infrastructure (new, but all pre-existing AWS primitives)

- **SQS standard queue** + **DLQ** (redrive `maxReceiveCount` ≈ 5).
- **Bucket notification configuration**: events `s3:ObjectCreated:*`,
  `s3:ObjectRemoved:*`, **and `s3:LifecycleExpiration:*`** (lifecycle deletions
  emit their own event types — required by the append-only+lifecycle answer);
  filter `suffix=.mcap` plus the deployment's key prefix. Non-MCAP objects never
  generate a message (contrast: LIST enumerates them and filters client-side).
- **IAM**: builder task role gains `sqs:ReceiveMessage/DeleteMessage/GetQueueAttributes`
  on the queue; the queue policy allows `s3.amazonaws.com` to `SendMessage` from
  the bucket ARN.
- **Visibility timeout**: must cover the worst case for an already-received
  message: the backpressure queue ahead of it (§3.2(c), ~100 × sub-second
  catalogs) plus one hot audit (§3.2(d) pauses *receiving*, not messages already
  in `work_q`). Proposed 300 s. A `DeleteMessage` on an expired receipt must log
  and move on — the redelivered copy is absorbed by worker idempotency.

### 3.2 Builder changes (hardening the existing `s3_producer.py` path)

The drain loop, event→`WatchEvent` translation, and the idempotent worker already
exist and are tested. Four gaps close before we *depend* on this path
(the first three were independently flagged by Codex's review; (b) is scoped down):

- **(a) Ack after commit, not after enqueue.** Today the producer deletes the SQS
  message as soon as records are enqueued in the in-memory `work_q`; a crash
  between enqueue and DB commit silently loses the event until the next full
  audit. Change: `WatchEvent` carries the receipt handle; the **worker** deletes
  the message only after `catalog_object`/`delete_by_key` returns. The worker is
  single-threaded so this is race-free; a crash mid-processing lets the visibility
  timeout redeliver, and idempotency (ETag skip / no-op delete) absorbs the replay.
  One SQS message can carry several records: ack when the *last* record of the
  message completes (per-message pending count carried on the events).
- **(b) HEAD-guard deletes instead of sequencer ordering.** SQS is at-least-once
  and unordered; a stale `ObjectRemoved` arriving after a re-upload must not
  delete the fresh row. Rather than persisting S3 sequencer tokens per key
  (Codex's proposal — correct but heavy), exploit the bucket's semantics: on a
  delete event, `source.stat(key)` first — object still exists → **re-catalog**
  (ETag skip makes it free if unchanged); object gone → delete the row. One HEAD
  per delete event; deletes are rare in an append-only bucket. Same correctness
  where it matters, a fraction of the machinery.
- **(c) Producer backpressure.** `work_q` is unbounded; SQS is the durable buffer.
  The producer stops calling `ReceiveMessage` while `work_q` exceeds a small
  threshold (e.g. 100) — backlog then lives in SQS (visible, alarmable, durable)
  instead of process memory.
- **(d) Pause receiving during audits.** A running audit blocks the worker for
  its full duration; messages received-but-unprocessed would blow their
  visibility timeout and redeliver in a storm. The producer checks an
  "audit in progress" flag — set for **any** audit, tier 2 or tier 3 (tier-2
  pauses are seconds-to-a-minute and negligible) — and simply does not receive
  while one runs; events wait in SQS and are drained immediately after. (With
  the tier-3 cadence at daily, this suspends event freshness for one
  audit-duration per day; the §6 status surface makes that visible.)

### 3.3 Deploy changes

`docker-compose.aws.yml` / `pj-cloud-builder.service` / `Dockerfile.builder`:
drop `--no-watch`, add `--sqs-url` (new env `MCAP_CATALOG_SQS_URL`), set
`--rescan-interval` per §5's phase. `run.sh`/`smoke.sh`/local Minio are
**unchanged**: MinIO has no SQS, local buckets are tiny — rescan-only at 300 s
remains correct there, and CI keeps exercising the rescan path.

## 4. Tier 2 — hot-window scoped audit (cheap self-healing)

A new periodic pass (default ~every 30 min; configurable) that LISTs **only the
prefixes where change can occur**:

1. Derive the prefix set from the catalog: for each known
   `(customer, site, robot, source)` dimension combo, emit the Hive prefixes for
   `date ∈ [today − W, today]` (window `W` default 2 days, covering upload lag and
   timezone skew). The catalog *is* the discovery index — no enumeration needed.
2. LIST those prefixes concurrently (reusing the sharded-LIST machinery with an
   explicit shard list). Each is tiny; the whole pass is O(active fleet × W), not
   O(bucket) — seconds-to-a-minute at current scale, flat as the bucket grows.
3. Run the *existing* reconcile classify/extract over the result, with the
   deletion sweep **restricted to rows inside the scanned scope**
   (`date ∈ window` AND combo ∈ scanned set). Never touch rows outside what was
   actually listed.

Properties:

- A create event lost by any means (crash before (a) lands, notification outage,
  mis-filtered key) is repaired within one hot-audit cadence — minutes-to-an-hour,
  not "tonight". This is the insurance that lets tier 3 run daily+ with a clear
  conscience, and later lets its substrate be a stale Inventory snapshot.
- New dimension combos (first upload of a brand-new robot) are discovered by
  events (tier 1); if that event is lost, the *combo itself* is unknown to tier 2
  and repair falls to tier 3. Accepted: new-robot-onboarding coinciding with an
  event outage is rare, and the failure is bounded staleness, not corruption.
- Deletion correctness is trivial here because the scan is point-in-time truth for
  its scope — no staleness guard needed.

## 5. Tier 3 — full audit: demoted, coalesced, substrate-pluggable

### 5.1 Scheduler correctness (prerequisite, ~20 lines)

- **Coalesce:** never enqueue a rescan while one is queued or running (a flag the
  worker clears on completion). The queue can then never hold >1 audit.
- **Completion-relative scheduling:** the next audit is due `interval` after the
  previous one *finished*, not on a fixed producer clock.
- **Failure backoff:** an audit that raises reschedules with exponential backoff
  (cap: the interval) instead of riding the fixed-rate stream.
- **Responsive stop:** SIGTERM sets `stop_event`; the LIST generator and the
  reconcile apply-loop check it between pages/files and abort cleanly (partial
  audit → no `record_build` stamp, next audit redoes it). Raise systemd
  `TimeoutStopSec` to cover one in-flight file. Queued (not started) audits are
  dropped on stop.

### 5.2 Cadence

With tiers 1+2 carrying freshness, tier 3 is authority-only: **daily** (nightly
fixed hour preferred over a rolling interval, so it lands off-peak). The
append-only + lifecycle answer says deletions tolerate this easily; lifecycle
expiry is *also* covered in minutes by delete events (§3.1), so the sweep is
belt-and-braces, not the primary delete path.

### 5.3 The `AuditFeed` seam and the Inventory substrate

Generalize the audit's enumeration input to a seam (shape, not final naming):

```python
class AuditFeed(Protocol):
    def listings(self) -> Iterator[Listing]: ...   # same Listing as Source.list_all
    def snapshot_ns(self) -> int: ...              # enumeration validity time
```

- **`LiveListFeed`** — wraps `Source.list_all()`; `snapshot_ns = now` (LIST is
  strongly consistent). This is today's behavior, unchanged. Ships first.
- **`InventoryFeed`** — reads the latest S3 Inventory report (daily; CSV.gz
  chosen over Parquet to avoid a `pyarrow` dependency; fields: key, size, ETag,
  last-modified — all already in `Listing`): discover the newest `manifest.json`
  under the inventory prefix, verify its checksum, stream the data files.
  `snapshot_ns` = the manifest's creation timestamp. Enumeration cost moves
  off-path to AWS; the builder parses local-speed files in seconds even at 100 M
  objects.

**Snapshot-staleness guard** (what makes a stale feed safe, and the one reconcile
change): the deletion sweep may only delete a row that is absent from the feed
**and** provably older than the snapshot:

```sql
absent from feed
AND last_modified_ns  < snapshot_ns − margin
AND cataloged_at_ns   < snapshot_ns − margin      -- margin ≈ 1 h for clock skew
```

Both columns already exist in `files` (schema v3) — **no schema change, no
CATALOG_CONTRACT bump**. With `LiveListFeed` the guard is a no-op
(`snapshot_ns = now`), so shipping the guard early is safe and gets it tested
before Inventory ever runs. Rows created after the snapshot (via events / hot
audit) are structurally protected; a *worst case* under Inventory is a stale row
surviving up to ~2 days past its object's deletion — acceptable per §1's table,
and delete events cover the common path in minutes anyway.

**Trigger for enabling `InventoryFeed`** (pre-decided so it's an ops toggle, not a
design debate later): the nightly live-LIST audit exceeding ~2 h wall-time, or
measurably delaying event servicing (§3.2(d) pause) — roughly tens of millions of
objects at current growth. Until then, `LiveListFeed` nightly is pennies and
30 min of background work. Enabling is config: inventory configuration on the
bucket (48 h until the first report — enable it *ahead* of need), destination
bucket + policy, `--audit-feed=inventory`.

## 6. Observability

- **Sidecar (`<db>.status.json`):** add per-tier fields — `last_event_at`,
  `events_applied`, `hot_audit_last/duration`, `full_audit_last/duration/substrate`,
  `feed_snapshot_age`. Additive fields → update `CATALOG_CONTRACT.md` §12 (both
  byte-identical copies) if the contract enumerates sidecar keys.
- **CloudWatch alarms (deploy docs):** SQS `ApproximateAgeOfOldestMessage`
  (event path stuck), DLQ depth > 0 (poison messages), and — once Inventory is
  on — report age > 2 days (audit silently starved).
- **Per-shard LIST timings** logged during the nightly audit (Codex's suggestion,
  adopted): if one customer prefix dominates wall-time, that's the signal for
  deeper sharding or the Inventory trigger — measured, not guessed.

## 7. Failure-mode review

| Failure | Detected by | Repaired by | Worst staleness |
|---|---|---|---|
| Create event lost (crash pre-(a), outage) | hot-audit diff | tier 2 | ~1 hot-audit cadence |
| Delete event lost | full-audit sweep | tier 3 | ~1 day (live) / ~2 days (inventory) |
| Stale delete after re-upload | HEAD-guard §3.2(b) | re-catalog instead of delete | none (guarded) |
| SQS poison message | DLQ redrive | operator + DLQ alarm | n/a (others unaffected) |
| Duplicate/replayed event | — | worker idempotency (ETag skip / no-op) | none |
| New-robot event lost | tier 3 only (§4 note) | full audit | ~1 day |
| Inventory report late/missing | report-age alarm | ops; tiers 1–2 unaffected | audit paused, freshness intact |
| Builder crash mid-audit | no `record_build` stamp | next scheduled audit | one cadence |
| Audit blocks event servicing | §3.2(d) pause + status surface | bounded by audit duration | one audit duration/day |

## 8. Rollout phases

| Phase | Content | Type | Freshness after |
|---|---|---|---|
| **0** | Bump prod `--rescan-interval` 300 → 21600 (6 h) | config-only, today | hours (stopgap) |
| **1** | §5.1 scheduler correctness (coalesce, completion-relative, backoff, stop) | small code | hours, but stable + responsive |
| **2** | §3 SQS tier: infra + producer hardening (a)–(d); deploy flips to `--sqs-url`, audit → nightly | infra + code | **minutes** |
| **3** | §4 hot-window scoped audit | code | minutes, self-healing |
| **4** | §5.3 `AuditFeed` seam + snapshot guard (guard ships here; `InventoryFeed` behind it) | code | unchanged; audit scalable |
| **5** | Enable `InventoryFeed` in prod when the §5.3 trigger fires | ops toggle | unchanged; audit O(minutes) at any scale |

Phases 0–2 resolve the incident. 3–4 are small, independently-shippable
insurance/scalability layers; 5 is a pre-planned toggle, not a project.

## 9. Testing

- **Unit (injected fakes — `s3_producer`/`S3Source` already take injected clients):**
  ack-after-commit (message deleted only post-commit; crash-before-commit leaves it),
  multi-record ack accounting, HEAD-guard (delete event + live object → re-catalog),
  producer backpressure + audit-pause, scheduler coalescing/backoff/stop,
  hot-window prefix derivation + scoped-sweep containment (never deletes outside
  scope), snapshot guard (row newer than snapshot survives an absent-from-feed
  sweep), `InventoryFeed` manifest discovery/checksum/parse.
- **CI:** existing `--once`/rescan legs unchanged (smoke/Minio stays rescan-only,
  which keeps the tier-3 path exercised). New unit suites run in the standard
  builder-tests job; no emulator needed thanks to client injection.
- **Live validation:** staging bucket (`run.sh --aws` shape) with a real queue —
  upload/delete/re-upload sequences, kill -9 between enqueue and commit
  (redelivery proof), event-outage drill (disable notifications, verify tier-2
  repair), and a nightly-audit run with per-shard timings captured.

## 10. Decision record (A vs C, resolved as hybrid)

The 2026-07-30 comparison (Claude + independent Codex investigation, converging):
polling-only can never meet minutes-freshness at Θ(bucket) LIST cost; events-only
has no authority for drift/deletions. "A" (events + nightly live LIST) and "C"
(events + Inventory audit) differ **only in the audit's enumeration substrate** —
so they are not competing architectures but two settings of §5.3's seam, and the
hot/cold split (§4) takes the best of each: live point-in-time listing where
change happens, off-path bulk enumeration where nothing changes. Sequencer-based
event ordering was considered (Codex) and replaced by the HEAD-guard, justified
by append-only semantics. SNS/EventBridge rejected for a single consumer.

## 11. Open questions

1. Nightly fixed-hour audit vs rolling 24 h interval — proposed: fixed hour (UTC,
   configurable), lands off-peak and makes per-shard timing comparisons meaningful.
2. Hot-window `W` and cadence defaults (proposed: 2 days / 30 min) — tune against
   real upload-lag distribution once §6 telemetry exists.
3. Whether the `--sqs-url` deploy should keep a long rescan interval as tier 3
   (proposed: yes, nightly) or move fully to on-demand audits triggered via the
   status/ops surface. Proposed: keep the timer; on-demand is a later nicety.
