# Builder discovery at scale: event-primary + hybrid audit (design)

**Date:** 2026-07-30 · **Status:** v2 — Phases 0–2 IMPLEMENTED (branch
`builder-event-discovery`, 2026-07-30); Phases 3–6 are ops toggles per
`server/deploy/README.md`, Phase 7 gated on §5.4's streaming prerequisite
**Scope:** `mcap_catalog/` builder discovery + `server/deploy/` wiring. No Go-server,
wire-protocol, or catalog-schema changes.
**Revision:** v2 incorporates the 2026-07-30 Codex adversarial review (19 findings —
all accepted; two with adjusted framing, see §10). Headline changes vs v1: Inventory
demoted to an *untrusted candidate feed* with HEAD-confirmed deletions; ack-after-commit
became a batch handshake (worker stays backend-agnostic); scoped sweeps fail closed
per-prefix; hot audits never stamp `build_metadata`; startup no longer blocks event/tag
service behind a full reconcile; rollout reordered for burn-in.

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
- Everything else sharing that queue starves: **tag edits (5 s deadline,
  `tag_ipc.py`) are effectively broken in prod today**, and the `stop` event from
  SIGTERM waits behind the backlog, exceeding the systemd kill timeout.

Cost scaling (Codex's independent analysis, confirmed against the code): the LIST
phase is Θ(all objects under the prefix) — the ETag skip eliminates per-file reads
for unchanged files but **cannot** eliminate enumeration, because fingerprints and
deletions are learned *from* the listing. No worker-count tuning changes the
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

### Deletion is the highest-stakes operation

`tags_override` (user-authored edits, never touched by the builder) is
`ON DELETE CASCADE` off `files` (`schema.sql`), and quarantine
(`_quarantine_existing`, `builder.py`) *deliberately* deletes the existing row.
A **false** deletion or a **false** quarantine therefore destroys data no rebuild
can reconstruct. Every deletion path in this design fails closed: when presence
is ambiguous, do nothing.

## 2. Design overview

Three cooperating mechanisms, replacing the single treadmill:

```
                       ┌───────────────────────────────────────────────┐
   S3 bucket           │                 builder daemon                │
   notifications ──► SQS ──► supervised producer ──► work_q ──► worker │  minutes
   (.mcap suffix,      │      (batch ack handshake:          (single   │  (tier 1)
    incl. lifecycle)   │       ack only after commit)         writer)  │
                       │                                               │
   registry-derived    │   hot-window scoped LIST ──► SCOPED reconcile │  ~30 min
   recent prefixes ────┼──────────────(fail-closed per prefix)─►       │  (tier 2)
                       │                                               │
   full enumeration ───┼── AuditFeed: live LIST ───► full reconcile    │  daily
   (substrate seam)    │      or S3 Inventory        (stale feeds:     │  (tier 3)
                       │      (candidates only)       HEAD-confirmed   │
                       │                              deletions)       │
                       └───────────────────────────────────────────────┘
```

- **Tier 1 — events (freshness):** S3 → SQS → a hardened version of the existing
  `s3_producer.py` drain path. Steady-state work becomes Θ(changes), independent
  of bucket size.
- **Tier 2 — hot-window audit (self-healing):** a frequent, cheap, scoped LIST of
  recent `date=` partitions over a prefix registry derived from the catalog
  (including quarantined keys). Repairs most missed events within its cadence
  (coverage limits stated honestly in §4.3). Point-in-time truth where change
  happens.
- **Tier 3 — full audit (authority):** today's `full_reconcile`, demoted to
  nightly, the sole stamp-writer of `build_metadata` and the broadest repair
  path. Its enumeration source is a seam (`AuditFeed`): live LIST now;
  **S3 Inventory** later — treated as an untrusted, stale *candidate* feed whose
  deletions are individually HEAD-confirmed (§5.4).

Rationale for the hybrid split: the bucket has two regions with opposite
characteristics. The **hot region** (recent dates) is small, mutable, and
recency-critical — LIST it live and often. The **cold region** (everything else)
is immutable except lifecycle expiry, huge, and staleness-tolerant — enumerate it
rarely, eventually via Inventory where AWS does the Θ(N) work off-path. Split by
*mutability*, using the physical `date=` boundary the Hive layout already provides.

### Non-goals

- GCS event parity (Pub/Sub producer) and a GCS Storage-Insights feed: future
  work, mirrored behind the same seams; out of scope here (prod is S3).
- Moving audit enumeration **off the writer thread** (scan-epoch/watermark design):
  the eventual fix for §5.2's maintenance window; recorded as follow-on work, not
  in this design's scope. One DB writer remains an invariant either way.
- The O(N)-listings reconcile *memory* baseline: unchanged by tiers 1–2, but it
  **gates Phase 7** (Inventory) — see §5.4 prerequisite. The streaming-reconcile
  rework is specified only as a prerequisite bullet, not designed here.
- Multi-consumer fan-out (SNS/EventBridge): one consumer today → S3→SQS direct.
  A second consumer later = bucket-config change to S3→SNS→SQS-per-consumer.
- `file_metrics`, `derive_tags()`: untouched.

## 3. Tier 1 — S3 event notifications (the primary discovery path)

### 3.1 Infrastructure

- **SQS standard queue** + **DLQ** (redrive `maxReceiveCount` ≈ 5). Message
  **retention 4+ days** on both — must cover an initial build plus a prolonged
  builder outage without losing the durable backlog.
- **Bucket notification configuration**: events `s3:ObjectCreated:*`,
  `s3:ObjectRemoved:*`, and `s3:LifecycleExpiration:*`; filter `suffix=.mcap`
  plus the deployment's key prefix. Non-MCAP objects never generate a message.
- **IAM**: builder role gains `sqs:ReceiveMessage/DeleteMessage/
  ChangeMessageVisibility/GetQueueAttributes` on the queue; queue policy allows
  `s3.amazonaws.com` `SendMessage` from the bucket ARN.
- **Visibility timeout**: 300 s, covering the backpressure buffer ahead of a
  received message plus one hot audit. A `DeleteMessage` on an expired receipt
  logs and moves on (the redelivered copy is absorbed by worker idempotency).

### 3.2 The event translator (`_enqueue_records`)

- **Lifecycle events are first-class** (v1 gap, Codex finding 3: today's
  translator matches only `ObjectCreated*`/`ObjectRemoved*` prefixes, so a
  lifecycle expiry would be silently dropped *and acked*). The translator
  classifies `LifecycleExpiration:Delete` and
  `LifecycleExpiration:DeleteMarkerCreated` as delete events, and is tested
  against captured real AWS payloads for all three families — including the
  versioned-bucket case where a noncurrent version expires while a live current
  version remains (the HEAD-guard then re-catalogs instead of deleting).
- Unknown-but-parseable event names are logged and counted (sidecar), never
  silently acked-and-dropped.

### 3.3 The ack protocol — batch handshake, worker stays backend-agnostic

v1 proposed carrying SQS receipt handles on `WatchEvent`s and having the worker
call `DeleteMessage`. Rejected (Codex finding 5): it puts backend network I/O on
the single DB-writer thread and leaves multi-record messages underspecified.
Instead, an **ackable batch context**:

1. The producer parses and validates the **complete** SQS message body *before*
   enqueueing anything. Malformed body → not acked → redriven to the DLQ.
   Record-less bodies (e.g. `s3:TestEvent`) → acked immediately.
2. All records translated from one message share one batch state (message id,
   receipt handle, pending-record count).
3. The worker marks a record **terminal** only when its DB outcome has
   committed. Terminal outcomes: cataloged, ETag-skipped, **committed
   quarantine** (a deliberate DB outcome, not a failure to process),
   confirmed delete, safe no-op. A *raised* exception (network, DB) is
   **not** terminal — the message stays unacked and redelivers.
4. When a batch's last record goes terminal, the worker pushes the batch onto an
   **ack queue**; the **producer thread** performs `DeleteMessage`. All SQS I/O
   stays on the producer side; the worker touches only in-process state.

Crash anywhere before step 4 ⇒ redelivery; idempotency (ETag skip / no-op
delete / re-quarantine) absorbs the replay.

### 3.4 Delete events: the HEAD-guard

SQS is at-least-once and unordered; a stale `ObjectRemoved` arriving after a
re-upload must not delete the fresh row. Rather than persisting S3 sequencer
tokens per key (considered; heavy), exploit the bucket's semantics: on a delete
event, `source.stat(key)` first —

- object still exists → **re-catalog using that `Stat`** (passed through, no
  second HEAD; ETag skip makes it free if unchanged);
- object gone (404) → delete the row.

This is *current-state-at-HEAD with eventual repair*, not an absolute guard: a
re-upload landing between the HEAD and the DB delete is transiently removed
until its own create event (or the hot audit) restores it — bounded staleness of
~one event cadence, never corruption. One HEAD per delete event; deletes are
rare in an append-only bucket.

### 3.5 Producer robustness

- **Supervision** (v1 gap, Codex finding 14: today an exception in
  `receive_message`/`delete_message` kills the thread silently while the daemon
  stays "healthy"): the drain loop wraps its body in bounded-backoff retry; a
  supervisor restarts a dead producer thread; the sidecar carries
  `last_poll_ok_at` / `last_ack_at` so a dead producer is distinguishable from a
  quiet bucket (`last_event_at` alone cannot tell them apart).
- **Backpressure**: bounded by **records buffered**, not `qsize()` (one message
  can carry many records). Above the bound the producer stops calling
  `ReceiveMessage`; backlog lives in SQS — durable, visible, alarmable.
- **Intake pause around audits** (handshake, not just a flag): before an audit
  starts, the coordinator (§5.3) signals pause; the producer stops receiving;
  the audit waits until every already-received batch is terminal **and acked**;
  only then does the audit run. The pause clears in a `finally` on every exit
  path. Alternative if drain latency ever hurts: `ChangeMessageVisibility`
  heartbeats on in-flight messages (IAM already granted). Tier-2 pauses are
  seconds-to-a-minute; tier-3 pauses are the §5.2 maintenance window.

### 3.6 Deploy changes

`docker-compose.aws.yml` / `pj-cloud-builder.service` / `Dockerfile.builder`:
drop `--no-watch`, add `--sqs-url` (env `MCAP_CATALOG_SQS_URL`), audit cadence
per §8's phase. `run.sh`/`smoke.sh`/local Minio are **unchanged**: MinIO has no
SQS, local buckets are tiny — rescan-only at 300 s remains correct there, and CI
keeps exercising the rescan path.

## 4. Tier 2 — hot-window scoped audit (cheap self-healing)

### 4.1 Prefix registry and scan

A periodic pass (default ~every 30 min) LISTs **only prefixes where change is
expected**:

1. Derive the prefix set from the catalog itself: dimension combos present in
   `files` **∪ parseable keys in `catalog_failures`** (a quarantined file must
   not exile its combo from the registry — quarantine deletes the `files` row).
   For each combo, emit Hive prefixes for `date ∈ [today − W, today]`
   (`W` default 2 days).
2. LIST those prefixes concurrently (the sharded-LIST machinery with an explicit
   shard list). O(active fleet × W), flat as the bucket grows.
3. Run reconcile classify/extract over the result through a **scoped reconcile
   entry point** (§4.2).

### 4.2 Scoped reconcile: a distinct, fail-closed entry point

Not an optional filter on `full_reconcile` (a partial listing reaching the
global sweep would turn enumeration failure into mass deletion). A separate
entry with a **mandatory scope**:

- Per-prefix completion tracking: a prefix counts as *covered* only if its
  pagination **completed without error**. Failed/cancelled prefixes are excluded.
- The deletion sweep considers only rows whose exact
  `(customer, site, robot, source, date)` lies in a *covered* prefix. Ambiguous
  coverage ⇒ **no deletions at all** for that pass. Global deletion is
  structurally impossible from a scoped feed.
- DB work is scoped too (v1 said O(scope) but reused O(catalog) internals —
  Codex finding 10): stored-fingerprint lookup and sweep candidates go through
  composite-indexed predicates / a temp scope table, so a hot pass is O(scope)
  on both the S3 and SQLite sides.
- A hot audit **never calls `record_build`** (finding 11: `build_metadata` is
  whole-catalog freshness surfaced by the Go server; a subset scan stamping it
  would lie). Hot-audit status goes to the sidecar (§6) only.

### 4.3 Coverage — stated honestly

The hot audit repairs a lost **create** event within one cadence *iff* the key's
combo is in the registry and its `date` is within `W`. It does **not** cover:

- the first-ever upload of a brand-new combo whose create event was lost
  (combo unknown to the registry) — repaired by tier 3;
- backfills with `date` older than `W` — repaired by tier 3;
- a combo whose last row aged out via lifecycle *and* whose `catalog_failures`
  entries were cleaned — repaired by tier 3.

These are bounded-staleness gaps (≤ one tier-3 cadence), acceptable because they
require a lost event *and* an unusual upload shape coinciding. Upload lag should
be monitored (§6) to validate `W`.

## 5. Tier 3 — full audit: demoted, coalesced, substrate-pluggable

### 5.1 Startup must not block service (v1 gap, Codex finding 7)

Today `_locked_main` runs a full synchronous reconcile **before** starting the
producer and the tag socket — under this design every restart would impose a
~30-minute event/tag-edit outage. Change: when a served DB already exists and
opens cleanly, start the worker, SQS producer, and tag IPC **immediately**, and
*schedule* the startup audit through the coordinator like any other. The
blocking build-then-publish path remains only for a missing DB or explicit
`--rebuild` (SQS retention covers the gap: events accumulate durably while the
initial build runs).

### 5.2 Scheduling contract

- **One audit arbiter.** At most one audit (hot *or* full) runs at a time; a due
  full audit supersedes/skips hot audits; a hot audit never delays a due full
  audit by more than its own runtime.
- **Full audit: fixed nightly hour (UTC, configurable), skip-missed.** Fixed-hour
  wins over completion-relative for the full tier (off-peak placement,
  comparable per-shard timings). **Hot audit: completion-relative** interval.
- **Result-bearing audits.** The audit item carries its outcome back to the
  coordinator (the worker's blanket exception suppression cannot be the signal);
  failures reschedule with exponential backoff (capped), success schedules
  normally. All state transitions clear in `try/finally`.
- **Responsive stop.** `stop_event` is checked between LIST pages, at extraction
  submission (pending futures cancelled), between apply steps, and inside retry
  backoffs. A partial audit stamps nothing and the next scheduled audit redoes
  it. Cancellation is tested against the real systemd `TimeoutStopSec` during
  each phase: LIST, extraction, DB apply, sweep.
- **Declared maintenance window.** While a full audit runs, event intake is
  paused (§3.5) and tag edits expire (5 s deadline vs a busy worker): this is an
  explicit, sidecar-visible **nightly maintenance window** (~30 min today),
  scheduled off-peak. This is the shipped trade-off; the off-writer-thread audit
  (non-goal, §2) is the recorded path to eliminating it. (Today's baseline is a
  *continuous* outage, so a bounded nightly window is strictly better.)

### 5.3 Cadence

With tiers 1+2 carrying freshness, tier 3 is authority-only: **nightly**, but
only *after* the event tier has burned in (§8 ordering — the audit cadence is
the safety net and is loosened last). The append-only + lifecycle answer says
deletions tolerate daily easily; lifecycle expiry is also covered in minutes by
delete events (§3.2).

Tier 3 additionally owns **`catalog_failures` hygiene** (Codex finding 18: the
current sweep only examines `files`, so failure rows for vanished objects live
forever): the full audit removes failure rows whose keys are absent from a
*complete* enumeration, under the same fail-closed rules as row deletion.

### 5.4 The `AuditFeed` seam and the Inventory substrate

```python
class AuditFeed(Protocol):
    def listings(self) -> Iterator[Listing]: ...  # same Listing as Source.list_all
    def snapshot_ns(self) -> int: ...             # enumeration validity time
    def trust(self) -> FeedTrust: ...             # LIVE | STALE_CANDIDATE
```

- **`LiveListFeed`** (ships first, default): wraps `Source.list_all()`;
  `trust=LIVE`. **Semantics identical to today** — a live LIST is
  strongly consistent, so absence from it authorizes deletion directly. (v1's
  single guard-SQL applied to live feeds would have *blocked* legitimate
  deletions of recent rows; trust is a per-feed policy, not one WHERE clause.)
- **`InventoryFeed`** (`trust=STALE_CANDIDATE`): reads the latest S3 Inventory
  report (daily; CSV chosen to avoid a `pyarrow` dependency). S3 Inventory is
  **eventually consistent and may omit recent objects — it is a candidate list,
  never live object metadata** (Codex findings 1–2, the decisive v1→v2 change).

**Rules for a `STALE_CANDIDATE` feed:**

1. **Never extract from feed stats.** Row exists and feed ETag matches → mark
   present, done. Row missing *or* ETag differs → **HEAD the live object** and
   extract using that live `Stat` — a stale size/ETag must never drive
   range-GETs or be written to the catalog (v1's silent corruption path:
   stale stat → misread → *quarantine deletes the healthy row*).
2. **Ignore feed entries older than the row.** If the row's
   `last_modified_ns`/`cataloged_at_ns` postdate `snapshot_ns`, the feed entry
   is obsolete — no action.
3. **Deletion candidates are HEAD-confirmed.** Absent-from-feed selects
   *candidates*, filtered by `last_modified_ns < snapshot_ns − margin AND
   cataloged_at_ns < snapshot_ns − margin` (margin ~1 h) — timestamps only
   **prune** the candidate set; the delete itself requires a **confirmed 404**
   per key. Cheap in practice: append-only means candidates ≈ actual deletions.
4. **Fail-closed manifest validation.** The audit runs only if the manifest's
   source bucket, inventory configuration ID, prefix scope, and version mode
   exactly match the builder's scope; manifest checksum and per-data-file
   checksums verify; CSV keys are URL-decoded; column positions come from
   `fileSchema`, never assumed. Versioned buckets: current versions only
   (`IsLatest`, no `IsDeleteMarker`) or the report is rejected.
5. **No Inventory cold rebuilds.** `--rebuild`/first-build always uses
   `LiveListFeed` (a from-scratch catalog built from a stale snapshot has no
   newer-row protection at all).
6. **Prerequisite: streaming reconcile.** The current reconcile holds all
   listings + fingerprints + presence in Python memory (Θ(M+C) — the known
   ~2 GB baseline). Inventory targets scales where that is not viable, so
   enabling `InventoryFeed` is **gated on** the streaming rework (feed rows
   staged into a temp indexed SQLite table; presence/diff computed in SQL).
   Until then Phase 7 does not exist operationally. Scale claims are to be
   *measured* at burn-in, not asserted.

**Trigger for enabling `InventoryFeed`**: the nightly live-LIST audit exceeding
~2 h wall-time or measurably delaying event servicing — roughly tens of millions
of objects at current growth. Enable the bucket's inventory configuration well
ahead of need (first report takes up to 48 h).

## 6. Observability

- **Sidecar (`<db>.status.json`)** — additive fields per tier:
  `producer_last_poll_ok_at`, `producer_last_ack_at`, `events_applied`,
  `events_unknown_name`, `hot_audit_{last,duration,covered_prefixes,skipped}`,
  `full_audit_{last,duration,substrate}`, `feed_snapshot_age`,
  `maintenance_window_active`, `tag_edit_{expired,failed}` counters. Update
  `CATALOG_CONTRACT.md` §12 (both byte-identical copies) if the contract
  enumerates sidecar keys.
- **CloudWatch alarms (deploy docs):** SQS `ApproximateAgeOfOldestMessage`;
  DLQ depth > 0; (Phase 7) inventory report age > 2 days.
- **Per-shard LIST timings** logged during the nightly audit: if one customer
  prefix dominates wall-time, that's the measured signal for deeper sharding or
  the Inventory trigger.
- **Upload-lag distribution** (object `LastModified` vs event arrival) to
  validate the tier-2 window `W`.

## 7. Failure-mode review

| Failure | Detected by | Repaired by | Worst staleness / blast radius |
|---|---|---|---|
| Create event lost (registry-known combo, date ≤ W) | hot-audit diff | tier 2 | ~1 hot cadence |
| Create event lost (new combo / backfill > W) | full-audit diff | tier 3 | ~1 day |
| Delete event lost | full-audit sweep | tier 3 | ~1 day (live) / bounded by HEAD-confirm (inventory) |
| Stale delete event after re-upload | HEAD-guard §3.4 | re-catalog with live Stat | none |
| HEAD-miss → re-upload → row deleted race | create event / hot audit | re-catalog | ~1 event cadence (transient absence, not corruption) |
| Lifecycle event with unsupported name | `events_unknown_name` counter | translator fix; tier 3 meanwhile | ~1 day, visible |
| Duplicate/replayed/out-of-order event | — | worker idempotency | none |
| Crash between receive and commit | unacked message | SQS redelivery (§3.3) | ~visibility timeout |
| Partial multi-record batch + crash | batch not terminal ⇒ unacked | redelivery; terminal records re-skip | ~visibility timeout |
| `DeleteMessage` fails / receipt expired | log + counter | redelivery absorbed by idempotency | none |
| SQS poison message | DLQ redrive | operator + DLQ alarm | n/a (others unaffected) |
| Producer thread dies / IAM breaks | `producer_last_poll_ok_at` stale | supervisor restart + alarm | bounded by alarm latency |
| Event already received when audit starts | drain-before-audit handshake §3.5 | acked before audit runs | none |
| Audit flag stuck after exception | `try/finally` clears; coordinator owns state | — | none |
| Partial / failed hot-prefix pagination | per-prefix completion tracking | sweep skips uncovered prefixes | no deletions from partial data |
| Full audit blocks events + tag edits | `maintenance_window_active` + tag counters | bounded nightly window §5.2 | ~30 min/night, off-peak, visible |
| Builder restart | §5.1 non-blocking startup | audit scheduled, service immediate | no outage (was: ~30 min) |
| SQS retention exhausted (long outage) | queue-age alarm long before | tier 3 | ~1 day |
| Inventory report late/missing/mis-scoped | §5.4 rule 4 fail-closed + report-age alarm | audit skipped; tiers 1–2 unaffected | audit paused, freshness intact |
| Stale inventory row vs newer catalog row | §5.4 rules 1–2 | ignored / live HEAD | none |
| False deletion cascading `tags_override` | — | **made structurally impossible**: live-LIST authority, per-prefix coverage, or HEAD-confirmed 404 | none (fail-closed) |
| Builder crash mid-audit | no `record_build` stamp | next scheduled audit | one cadence |

### 7.1 Reader-side fallback during the deletion-staleness window (verified 2026-07-30)

The design tolerates a window where the catalog lists an object that no longer
exists (minutes via delete events; up to ~one tier-3 cadence if a delete event
is lost). The Go server's behavior in that window was verified against the code
— it fails safe on every surface:

- **Browse** (`ListFiles`/`GetVocabulary`): pure catalog reads, no object
  access — the stale row is listed. Staleness is visible, never harmful.
- **Session open** (`OpenFresh`): plan-build loads chunk indexes from the
  object; a vanished object surfaces `NoSuchKey` → `storage.ErrPermanent`
  (`storage/s3.go classify`) → a **request-scoped**
  `ERROR_S3_UNAVAILABLE` with a diagnosable message
  (`ws/handlers_session.go` plan-build error path). The connection and other
  sessions are unaffected. The chunk-index cache stores only successful loads,
  keyed by `(key|etag)` — a 404 is never negatively cached.
- **Mid-stream**: every chunk read is `GetRangeVersioned`
  (`session/codec_io.go`) — pinned to the catalog's ETag/generation. Deleted
  object → permanent error → session `Error` frame ("stream failed" + detail);
  **replaced** object → `PreconditionFailed` → clean "object changed
  mid-session" failure. The pin makes serving mixed or wrong bytes under stale
  catalog metadata **structurally impossible** — the worst outcome of the
  staleness window is an explicit failed request, never silent corruption.
- **Client**: the plugin opens a fresh `BackendConnection` per download and
  fails that download on `Error`; `RESUME_NOT_POSSIBLE` fails verbatim with the
  partial kept (both pinned decisions).

Optional refinement (nicety, not safety): plan-build currently reports a
vanished object as `ERROR_S3_UNAVAILABLE`, indistinguishable from a bucket
outage. Mapping the `ErrPermanent`/`NoSuchKey` case to `ERROR_NOT_FOUND` would
let the UI say "recording was deleted — refresh the list" instead of implying a
retryable outage. Candidate for the implementation plan's server-side touch.

## 8. Rollout phases (reordered per review: burn in events *before* loosening the safety net)

| Phase | Content | Type |
|---|---|---|
| **0** | Bump prod `--rescan-interval` 300 → 21600 (6 h) | config-only, today |
| **1** | Scheduler correctness: coalesce/arbiter, completion-relative, backoff, responsive stop, non-blocking startup (§5.1–5.2) | code |
| **2** | Queue/DLQ/IAM created; retention + redrive configured. Ship lifecycle-aware, ack-hardened, supervised producer code **while prod still runs `--no-watch`** | infra + code |
| **3** | Enable bucket notifications (queue accumulates durably, consumer still off) | infra toggle |
| **4** | Enable the consumer (`--sqs-url`); **keep 6 h audits**. Burn-in: producer/ack/DLQ metrics, failure drills (kill −9 mid-batch, notification outage, IAM break) | ops + drills |
| **5** | Hot-window scoped audit (§4) deployed and observed | code |
| **6** | Full audit → nightly fixed-hour (§5.3): the safety net loosens **last**, only after tiers 1–2 are proven | config |
| **7** | Streaming reconcile ⇒ then `AuditFeed`/`InventoryFeed` (§5.4), enabled on trigger | code, gated |

Phases 0–4 resolve the incident with the safety net intact throughout; 5–6
deliver the steady state; 7 is the pre-planned scale escape hatch.

## 9. Testing

- **Unit (injected fakes — producer/`S3Source` already take injected clients):**
  translator: real captured payloads for `ObjectCreated:*`, `ObjectRemoved:*`,
  `LifecycleExpiration:{Delete,DeleteMarkerCreated}`, `s3:TestEvent`, malformed
  bodies, unknown names; batch ack: multi-record accounting, terminal-outcome
  matrix (incl. committed-quarantine ⇒ ack, raised exception ⇒ no ack), crash
  points around commit/ack, expired-receipt tolerance; HEAD-guard: live object ⇒
  re-catalog with passed Stat (no second HEAD), 404 ⇒ delete,
  HEAD/delete/re-upload race; producer: supervision restart, backpressure by
  record count, pause handshake drains before audit, `finally` clears;
  scheduler: coalescing, arbiter mutual-exclusion, skip-missed, backoff,
  stop during each audit phase; scoped audit: registry derivation (incl.
  `catalog_failures` combos), page-2 failure, one-failed-prefix-among-many,
  cancellation, empty successful scope, overlapping scopes — each proving **no
  deletion outside covered prefixes**; no `record_build` from hot audits;
  `catalog_failures` hygiene; feed rules: stale-ETag ⇒ live HEAD (never feed
  stats), older-than-row entries ignored, deletion candidates HEAD-confirmed,
  manifest validation rejections.
- **CI:** existing `--once`/rescan legs unchanged (smoke/Minio stays
  rescan-only, keeping tier 3 exercised). New suites run in the standard
  builder-tests job; no emulator needed thanks to client injection.
- **Live validation (staging, `run.sh --aws` shape + real queue):**
  upload/delete/re-upload sequences; kill −9 between receive and commit
  (redelivery proof); notification-outage drill (verify tier-2 repair);
  restart-under-load (verify non-blocking startup); a nightly audit with
  per-shard timings and maintenance-window telemetry captured.

## 10. Decision record

2026-07-30, three rounds: (1) independent Claude + Codex investigations
converged on the diagnosis (unstable queue; Θ(N) LIST cannot meet minutes
freshness). (2) "A" (events + nightly live LIST) vs "C" (events + Inventory)
resolved as a **hybrid** — they differ only in the audit's enumeration
substrate, one seam with two trust levels, plus the hot/cold scoped tier
exploiting Hive `date=` mutability boundaries. (3) Codex adversarial review of
v1 (19 findings, all verified against code and accepted) drove v2's major
corrections: Inventory demoted from "trusted with a snapshot guard" to
**untrusted candidate feed with HEAD-confirmed deletions** (the snapshot SQL
proves a row is old — it cannot prove an object is absent, and a false delete
cascades `tags_override` user data); ack redesigned as a producer-side batch
handshake; scoped sweeps fail closed per-prefix; hot audits barred from
`build_metadata`; startup made non-blocking; producer supervised; rollout
reordered for burn-in. Framing adjustments kept after verification:
sequencer-based ordering stays **rejected** in favor of the HEAD-guard
(documented as eventual-repair, not absolute); the audit maintenance window is
an explicit shipped SLA with the off-writer-thread audit as recorded follow-on.
SNS/EventBridge rejected for a single consumer.

## 11. Open questions

1. Nightly audit hour + tier-2 defaults (`W`=2 d, cadence 30 min) — tune against
   §6 telemetry (upload-lag distribution) once live.
2. Captured real AWS lifecycle payloads needed for §9's translator tests —
   collect from the staging bucket during Phase 4 burn-in.
3. Whether the off-writer-thread audit (scan-epoch/watermark) is worth designing
   immediately after Phase 6, or only if the maintenance window proves painful.
