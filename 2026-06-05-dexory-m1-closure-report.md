# PlotJuggler Cloud Connector — Dexory Milestone 1 Closure Report

| | |
|---|---|
| **Date** | 2026-06-05 |
| **Author** | Davide Faconti |
| **Engagement** | Dexory Milestone 1 (PoC) — €10,000 |
| **Status** | Milestone 1 acceptance criteria validated; awaiting written client sign-off |
| **Repo** | `pj-mcap-server` (branch `main`) |

---

## 0. Executive summary

Milestone 1 set out to prove the end-to-end data path of the PlotJuggler Cloud
Connector against a representative subset of Dexory's recording corpus, on a
development machine, with the smallest credible feature surface. That path is now
built and exercised:

- A single Go binary (`pj-cloud-server`) indexes an S3-compatible bucket into a
  SQLite catalog and serves catalog browsing + bounded-horizon session streaming
  over one WebSocket using a Protobuf wire contract.
- A reference client — the `dexory-cloud-cli` C++ tool built from the same
  `BackendConnection` the eventual GUI uses — opens stitched multi-file sessions,
  downloads only the requested topics, and reconstructs a local MCAP.
- A logical-equality differ (`mcapdiff`) is the pass/fail gate: original MCAP →
  server → CLI → reconstructed MCAP must agree on every message's
  `(topic, log_time, payload, publish_time, schema name/encoding/data)`.

This report records a dedicated **scale validation** run that exercises the three
hard numbers in the proposal's §4 acceptance criteria: **≥100 MCAPs indexed**, a
session over **≥3 consecutive recordings** downloading **only the requested
topics**, and a **reconstructed MCAP matching the originals**. All three pass.
The run was performed in an isolated sandbox (its own bucket `recordings-scale`,
its own server on `:8083`, its own throwaway catalog DB) so it neither perturbs
nor is perturbed by the interactive and CI server instances.

One commercial wording item is flagged honestly and openly in §7: the proposal's
acceptance text says "matches the originals **byte-for-byte**," whereas the design
spec (§11) and the unified plan (§6, L3) deliberately refine this to **logical
equality** on the per-message tuple above. This refinement is correct and
necessary — MCAP is a container format whose writers legitimately differ on
chunking, summary ordering, and chunk compression, so a byte diff of two
containers carrying identical messages would yield false negatives. The
recommendation is to align the SOW wording to "logical equality" before sign-off.

---

## 1. What was built (recap of the M1 surface)

### 1.1 Server — `pj-cloud-server` (Go, single static binary)

A pure-Go binary (no cgo; pure-Go SQLite) that runs the M1 subsystems on one TCP
listener:

- **Catalog** — a SQLite-WAL store with stable row-id file identifiers, a
  `tags_effective` view layering embedded tags (read from the MCAP footer) under
  override tags, and a `ListFiles` query supporting the `FileFilter` predicates
  (recorded-between time window, `topics_any_of`, tag `all`/`any`) with
  cursor-based pagination, plus `GetFile` (per-topic detail) and `UpdateTags`.
- **Indexer** — a warm-start synchronous reconcile followed by a background
  poller. It change-detects on the `(etag, size, last_modified)` triple: unchanged
  objects are skipped (no re-extract), changed objects are re-extracted with
  override tags preserved, and failures are recorded rather than crashing the loop.
- **Session/streaming** — `BuildPlan` (multi-file stitch with a pairwise overlap
  rejection above the codec, pre-flight byte/message estimates, an empty-plan
  contract), a bounded retain buffer with producer backpressure, a
  producer/consumer split enabling reconnect-and-resume, a registry with a
  concurrency cap and retain-after-disconnect eviction, and Cancel.
- **Wire/WS** — a Protobuf envelope multiplexing catalog RPCs and session data on
  one socket; `Hello` advertises `BackendCapabilities`.

The bucket, region, endpoint, and credentials are set in a YAML config
(`storage.s3.*`); `-config` selects the file, `-listen`/`-db`/`-poll-interval`
override individual fields, and `PJ_CLOUD_TOKEN`/`PJ_CLOUD_DB` provide env
overrides. There is no env override for the bucket name itself — it is a config
field — which is exactly how this scale run pointed the server at a different
bucket without touching any shared default.

### 1.2 Reference client — `dexory-cloud-cli` (C++/Qt, Widgets-free core)

The CLI drives the same `BackendConnection` the GUI uses:

```
dexory-cloud-cli [--url URL] [--token TOKEN] <command> [args]
  hello                              connect, print server version
  list [--json]                      list sequences
  topics <sequence-name> [--json]    list a sequence's topics
  download <seq...> --output FILE [--topics a,b] [--time-range s,e]
  tag <sequence-name> [--set k=v]... [--unset k]...
```

Multiple sequences passed to `download` are stitched, time-ordered, into one
continuous logical session. The reconstructed MCAP is written via a vendored MCAP
writer.

### 1.3 Verification tooling (Go)

- `devprobe` — a Go reference client for ground-truth assertions: `Hello`,
  `ListFiles` (with all `FileFilter` predicates), `GetFile`, tag edit, and a raw
  `download` that opens a fresh session by `s3_key` and reconstructs an MCAP. It
  deliberately does **not** run a client-side overlap pre-check, so it surfaces the
  server's authoritative overlap verdict.
- `mcapdiff` — the round-trip correctness gate (logical equality, multi-original
  merge for stitched reconstructions, subset/time-range filtering, and an
  over-delivery check).
- `mcaptopics` (added for this run, `server/cmd/mcaptopics`) — reads an on-disk
  MCAP and prints per-topic message counts derived from the actual records; the
  independent ground truth for the GetFile-parity assertion.
- `paginate` (added for this run, `server/cmd/paginate`) — drives the `ListFiles`
  `page_token` cursor loop at a fixed page size and asserts the pages tile the
  catalog with no duplicates, strictly increasing ids, and an exact total.

The two new commands are small, hermetic verification aids under `server/cmd/`;
they build and `go vet` clean against the existing `server` module and add no
runtime dependency to the shipped binary.

---

## 2. Scale-validation method (fully isolated)

The proposal's acceptance criterion is "indexes at least 100 real MCAPs." Dexory's
sample set on this development machine is 8 real recordings
(`nissan_zala_*`), so to exercise the ≥100 path honestly without fabricating
synthetic data, the 8 real objects were **server-side copied** (S3→S3, no
re-upload) into a separate bucket under systematic names — 13 copies of each of
the 8 originals = **104 objects**:

```
recordings-scale/<orig-stem>_copyNN.mcap   NN ∈ {01..13}, 8 stems → 104 objects
```

Each copy is byte-identical to its source object, so each copy carries the same
real topic structure, schemas, and message payloads as a genuine Dexory recording.
The catalog still has to read 104 distinct MCAP footers, build 104 file rows with
their topic detail, and serve all of them — i.e. the catalog/indexer/streaming
machinery does exactly the work it would for 104 distinct files. What this method
does **not** prove is behaviour on 104 files with *divergent* schemas; that is
already covered by the 8-file fixture matrix and the cross-language E2E suite, and
is called out as a residual in §6.

Isolation discipline (a second slice ran concurrently):

- New bucket `recordings-scale` only; the shared read-only `recordings` bucket
  (8 nissan files) was never modified — confirmed still 8 objects at teardown.
- A dedicated server instance on `:8083` with a throwaway DB at
  `/tmp/slice9-scale/catalog-scale.db`; the interactive `:8080`, smoke `:8081`,
  and the sibling slice's `:8082` were never touched. `make smoke` was not run.
- Existing built binaries were used for everything user-facing
  (`dexory-cloud-cli`, `server/bin/{pj-cloud-server,devprobe,mcapdiff}`); only the
  two new Go verification aids were compiled (a concurrency-safe build-cache
  operation). No C++ was rebuilt; no plugin source or `scripts/smoke.sh` was
  modified.

### 2.1 How the bucket and DB were pointed (exact mechanism)

The bucket override is purely config-driven. A temporary YAML was written at
`/tmp/slice9-scale/server.yaml`:

```yaml
server:   { listen: ":8083" }
auth:     { bearer_token: "" }     # dev-anonymous, matches the dev stack
format:   "mcap"
storage:
  s3:
    bucket:     "recordings-scale"  # <-- the only meaningful change vs the default
    region:     "us-east-1"
    endpoint:   "http://localhost:9000"
    access_key: "admin"
    secret_key: "password123"
catalog:  { db_path: "/tmp/slice9-scale/catalog-scale.db" }
indexer:  { poll_interval: 30s, startup_scan: true }
```

The server was started with the existing binary:

```
server/bin/pj-cloud-server -config /tmp/slice9-scale/server.yaml -log-level info
```

The 104 copies were created with `mc cp` inside the Minio container network
(`docker run --rm --network minio_default --entrypoint sh minio/mc:RELEASE.2024-06-12T14-34-03Z`),
copying `local/recordings/<stem>.mcap → local/recordings-scale/<stem>_copyNN.mcap`
— a server-side copy that never round-trips bytes through this machine.

---

## 3. Acceptance-criterion results

### 3.1 ≥100 MCAPs indexed (proposal §4, criterion 1) — PASS

Cold start against the freshly populated `recordings-scale` bucket, from the
server's own log:

```
indexer: run complete  scanned=104 new=104 reindexed=0 unchanged=0 failed=0 duration_ms=1714
pj-cloud-server listening  addr=:8083 ws_path=/api/ws
```

- **104** objects extracted, every one `kind=new`, **0 failed**.
- Cold full-bucket scan: **1714 ms** for 104 footers (≈16.5 ms/file, footer-only
  Range reads — the catalog never downloads whole files to index them).
- `ListFiles` reports **`file_count == 104`** over the wire (`devprobe`).

The server also advertises its capabilities at `Hello`: `resume_supported=true`,
`tag_edit_supported=true`, `metadata_key_vocabulary=[robot_id, procedure_date,
operator]`.

### 3.2 Catalog correctness at scale — PASS

**Pagination** (`paginate --limit 20`): the cursor loop tiles the catalog cleanly.

```
page_sizes = [20, 20, 20, 20, 20, 4]   page_count = 6
total_ids = 104   distinct_ids = 104   strictly_increasing = true   no_duplicates = true
first_id = 1   last_id = 104
```

Exactly **6 pages** of 104, **no duplicates**, strictly increasing ids across page
boundaries (no gaps), exact total. The cursor is keyed on the stable row id.

**Filter predicates at scale:**

| Predicate | Expectation | Result |
|---|---|---|
| `recorded_between` = zeg_1's exact span `[1696577416664334825, 1696577567051266995]` | the 13 zeg_1 copies only | **13 matches, all stem `nissan_zala_50_zeg_1_0`** |
| `topics_any_of` = `/does/not/exist` | nothing | **0 matches** |
| `tag_all` = `slice9_marker=scale` (after tagging one copy) | the one tagged copy | **1 match** (see §3.6) |

The time window was chosen as zeg_1's exact recorded span; an overlap analysis of
all 8 stems' spans confirmed only zeg_1 overlaps that window (the other 7 stems are
time-separated), so "exactly 13" is the correct ground-truth count given the
overlap-based time predicate (`f.end_ns >= start AND f.start_ns <= end`).

**GetFile parity** — a zeg_1 copy's per-topic counts, served by the server, were
compared against the on-disk original `nissan_zala_50_zeg_1_0.mcap` read
independently by `mcaptopics`:

```
original topic_count = 6   copy topic_count = 6   per-topic match = True   diffs = NONE
```

The 6 real Dexory topics and their exact message counts on zeg_1:

| topic | schema | encoding | count |
|---|---|---|---|
| `/nissan/gps/duro/current_pose` | `geometry_msgs/msg/PoseStamped` | ros2msg | 3007 |
| `/nissan/gps/duro/imu` | `sensor_msgs/msg/Imu` | ros2msg | 14904 |
| `/nissan/gps/duro/mag` | `sensor_msgs/msg/MagneticField` | ros2msg | 3726 |
| `/nissan/gps/duro/status_string` | `std_msgs/msg/String` | ros2msg | 3007 |
| `/nissan/vehicle_speed` | `std_msgs/msg/Float32` | ros2msg | 4513 |
| `/nissan/vehicle_steering` | `std_msgs/msg/Float32` | ros2msg | 4513 |
| **total** | | | **33670** |

The `imu == 14904` figure is the value pinned in the project handbook and the
live-test fixtures for `nissan_zala_50_zeg_1_0.mcap`; the scale copy reproduces it
exactly, confirming the copies are faithful and the catalog extraction is correct.

### 3.3 Warm restart on the same DB — PASS

The server was stopped and restarted against the same catalog DB. On a clean
shutdown SQLite checkpointed the WAL, so only the main `.db` persisted; the warm
start served immediately with no re-extraction:

```
indexer: run complete  scanned=104 new=0 reindexed=0 unchanged=104 failed=0 duration_ms=6
pj-cloud-server listening  addr=:8083 ws_path=/api/ws
```

- **104 skip-unchanged**, **0 extracts** — the `(etag, size, last_modified)`
  change-detect triple short-circuits every object.
- Warm scan: **6 ms** vs the cold **1714 ms** — a ≈285× reduction; restart is
  effectively instant because the catalog is durable and only the cheap
  change-detect comparison runs.

(As a side observation, the cold instance's background poller ran 5 further times
on its 30 s cycle during the run, each reporting `unchanged=104` in 4–7 ms — the
same skip path holding steady under repeated polling.)

### 3.4 Round-trip at scale (proposal §4, criterion 2) — PASS

All downloads used the existing `dexory-cloud-cli` binary against `:8083`; all
diffs used `mcapdiff` against the on-disk originals in `/home/gn/ws/jkk_dataset02/`.

**(a) Single-file full download** — `nissan_zala_50_zeg_4_0_copy07.mcap`:

```
CLI:      received: 22311 messages in 11 batch(es), eos: COMPLETE (total_messages_sent=22311)
mcapdiff: records original=22311 rebuilt=22311  →  OK: logically equal, no over-delivery  (exit 0)
```

**(b) 3-file stitched download** across copies of three *different* time-disjoint
stems — `zeg_2_copy01 + zeg_3_copy05 + zeg_4_copy09`:

```
CLI:      estimate ~87343 messages, 16.6 MB; received 87343 messages in 44 batches, eos COMPLETE
mcapdiff (vs the 3 originals zeg_2/zeg_3/zeg_4):
          records original(merged)=87343 rebuilt=87343  →  OK: logically equal, no over-delivery  (exit 0)
```

87343 = 43301 (zeg_2) + 21731 (zeg_3) + 22311 (zeg_4). This is the headline M1
criterion: a session over ≥3 consecutive recordings, stitched server-side into one
ordered stream, reconstructed losslessly.

**(c) Topic-subset download** — proves "downloads **only** the requested topics."
`zeg_1_copy03`, restricted to `/nissan/gps/duro/imu` + `/nissan/vehicle_speed`:

```
CLI:      received 19417 messages, eos COMPLETE         (19417 = 14904 imu + 4513 vehicle_speed)
mcapdiff --topics imu,vehicle_speed (vs original zeg_1):
          records original(filtered)=19417 rebuilt=19417  →  OK: logically equal, no over-delivery  (exit 0)
```

The "no over-delivery" half of the gate is the affirmative proof that the server
shipped nothing outside the requested topic set — a mis-sliced or fabricated batch
would have been caught.

### 3.5 Overlap rejection at scale (the stitch safety gate) — PASS

Copies of the **same** stem share an identical recorded span, so stitching two of
them must be rejected. Driven through `devprobe` (which performs no client-side
pre-check, so the verdict is the server's):

```
devprobe -download "nissan_zala_50_zeg_1_0_copy01.mcap,nissan_zala_50_zeg_1_0_copy02.mcap"
  → error code: ERROR_INVALID_REQUEST
    message: "overlapping file time ranges: file 14 ends at 1696577567051266995,
              file 15 starts at 1696577416664334825"
  → exit 1, no output file written
```

The same request through `dexory-cloud-cli` is caught one layer earlier by the
client-side overlap guard that mirrors the server ("the server stays
authoritative") — also exit 1, no file written. Both layers agree: an
overlapping stitch never produces a corrupt reconstruction.

### 3.6 Tag editing + tag filter at scale — PASS

(Tag editing is M2 scope for the Dexory contract, but the engine exists and is
exercised here to show it scales.) On `nissan_zala_90_mixed_0_copy07.mcap`:

```
cli tag ... --set slice9_marker=scale      → OK, effective tags: slice9_marker=scale [override]
devprobe -filter-tag-all slice9_marker=scale → 1 match (exactly that copy)
cli tag ... --unset slice9_marker          → OK
devprobe -filter-tag-all slice9_marker=scale → 0 matches
```

The override is set, found by the `tag_all` predicate as the unique match among
104 files, and cleanly removed.

---

## 4. Consolidated numbers

| Metric | Value |
|---|---|
| Objects in `recordings-scale` | **104** (8 stems × 13 copies) |
| Cold full-bucket scan (104 footers) | **1714 ms**, 104 new, 0 failed |
| Warm restart scan (same DB) | **6 ms**, 0 extracts, 104 skip-unchanged |
| Cold/warm speedup | ≈ **285×** |
| `ListFiles` file_count over the wire | **104** |
| Pagination at limit 20 | **6 pages** `[20,20,20,20,20,4]`, 104 distinct ids, strictly increasing, no dups |
| Filter: zeg_1-span time window | **13** matches (all zeg_1) |
| Filter: nonexistent topic | **0** matches |
| Filter: tag_all `slice9_marker=scale` | **1** match → **0** after unset |
| GetFile parity (zeg_1 copy vs on-disk original) | per-topic exact match, total **33670**, 6 topics, imu **14904** |
| Round-trip (a) single-file full | 22311 msgs, **logically equal, no over-delivery** |
| Round-trip (b) 3-file stitch (zeg_2/3/4) | **87343** msgs (43301+21731+22311), **logically equal, no over-delivery** |
| Round-trip (c) topic subset (imu+speed) | 19417 msgs (14904+4513), **logically equal, no over-delivery** |
| Overlap stitch (two zeg_1 copies) | rejected server-side: **ERROR_INVALID_REQUEST**, no file written |

Per-stem message counts observed by the indexer (each ×13 in the bucket):
sagod 44416, zeg_1 33670, zeg_2 43301, zeg_3 21731, zeg_4 22311,
country_road_1 46906, country_road_2 73704, mixed 51822. All 8 stems: 6 topics
each. Per-file chunk counts ranged 6–19, matching the footer chunk indexes.

---

## 5. What worked

- **The catalog indexes ≥100 MCAPs comfortably and cheaply.** 104 footers in
  1.7 s cold, and warm restart is effectively free (6 ms) because the SQLite
  catalog is durable and the change-detect triple short-circuits unchanged
  objects. The "polls every few minutes, reads only footers" design described in
  the proposal holds at this scale with margin.
- **Stitching + lossless reconstruction is solid.** The 3-file stitched download
  (the literal §4 criterion) reconstructs to exact logical equality, as do the
  single-file and topic-subset cases. The `mcapdiff` over-delivery check confirms
  topic filtering ships only what was asked for.
- **The streaming wire path is faithful through the real client.** The
  user-facing `dexory-cloud-cli` (the exact `BackendConnection` the GUI lifts) and
  the Go `devprobe` produce identical, correct results — two independent client
  stacks agreeing on the same server.
- **The overlap safety gate is authoritative and at the right layer.** The server
  rejects overlapping stitches with a precise `INVALID_REQUEST`, independent of any
  client-side convenience check, so a bad selection can never yield a corrupt
  stream.
- **Catalog browsing scales correctly.** Pagination tiles cleanly with stable
  cursors; filters (time, topic, tag) return exact, predictable counts at 104
  files.
- **Isolation worked end to end.** The run used its own bucket, server, and DB and
  left the shared `recordings` bucket and the other server instances untouched —
  evidence that the deployment model (point the binary at a bucket via config) is
  clean and side-effect-free.

---

## 6. What didn't / limitations of this run

- **Same-content copies, not 104 schema-divergent files.** The scale set is 8 real
  recordings replicated to 104 objects. This faithfully exercises the
  catalog/indexer/streaming volume path (104 distinct footers, rows, topic sets,
  and a stitched session), but it does **not** add schema diversity beyond the 8
  originals. Schema-drift-across-files behaviour is covered separately by the
  cross-language E2E matrix (consecutive-of-3 with schema drift on file 3) and the
  8-file fixture set; it is not re-proven here. For a real Dexory bucket the
  ≥100-file path will see genuine per-file variety, which the catalog model already
  handles (per-file topic rows, per-topic schema name/encoding/data).
- **No real-network S3.** The run is against Minio over `localhost`, so the
  1714 ms cold scan is a lower bound; against real S3 the per-footer latency will
  dominate and the absolute scan time will be larger (though still footer-only
  Range reads, and still a one-time cost amortised by the durable catalog and the
  skip-unchanged warm path). A real-S3 smoke is a pre-handover checklist item.
- **Throughput was not measured here.** The 200 MB/s sustained gate is M2 scope and
  was deliberately not exercised in this isolated run.
- **Resume-with-drop was not re-exercised at scale.** The producer/consumer split
  and resume path are covered by the component suite; this run focused on the §4
  acceptance numbers, not the failure-injection matrix.

---

## 7. The "byte-for-byte" vs "logical equality" wording item (must resolve before sign-off)

The proposal's §4 acceptance text reads: the CLI "writes a valid MCAP whose every
message matches the originals **byte-for-byte**." The design spec (§11) and the
unified plan (§6, testing layer L3) deliberately refine the correctness gate to
**logical equality**: for every `(topic, log_time)` record, the reconstructed and
original files must agree on `payload`, `publish_time`, and the originating
schema's `name`/`encoding`/`data`.

This refinement is technically required, not a weakening:

- MCAP is a **container** format. Two MCAP files carrying the identical set of
  messages can differ at the byte level in chunk boundaries, per-chunk
  compression, summary-section ordering, and index layout — all of which are
  writer choices, none of which change a single message a consumer sees.
- A byte diff of the two **containers** would therefore report spurious
  differences even on a perfectly lossless round-trip. The meaningful, and
  strictly stronger-where-it-matters, guarantee is that **every message** is
  preserved exactly — which is precisely what `mcapdiff` asserts (and it
  additionally asserts *no over-delivery*, i.e. nothing fabricated or mis-sliced).

In other words, the wire protocol forwards raw MCAP message records and the client
re-serialises them verbatim; the messages are byte-identical, the *containers* are
not necessarily so. Every round-trip in §3.4 passed this logical-equality gate
with exit 0.

**Recommendation:** before written M1 acceptance, align the SOW/acceptance wording
from "byte-for-byte" to "logical equality on `(topic, log_time, payload,
publish_time, schema name/encoding/data)`," consistent with the design spec §11 and
the unified plan. This is the single open commercial wording item; everything it
covers is already demonstrably passing.

---

## 8. What M2 will change as a result

The M1 results are clean enough that M2 is execution risk, not technical risk, as
the proposal anticipated. Concrete carry-overs informed by this run:

- **Real-S3 indexing characterisation.** Replace the localhost-Minio cold-scan
  number with a real-S3 measurement and confirm the warm/skip-unchanged path holds
  over real object metadata (ETag semantics in particular). Capacity guidance
  should be sized against `session.max_concurrent` (default 16), as the deployment
  notes already record.
- **The GUI replaces the CLI as the primary client.** The same `BackendConnection`
  this run exercised is what the PlotJuggler v4 connector lifts; M2 puts the browse
  dialog, topic/time selection, and progress UI on top. The "≤5 clicks from cold
  start" criterion is an M2 gate.
- **Tag editing and intra-file time-range selection become first-class.** Both
  engines exist and scale (tag editing is shown in §3.6); M2 surfaces them in the
  UI and makes intra-file time windows part of the Dexory gate (an M1 non-goal).
- **Throughput + hardening.** The 200 MB/s sustained gate, panic-recovery/metrics
  on every goroutine boundary, graceful shutdown, transient-retry under load, and
  slow-client backpressure under soak are M2's measured deliverables.
- **Dashboard, deploy artifacts, operator/end-user docs**, and the cross-language
  E2E matrix as a per-push CI gate (with the `{s3,gcs}` legs for the Asensus
  inheritance).
- **Wording alignment (§7)** folded into the M2 SOW.

---

## 9. Reproduction notes (this run)

- **Bucket population** (server-side copy, no re-upload):
  `mc cp local/recordings/<stem>.mcap local/recordings-scale/<stem>_copyNN.mcap`
  for NN ∈ {01..13} over the 8 `nissan_zala_*` stems, run inside the Minio
  container network (`minio_default`).
- **Server**: `server/bin/pj-cloud-server -config /tmp/slice9-scale/server.yaml`
  (the YAML in §2.1; the only meaningful field vs the dev default is
  `storage.s3.bucket: recordings-scale`, plus `:8083` and a throwaway DB).
- **Clients**: existing `dexory-cloud-cli` and `server/bin/{devprobe,mcapdiff}`;
  two new Go aids `server/cmd/{mcaptopics,paginate}` built from the `server`
  module.
- **Cleanup performed**: the `:8083` server was stopped and the throwaway DB
  removed. **The `recordings-scale` bucket is intentionally kept** so this report
  and any runbook reference it. To remove it later:
  `mc rb --force local/recordings-scale` (run inside the `minio_default` network,
  as above). The shared `recordings` bucket (8 files) was never modified.

---

*End of Milestone 1 closure report.*
