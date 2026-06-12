# PlotJuggler Cloud Connector — Dexory Milestone 1 Report

| | |
|---|---|
| **Date** | 2026-06-05 |
| **Engagement** | Dexory Milestone 1 (PoC) — proposal `2026-06-01-dexory-proposal.md` §4 |
| **Author** | (PlotJuggler Cloud Connector engagement) |
| **Companion docs** | `2026-06-05-m1-demo-runbook.md` (repeatable live demo), `2026-06-05-dexory-m1-closure-report.md` (scale-validation deep dive with the consolidated numbers) |

This is the proposal-§4 written report: **what was built, what worked, what didn't, and
what M2 will change as a result.** It is written against the repository as it actually
stands on 2026-06-05, citing test names, `make smoke` steps, and commit hashes so every
claim is checkable. Where the as-built shape differs from the original plans, the
amendment is stated plainly together with its reason. The tone is deliberately honest:
the things that are not done, or are done differently than the proposal implied, are
called out as clearly as the things that are.

---

## 0. Executive summary

Milestone 1 set out to prove the end-to-end data path: a Go server in front of an S3
bucket, exposing a queryable MCAP catalog and an on-demand streaming session pipeline,
with a reference client that reconstructs the streamed session into a local MCAP that
can be diffed against the original. **That data path works and is gated by automated
tests.**

- The **server** (`pj-cloud-server`, one Go binary) runs the catalog (SQLite-WAL,
  single-writer, server-side filter predicates + pagination, override-tag editing), the
  background **indexer** (footer-only Range reads, change detection, override-preserving
  re-index), and the full **session/streaming** pipeline (multi-file stitch with overlap
  rejection, pre-flight estimate, producer/consumer split, bounded retain buffer,
  reconnect-resume, cancel).
- The **reference client** is a C++/Qt CLI (`dexory-cloud-cli`) built on a deliberately
  Widgets-free `BackendConnection` core. It browses the catalog, opens stitched
  sessions, and reconstructs them to local MCAP. A Go reconstruction client
  (`devprobe`) and a logical-diff tool (`mcapdiff`) provide a second, independent stack.
- A **regression gate** (`make smoke`, steps a–h including f1–f5) proves the whole
  pipeline without the GUI on every run and is the project's release gate.
- The **acceptance criteria** are met with one honesty caveat and one wording item:
  ≥100 MCAPs indexed is demonstrated at **104** objects on an isolated bucket (real
  Dexory-bucket run pending client access); ≥3-consecutive-recording stitched downloads
  with topic filtering pass with exact counts; equality is **logical equality** on
  `(topic, log_time, payload, publish_time, schema)` per the design spec, which is the
  correct refinement of the proposal's "byte-for-byte" wording (§7).

Beyond the proposal's M1 surface, the engagement also stood up the **GUI** end early —
a cloud **Toolbox** plugin inside PlotJuggler v4 that browses, filters (Lua), edits
tags, and fetches stitched/time-windowed sessions with **in-plugin ROS2/CDR decode** of
all six topic types. The proposal explicitly deferred the GUI to M2; building it early
de-risks M2 and made the demo more convincing, but it also drove several **recorded
amendments** to the plans (§2), each with a documented reason.

---

## 1. Acceptance criteria vs. proposal §4

| # | Proposal §4 acceptance criterion | Status | Evidence |
|---|---|---|---|
| 1 | Server runs against Dexory's S3 bucket and **indexes ≥100 real MCAPs**. | **DONE (synthetic, 104)**; real-bucket run **pending client access** | Cold scan `scanned=104 new=104 failed=0` in ~1.7 s (one observed run; localhost-Minio lower bound); `ListFiles file_count=104`; warm restart `unchanged=104` in ~6 ms. Bucket `recordings-scale` = 8 real stems × 13 server-side copies. Full numbers in the closure report. |
| 2a | CLI opens a session over **≥3 consecutive recordings**. | **DONE** | 3-file stitch `zeg_2+zeg_3+zeg_4` → 87343 msgs (43301+21731+22311), EOS COMPLETE. `make smoke` f5 = 2-file stitch (65032). |
| 2b | Downloads **only the requested topics** (whole-file topic filtering — the Dexory M1 gate). | **DONE** | Subset `imu+vehicle_speed` → 19417 msgs, `mcapdiff --topics` clean with **no over-delivery**. `make smoke` f2. |
| 2c | Writes a valid MCAP whose every message **matches the originals**. | **DONE (logical equality)** | `mcapdiff` exit 0 (`OK: logically equal, no over-delivery`) on full / subset / time-range / 3-file-stitch. Equality on `(topic, log_time, payload, publish_time, schema name/encoding/data)`. See §7 on the "byte-for-byte" wording. |
| 3 | A short written report (8–12 pages). | **DONE** | This document + the runbook + the closure report. |

Two honest qualifications, expanded in §6:

1. **Criterion 1 is met against a representative copy of the corpus, not the live Dexory
   bucket.** The proposal's own §4 allows "Dexory's, or a copy in a sandbox bucket." The
   104 objects are 8 *real* recordings replicated 13× via server-side copy (no synthetic
   message data fabricated). This faithfully exercises the catalog/indexer/streaming
   **volume** path at 104 distinct footers, rows, and topic sets, and a 104-file
   stitched-capable catalog. It does **not** add schema diversity beyond the 8 originals
   — that dimension is covered by the existing 8-file fixture plus the cross-language E2E
   matrix, not re-proven by the scale copies. A run against the real Dexory bucket is a
   pre-handover checklist item once bucket access is provisioned.
2. **"Byte-for-byte" is refined to "logical equality."** This is a deliberate, correct
   refinement, not a weakening — see §7. It is the one **commercial wording item** that
   should be reconciled in the SOW before written sign-off.

---

## 2. Architecture as built (with the recorded amendments)

The system is the two-component, one-wire-contract design from the proposal §3: a Go
server in front of S3, a C++/Qt client, and a single WebSocket carrying a Protobuf
envelope for both catalog RPCs and streamed session data.

### 2.1 Server — `pj-cloud-server` (Go, single static binary)

One process, one TCP listener, pure-Go SQLite (no cgo). Subsystems:

- **Catalog** — `server/internal/catalog`: SQLite in WAL mode with a **single writer
  goroutine**; a `tags_effective` view that overlays override tags on embedded tags
  (override wins); restart-stable rowids; server-side `FileFilter` predicates (time
  range, `topics_any_of`, tag `any`/`all`) with cursor pagination; `GetFile`;
  `UpdateTags`. Catalog reads are SQLite queries that return in milliseconds.
  (`store.go`, `files.go`, `topics.go`, `tags.go`, `filter.go`, `schema.sql`.)
- **Indexer** — `server/internal/indexer`: background poller that, for each new/changed
  object, reads only the **MCAP footer** over HTTP Range requests (not the whole file),
  extracts file/topic/embedded-tag metadata, and writes catalog rows. Change detection
  is the `(etag, size, last_modified)` triple; re-index **preserves override tags**.
  Warm start over an existing DB re-extracts nothing (`unchanged=N` in single-digit ms).
- **Session / streaming** — `server/internal/session`: `BuildPlan` validates the
  selection (consecutive non-overlapping files, valid topics, sane window), computes the
  minimal ordered chunk list, and returns a **pre-flight estimate**
  (`estimated_chunk_bytes`, `approximate_messages`). A **producer** goroutine fetches
  chunks and packs messages into ZSTD-compressed batches (one-shot per batch, singleton
  fallback below a threshold); a **consumer** goroutine drains a bounded **retain
  buffer** (256 seqs / 64 MiB default) to the WebSocket. The producer/consumer split is
  what makes **reconnect-resume** work: on `OpenResume` the consumer replays from the
  last acked sequence in the retain window; eviction past the window yields
  `RESUME_NOT_POSSIBLE`. A session **registry** with 60 s eviction and `CancelSession`
  complete the lifecycle. Stitching of N files into one logical stream is built **above
  the codec**, in `BuildPlan`.
- **Format codec** — `server/internal/format`: the MCAP impl behind the `FormatCodec`
  seam (footer extract incl. footer-metadata → embedded tags; chunk planning; iterate
  with topic + time-range filtering).
- **WS dispatcher** — `server/internal/ws`: the Protobuf-envelope handler surface
  (`Hello`, catalog handlers, session handlers, `Ack`, `Progress`, `Eos`), with
  priority and bulk channels on one connection.
- **Dashboard / metrics** — **not built in M1** (M2 scope; see §8).

The wire schema is the single canonical Protobuf file `proto/pj_cloud.proto`; generated
bindings are checked in for both languages so neither side needs `protoc` at build time.

### 2.2 Reference client — `dexory-cloud-cli` (C++/Qt)

The reference client is the proposal's correctness signal: it reconstructs the streamed
session into a local MCAP that can be diffed against the original. Built on a
**Widgets-free `BackendConnection` core** so the same code drives both the CLI and the
GUI plugin — the CLI is the *exact* `BackendConnection` the GUI uses, which is why the
smoke harness exercising the CLI is meaningful for the GUI too. Commands: `hello`,
`list [--json]`, `topics <seq> [--json]`, `download <seq...> --output FILE [--topics
a,b] [--time-range s,e] [--json]` (variadic = stitched, time-ordered), `tag <seq>
[--set k=v]... [--unset k]...`. Exit codes 0/1/2 = ok/connection-failure/usage. The
client mirrors the server's overlap check as a fast pre-validation; the server remains
authoritative. The MCAP reconstruction uses a vendored MCAP writer.

A second, independent reconstruction stack exists in Go (`devprobe -download`,
`mcapdiff`) so the round-trip is proven through two unrelated codebases.

### 2.3 The GUI — a cloud Toolbox inside PlotJuggler v4 (built early)

The proposal deferred the GUI to M2. It was built early as the "start endpoint" and now
browses, Lua-filters, edits tags, and fetches stitched/time-windowed sessions with the
data landing in PlotJuggler's time-series store. The dialog/worker design is lifted from
the Mosaico cloud-browser plugin (three-panel UI, connect-to-URI + history, Lua query
engine, command-queue worker drained on tick, `host_write_mu_` serialization), with its
Arrow-Flight transport swapped for the `BackendConnection` WS+Protobuf core.

### 2.4 The recorded amendments (and why)

The following are deliberate divergences from the plans. Each is documented in
`CLAUDE.md` and the two-endpoints approach note; restated here so a reviewer sees them in
one place.

**(A) The plugin is a cloud Toolbox, not a DataSource.** The proposal §3.5 and Plan D
specify a PlotJuggler *DataSource* (`pjcloud://`). The shipped plugin is a *Toolbox*
(Mosaico-style non-modal panel). This was a **user product decision** forced by two
grounded SDK facts, verified by reading the real PlotJuggler v4 SDK headers (this is
exactly the kind of spec-vs-SDK mismatch Plan D §0 warned about):

  1. Parser dispatch (`ensureParserBinding` / `pushMessage`) lives **only** on the
     DataSource runtime host view (`DataSourceRuntimeHostView`, "pj.runtime.v1") and is
     **never registered for Toolboxes** (`ToolboxRuntimeHost.cpp:31-35`). A Toolbox
     cannot delegate ingest to the host's MessageParser plugins.
  2. A `FileSourceBase` **without** `file_extensions` is **unreachable** in the host —
     the host gates DataSource loaders by file extension (`FileLoader.cpp:141-149`), and
     a cloud connector has no on-disk extension to register.

  Together these mean the clean "DataSource that forwards raw messages to the host
  parsers" shape the unified plan assumed is not reachable for a cloud browser in this
  SDK. The Toolbox shape *is* reachable and gives the non-modal browse-while-plotting UX
  Dexory wants.

**(B) The connector decodes ROS2/CDR in-plugin, instead of "shipping no decoders."**
Because of fact (A.1), a Toolbox cannot hand raw messages to the host parsers — so to
get series onto the plot, the plugin decodes them itself. It does this with
`rosx_introspection` 3.1.0 — *the same library the engine's `parser_ros` wraps* — in
`src/ros_decode_driver.{hpp,cpp}`, flattening ROS2/CDR messages to scalars and writing
them through the Toolbox write API. **All 6 topic types ingest.** This is a
user-decided amendment to the unified plan's "connector ships no decoders" rule, and it
is gated hard: a **triple-parity** test (`DexoryCloudRosDecodeParityTest`) asserts our
flatten == a `dlopen`'d copy of the **real** engine `parser_ros` == `rosx`-direct, on
committed fixtures (`tests/fixtures/ros/`) and on fresh adversarial payloads.

  *Documented decode gap:* `Imu` and `PoseStamped` use the **generic** flatten (arrays +
  `/header/stamp/sec|nanosec`), not `parser_ros`'s specialized handlers (single stamp
  double + covariance upper-triangle). Series still appear; the field *layout* differs
  from a locally-opened file for those two types. This is a scoped M2 follow-up if
  file-open parity for those two types is mandated.

**(C) The client uses Qt WebSockets, not ixwebsocket.** PlotJuggler's in-repo streaming
plugins use ixwebsocket; the `BackendConnection` core deliberately uses **QtWebSockets**
(design spec §9) so the same Widgets-free core serves the CLI and the GUI unchanged.
This is per-spec, not a regression.

**(D) Catalog went straight to SQLite (the catalog-lite step was skipped).** Slice 1
browsed against an in-memory catalog-lite; Slice 6 replaced it with the Plan A 7–12
SQLite-WAL store (single-writer, `tags_effective`, restart-stable). The on-disk catalog
is what makes warm-start zero-re-extract and tag persistence work.

---

## 3. What worked

- **The round-trip is lossless** on every shape tested — full, topic-subset,
  intra-file time-window, and N-file stitch — through **two independent client stacks**
  (C++ `dexory-cloud-cli` and Go `devprobe`), checked by `mcapdiff` for both
  under-delivery and **over-delivery**. The subset and time-range legs additionally
  prove the server ships *only* the requested data.
- **Stitching is exact and ordered.** Three time-disjoint files present as one
  continuous logical stream; the reconstructed message set equals the merged-and-resorted
  originals to the message (87343 = 43301+21731+22311). Reordering the selection
  produces an identical request (sorted by `(min_ts, name)`).
- **The catalog is durable and cheap.** Cold-scanning 104 footers took ~1.7 s
  (footer-only Range reads, ~16.5 ms/file); a warm restart on the same DB re-extracted
  nothing and served all 104 files in low-single-digit milliseconds — a two-orders-of-magnitude cold/warm speedup (observed samples: ~1.7 s cold vs ~6–12 ms warm). Filters
  (recorded-between, topics_any_of, tag_all) and cursor pagination tile cleanly at scale
  (104 ids across 6 pages of limit-20, strictly increasing, no dup/gap).
- **Override tags survive re-indexing.** Setting `verified=yes`, forcing a re-index by
  re-putting the object, and re-reading shows the override intact and the file id stable
  (`make smoke` step h).
- **Overlap is rejected, not mis-stitched.** Stitching overlapping spans returns
  `ERROR_INVALID_REQUEST` from the server with the exact offending boundary; the CLI
  mirrors the check and exits non-zero with no output file.
- **Reconnect-resume and in-memory session reuse work end-to-end.** A live test forces a
  mid-pull socket drop and asserts the resume loop drives the stream to COMPLETE with the
  exact count and no dupes, plus an in-memory cache hit serving a re-opened identical
  selection with zero transport (`DexoryCloudSessionResumeLiveTest`, smoke step d). This
  landed during the M1 window; the proposal scopes resume-through-the-plugin to M2.
- **The GUI demo is convincing.** Connect → Lua-filter → tag → multi-select stitched
  fetch → time-window → cancel all work in one non-modal panel, with all six ROS2 topic
  types plotting.

---

## 4. The verification machinery

The project's release gate is **`make smoke`** (`scripts/smoke.sh`) — a single,
self-contained, idempotent script that proves the whole pipeline without the GUI. It
stands up its **own** server on `:8081` (never the user's `:8080`), runs every assertion,
and always reaps its server on exit. The final line is exactly `SMOKE PASS` or
`SMOKE FAIL: <step>` and the exit code matches.

| Step | What it proves |
|---|---|
| a | Minio up; `recordings` bucket holds the MCAP corpus (8). |
| b | Go server builds; cold start over a fresh DB extracts the whole corpus (`new=8`). |
| c | Go discovery RPCs return pinned ground truth (`file_count=8`; `zeg_1` = 6 topics, imu=14904, total=33670). |
| d | C++ SDK tests: hermetic (live tests correctly **skip**) then **live** (live tests RUN and pass), including `DexoryCloudBackendLive` and `DexoryCloudSessionResumeLive`. |
| e | CLI spot check (hello/list/topics) through **both** stacks (`dexory-cloud-cli` and `devprobe`). |
| f | **Round-trip** family (the v1 correctness gate): |
| f1 | C++ CLI full download → `mcapdiff` clean. |
| f2 | C++ CLI subset download (`--topics`) → clean **and zero over-delivery**. |
| f3 | Go `devprobe` full download → `mcapdiff` clean. |
| f4 | C++ CLI **time-range** download → clean + no over-delivery (10098 in-window). |
| f5 | C++ CLI **stitched** download (2 files) → 65032 msgs, `mcapdiff` vs both originals merged; duplicate-seq exits 2. |
| g | **Restart persistence**: warm start on the same DB re-extracts 0, skips 8, serves immediately. |
| h | **Tag flow**: set → visible (`is_override=true`) → **survives forced re-index** → unset → gone. |

Test counts (checkable in-repo):

- **Go:** 21 `_test.go` files across `catalog`, `indexer`, `format`, `session`, `wire`,
  `ws`; ~97 `Test*`/`Benchmark*` functions. `go test -race` is the unit/race gate.
- **C++:** ~24 ctest cases (`CTestTestfile.cmake`), including unit suites
  (`wire_mapping`, `session_decode`, `stitch_select`, `query_engine`, `session_key`,
  `session_cache`, `settings_store`, `tls_utils`, …) and the live suite
  (`backend_live`, `ros_decode_live`, `session_download_live`, `session_resume_live`)
  which **skip** without `DEXORY_CLOUD_LIVE_URL` and **run** with it.
- **Decode triple-parity gate:** `DexoryCloudRosDecodeParityTest` and
  `DexoryCloudRosDecodeIngestTest` — our flatten == `dlopen`'d real `parser_ros` ==
  `rosx`-direct.

The scale validation in this slice added two Go helper commands built with `go build`
only (concurrency-safe), used as **independent ground truth** rather than trusting the
server's own numbers: `server/cmd/mcaptopics` (reads an on-disk MCAP and prints
per-topic counts — an independent `GetFile` parity check) and `server/cmd/paginate`
(drives the `ListFiles` page-token cursor and asserts no dup/gap, strictly increasing
ids, exact total).

---

## 5. Defects found and fixed by this process (evidence of rigor)

The harness and the parity gates caught real bugs that a less rigorous process would
have shipped. Each is recorded so the reviewer can see the verification machinery is not
ceremonial:

- **Chunk `MessageEndTime` boundary bug (spec §6.3).** The MCAP chunk index's
  `MessageEndTime` was treated as an *exclusive* upper bound, which dropped the last
  in-window message of interior chunks. Full and topic-subset downloads never slice
  time, so f1–f3 missed it; it surfaced only when the **time-range** leg (f4) was added.
  Fixed and pinned by the regression test `TestIterate_TimeRange_NoChunkEndDrop` (the
  f4 window — 10098 messages — moves in lockstep with it).
- **Stale-EOS poisoning across pulls.** A cancelled session's stale `Eos` could poison
  the *next* pull on a reused connection. Caught by a live test; fixed by using a
  per-download fresh `BackendConnection`.
- **ixwebsocket `ReadyState` race** in early streaming work — caught and addressed
  before the QtWebSockets core was settled.
- **Tag sub-dialog dead on open** — an illegal `--` inside a `.ui` XML comment broke the
  embedded UI (commit `2a516af`); `.ui` files must stay ASCII for the hex embed. Fixed.
- **Orphaned harness server.** An early smoke script left its server listening after
  exit because `$!` captured a subshell PID; fixed with `exec` so the trap reaps the
  real server (and only it) — the harness now never leaks a `:8081` process.

---

## 6. What didn't / known gaps

Stated plainly, so there are no surprises at sign-off:

- **No run against the real Dexory bucket yet.** Criterion 1 is met at 104 objects on an
  isolated copy bucket; the live-bucket run needs Dexory bucket access and is a
  pre-handover checklist item (§1, qualification 1). The cold-scan time (~1.7 s/104
  footers) is a localhost-Minio lower bound; real-S3 latency will raise the *one-time*
  cold-scan cost (still footer-only Range reads, amortised by the durable catalog and
  the ~6 ms warm path).
- **The 104 scale objects are same-content copies**, not 104 schema-divergent files
  (§1, qualification 1). Volume path: exercised. Schema diversity: covered by the 8-file
  fixture + E2E matrix, not the copies.
- **No throughput gate.** The 200 MB/s sustained-throughput gate is **M2 scope**
  (proposal §5) and was not exercised in M1.
- **No dashboard / metrics / TLS / deploy artifacts.** All **M2 scope** (proposal §5;
  §8 below). The server today is HTTP + a single shared bearer token (dev-anonymous when
  unset), which is exactly the M1 non-goal set (proposal §4 "out of scope for M1").
- **Tag editing and intra-file time-range are M2 scope for Dexory** (proposal §4
  lines 119–120). The *engines* for both are built and demonstrable (CLI `tag`, GUI Edit
  Tags…, `--time-range`), but they are **not** Dexory M1 acceptance gates — the M1 gate
  is whole-file topic filtering only. This matches the unified plan's M1 scope
  reconciliation.
- **Decode parity gap for `Imu`/`PoseStamped`** in the GUI (generic vs specialized
  flatten — §2.4 B). Series appear; field layout differs from a local file for those two
  types. Scoped M2 follow-up.
- **GCS leg not exercised in M1.** The `BlobStore` seam exists; the GCS impl is
  Asensus-funded M1b/M2 work, not part of Dexory M1.

---

## 7. The "byte-for-byte" vs "logical equality" wording item

The proposal §4 acceptance criterion 2 says the reconstructed MCAP must match the
originals **"byte-for-byte."** The design spec §11 and the unified plan §6 (Layer 3)
deliberately refine this to **logical equality** on `(topic, log_time, payload,
publish_time, schema name/encoding/data)`. This is the equality `mcapdiff` enforces and
that all round-trips pass (exit 0, plus a no-over-delivery check).

**This refinement is correct, not a weakening.** MCAP is a *container* format. Two
conforming writers legitimately differ on chunking boundaries, per-chunk compression
codec/level, summary-section ordering, and optional index records — while carrying
**identical messages**. A literal byte-diff of two such containers reports differences
that have nothing to do with data fidelity (false negatives). The data-fidelity question
the customer actually cares about — "is every message, with its timestamps and schema,
present and unchanged, and nothing extra delivered?" — is exactly logical equality.

**Recommendation:** align the SOW wording for criterion 2 to "logical equality on
`(topic, log_time, payload, publish_time, schema)`." This is the single open commercial
wording item and should be confirmed in writing before M1 sign-off. (It is the same
wording the unified plan's M1c gate already uses.)

---

## 8. What M2 will change as a result

M2 (proposal §5; unified plan M2a/M2b/M2c-DEX), gated by written M1 approval:

- **Plugin shape decision is settled, not deferred.** M2's "PlotJuggler integration" is
  already substantially de-risked: the cloud Toolbox exists and works. M2 hardens it
  rather than building it from scratch. The DataSource-vs-Toolbox reconciliation the
  plans flagged is resolved in favour of the Toolbox, with the SDK reasons recorded
  (§2.4 A). If Dexory specifically requires the `pjcloud://` *DataSource* UX (File →
  Open), that is a defined extra increment, not an open question.
- **Resume-through-the-plugin and in-memory session cache:** the engine and live tests
  landed during M1 (`DexoryCloudSessionResumeLiveTest`, smoke step d). M2 wires the
  resume loop and the in-memory exact-tuple LRU cache through the GUI and proves the
  GUI-level "simulated drop → resume from last ack, no refetch, no dupes" gate.
- **Tag editing as a Dexory feature:** promoted from "engine built" to an accepted
  feature with the persists-across-reindex guarantee surfaced in the GUI (already
  demonstrable; M2 makes it a gate).
- **Intra-file time-range as a Dexory feature:** the `--time-range` / slider engine is
  built (M1-demonstrable); M2 promotes it to an accepted Dexory feature if the expanded
  M1/M2 scope is agreed.
- **Operator dashboard + `/metrics` + `/health` over TLS+Basic auth**, **production
  deployment artifacts** (distroless Dockerfile, docker-compose, systemd, config
  reference, runbook), and the **hardening pass** (panic recovery + counters on every
  goroutine boundary, graceful shutdown, S3 transient-retry under load, slow-client
  backpressure) — all net-new M2 work.
- **The 200 MB/s throughput gate** and the **cross-language E2E matrix as a per-push CI
  gate** (the v1 benchmark suite), run on both `{s3,gcs}` legs.
- **Decode parity:** specialized-handler flatten for `Imu`/`PoseStamped` if file-open
  parity for those types is mandated.
- **WASM bonus:** client-core → WASM running the PJ4 WASM UI against the unmodified
  wire, with at minimum a CI compile job so the path cannot silently rot. A demo
  deliverable, not a pass/fail gate.

The unified plan's commercial framing holds: M0 + M1a-S3 + M1c = Dexory M1; written
approval hard-gates M2. The GCS leg and the Asensus-facing adapter are Asensus-funded
increments outside Dexory's fixed-price M1.

---

## 9. Appendix — how to reproduce every number

Every figure in this report is reproducible from two places:

1. **`make smoke`** — the steps a–h (f1–f5) gate. Run from the repo root with the Go
   toolchain on `PATH`. Final line `SMOKE PASS`. This proves the corpus counts (8 files,
   6 topics, imu=14904, total=33670), the round-trip family (full/subset/time-range/
   2-file-stitch), restart persistence, and the tag flow.

2. **`2026-06-05-m1-demo-runbook.md`** — the repeatable live demo, with exact
   copy-paste commands and expected output for: Minio + corpus check; the interactive
   `:8080` server; the CLI flow (hello/list/topics, full/subset/stitch-3/time-range/
   overlap-reject/tag); the GUI Toolbox flow; the round-trip `mcapdiff` invocations; and
   the **scale demo** (`recordings-scale` bucket on `:8083`, the 104-object cold scan,
   the `file_count=104` wire check, the 3-file stitched round-trip at scale, the warm
   restart). It includes a reset/cleanup section that leaves `recordings` and `:8080`
   untouched.

3. **`2026-06-05-dexory-m1-closure-report.md`** — the scale-validation deep dive: the
   consolidated per-stem counts (each ×13), the pagination tiling, the filter-at-scale
   results, the GetFile parity check via `mcaptopics`, and the full mechanism (config
   pointing, server-side copy population, isolated `:8083` instance, teardown).

Relevant commit hashes for the M1 surface: `522378b` (server v0.2.0 + canonical proto +
dev tools), `e74925d` (cloud Toolbox plugin with browse/streaming/in-plugin decode),
`bed90ac` + `cd8d78f` (SQLite catalog + tags), `70d3011` + `6f52644` (stitched
multi-file selection), `ec8fa61` (Minio infra + `make smoke` gate), `2a516af` (the `.ui`
XML-comment fix). Reconnect-resume + in-memory cache (Slice 8) and the scale-validation
Go helpers (`mcaptopics`, `paginate`) are present in the working tree at the time of
writing.
