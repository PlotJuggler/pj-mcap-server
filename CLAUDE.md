# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this directory is

This is the **design / planning workspace AND implementation repo for the "PJ Cloud
Connector"** — a commercial engagement (clients: Dexory/S3, Asensus/GCS) to build a
self-hosted server + client that serves MCAP recordings from a cloud bucket to
PlotJuggler-class clients on demand.

This repo (`pj-mcap-server`, a git repo on branch `main`) holds the design spec, the
implementation plans, the commercial proposal, **the vendored PJ4 working tree**
(`PJ4/`), and the first implementation artifacts. **[LOCAL DECISION 2026-06-04]** The
implementation code goes **in this repo**: every `pj-cloud/<path>` reference in the
plans maps to `/home/gn/ws/PJ4_Server_Template/pj-mcap-server/<path>` (the plans were
originally written for a separate `pj-cloud/` repo at
`/home/davide/ws_plotjuggler/pj-cloud`; the absolute paths in the plans have been
rewritten to this repo, and Plan A Task 1 Step 1 was amended — do **not** `git init` a
new repo).

## Current approach (2026-06-04): two endpoints first — see `arch/2026-06-04-two-endpoints-approach.md`

The active build order stands up the pipeline's two **ends** before the middle:

1. **START endpoint — `PJ4/pj-official-plugins/toolbox_dexory_cloud/`**: a PJ4 Toolbox
   plugin that is a 1:1 visual copy of `toolbox_mosaico` with all Arrow/Flight/gRPC/
   `mosaico_sdk` removed and an **inert transport stub** ("backend not implemented").
   Lives only in the vendored tree, never in the official upstream repos. Mosaico
   coexists untouched.
2. **END endpoint — `infra/minio/`**: local S3-compatible Minio
   (`docker compose up -d` there), bucket `recordings`, identifiers pinned to Plan C.
   Synthetic MCAP data seeded later via Plan C `gen-fixtures`.
3. **PLUMBING (in progress)**: Plans A → B → C as written. **Slice 1 (catalog-browse)
   DONE; Slice 2 (session/streaming) DONE (2026-06-04):** the server (v0.2.0) implements
   the full spec-§6/§8 session path — `internal/session` (BuildPlan with overlap
   validation + empty-plan contract, retain buffer 256/64MB with producer backpressure,
   one-shot-ZSTD batch bodies + singleton fallback, producer/consumer split, registry +
   eviction), WS priority/bulk channels, OpenSession Fresh/**Resume**, Cancel, Ack,
   Progress, Eos. The C++ SDK downloads sessions (`session_decode` + vendored-mcap
   writer; `dexory-cloud-cli download <seq> [--topics …] [--time-range …] --output …`);
   `server/cmd/mcapdiff` asserts logical round-trip equality; smoke step f covers
   full/subset/time-range/dual-stack round-trips. **Slice 3 (GUI ingest, scoped) DONE
   (2026-06-04):** the dialog's Fetch now streams for real — `fetch_worker` wires
   `pullTopicsAsync` → `openSessionFresh`/`downloadSession`; **`std_msgs/msg/Float32`
   topics ingest** into the datastore (one dataset per download, `<topic>/data` series,
   native-float `appendRecord`); all other ROS2/CDR topics report *"requires parser
   integration (Plan D)"* per-topic — the connector still ships no decoders (the
   Float32 read is a hard-gated fixed 8-byte layout in `cdr_float32.hpp`, one exact
   schema). **Slices 4+5 (ingest shape, final) DONE (2026-06-04):** after a brief
   DataSource/Streaming-combo detour (Slice 4), the **USER PRODUCT DECISION** landed
   the permanent shape — **"Dexory Cloud" IS a cloud TOOLBOX, forever** (Mosaico-style
   non-modal panel; sidecar family=toolbox; positively verified NOT a
   stream/file source via the real `PluginRuntimeCatalog`). **ALL 6 topics ingest**
   via the in-plugin `RosDecodeDriver` (`src/ros_decode_driver.*`): rosx_introspection
   3.1.0 (the engine parser_ros wraps; CPM, cache at `pj-official-plugins/.cpm-cache`)
   decodes ROS2/CDR and writes flattened scalars through the toolbox write API —
   **a user-decided amendment to the unified plan's "connector ships no decoders"
   rule**, required because of GROUNDED FACT (1) below. **Triple-parity gate**: our
   flatten == dlopen'd REAL parser_ros == rosx-direct, on committed fixtures
   (`tests/fixtures/ros/`) AND fresh adversarial payloads. Known documented gap:
   Imu/PoseStamped use the GENERIC flatten (arrays + `/header/stamp/sec|nanosec`),
   not parser_ros's specialized handlers (single stamp double + covariance
   upper-triangle) — scoped follow-up if file-open parity for those two types is
   mandated. Per-download fresh `BackendConnection` (a cancelled session's stale Eos
   must not poison the next pull — live-test-caught). **Slice 6 (SQLite catalog +
   tags) DONE (2026-06-05):** `internal/catalog` is now the Plan A 7–12 SQLite-WAL
   store (modernc, single writer goroutine, `tags_effective` override-wins view,
   restart-stable rowids, warm-start = zero re-extracts), server-side `FileFilter`
   predicates + cursor pagination, `UpdateTags` (arm 13) live end-to-end: CLI
   `tag` verb + the dialog's "Edit Tags…" sub-dialog (PanelEngine harvest model —
   sub-dialog push buttons are inert; `.ui` files must stay ASCII for the hex
   embed). Flat metadata = derived keys overlaid by `tags_effective` (override
   wins, documented in `handlers_catalog.go`). DB at `-db`/`PJ_CLOUD_DB`
   (default `/tmp/pj-cloud-catalog.db`); smoke gained steps g (restart
   persistence) + h (tag flow incl. override-survives-forced-reindex).
   **Slice 7 (stitched multi-file selection) DONE (2026-06-05):** the Dexory
   headline — seqTable is ExtendedSelection; N selected sequences present as ONE
   synthetic stitched record (`src/stitch_select.h`: sorted by (min_ts,name) so
   reordered selection → identical request; client-side pairwise non-overlap
   pre-validation, server authoritative) → topics panel shows the UNION
   (per-sequence cache), Info shows the stitched summary, slider spans the
   union, ONE `OpenFresh` (dataset "first (+N-1 more)"); Edit Tags is
   single-selection-only. CLI `download` is variadic (duplicate → exit 2);
   `mcapdiff` accepts N originals (merged); smoke f5 = stitched round-trip
   (65032). Torture-verified: 3-file 87343 exact + monotonic, subset clean,
   boundary-spanning time window clean.
   **Slices 8+9 DONE (2026-06-05, in parallel):** **Slice 8 (resilience)** — a
   mid-download WS drop now reconnects + `OpenResume`s from the last delivered
   batch seq (gap/dupe-free; 3 attempts, 1s/4s/16s, cancel-interruptible;
   `RESUME_NOT_POSSIBLE` fails verbatim with partial kept, never a silent
   OpenFresh); `src/session_key.hpp` (Plan B T8a verbatim) + COMPLETE-only LRU
   `src/session_cache.hpp` (repeat Fetch = zero transport, ledger re-emit);
   smoke step d gains the resume-live leg; 26 tests. **Slice 9 (M1 pack)** —
   104-MCAP scale validated on the isolated `recordings-scale` bucket (cold
   ~1.7s, warm 0 re-extracts in ms, pagination/filters/round-trips/overlap at
   scale); the M1 deliverables (report, demo runbook, closure report — since
   removed from arch/) plus scale tooling `server/cmd/{mcaptopics,paginate}`.
   **Slice 10 (M2a hardening + verification deepening) DONE (2026-06-05):**
   both flagged server defects fixed failing-first — post-resume Eos totals
   exact (33670; per-seq cumulative ledger on `SessionState`,
   `RecordAppend`/`CountersThrough(resume_after_seq)` seeds the reattached
   consumer — a naive carry OVER-counted replayed in-flight batches to 33753,
   caught by the test) and `Store.Write`-after-`Close` returns `ErrStoreClosed`
   (idempotent Close, no send-on-closed-channel). Audit follow-ups landed: the
   `estimated_chunk_bytes`-within-5% gate is a Go component test
   (`ws/estimate_test.go`, counting BlobStore — 4 shapes, all delta=0.000%) +
   per-session `logSessionAccounting` slog (log-only; proto stays frozen); the
   spec §11 L3 legs run in the NEW **`make matrix`** (see harness section);
   smoke gained only cheap f6 (half-topics) + f7 (none-matching). M2a surface,
   config-gated OFF by default so the smoke path is unchanged: Basic-auth HTML
   dashboard (A 32–33; pico.css go:embed; 5 pages; registered only when
   `dashboard.Active()`), Prometheus `/metrics` (A 34; 11 `pj_cloud_*` series)
   + `/health` (always unauth), panic recovery in 6 scopes (spec §8.1; counted,
   process-survives test), TLS via `server.tls.{cert,key}` / `-tls-cert/-key`
   (spec §8.6; `scripts/gen-dev-cert.sh`; wss round-trip verified through BOTH
   stacks — `devprobe -insecure` and the C++ CLI's new `--insecure`), graceful
   shutdown (`Registry.CancelAll` + idempotent catalog Close; SIGTERM clean),
   deploy artifacts (A 36 at `server/deploy/`: distroless Dockerfile —
   container-verified serving against Minio — compose, systemd unit,
   config.example.yaml, README) and `make bench` (A 45: build-tagged
   throughput test + `server/bench/baseline.json`; ~15 MB/s is a documented
   LOCALHOST LOWER-BOUND, not the 200 MB/s reference-machine M2a SOW gate).
   Wiring-order bug found+fixed in main: observability must attach BEFORE
   `loop.Start` or warm-start indexer counters are lost. All 9 verify gates
   PASS; :8080 upgraded to the new binary (warm-start zero re-extracts).
   **Slice 11 (GCS leg — Plan A 14b + 46a, Asensus M1b) DONE (2026-06-05):**
   `internal/storage/gcsreader.go` behind the IDENTICAL BlobStore seam —
   **ETag-mapping pin**: change-detect identity = GCS `Generation` (monotonic
   int64 as a decimal string) + `Updated`, NOT MD5/CRC32C; slots into the
   existing `(etag,size,last_modified)` triple with zero indexer/schema
   change (verified bidirectionally: overwrite in fake-gcs → only that file
   re-extracts; restore → re-extracts back). Mirrors s3.go's as-built
   classify-only idiom (NO storage-layer retry/semaphore — s3.go has
   neither; documented deviation from the 14b plan body). Config is now the
   `storage.{s3 XOR gcs}` tagged union (exactly-one-of fail-fast;
   `Default()` stays S3/Minio) feeding a `storage.New` dispatcher; main.go
   is backend-neutral (`storageIdentity`). Deps: cloud.google.com/go/storage
   v1.43.0 + google.golang.org/api v0.187.0 (clean under go 1.23). ADC
   baseline, `credentials_file` dev-only, `STORAGE_EMULATOR_HOST`
   auto-handled by the SDK (empirically verified — no explicit
   WithoutAuthentication). Dual-leg harness: `infra/fake-gcs/` (fsouza
   fake-gcs-server :4443 + idempotent `seed.sh` uploading EXACTLY the 8
   ground-truth keys — `jkk_dataset02` contains a 9th "(Copy)" duplicate of
   zeg_2, so a bulk preload mount would break count/overlap invariants);
   matrix gained **m8** (GCS leg through BOTH client stacks: list==8,
   zeg_1 33670 mcapdiff-clean, warm-start 0 re-extracts);
   `TestGCS_EmulatorRoundTrip` is STORAGE_EMULATOR_HOST-gated (hermetic
   skip); `make bench-storage` = storage-parity microbench (gcs measured
   105–144% of s3 on loopback; generous 25% floor, the plan's ~10% parity
   documented as a reference-machine criterion). Anti-drift: a GCS-only
   failure fails `make matrix` exactly like an S3-only one; smoke stays
   S3-only, fast, and independent of fake-gcs. All 7 verify gates PASS.
   **Slice 12 (CI — Plan A 46 + 46a activation) DONE (2026-06-06):**
   `.github/workflows/ci.yml`, 4 jobs: unit (vet + gofmt + `go test ./...`
   — the default suite is PROVEN hermetic: passes network-less in a clean
   golang:1.23 container with no dataset/Minio, exactly 2 guarded skips),
   race, seam (the no-cloud-sdk-leak grep, sabotage-verified non-vacuous,
   + committed-wire-bindings compile check — **NO protoc in CI**: the plan
   body's apt-protoc/`make proto` steps are superseded by the checked-in
   Go bindings, recorded deviation), and integration with matrix
   `backend: [s3, gcs]`, fail-fast:false, BOTH legs blocking (the gcs leg
   is REAL — minio + fake-gcs service containers at the exact `infra/`
   image pins). The CI legs run `internal/ws/ci_integration_test.go`
   (`//go:build ci_integration`, invisible to the default suite):
   table-driven per-backend assertions, zero backend branching, over 3
   deterministic synthetic MCAPs from `internal/genmcap` +
   `cmd/gen-ci-fixtures` (chunked + summarized + Statistics — the format
   codec REJECTS unsummarized files; 120/130/90 msgs, time-disjoint;
   stitched 340) — cold-extract counts, full/subset/window/stitched
   round-trips, warm-start 0 re-extracts. `make ci-integration` is the
   local driver (own emulators on fresh high ports 19010/14450, teardown
   trap; corrupted-fixture sabotage fails 4 assertions loudly). NOT pushed:
   activating CI on GitHub requires a push to origin — a user decision.
   NOTE: the local clone is ~20 commits AHEAD of origin/main and 0 behind
   (origin still sits at the pre-implementation docs commit 1e344c6); a
   push publishes the entire implementation history — nothing to reconcile.
   **Slice 13 (audit-gap closure, 11 items) DONE (2026-06-06):** the
   full-plan audit's left.local list minus WASM. A1 LICENSE
   (MIT — pinned verbatim by Plan A Task 1 Step 4); A14 shared
   `storage.retryWith` (50–800ms, permanent short-circuits,
   ctx-cancel-aware) now wraps ALL 6 S3+GCS call bodies + retry/classify
   unit tests; A24a `internal/authn` seam — WS bearer auth goes through
   `ClientAuthenticator` with `crypto/subtle.ConstantTimeCompare` (was a
   plain `!=`), dev-anonymous preserved; A45 CompressionCPU (~99 MB/s) +
   BackpressureLatency (p99 ~321ms over a real 1200-batch backpressured
   stream) benches; B3 `BackendCaps` parsed+exposed in C++ (CLI `hello`
   prints it; live test pins the server-populated values); B13 CLI `debug`
   verb (first-N, no file; the as-built sink-abort contract is
   eos=Cancelled + 'download aborted by sink' marker — asserted); B14
   no-Qt ldd ctest guard (negative-tested); C2 genmcap dimensions
   (zstd/none/large/tiny/metadata-tags) + `CorruptChunkBody` + rejection
   test — **two codec findings**: chunk `UncompressedCRC` is read but
   NEVER verified (integrity surface = the zstd decode failure, asserted
   as such), and **LZ4 chunks cannot be decoded** (codec uses
   `lz4.UncompressBlock` raw-block but writers emit LZ4 FRAMES —
   `chunks.go:411`; fix scoped to Slice 14); ci-integration now 5
   fixtures/375 msgs both legs; C8 nightly `bench.yml` (soft gate,
   artifact, actionlint-clean); C9 `scripts/RUNBOOK.md` (operator runbook,
   port map, lockstep pins); D11 plugin-load smoke ctest (dlopen + both
   extern-C vtables); D12 plugin README + `cli_url_resolve.hpp`
   (explicit>env>default, unit-tested; fixed latent empty-env bug).
   ctest 29/29 both modes; all 11 verify gates PASS.
   **Slice 14 (LZ4 fix + D6 + D8) DONE (2026-06-06):** the LZ4 codec bug is
   FIXED — `decompressChunk` decodes LZ4 **frames** (`lz4.NewReader`, per
   the MCAP spec + foxglove writer/lexer), `ci_synth_f_lz4` joined the CI
   fixture set (6 fixtures/420 msgs both legs), corruption rejection
   table-driven {zstd,lz4}. **D6 (ADAPT)**: no clean libsecret here
   (sudo-only dev pkg; conan recipe = from-source glib toolchain; Plan D
   A3 authorizes the fallback) → `src/credential_store.{hpp,cpp}` seam
   (get/set/erase by normalized URL) with an atomic 0600-file JSON backend
   under XDG_CONFIG_HOME — the api_key NO LONGER persists in plaintext
   SettingsView (cert_path/allow_insecure stay there; they are not
   secrets); libsecret is a drop-in behind the seam; dialog-only, CLI
   stays env/flag (no-Qt guard intact). **D8**: server caps are LIVE —
   vocabulary = 8 derived keys UNION distinct `tags_effective` keys
   (`catalog.DistinctMetadataKeys`), `supports_file_hierarchy` = any
   s3_key contains '/' (object key, not topics; flat nissan corpus →
   false). Client ADAPT: PanelEngine's widget whitelist has NO QTreeWidget
   branch (plan's tree unrenderable — grounded at widget_binding.cpp), so
   hierarchy is an additive prefix-filter combo (`comboPrefix` +
   `hierarchy_prefix.h`) over seqTable, hidden when off; caps arrive via
   the new `capabilitiesReady` worker callback; B3 live test asserts the
   derived vocabulary (subset+sorted, step-h-transient-immune). ctest
   31/31 both modes. NOTE: repeated API-529s killed the workflow verifier
   3x — verification was re-run INLINE (full -race, smoke, matrix,
   ci-integration, scope/UI/proto checks all green).
   **Slice 15 (WASM compile path — M2c-DEX minimum + risk-8 spike) DONE
   (2026-06-06):** `toolbox_dexory_cloud/wasm/` — `build.sh` compiles the
   PURE client decode core (session_key/session_cache/stitch_select/
   hierarchy_prefix/cli_url_resolve + a vendored, source-regenerated
   zstd-1.5.7 DECODER amalgamation matching the native pin) to a ~146KB
   .wasm and RUNS it under node: re-executes the native unit assertions
   and decodes a REAL native-encoded ZSTD frame byte-for-byte (+
   corrupt-frame rejection). Risk-8 findings in `wasm/README.md`:
   ixwebsocket/std::thread can never target browsers (transport = future
   JS-WebSocket binding); TLS/--insecure does not translate; protobuf
   COMPILES to wasm objects (tracked non-gating) but LINKING needs a
   protobuf-lite+abseil cross-build (20 undefined symbols) or a forbidden
   LITE_RUNTIME proto change; no browser demo claimed (external PJ4-WASM
   work stream). ONE existing-TU edit: session_decode's LZ4 branch behind
   `#ifdef __EMSCRIPTEN__` (native byte-for-byte unchanged).
   `.github/workflows/wasm.yml` = separate NON-REQUIRED job (the plan
   mandates WASM never gates paid-core). All 5 verify gates PASS.
   **Slice 16 (toolbox parser delegation — host tail slots + plugin sheds all
   decoders) DONE (2026-06-10):** the GROUNDED-FACT-1 gap is CLOSED BY DESIGN,
   not worked around — `PJ_toolbox_runtime_host_vtable_t` gained two
   ABI-appendable tail slots `create_parser_ingest`/`release_parser_ingest`
   (offsets 24/32 sentinel-pinned, struct_size-gated, no protocol bump; the
   documented ABI-v5 growth mechanism), backed by per-toolbox-dataset
   `DataSourceRuntimeHost` contexts owned by `ToolboxRuntimeHost`
   (`ParserIngestDeps{catalog, registrar}`; MainWindow wires the
   ExtensionCatalogService + a GUI-marshalled SessionManager registrar) — the
   ENTIRE file-load parser pipeline (catalog lookup, classifySchema,
   ObjectStore registration with `builtin_object_type` metadata, render-parser
   registration) runs UNCHANGED on toolbox datasets. SDK repackaged 0.6.1
   (recorded deviation: bump_core_version.py bypassed; package created from
   the edited in-tree SDK). Plugin side: `src/parser_ingest_driver.{hpp,cpp}`
   binds once per topic + pushes raw CDR per message; **rosx_introspection,
   RosDecodeDriver, cdr fixtures and the triple-parity tests are DELETED** —
   the connector ships ZERO message decoders again (the Slice-3/5 amendment is
   retired); scalars now come from parser_ros's SPECIALIZED handlers, closing
   the documented Imu/PoseStamped parity gap (file-open == cloud-fetch trees).
   TWO EMPIRICAL PINS (ToolboxParserIngestRealRosTest, env-gated on
   PJ_REAL_ROS_PARSER_DIR): `parser_config_json` MUST be non-empty ("{}") or
   parser_ros silently degrades to generic scalar-only (loadConfig is the only
   path to specialized-handler registration), and type names pass VERBATIM
   (`tf2_msgs/msg/TFMessage`; parser_ros normalizes internally). tf payloads
   classify kFrameTransforms end-to-end (metadata `builtin_object_type` is
   "kFrameTransforms" — enum-style `sdk::name()`). Live gates: worker-level
   `DexoryCloudParserIngestLiveTest` (33670 pushes / imu 14904 via the
   FakeIngestHost recorder; cancel releases the context) + the cache-HIT
   resume leg reworked to the recorder; smoke step d gained the
   parser-ingest live leg. Host tests 11/11 + real-ros test; plugin ctest
   29/29 hermetic; smoke + matrix PASS. Plugin .so has zero RosMsgParser
   symbols; CLI still links zero Qt.
   **GROUNDED FACTS (do not re-litigate):** (1) **[SUPERSEDED: Slice 16]**
   parser dispatch (`ensureParserBinding`/`pushMessage`) originally lived ONLY on
   `DataSourceRuntimeHostView` ("pj.runtime.v1"), never registered for Toolboxes —
   true through Slice 15 and the reason for the in-plugin rosx amendment; Slice 16
   closed it BY DESIGN with the `create_parser_ingest`/`release_parser_ingest`
   tail slots (toolboxes now reach the full parser pipeline; see the Slice 16 entry);
   (2) a `FileSourceBase` WITHOUT `file_extensions` is UNREACHABLE in the host
   (extension-gated, `FileLoader.cpp:141-149`). Remaining: qtkeychain (D T6 —
   needs grounding: plugin is Qt-free, qtkeychain would pull Qt6; libsecret is
   the alternative), push + CI activation on GitHub (workflows are authored +
   locally verified; origin/main is ~20 commits stale at 1e344c6 — pushing
   publishes the entire implementation history, a user decision),
   specialized-handler flatten parity (Imu/PoseStamped) [CLOSED: Slice 16 — parser delegation delivers specialized handlers end-to-end], the 200 MB/s
   reference-machine bench gate (M2a SOW item; `make bench` asserts a
   localhost regression floor only), real-S3 + real-bucket M1 run pending
   client access, byte-for-byte→logical-equality SOW wording (commercial).
   **From the 2026-06-05 verbatim audit (47 reqs checked):** all technical
   follow-ups landed in Slice 10 (estimate gate, L3 matrix legs); the one
   recorded clarification stands — `{s3,gcs}` dual-leg gate text in
   unified-plan M1c is Asensus-M1b scope, not a Dexory M1 gate.

**Plugin-shape note:** the endpoint plugin is a Toolbox (like Mosaico) for now; Plan D
specifies a DataSource shape for the final integration — reconcile at plumbing time
(recorded in `arch/2026-06-04-two-endpoints-approach.md`).

## Reference codebases (MANDATORY context — always reuse these)

Any agent doing design or implementation work here **must** ground itself in these
local codebases first. Do not guess SDK/plugin APIs — read the real headers. Paths below
were verified on this machine on 2026-06-04.

**Vendoring model — version-pinned PRIVATE fork submodules (2026-06-13).** PJ4, its
nested `plotjuggler_sdk`, and `pj-official-plugins` are **git submodules** of this repo,
each pinned to the `cloud` branch of a **private** fork under the PlotJuggler org (never
public — see [[private-repos-only]]):

| Path | Submodule → fork @ branch | Upstream base |
|---|---|---|
| `PJ4/` | `PlotJuggler/PJ4-cloud` @ `cloud` | `PlotJuggler/PJ4` @ `a19d49e` (Qt 6.11.1) |
| `PJ4/plotjuggler_sdk/` (nested) | `PlotJuggler/plotjuggler_sdk-cloud` @ `cloud` | `PlotJuggler/plotjuggler_sdk` @ `8f485e5` (v0.8.0) |
| `pj-official-plugins/` (**sibling** of `PJ4/`) | `PlotJuggler/pj-official-plugins-cloud` @ `cloud` | `PlotJuggler/pj-official-plugins` @ `bb0ebd5` |

Each `cloud` branch = `upstream-base + our-delta`; `git log <upstream-base>..cloud` in a
fork is exactly our changes (`git submodule status` records the pinned commit). The
**connector plugin lives in THIS repo** at `plugin/toolbox_dexory_cloud/` — it builds
standalone against the forked SDK Conan package (0.8.1), NOT inside the plugins submodule.
The original `/home/gn/ws/PJ4` is the pristine upstream — read it for reference, **never
modify it**. The reference section below cites pristine `/home/gn/ws/PJ4/...` paths for
reading; when **editing**, edit the submodule copy at `<repo>/PJ4/...` (or
`<repo>/pj-official-plugins/...`).

**Editing a vendored tree:** edit inside the submodule, commit to the fork's `cloud`
branch, push, then `git add <submodule>` here to bump the pointer. **Sync to a new
mainstream release:** `cd <submodule> && git fetch upstream && git merge <new-tag>` on
`cloud`, push, bump the pointer. A fresh clone needs `git submodule update --init
--recursive` + read access to the private forks (deploy key / token for CI). Migration
rollback tags: `_pre_rewire` (after the plugin relocation, before the submodule swap),
`_pre_submodule` (before everything).

### 1. `/home/gn/ws/PJ4` — the PlotJuggler 4 application workspace

The product this connector ultimately plugs into. Read first:

- `/home/gn/ws/PJ4/CLAUDE.md` — project handbook (module placement, build, conventions).
- `/home/gn/ws/PJ4/PJ4_PLAN.md` — authoritative architecture doc (three-level
  architecture; plugin discovery/load model; `IDataWidget` contract).
- `/home/gn/ws/PJ4/plotjuggler_sdk/` — **the plugin SDK** (what the future `pj_cloud`
  plugin compiles against). Key headers:
  - `pj_base/include/pj_base/sdk/data_source_patterns.hpp` — `PJ::FileSourceBase`
    (one-shot importer; Plan D's chosen base) and `PJ::StreamSourceBase`.
  - `pj_base/include/pj_base/sdk/data_source_host_views.hpp` — `ParserBindingRequest`
    (line ~90), `ensureParserBinding` (~191), `pushMessage` (~241) and the **payload
    lifetime contract** (closure returns `sdk::PayloadView` zero-copy + anchor, must be
    idempotent/thread-safe; lines ~211–301). This is the delegated-ingest seam Plan D
    Task 4 builds on.
  - `pj_base/include/pj_base/sdk/data_source_plugin_base.hpp` —
    `PJ_DATA_SOURCE_PLUGIN(Class, manifest)` macro (line ~239). The class **must be
    default-constructible** (factory calls `new Class()`; config arrives via
    `loadConfig()`).
  - `pj_plugins/include/pj_plugins/sdk/dialog_plugin_base.hpp` — `PJ_DIALOG_PLUGIN(...)`
    (variadic: legacy 1-arg + manifest 2-arg forms) and the real dialog callback surface
    (verify exact handler names here before implementing).
  - `pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp` — `ToolboxPluginBase`
    (Mosaico's base; `pj_cloud` is a DataSource instead — see Plan D §0 note 1).
  - `pj_base/include/pj_base/sdk/plugin_data_api.hpp` — direct-ingest write API
    (`ensureTopic`/`ensureField`/`appendRecord`); NOT used by `pj_cloud` (delegated
    ingest), listed for orientation.
- Build: `./build.sh` (Conan 2 + CMake, Qt 6.11.1 from `.qt/` via `install_qt6.sh`), run via `./run.sh`
  (defaults plugin discovery to `pj-official-plugins/build`).

### 2. `/home/gn/ws/PJ4/pj-official-plugins` — the official plugin collection

How real PJ4 plugins are shaped, built, and shipped. **Note: on this machine it lives
*inside* `PJ4/`** (the original author had it as a sibling). Read first:

- `CLAUDE.md`, `PLUGIN_DEVELOPMENT.md`, `porting_guide.md` — plugin shapes
  (self-parsing vs delegating DataSource, MessageParser catalog pattern), data-write
  rules, mechanical-translation policy.
- `SDK_VERSION` — single source of truth for the SDK pin (currently `0.8.1`, a
  local-fork bump = mainstream v0.8.0 + the parser-ingest tail slots; the package
  is `conan create`d from the in-tree `PJ4/plotjuggler_sdk`, NOT the extern
  submodule — `./build.sh` does this automatically if the package is absent); every
  plugin's `conanfile.py` reads it live. Never hardcode the SDK version.
- Build: `./build.sh [plugin_dir]` (Conan 2 + CMake, C++20, `-Wall -Wextra -Werror`…).
  CMake helpers: `pj_embed_ui` / `pj_embed_manifest` (local `cmake/`),
  `pj_emit_plugin_manifest` (ships with the SDK package — emits the `.pjmanifest.json`
  sidecar the host scans pre-dlopen).
- Closest analogs for this project:
  - `data_load_mcap/` — how PJ4 loads MCAP today (`McapSource : FileSourceBase`,
    delegated ingest, lazy `PayloadView` closures). **`contrib/mcap/` vendors a full
    MCAP writer too** (`writer.hpp`/`writer.inl`) — reusable for round-trip work.
  - `data_stream_foxglove_bridge/`, `data_stream_pj_bridge/` — WS-streaming sources;
    the threading discipline to copy: WS callback thread only *queues*; host calls
    (`ensureParserBinding`/`pushMessage`) only from the poll thread. pj_bridge also
    shows ZSTD-batch framing (nearest analog to our wire batches).
  - `data_stream_zmq/` — minimal delegated-ingest streaming reference.
  - Transport note **[AMENDED 2026-06-05, audit-verified]**: the design spec §9 /
    Plan B envisioned a Qt-WebSockets `client-core`; the **as-built transport is
    ixwebsocket** (the in-repo plugin convention — `src/backend_connection.*`), and
    `dexory-cloud-cli` links **zero Qt** (`ldd`-verified). There is no standalone
    `client-core` library; the transport units compile into the plugin and the CLI.
    Plan B's Qt shape is superseded — do not "restore" QtWebSockets without a new
    decision.

### 3. `/home/gn/ws/PJ4/pj-official-plugins/toolbox_mosaico` — the UI/worker design source

(Directory name has an **underscore**, not a hyphen.) The Mosaico cloud-browser plugin
whose dialog/state/worker design Plan D and the unified plan **lift**: connect-to-URI +
history, Lua metadata query engine (`src/query/`), three-panel UI
(`ui/mosaico_panel.ui`), command-queue worker + `onTick`-drained event queue
(`src/fetch_worker.{hpp,cpp}`, `src/mosaico_dialog.{hpp,cpp}`), `host_write_mu_`
serialization, `src/settings_store.hpp` (over `PJ::sdk::SettingsView`),
`src/server_history.h`, TLS/cert handling (`src/tls_utils.h`). What gets **swapped**:
its Arrow-Flight transport (`MosaicoClient`) → our `client-core` WS+Protobuf, and its
Arrow ingest (`src/arrow_ingest.*`) → raw-record forwarding to host MessageParsers.

### Path translation (for any residual stale reference)

| In the docs (original author's machine) | On this machine |
|---|---|
| `/home/davide/ws_plotjuggler/pj-cloud` | `/home/gn/ws/PJ4_Server_Template/pj-mcap-server` (this repo) |
| `/home/davide/ws_plotjuggler/PJ4` | `/home/gn/ws/PJ4` |
| `/home/davide/ws_plotjuggler/pj-official-plugins` | `/home/gn/ws/PJ4/pj-official-plugins` |
| `~/ws_plotjuggler/` | `/home/gn/ws/` (approx.; layout differs as above) |

## Documents (read in this order)

0. `arch/2026-06-04-two-endpoints-approach.md` — **the ACTIVE execution approach** (endpoints
   first, then plumbing; vendored-tree rules; the Toolbox-vs-DataSource reconciliation note).
1. `arch/2026-05-28-pj-cloud-connector-design.md` — **the canonical design spec (single source
   of truth).** 14 sections: architecture, repo layout, catalog/SQLite model, wire protocol,
   Go server design, Qt client design, failure/resume, testing strategy, phased build order.
   The plans below all reference this spec and must not contradict it.
2. `arch/2026-06-03-unified-cloud-connector-plan.md` — **the unified plan** (Dexory S3 +
   Asensus GCS, one codebase): the six abstraction seams (`BlobStore`, `FormatCodec`,
   `ClientAuthenticator`, …), milestones M0/M1a/M1b/M1c/M2a/M2b/M2c, testing matrix,
   risks, resolved + open commercial items. Where it **[REVISES …]** a source doc, it wins.
3. `arch/2026-05-28-pj-cloud-server-v1.md` — **Plan A**: Go server, 46 tasks + letter-suffixed
   seam tasks (14a storage, 14b GCS, 15a format, 24a authn, 46a CI matrix), `- [ ]` checkboxes.
4. `arch/2026-05-28-pj-cloud-client-cpp.md` — **Plan B**: Qt C++ test client (`client-core` lib +
   `client-cli` exe), tasks 1–14 + 8a (SessionKey). Depends on Plan A's `proto/pj_cloud.proto`.
5. `arch/2026-05-28-pj-cloud-integration.md` — **Plan C**: cross-language E2E correctness
   harness (Docker + Minio + fake-gcs + round-trip MCAP logical diff), tasks 1–9 + 8a
   (GCE smoke). Depends on the binaries from A & B.
6. `arch/2026-06-03-pj-cloud-pj4-plugin.md` — **Plan D (DEFERRED, M2b)**: the PJ4 DataSource
   plugin lifting `client-core` + the Mosaico dialog design. Read its §0 grounding notes —
   they correct spec assumptions against the **real** SDK headers (e.g. no
   `launchCustomOpenDialog`, no URI-scheme hook).
7. `arch/2026-06-01-dexory-proposal.md` (source) + the rendered `docs/2026-06-01-dexory-proposal.{html,pdf}` — the commercial proposal. `*.md` is the
   source; `*.html` and `*.pdf` are generated artifacts (do not hand-edit them).
   (`docs/pj-cloud-connector-overview.html` is likewise a generated overview artifact.)

`proto/pj_cloud.proto` (defined in Plan A) is the **canonical wire schema** shared by all
plans — treat it as the single source of truth for the protocol. The **Go** bindings are
checked in (`server/internal/wire/pj_cloud/`); the **C++** bindings are generated at
build time by the Conan protoc (never the system protoc — version must match the
linked libprotobuf).

## The headless SDK harness — the regression gate (2026-06-04)

**All backend (server) work MUST keep `make smoke` green — run it before declaring any
slice done.** The harness proves the whole pipeline without the GUI:

- `make smoke` (= `scripts/smoke.sh`): ensures Minio is up + bucket seeded → builds and
  starts its **own** server on **:8081** (never touches the interactive `:8080`
  instance; always reaps its server on exit) → Go `devprobe` ground-truth assertions →
  plugin ctest twice (hermetic: live tests **skip**, error tests pass; live: everything
  runs and passes) → CLI spot checks through **both** client stacks (the C++
  `dexory-cloud-cli` — the exact `BackendConnection` the GUI uses — and `devprobe`).
  Final line `SMOKE PASS` / `SMOKE FAIL: <step>`; exit code matches.
- `make matrix` (= `scripts/matrix.sh`): the deeper, slower machine-local gate
  (own server on **:8082**; hard-requires the `/home/gn/ws/jkk_dataset02`
  originals). Legs m1–m8: half-topics (23930), none-matching (0),
  outside-range (0), boundary-spanning stitched window (29461), 8-file FULL
  stitch (337861 — the corpus-bound reading of the spec §11 “10-file” cell;
  monotonicity via `mcaptopics --monotonic`), 4 parallel sessions
  (each mcapdiff-clean), duplicate-seq exit 2 + overlap rejection, and
  **m8 = the GCS dual-leg** (fake-gcs :4443 via `infra/fake-gcs/`, seeded
  with the same 8 keys; both client stacks; warm-start 0 re-extracts;
  anti-drift — a GCS-only failure fails the matrix). Final line
  `MATRIX PASS` / `MATRIX FAIL: <leg>`.
- `dexory-cloud-cli` (built by `./build.sh toolbox_dexory_cloud`, lands under
  `build/toolbox_dexory_cloud/Release/toolbox_dexory_cloud/`): `hello` / `list [--json]`
  / `topics <sequence> [--json]` / `download <seq…> [--topics …] [--time-range …]
  --output …` (variadic = stitched; duplicate → exit 2) / `tag`, `--url` or
  `DEXORY_CLOUD_URL` (default `ws://localhost:8080`), `--insecure` for
  self-signed `wss://`. Exit 0/1/2 = ok/connection-failure/usage.
- Ground truth is pinned in TWO places that must move in lockstep when the bucket is
  reseeded: `scripts/smoke.sh` constants and
  `toolbox_dexory_cloud/tests/backend_connection_live_test.cpp` (8 sequences; 6 topics
  + imu==14904 for `nissan_zala_50_zeg_1_0.mcap`).
- `make server-start` / `make server-stop` manage the interactive `:8080` instance
  (`/tmp/pj-cloud-server.{pid,log}`).

## Commands

### One-command local bring-up (the tester-facing path, 2026-06-12)

**Root `./build.sh` + `./run.sh`** make the whole stack runnable from a fresh checkout
with **synthetic** data — no AWS, no credentials, no manual seeding (the steps that were
easy to forget). Use these unless you have a reason not to:

```bash
./build.sh   # builds server + dev tools (server/bin/, direct `go build` — NO protoc
             # needed, the wire bindings are checked in) + the plugin (+ the GUI app
             # if Qt is installed; otherwise it prints the one-time Qt install command).
./run.sh [--dexory_minio]  # LOCAL (the default): docker compose up Minio -> SEED synthetic MCAPs INTO
             # `recordings` IFF the bucket is empty (your own corpus is never clobbered) -> server on :8080.
./run.sh --dexory_aws      # Dexory staging bucket (AWS S3): no Minio/seed; -config config.dexory-staging.yaml,
             # :8084, AWS_PROFILE auto-defaults to dexory-staging. First scan ~1s/file over WAN (synchronous).
./run.sh --asensus_google  # Asensus GCS: -config config.asensus-staging.yaml (TEMPLATE — refuses with guidance
             # until you replace the REPLACE_ME bucket/prefix), :8085, creds = ADC. (Plan A 14b GCS leg / Asensus M1b.)
./run.sh <config.yaml>     # power-user escape hatch: any S3/GCS server config.
             # ONE server at a time (shared /tmp/pj-cloud-server.{pid,log}); `make server-stop` to switch targets.
             # Stop LOCAL Minio too: `(cd infra/minio && docker compose down)`. Targets validated 2026-06-12
             # (dexory_aws = 34 real sequences on :8084; dexory_minio = local corpus on :8080; asensus_google = template-guard).
```

Seeding is a new tool, **`server/cmd/seed`** (`go build -o bin/seed ./cmd/seed`): a tiny
idempotent S3 uploader. `seed -check` exits 0 (empty/seed-needed) / 3 (has data/skip) —
`run.sh` uses it as the Minio readiness probe AND the skip gate; `seed -dir <fixtures>`
uploads `*.mcap` (path-style Minio defaults from `config.Default()`; `-bucket`/`-endpoint`
for any S3). `run.sh` generates the fixtures with the existing `gen-ci-fixtures` (browsable
multi-topic recordings) + `gen-3d-fixture` (one /tf + pointcloud recording for the 3D scene).
**`run.sh` does NOT use `make server-start`** (that target depends on `make build` → `make proto`
→ needs protoc); it starts `server/bin/pj-cloud-server` directly, writing the same
`/tmp/pj-cloud-server.{pid,log}` so `make server-stop` still works. **Verified end-to-end
2026-06-12** (skip-path against the maintainer corpus + upload-path against a throwaway bucket).

### Runnable today (the underlying manual steps `./run.sh` automates)

```bash
# END endpoint: local S3 (bucket `recordings`; console :9001, admin/password123)
cd infra/minio && docker compose up -d

# START endpoint: build the connector plugin (standalone, in THIS repo).
# Easiest: ./build.sh from the repo root builds server + SDK pkg + official
# plugins (from the fork) + the connector + the app, and stages the .so. Or just
# the connector:
cd plugin/toolbox_dexory_cloud \
  && conan install . --output-folder=build --build=missing -s compiler.cppstd=20 \
  && cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release \
  && cmake --build build -j"$(nproc)"   # -> plugin/toolbox_dexory_cloud/build/bin/

# Run PJ4 — ALWAYS the VENDORED app (the PJ4/ submodule; its run.sh auto-loads
# plugins from the sibling ../pj-official-plugins/build/all/Release/bin). NEVER
# `cd /home/gn/ws/PJ4 && ./run.sh`: that is the PRISTINE app, which carries NONE
# of the host-side changes (SDK tail slots, RangeSlider markers, widget
# bindings, …) — the plugin .so still loads there, so plugin features appear
# while host features silently vanish, which looks like "nothing changed".
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4 && ./run.sh
# After a host edit (in the PJ4/ submodule): rebuild the vendored app (./build.sh
# in PJ4/), and commit the edit to the PJ4-cloud fork's `cloud` branch.
# After a connector edit: rebuild plugin/toolbox_dexory_cloud and re-stage its .so
# into ../pj-official-plugins/build/all/Release/bin (the root ./build.sh does this).
```

Render the proposal `*.md` → self-contained `*.html`:

```bash
python3 _render_proposal.py          # needs the `markdown` pip package
```

PDF is then produced from that HTML with headless Chrome
(`google-chrome --headless --print-to-pdf=docs/2026-06-01-dexory-proposal.pdf docs/2026-06-01-dexory-proposal.html`).
Editing the proposal = edit the `.md`, re-run the script, regenerate the PDF.

### Once implementation lands in this repo (per the plans)

- Go server (Plan A, top-level `Makefile`): `make proto` (generate bindings) ·
  `make build` · `make test` (unit + race) · `make integration` (needs Docker for Minio) ·
  `make bench` (v1 benchmark gate). Single test: `cd server && go test ./internal/<pkg>/ -run <Name>`.
- Qt C++ client (Plan B): Conan 2 + CMake ≥ 3.21, C++20. Two targets `client-core`
  (Qt-aware static lib, **no `Qt6::Widgets`**) and `client-cli` (`QCoreApplication` exe),
  tests via gtest/ctest.
- Integration (Plan C): `docker-compose up -d --build` (Minio + fake-gcs + server),
  `go run ./cmd/gen-fixtures --out fixtures`, then the `go test` matrix driver
  (`PJCLOUD_BACKEND` pins one of `{s3,gcs}`; unset runs both legs).

## Architecture being designed (the big picture)

A single WebSocket per client carries everything, multiplexed via a Protobuf envelope:
catalog RPCs **and** session data streaming on one connection. Streaming is
**bounded-horizon, as-fast-as-possible** (a bulk download with a known size), *not*
wall-clock-paced playback. "Streaming" here means *incremental/progressive download* — bytes arrive
in batches so the client can show already-received data while the rest downloads in the background —
**not** real-time pacing. Supports reconnect-and-resume (short retain window) and
cancel-mid-stream.

- **Go server** — one static binary, five subsystems on one TCP listener: Catalog
  (WS RPC + SQLite reads, WAL + single writer goroutine), Session (WS streaming + storage
  fetcher with producer/consumer split for resume), Indexer (background storage poller),
  Dashboard (read-only HTML + Prometheus `/metrics` + `/health`). Pure-Go SQLite (no cgo).
  Storage/format/auth go through the unified plan's seams: `BlobStore` (S3 + GCS impls),
  `FormatCodec` (one MCAP impl), `ClientAuthenticator` (bearer token). Only
  `internal/storage` may import a cloud SDK.
- **Qt C++ client** — `client-core` owns the WS/protocol/decompression and exposes a
  `SessionSink` seam; `client-cli`'s `McapWriterSink` reconstructs the streamed session as a
  local MCAP file. **`client-core` is deliberately Widgets-free so the PJ4 DataSource
  plugin (Plan D, deferred to M2b) can lift it in unchanged.**
- **Integration harness** — deterministic MCAP fixture matrix (compression, payload sizes,
  multi-file stitching, tags, time-range edges), run on **both** `{s3,gcs}` legs. The v1
  gate: original MCAP → server → CLI → reconstructed MCAP must be **logically equal** on
  `(topic, log_time, payload, publish_time, schema name/encoding/data)`.

Server-side **stitching**: selecting N consecutive MCAPs presents one continuous logical
session (one time range, union of topics, ordered stream). The client commits to
`(file_ids[], topic_names[], time_range)` before streaming; the server returns pre-flight
estimates (`estimated_chunk_bytes`, `approximate_messages`).

## Working conventions

- The plans are written **for agentic execution**: each starts with a required
  sub-skill (`superpowers:subagent-driven-development` or `superpowers:executing-plans`) and
  uses `- [ ]` checkboxes for task tracking. Follow that workflow when implementing them, and
  follow the spec's **§13 phased build order** (refined by the unified plan's §5 milestones)
  for sequencing across plans.
- **Before implementing anything that touches PJ4 or the plugin SDK, consult the
  "Reference codebases" section above and read the real headers** — Plan D §0 exists
  precisely because the spec named APIs that don't exist in the SDK.
- Engagement shape (from the proposal + unified plan): **Milestone 1** = PoC (Go server +
  Qt CLI, round-trip validated), gated on written client approval before **Milestone 2** =
  hardening + PlotJuggler plugin integration (with a browser/WASM bonus). Keep v1 work
  inside the M1 non-goals (no multi-tenancy, no realtime pacing, no PJ4 plugin, single
  shared bearer token; Dexory M1 gate = whole-file topic filtering — tag editing and
  intra-file time windows are M2 scope for Dexory).
