# History — PJ Cloud Connector

Implementation history of the PJ Cloud Connector — provenance record, not current
instructions; see `CLAUDE.md` for the live picture. Entries describe the state AT
THEIR TIME and may be superseded by later entries (including later entries in this
same file). Content below is preserved verbatim from earlier `CLAUDE.md` revisions
(only section-heading levels were adjusted for standalone navigation; no wording
was changed).

## Approach (2026-06-04, historical): two endpoints first, then plumbing

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

**Plugin-shape note:** the endpoint plugin is a Toolbox (like Mosaico) — settled by
Slice 16 (the host parser-delegation tail slots let a Toolbox reach the full parser
pipeline, so no DataSource shape is needed).

## Vendoring model — private fork submodules (historical, 2026-06-13; superseded 2026-06-22)

> **[STALE — corrected 2026-06-22]** The `PJ4/`, `plotjuggler_sdk/`, and
> `pj-official-plugins/` submodules described just below were **REMOVED from this repo**
> (commit `82a8c2f`). PJ4 + the plugins are now managed **externally** as sibling
> checkouts (e.g. `~/ws_plotjuggler/PJ4-cloud`); the connector plugin builds standalone at
> `plugin/toolbox_dexory_cloud/` against the SDK Conan package (now **0.11.0**, not 0.8.1).
> The **only submodule of this repo today is `mcap_catalog/`** (the auryn Python catalog
> builder). The table below is retained only as a historical reference to the fork structure.

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

## Path translation (for any residual stale reference)

| In the docs (original author's machine) | On this machine |
|---|---|
| `/home/davide/ws_plotjuggler/pj-cloud` | `/home/gn/ws/PJ4_Server_Template/pj-mcap-server` (this repo) |
| `/home/davide/ws_plotjuggler/PJ4` | `/home/gn/ws/PJ4` |
| `/home/davide/ws_plotjuggler/pj-official-plugins` | `/home/gn/ws/PJ4/pj-official-plugins` |
| `~/ws_plotjuggler/` | `/home/gn/ws/` (approx.; layout differs as above) |

