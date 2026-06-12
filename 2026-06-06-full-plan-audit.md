# Full Plan Audit — PJ Cloud Connector (2026-06-06)

> **Updated 2026-06-06 (post Slices 13-15):** re-verified every PARTIAL/NOT-DONE row
> against the repo. 18 rows flipped to DONE — Plan A (1/14/24a/45), Plan B (3/13/14),
> Plan C (2/8/9), Plan D (6/8/11/12), Unified (U-SEAM3/U-SEAM-Auth/U-SEAM-Caps/S11-L3).
> Plans A, B, D are now 100% satisfied; C is 90% (only GCE smoke EXT-blocked); the only
> remaining engineering gaps live in the Unified milestone/seam rows and are all either
> EXTERNAL-BLOCKED (real cloud / soak / ref-machine / sign-off) or deferred-by-amendment
> (U-SEAM6 decode inversion). Per-plan stats are now recomputed **directly from the table
> rows** (the prior header miscounted Plan A DONE as 41 vs the 46 actual rows, and used
> Unified Items=35 vs the 37 actual rows); new TOTAL = **88.8%** satisfied (111/125).

Read-only synthesis of six per-document auditor ledgers, sanity-checked for coverage and
spot-corrected against the repo. Status vocabulary: **DONE**, **PARTIAL**,
**SUPERSEDED** (built to a recorded-amendment shape that replaces the plan text),
**NOT-DONE**, **EXTERNAL-BLOCKED** (needs client access / real cloud / written sign-off).
"Satisfied %" counts **DONE + SUPERSEDED** as satisfied.

Recorded amendments that supersede plan text (from CLAUDE.md): (a) code lives in THIS
repo, not a separate `pj-cloud` repo; (b) transport is ixwebsocket compiled into
plugin+CLI — no standalone Qt `client-core`/`client-cli`; (c) "Dexory Cloud" is a
TOOLBOX forever (Plan D DataSource shape superseded by USER PRODUCT DECISION); (d) the
connector DOES ship a decoder (in-plugin rosx `RosDecodeDriver`, user-approved); (e) Go
proto bindings checked in, C++ generated at build; (f) integration harness = as-built
shell gates (smoke/matrix/ci-integration) + in-process Go CI harness, not Plan C's
docker-compose + fake-gcs Go-module shape.

---

## Per-plan completion stats

| Source document | Items | DONE | SUPERSEDED | PARTIAL | NOT-DONE | EXT-BLOCKED | Satisfied % |
|---|---:|---:|---:|---:|---:|---:|---:|
| Plan A (Go server, 1–46 + 14a/14b/15a/24a/46a) | 51 | 50 | 1 | 0 | 0 | 0 | **100.0%** |
| Plan B (Qt C++ client, 1–14 + 8a) | 15 | 4 | 11 | 0 | 0 | 0 | **100.0%** |
| Plan C (integration harness, 1–9 + 8a) | 10 | 6 | 3 | 0 | 0 | 1 | **90.0%** |
| Plan D (PJ4 plugin, 1–12) | 12 | 9 | 3 | 0 | 0 | 0 | **100.0%** |
| Unified + Spec + Proposal | 37 | 20 | 4 | 7 | 0 | 6\* | **64.9%** |
| **TOTAL** | **125** | **89** | **22** | **7** | **0** | **7** | **88.8%** |

\* The 6 EXTERNAL-BLOCKED in Unified are the 6 open *commercial* items (contract/SOW
sign-off), none engineering-resolvable in-repo.

**Item counts are now the actual table-row counts** (Plan A = 51 rows: tasks 1–46 plus the
5 letter-suffixed seam tasks; Unified = 37 rows). The pre-Slice-13 header rolled the 5
Plan-A seam rows into "46" and the 2 extra Unified seam rows into "35"; the recompute
makes the table internally consistent (status columns now sum to Items on every line).

Coverage sanity: every ledger is complete (no truncation). Plan A has all 5
letter-suffixed seam tasks (14a/14b/15a/24a/46a) → 51 rows; Plan B 8a present (15 rows);
Plan C 8a present (10 rows); Plan D full 1–12 (12 rows); Unified covers 6 seams + 2 smaller
seams + 7 milestones + 6 resolved + 6 open-commercial + 4 phased-build + 4 testing-layers
→ 37 rows. Status columns now sum to the Items count on every line.

---

## Spot-check corrections

The original ledger's five surprising verdicts (preserved below as the historical record),
each now **closed by Slices 13–15** — re-verified against the repo on 2026-06-06:

- **U-SEAM3 / Plan A 24a (ClientAuthenticator, was PARTIAL/NOT-DONE → now DONE)** —
  the original audit confirmed the bearer gate was a plain `!=` at `ws/server.go:293`
  with no `internal/authn`. **Slice 13 closed it:** `server/internal/authn/authn.go`
  defines `ClientAuthenticator` + `NewBearerToken` using `crypto/subtle.ConstantTimeCompare`;
  `ws/server.go:312` now routes the Hello token through `c.h.auth.Verify(...)`; tests in
  `authn_test.go` pass.
- **Plan A 36 (deploy, DONE)** — re-confirmed (no regression): `server/deploy/` has
  Dockerfile, docker-compose.yml, pj-cloud-server.service, config.example.yaml, README.md.
- **Plan D 8 / U-SEAM-Caps (hierarchy browser, was NOT-DONE/PARTIAL → now DONE via
  adaptation)** — the original audit confirmed zero `QTreeWidget`/`BackendCapabilities`
  hits. **Slice 14 closed it with a grounded adaptation:** server caps are LIVE
  (`catalog.DistinctMetadataKeys` + `supports_file_hierarchy`, populated in
  `ws/server.go:348`); the client uses a prefix-filter combo (`src/hierarchy_prefix.h`,
  `comboPrefix`) instead of the host-unrenderable `QTreeWidget` (grounded at
  `widget_binding.cpp`). The C++ `BackendCaps` accessor + live test pin it.
- **Plan C 2 (corrupt-chunk fixture, was PARTIAL → now DONE)** — the original audit
  confirmed `genmcap` wrote only valid CRCs. **Slices 13–14 closed it:**
  `genmcap.CorruptChunkBody` + a rejection test, plus compression dimensions
  (ZSTD/LZ4/None) and payload dimensions; the LZ4-frame decode bug surfaced here was
  fixed in Slice 14 (`chunks.go:417` `lz4.NewReader`).

The one verdict not engineering-closable — none here; all five flipped. No previously-DONE
verdict regressed (Plan A 36 re-checked clean).

---

## Plan A — Go server (`2026-05-28-pj-cloud-server-v1.md`)

| ID | Task | Status | Evidence |
|---|---|---|---|
| 1 | Bootstrap repo skeleton | DONE | README/Makefile/.gitignore + `LICENSE` (MIT, Slice 13); no-git-init honored |
| 2 | Go module + skeleton | DONE | `module pj-cloud/server`, go 1.23; main.go is the full wired binary |
| 3 | pj_cloud.proto + M0 edits | DONE | Full envelope, BackendCapabilities, FlatMetadata map, legacy s3_key retained |
| 4 | protoc codegen (Go) | DONE | protobuf v1.34.2; `make proto`; pj_cloud.pb.go checked in; wire_test round-trip |
| 5 | Config loader (tagged union) | DONE | s3-XOR-gcs union, format=mcap default, ${ENV} expand. No `${VAR:-default}` form |
| 6 | Config tests + M0 gcs-only | DONE | config_test.go: ExactlyOneOf/TLS/GCSOnly. Gap: no `${VAR:-}` unit |
| 7 | Catalog Store skeleton | DONE | schema.sql WAL + tags_effective view; single-writer goroutine + panic recovery |
| 8 | File CRUD (override-preserving) | DONE | UpsertFile in-place UPDATE keeps rowid; tests assert id stable + override survives |
| 9 | Topic CRUD | DONE | ReplaceTopicsForFile/ListTopicsForFile + 3 tests |
| 10 | Tags embedded CRUD | DONE | ReplaceEmbeddedTags/EffectiveTags(view)/HasEmbeddedTag + tests |
| 11 | Tag overrides | DONE | SetOverride/UnsetOverride/MaskEmbedded + 5 tests |
| 12 | FilterFiles + pagination + FlatMetadata | DONE | filter.go predicates + cursor; FlatMetadata() M0 shape; full test set |
| 13 | S3 ReadSeekerAt over Range GETs | DONE | rangeReaderAt behind storage seam; exercised via memBlob fake. Gap: no micro-unit |
| 14 | S3 prod adapter + retry policy | DONE | Slice 13: shared `retry.go` `retryWith` (exp backoff, ctx-aware, permanent short-circuit) wraps all S3 Get/Head/List bodies; `retry_test.go` (5 cases) + `TestS3_ClassifyErrors` |
| 14a | internal/storage BlobStore seam (M1a) | DONE | BlobStore iface + S3 impl + New factory + indexer adapters; concurrency lives in session |
| 14b | GCS BlobStore impl (M1b) | DONE | gcsreader.go: Generation+Updated etag pin; 3 tests incl. fake-gcs round-trip |
| 15 | Indexer extractor + fixture helper | DONE | summaryToResult mapping; genmcap deterministic writer; extract-cleanly tests |
| 15a | internal/format codec seam (M1a) | DONE | Codec.Extract/ChunkIndex + NewCodec + TopicInfo; PlanChunks/Iterate; full tests |
| 16 | Indexer scanner (list + diff) | DONE | RunOnce list→signature-diff→skip-unchanged→record-failure; 4 tests |
| 17 | Indexer loop (warm-start + poll) | DONE | StartupScan then ticker; warm DB = 0 re-extracts test |
| 18 | Session plan builder (overlap valid.) | DONE | BuildPlan stitch + pairwise non-overlap + empty-plan + estimates; 6 tests |
| 19 | Retain buffer (bounded, backpressure) | DONE | 256 seqs/64MiB caps, blocking Append, Prune-on-ack; 6 tests |
| 20 | Producer goroutine (ZSTD bodies) | DONE | one-shot ZSTD + NONE singleton fallback + drop; 7 tests |
| 21 | Consumer goroutine + ack | DONE | seq-ordered drain, AckCh prune, Progress, terminal Eos; 4 tests |
| 22 | Registry, eviction, cancel, resume | DONE | Register/Reattach/Cancel/Detach + SessionState ledger; registry+resume tests |
| 23 | Wire envelope helpers | DONE | marshal round-trip + invalid-payload→INVALID_REQUEST + 256/2048 error caps |
| 24 | WS server (upgrade + auth gate) | DONE | /api/ws Accept; Hello protocol+token gate (Handler struct, not Server{}) |
| 24a | internal/authn ClientAuthenticator (M1a) | DONE | Slice 13: `internal/authn` `ClientAuthenticator`/`NewBearerToken` with `crypto/subtle.ConstantTimeCompare`; `ws/server.go:312` routes Hello token through `auth.Verify`; dev-anonymous preserved; `authn_test.go` |
| 25 | WS Connection (priority/bulk channels) | DONE | priorityCh 64 + bulkCh 16, priority-before-bulk drain; conn_test |
| 26 | WS Dispatcher | DONE | inline dispatch switch over all ClientMessage variants + INVALID default |
| 27 | Catalog WS handlers | DONE | ListFiles/GetFile/UpdateTags + full handlers_catalog_test |
| 28 | Session WS handler (OpenFresh) | DONE | openFresh validate→plan→register→response→spawn; 5 lifecycle tests |
| 29 | OpenResume | DONE | reattach or RESUME_NOT_POSSIBLE; resume no-gap/no-dupe test + C++ live leg |
| 30 | Cancel + Ack | DONE | handleCancel/handleAck; Cancel→Eos CANCELLED test; ack-prune test |
| 31 | Wire chunk-reader/iterator to prod | DONE | blobChunkReader Range-GET + FileChunkIndex.Iterate decode; e2e tests |
| 32 | Dashboard scaffolding (Basic-auth) | DONE | auth.go constant-time + 503/401; templates; 4 tests |
| 33 | Dashboard remaining pages | DONE | files/file-detail/sessions/indexer pages; AllPagesRender test |
| 34 | Health + Metrics (Prometheus) | DONE | metrics.go 11 series on private registry; /health 200/503; auth-gated /metrics |
| 35 | main.go full wiring | DONE | storage.New/format.NewCodec/catalog/indexer/registry/ws/TLS/graceful; SDK-leak guard passes |
| 36 | Deployment artifacts | DONE | deploy/{Dockerfile distroless, compose, systemd, config.example, README}; `make docker` |
| 37 | Integration test infra (testcontainers) | SUPERSEDED | Amendment f: ci_integration_test.go in-process over real BlobStore; no testcontainers |
| 38 | Integ: full session COMPLETE | DONE | ci_integration full→EOS_COMPLETE + count (both legs); unit + smoke f1/f3 |
| 39 | Integ: file-overlap rejection | DONE | TestSession_OverlapRejected→INVALID_REQUEST; matrix m7 cross-stack |
| 40 | Integ: cancel mid-stream | DONE | TestSession_Cancel→Eos CANCELLED; devprobe maps CANCELLED |
| 41 | Integ: resume within retain window | DONE | TestSession_ResumeNoGapsNoDupes + C++ live resume (smoke d) |
| 42 | Integ: retain expiry→RESUME_NOT_POSSIBLE | DONE | DetachArmsEvictionTimer + ReattachEvicted→RESUME_NOT_POSSIBLE |
| 43 | Integ: tag override flow | DONE | UpdateTags/UnsetMasksEmbedded unit + smoke h (survives forced reindex). Unset MASKS (as-built) |
| 44 | Integ: dashboard rendering + auth | DONE | dashboard/metrics tests cover 401/200/health/metrics |
| 45 | V1 benchmark gate | DONE | TestThroughputGate + baseline.json; Slice 13 added `BenchmarkCompressionCPU` + `BenchmarkBackpressureLatency` (p99 over a real 1200-batch backpressured stream) in `server/bench/micro_test.go` (both run) |
| 46 | GitHub Actions CI | DONE | ci.yml: unit/race/seam/{s3,gcs} matrix; consumes committed bindings, ci_integration tag |
| 46a | {s3,gcs} param + fake-gcs + parity bench (M1b) | DONE | all 4 pieces; fake-gcs service; storage_parity_test. Real-GCS run EXT-blocked |

## Plan B — Qt C++ client (`2026-05-28-pj-cloud-client-cpp.md`)

| ID | Task | Status | Evidence |
|---|---|---|---|
| 1 | Top-level CMake + Conan (Qt client) | SUPERSEDED | Amendment b: no Qt client; plugin/CLI CMake builds the wire client instead |
| 2 | client-core + protobuf codegen + Expected<T> | SUPERSEDED | C++ bindings generated at build; error channel = bool+out-param+string* (no Expected<T>) |
| 3 | CloudConnection (Hello) + BackendCapabilities + EnvelopeTest | DONE | Slice 13/14: `BackendConnection::backendCapabilities()` parses+exposes `HelloResponse.backend` (`backend_connection.cpp:330`); CLI `hello` prints it; live test pins the server-derived vocabulary |
| 4 | MessageDispatcher (RPC correlation) | SUPERSEDED | In-class pending_ (request_id) + session_inbox_ (subscription_id) split; live-exercised |
| 5 | CatalogClient typed wrappers + flat metadata | SUPERSEDED | listSequences/getTopicMetadata/updateTags; wire_mapping flat metadata + test |
| 6 | SessionSink interface | SUPERSEDED | MessageHandler callback = the consumer-plug-in seam; two concrete sinks |
| 7 | Decompression (ZSTD+LZ4) | SUPERSEDED | session_decode.cpp zstd/lz4 one-shot + corrupt-reject tests |
| 8 | SessionClient (open/route/cancel/resume) | SUPERSEDED | openSessionFresh/downloadSession + decode + ack; decode + live tests |
| 8a | SessionKey (FNV-1a normalize) | DONE | session_key.hpp + 4 verbatim Plan-B test cases; M2b cache also built (Slice 8) |
| 9 | client-cli + CommandDispatch | SUPERSEDED | dexory_cloud_cli.cpp Qt-free main; verb scheme hello/list/topics/download/tag |
| 10 | files list/show/tag (--json metadata) | SUPERSEDED | runList/runTopics/tag; --json carries flat user_metadata; smoke e1 |
| 11 | McapWriterSink | SUPERSEDED | session_download.cpp → vendored mcap writer; mcapdiff round-trip gate |
| 12 | session download command | SUPERSEDED | download verb variadic/stitched; live AllTopics 33670 / Subset 4513 |
| 13 | session debug + live integration test | DONE | Slice 13: distinct `debug` verb (`runDebug`, first-N print, `--limit`, sink-abort = eos=Cancelled asserted) in `tools/dexory_cloud_cli.cpp`; live integration retained |
| 14 | CI build + assert no Qt6::Widgets | DONE | Slice 13: `DexoryCloudCliNoQtGuard` ctest runs `cmake/check_no_qt.cmake` (ldd the built CLI, fail on any `Qt6` link); negative-tested |

## Plan C — Integration harness (`2026-05-28-pj-cloud-integration.md`)

| ID | Task | Status | Evidence |
|---|---|---|---|
| 1 | Module scaffold + docker-compose (s3/gcs cfg) | SUPERSEDED | Amendment f: standalone emulators + in-process PJ_CLOUD_BACKEND config; no combined compose |
| 2 | Fixture generator + canned corpus + corrupt-chunk | DONE | Slice 13/14: genmcap `Compression` dimension (ZSTD/LZ4/None) + large/tiny payload + metadata-tags specs; `CorruptChunkBody` + rejection test (zstd+lz4); LZ4-frame decode bug found here and FIXED in Slice 14 |
| 3 | Upload + CLI exec helpers | SUPERSEDED | mc/curl seeding + in-process Go driver + C++ CLI; no minio-go/subprocess helpers |
| 4 | MCAP diff (logical equality) | DONE | cmd/mcapdiff (topic,log_time,payload,publish_time,schema) + Filter + OverDelivered; smoke/matrix-driven |
| 5 | Compose orchestration helper | SUPERSEDED | ci-integration.sh per-leg readiness + cleanup trap; server in-process not container |
| 6 | Matrix driver over {s3,gcs} | DONE | ci_integration_test.go per-backend, zero branching; both legs required; fake-gcs upload path |
| 7 | Benchmark gate (throughput+parity+baseline) | DONE | throughput_test + storage_parity_test + baseline.json; compare folded into test |
| 8 | CI matrix + nightly bench.yml | DONE | ci.yml {s3,gcs} axis + Slice 13 `.github/workflows/bench.yml` (nightly, soft-gate, artifact, actionlint-clean; runs the in-process A45 benches) |
| 8a | GCE deploy smoke (Asensus) | EXTERNAL-BLOCKED | No gce_smoke.sh/GCE_SMOKE.md/gce-smoke.yml; needs real GCE+ADC+persistent disk |
| 9 | RUNBOOK (harness operator notes) | DONE | Slice 13: `scripts/RUNBOOK.md` (as-built operator runbook — per-gate purpose, port map, ground-truth lockstep pins, failure recovery) |

## Plan D — PJ4 plugin (`2026-06-03-pj-cloud-pj4-plugin.md`)

| ID | Task | Status | Evidence |
|---|---|---|---|
| 1 | Plugin skeleton (FileSourceBase + dialog + manifest) | SUPERSEDED | Amendment c: built as TOOLBOX (PJ_TOOLBOX+PJ_DIALOG); no pj_cloud_source.cpp |
| 2 | Lift client-core Widgets-free | SUPERSEDED | Amendment b: no client-core lib; ixwebsocket units compiled into toolbox; Qt-free |
| 3 | file_ids[]→stitched SequenceRecord→one OpenFresh | DONE | backend_connection + stitch_select.h; reorder→identical-request test; live f5 |
| 4 | RawMcapForwardingDriver → host MessageParser | SUPERSEDED | Amendment d: in-plugin RosDecodeDriver (rosx); triple-parity gate; host dispatch unreachable |
| 5 | In-memory SessionCache (LRU, COMPLETE-only) | DONE | session_cache.hpp keyed by session_key.hpp; zero-transport HIT + LRU tests |
| 6 | AuthProvider + qtkeychain | DONE | Slice 14 (Plan D A3-authorized ADAPT): `src/credential_store.{hpp,cpp}` seam (get/set/erase by normalized URL) with an atomic 0600-file JSON backend under XDG_CONFIG_HOME — api_key no longer persists in plaintext SettingsView; libsecret is the documented drop-in (clean qtkeychain pulls Qt6 into the Qt-free plugin); `credential_store_test.cpp` |
| 7 | CloudOpenDialog (lift Mosaico, swap seams) | DONE | dexory_cloud_dialog + fetch_worker + Lua query/ + 3-panel UI; both seams swapped; handlers present |
| 8 | BackendCapabilities file-hierarchy browser | DONE | Slice 14 (grounded ADAPT): server caps LIVE (`catalog.DistinctMetadataKeys` + `supports_file_hierarchy` from `/`-bearing keys, populated `ws/server.go:348`); client uses an additive prefix-filter combo (`src/hierarchy_prefix.h`, `comboPrefix`) — the plan's `QTreeWidget` is host-unrenderable (no whitelist branch at `widget_binding.cpp`); `hierarchy_prefix_test.cpp` + B3 live caps assert |
| 9 | Tag editing via updateTags | DONE | updateTags RPC + updateTagsAsync + Edit Tags sub-dialog; smoke h; single-selection |
| 10 | Reconnect-resume (OpenResume) | DONE | resume loop + backoff 1/4/16s + RESUME_NOT_POSSIBLE no-fallback; live test + smoke d |
| 11 | Component/integration tests | DONE | Slice 13/14 added the missing ctests: `plugin_load_smoke_test` (dlopen + both extern-C vtables), `credential_store_test`, `hierarchy_prefix_test`, `cli_url_resolve_test` (env-var fallback); ctest 31/31 both modes |
| 12 | End-user documentation | DONE | Slice 13: plugin-level `toolbox_dexory_cloud/README.md` (Toolbox flow, CLI verb table inc. `hello`/`tag`, env vars, tag editing, hierarchy flag, documented deferrals); repo demo runbook retained |

## Unified Plan + Design Spec + Proposal

| ID | Item | Status | Evidence |
|---|---|---|---|
| U-SEAM1 | BlobStore (GetRange/Head/List, S3+GCS) | DONE | storage.go iface + sentinels + s3.go + gcsreader.go; {s3,gcs} ci matrix |
| U-SEAM2 | StorageCredentials boundary (s3 XOR gcs) | DONE | storage.New sole reader; config union; CI no-SDK-leak grep |
| U-SEAM3 | ClientAuthenticator (bearer Verify) | DONE | Slice 13: `internal/authn` `ClientAuthenticator`/`NewBearerToken` with `crypto/subtle.ConstantTimeCompare`; `ws/server.go:312` Hello token → `auth.Verify`; dev-anonymous when token empty |
| U-SEAM4 | FormatCodec (Extract/PlanChunks/Iterate) | DONE | format.go + chunks.go; one MCAP impl; indexer wires Extract |
| U-SEAM5 | BackendConnection + stitch adapter | DONE | backend_connection (ixws) + stitch_select.h synthetic record |
| U-SEAM6 | TopicIngestDriver + SessionCache | PARTIAL | Cache DONE. Forward-not-decode INVERTED→in-plugin rosx decode (amendment d) |
| U-SEAM-Auth | AuthProvider (per-URI bearer+TLS; qtkeychain) | DONE | Slice 14 (Plan D A3 ADAPT): per-URI token/TLS + the `CredentialStore` seam (0600-file backend, libsecret drop-in) replaces plaintext secret storage; qtkeychain superseded (would pull Qt6 into the Qt-free plugin) |
| U-SEAM-Caps | BackendCapabilities → hierarchy browser | DONE | Slice 14: caps now server-populated (`DistinctMetadataKeys` + `supports_file_hierarchy`); client adapts to a prefix-filter combo (`hierarchy_prefix.h`) since `QTreeWidget` is host-unrenderable (grounded) |
| U-M0 | M0 gate (proto+seams+codegen+CI skeleton) | DONE | proto/bindings/config-union/flat-metadata/{s3,gcs} axis |
| U-M1a | M1a gate (Go core, S3+MCAP, streaming, Minio suite) | DONE | 5 subsystems; smoke green; race CI |
| U-M1b | M1b gate (GCS drop-in, both backends, parity) | DONE | gcsreader + ci_integration both legs + fake-gcs + parity bench; real-GCS EXT |
| U-M1c | M1c gate (Dexory M1: real S3 ≥100 MCAPs, approval) | PARTIAL | Engineering proven on synthetic 104-MCAP; real-bucket + written approval EXTERNAL |
| U-M2a | M2a gate (hardening, deploy, 200MB/s, soak) | PARTIAL | Hardening+deploy+throughput floor DONE; 200MB/s ref-machine + 1wk soak EXTERNAL |
| U-M2b | M2b gate (PJ4 plugin, raw-forward, cache, resume) | SUPERSEDED | Toolbox shape (amendment c); decode inverted (d); functionally equivalent |
| U-M2c-DEX | M2c-DEX (Dexory deploy, aws chain, WASM bonus) | PARTIAL | Generic deploy + S3 chain DONE; Slice 15 added the WASM minimum (`wasm/build.sh` compiles+node-runs the pure decode core; non-gating `wasm.yml`; risk-8 constraints README) — the "at-minimum compiles" bar is met. REMAINING (EXT): ops-review + real-S3 deploy |
| U-M2c-ASEN | M2c-ASEN (GCE ADC, GCS L3, hierarchy, GCE guide) | PARTIAL | GCS engine path + hierarchy (prefix-filter adaptation, Slice 14) DONE. REMAINING: the GCE deploy guide artifact (deferred); real-GCE EXTERNAL |
| U-RES-A | Resolved A: cache = in-memory only | DONE | session_cache.hpp in-memory COMPLETE-only LRU; no disk tier |
| U-RES-B | Resolved B: Asensus = GCE, Cloud Run out | PARTIAL | Architecture consistent; the documenting GCE deploy guide artifact missing |
| U-RES-C | Resolved C: GCS = single SA / ADC | DONE | NewGCS default ADC; creds confined to storage.New |
| U-RES-D | Resolved D: metadata = MCAP footer | DONE | Extract→tags_embedded; no object-custom-metadata path |
| U-RES-E | Resolved E: client forwards raw, no decoders | SUPERSEDED | REVERSED (amendment d): in-plugin rosx decode; toolbox can't reach host parser dispatch |
| U-RES-F | Resolved F: one FormatCodec, no plugin system | DONE | single Codec + one mcap impl + documented extension path |
| U-OPEN-1 | Open: Dexory M1 scope (tags/time-range out?) | EXTERNAL-BLOCKED | Both built early; contract scoping is a written Dexory call |
| U-OPEN-2 | Open: byte-for-byte vs logical equality | EXTERNAL-BLOCKED | Logical equality implemented+tested; written acceptance external |
| U-OPEN-3 | Open: timeline vs price | EXTERNAL-BLOCKED | Pure commercial negotiation |
| U-OPEN-4 | Open: Asensus boundary + M2a/b funding decouple | EXTERNAL-BLOCKED | Commercial funding/trigger decision |
| U-OPEN-5 | Open: cache-wording / cross-restart SOW | EXTERNAL-BLOCKED | SOW phrasing sign-off with Asensus |
| U-OPEN-6 | Open: pin reference machine for 200MB/s | EXTERNAL-BLOCKED | SOW pinning decision; bench defers it |
| S13-1a | Phase 1a: proto + codegen (Go+C++) | DONE | proto + Go bindings checked in + C++ build-gen; repo-root proto/ |
| S13-1b | Phase 1b: Go server v1 (§4/§6/§8) | DONE | full subsystems; smoke green-gate |
| S13-2 | Phase 2: client-core Qt lib (§9.1–9.4) | SUPERSEDED | Amendment b: ixws units in plugin+CLI; no Qt lib packaging |
| S13-3 | Phase 3: client-cli + McapWriterSink + round-trip | DONE | dexory-cloud-cli + session_download + mcapdiff gate |
| S13-4 | Phase 4 (deferred): DataSource plugin + pjcloud:// | SUPERSEDED | Amendment c: Toolbox; no URI/FileSourceBase/qtkeychain |
| S11-L1 | Testing L1: unit (hermetic, per-language) | DONE | Go unit+race CI; C++ ctest suite; wire round-trips |
| S11-L2 | Testing L2: component (live local storage) | DONE | ci_integration both legs; plugin live tests; isolated ports |
| S11-L3 | Testing L3: E2E cross-language matrix (~32 cells) | DONE | matrix.sh m1–m8 logical-equality incl. m8 GCS dual-leg through the C++ `dexory-cloud-cli`; `ws/estimate_test.go` `TestEstimateWithin5Pct_*` asserts the est_chunk_bytes<5% contract (both previously-missing halves verified present, Slice 10/12) |
| S11-L4 | Testing L4: V1 bench gate (200MB/s, CPU, p99) | PARTIAL | baseline-compare + `BenchmarkCompressionCPU` + `BenchmarkBackpressureLatency`(p99) all present (Slice 13). REMAINING (EXT): the 200 MB/s **reference-machine** gate needs a pinned SOW machine |

---

## What is actually left

### (a) Locally buildable now (no external/commercial dependency)

**Nothing remains in this bucket.** Every previously-listed locally-buildable gap was
closed by Slices 13–15 and re-verified against the repo on 2026-06-06:

- Plan A 1 (LICENSE), A14 (`retry.go` `retryWith` + retry/classify tests), A24a / U-SEAM3
  (`internal/authn` + `crypto/subtle.ConstantTimeCompare`, wired at `ws/server.go:312`),
  A45 / S11-L4-benches (`BenchmarkCompressionCPU` + `BenchmarkBackpressureLatency`) — DONE.
- Plan B 3 (`backendCapabilities()` accessor + live test), B13 (`debug` verb), B14 /
  no-Qt guard (`DexoryCloudCliNoQtGuard` ctest) — DONE.
- Plan C 2 (genmcap dimensions + `CorruptChunkBody` + rejection test; LZ4-frame decode
  bug fixed in Slice 14), C8 (`bench.yml`), C9 (`scripts/RUNBOOK.md`) — DONE.
- Plan D 6 / U-SEAM-Auth (`CredentialStore` 0600-file seam, Plan D A3-authorized,
  libsecret drop-in documented), D8 / U-SEAM-Caps (live server caps + client prefix-filter
  adaptation, `QTreeWidget` grounded as host-unrenderable), D11 (plugin-load smoke +
  credential + hierarchy + env-var ctests), D12 (plugin README) — DONE.
- U-M2c-DEX WASM **minimum** (`wasm/build.sh` compiles + node-runs the pure decode core;
  non-gating `wasm.yml`; risk-8 constraints README) — DONE to the "at-minimum compiles"
  bar the plan sets (real-S3 deploy + ops-review remain EXTERNAL, see (b)).

### (b) Blocked on client / external access

- **Plan C 8a / U-M1c (real-bucket leg) / U-M2c-DEX (real-S3) / U-M2c-ASEN (real-GCE)**:
  real S3 (Dexory ≥100-MCAP bucket) and real GCE+ADC+persistent-disk validation — pending
  Dexory/Asensus access. Engineering is proven on synthetic corpora + emulators.
- **U-M2a (soak + 200 MB/s)**: the ≥1-week soak run and the 200 MB/s **reference-machine**
  throughput gate need a pinned machine (SOW item); only a localhost regression floor
  exists.

### (c) Deliberately deferred (M2 scope / needs gate or user decision)

- **U-SEAM6** (Resolved-E inversion): the connector ships an in-plugin rosx decoder
  (amendment d) rather than the planned raw-forward — a recorded design reversal, not a
  gap. Stays PARTIAL only to flag the inversion; the cache half is DONE.
- **U-RES-B / U-M2c-ASEN**: the Asensus GCE deploy guide artifact (incl. "Cloud Run out")
  — gated on the Asensus engagement trigger. The GCS engine path itself is DONE.
- **Specialized-handler flatten parity** (Imu/PoseStamped) — scoped follow-up only if
  file-open parity for those two types is mandated.
- **CI activation**: `ci.yml`/`bench.yml`/`wasm.yml` are authored + locally verified but
  NOT pushed (origin/main is stale); activating on GitHub is a user decision (publishes
  full history).

> Closed since the morning audit and removed from this list: the `QTreeWidget`
> file-hierarchy browser (Plan D 8 / U-SEAM-Caps — replaced by the grounded prefix-filter
> adaptation) and the qtkeychain-vs-libsecret secret-store decision (Plan D 6 /
> U-SEAM-Auth — settled by the 0600-file `CredentialStore` seam, libsecret drop-in).

### (d) Commercial / process (not engineering-resolvable in-repo)

- **U-OPEN-1**: Dexory M1 scope vs contract — does tag editing / intra-file time-range
  count toward M1? (both built early; scoping is a written call).
- **U-OPEN-2**: "byte-for-byte" acceptance vs implemented logical equality — needs
  written Dexory acceptance.
- **U-OPEN-3**: timeline vs price (effort > 1 month/milestone) — calendar/price revision.
- **U-OPEN-4**: Asensus engagement boundary + M2a/M2b funding decoupling from the Dexory
  M2 gate.
- **U-OPEN-5**: cache-wording — confirm cross-restart persistence is Asensus "Task C" or
  renegotiate SOW.
- **U-OPEN-6**: pin the reference machine (CPU/NIC/disk) for the 200 MB/s SOW gate.
- **U-M1c (approval gate)**: written Dexory M1 sign-off before M2 work.
