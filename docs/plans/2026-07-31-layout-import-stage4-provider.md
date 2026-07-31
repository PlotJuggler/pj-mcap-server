# Stage 4 — `pj.descriptor_import.v1` Provider in `toolbox_mcap_cloud` — Implementation Plan

> Execution: `superpowers:subagent-driven-development` (fresh implementer + spec review +
> quality review per task), in a worktree `.worktrees/layout-import-provider` off
> `origin/main`. Gates per task: plugin ctest (hermetic); at the end: `make smoke`
> (SERIALIZE across Claude sessions — fixed /tmp paths, :8081, shared bucket) + the
> wasm-compile rot check (provider/cache code must stay OUT of the wasm source set).

Grounding: 2026-07-31 recon of the plugin + spec v3.6 + SDK 0.20.0 (`4e7e14d`), after
PJ4 host half fully merged (#490 `341fe995`, #492 `be71e3ae`). Spec §6.3/§6.4/§8/§9.6/
§9.8/§13-step-4 are the authorities. SDK headers: read at
`~/ws_plotjuggler/plotjuggler_sdk-cloud/.worktrees/v0.20.0/pj_base/include/pj_base/
{descriptor_import_protocol.h, sdk/descriptor_import.hpp}` (Conan package has no
unpacked headers to grep).

## 0. Recon deltas (verified 2026-07-31 — the plan builds on these facts)

- `McapCloudToolbox` (`src/mcap_cloud_toolbox.cpp:34`) has **no `pluginExtension`
  override**; base default returns nullptr (`toolbox_plugin_base.hpp:155-158`).
  Manifest id = `"mcap-cloud"` (`manifest.json:2`).
- **`bind()` today auto-connects**: `mcap_cloud_toolbox.cpp:57` `dialog_.setSettings(…)`
  → `initFromSettings()` (`mcap_cloud_dialog.cpp:583-586`) → `connectAsync` postCommand
  (`:640-653`). Spec §6.3:321-325 forbids exactly this for a provider bind.
- `src/source_descriptor.cpp` is **NOT in `MCAP_CLOUD_SOURCES`** (`CMakeLists.txt:113-131`)
  — only test targets compile it. The `.so` cannot currently link the parser.
- `SessionFileCache` (stage 1) is deliberately consumer-free and is the designed §6.3
  query surface: `lookup()` = bounded-I/O validity check (footer+summary+Statistics+
  provenance match, LRU touch), `pathFor()`, `MaterializeLock`/`partialPathFor`/
  `finalize(ExpectedContent)` (validate→fsync→rename), `cleanup()`. Partials NEVER
  survive (inverse of the export path — pinned divergence `fetch_worker.cpp:934-938`).
- `FetchWorker::pullTopicsAsync` is **synchronous on the calling thread**; the command
  pump lives in the DIALOG (`mcap_cloud_dialog.hpp:606-611`), so headless import owns
  its own thread. Cancel = `requestCancel()` (thread-safe: cancel flag + session cancel
  under `cancel_mu_`, reaches the wake_on_cancel predicates). #470 progress + host-stop
  watchdog + the single `cancelled_after_download` terminal boundary all live inside
  the pull (`fetch_worker.cpp:771-977`).
- The tee seam: `SessionMcapWriter::open(mcap::IWritable&…)`/`writeMetadata`/`close`;
  today opened only on the EXPORT path via `prepareMcapOutputPaths`
  (`fetch_worker.cpp:757-769`).
- Promotion service: `services.get<PJ::sdk::SourcePromotionHostService>()` (OPTIONAL —
  absence = host without promotion; `get<>`, never `require<>`), view
  `promoteToFileSource(request, on_result)`; on_result may fire re-entrantly, on the
  host-callback thread, and the binding must stay alive while a promotion is
  outstanding (`descriptor_import.hpp:492-544`). `SourcePromotionRequest.dataset` =
  `DataSourceHandle::.id`.
- Provider side is **raw vtable** (no SDK base): the `FakeImportToolbox` pattern —
  `pluginExtension` returns a `PJ_descriptor_import_provider_v1_t` member; every query
  result field write is `struct_size`-covered (`offsetof` guard); flags fail-closed
  BEFORE any effect; `out_job` populated before the worker starts; job vtable trio
  cancel(idempotent,non-blocking)/join(after terminal returned)/destroy(cancel+join+
  free); NEVER call join/destroy from a callback; query-result strings live until the
  NEXT query on the same instance.
- `estimated_bytes` may NOT come from `session_info.estimated_chunk_bytes` (network).
  Sources allowed: descriptor/local metadata only.
- `SessionCache` (in-memory) has no `DatasetId` member yet (`session_cache.hpp:55-61`).
- wasm-compile compiles only the pure decode core (`wasm/build.sh:13-15,134`) —
  provider/cache files must not be added there.

## Decisions to lock (Codex consult before implementation)

- **D1 — Headless bind mode shape.** Move the auto-connect OUT of `bind()`: defer
  `initFromSettings()` (and its `connectAsync`) from `setSettings()` to the first
  `getDialog()` call (the interactive-only entry point; headless sessions never call
  it). `bind()` keeps wiring settings/host providers. Risk: any interactive flow that
  relied on connect-before-dialog-shown; mitigated because the dialog cannot be
  interacted with before `getDialog()`. Alternative (rejected lean): an explicit
  headless flag threaded from the host — no such ABI channel exists; first-`getDialog`
  deferral needs zero ABI.
- **D2 — Provider module shape.** New `src/descriptor_import_provider.{hpp,cpp}`:
  a `DescriptorImportProvider` class owned by `McapCloudToolbox`, exposing the
  C vtable + thunks (FakeImportToolbox pattern), holding: `SessionFileCache`,
  `TrustedOrigins`, the promotion view (`std::optional<SourcePromotionHostView>`
  captured in `bind()`), a settings view for credential resolution at IMPORT time
  (never query time), and the per-job registry. `McapCloudToolbox::pluginExtension`
  returns it for `PJ_DESCRIPTOR_IMPORT_EXTENSION_V1`.
- **D3 — query_descriptor semantics** (all bounded, no network, no credentials):
  parse via `parseSourceDescriptor` (malformed ⇒ return false + PJ_error_t — a
  CONTRACT failure, not a trust verdict); `source_identity = descriptorIdentity(d)`
  and `local_path_utf8 = cache.pathFor(identity)` ALWAYS returned;
  `is_materialized = cache.lookup(identity, …)` (bounded I/O by design);
  trust: `TrustedOrigins::isTrusted(server_uri)` ⇒ kTrusted else kNeedsConfirmation;
  kRefused reserved for policy refusals (v1: descriptor kind/version mismatches that
  parse but are unsupported — confirm exact set in consult);
  `estimated_bytes`: file size when materialized; else 0 (unknown) — the v1
  descriptor carries no authoring-time byte estimate (adding one = descriptor v2,
  out of scope).
- **D4 — start_import = one owned FetchWorker + thread per job.** Sequential jobs
  arrive from the host batch anyway. Flow: flags fail-closed sync-reject → parse
  descriptor (sync-reject on malformed) → populate `out_job` (vtable trio over a
  heap job state) → worker thread: resolve credentials (allowed here), acquire
  `MaterializeLock` (busy ⇒ FAILED with "materialize in progress"), `connectAsync`,
  `pullTopicsAsync(descriptor fields…, cache-tee mode)`; `on_dataset` fired once from
  the pull's dataset creation; terminal mapping: EOS-complete + finalize ok +
  promotion ok=true ⇒ SUCCEEDED_PROMOTED; EOS-complete + (no promotion service |
  promotion rejected/failed | finalize failed per §9.6) ⇒ SUCCEEDED_EAGER_ONLY
  (dataset stays usable; diagnostic message); cancel ⇒ CANCELLED + partial deleted;
  anything else ⇒ FAILED. `max_transfer_bytes`: enforced in the pull's byte
  accounting (wire bytes) — abort + FAILED when exceeded (0 = no ceiling).
  cancel() = `FetchWorker::requestCancel()`; join() = thread join (pull returns via
  the cancel wake predicates); destroy() = cancel+join+free.
- **D5 — The cache tee (FetchWorker re-target) is ONE mechanism for BOTH paths.**
  `pullTopicsAsync` gains a tee target variant (param struct or a second save-mode):
  export mode (today's `save_directory`, partial survives cancel) vs cache mode
  (SessionFileCache partial via MaterializeLock, `writeMetadata("mcap_cloud/
  source_descriptor", canonical_json)` before first write, `finalize(ExpectedContent
  {message_count, channel_count from EOS})`, partial deleted on ANY failure/cancel,
  tee failure never aborts the ingest — §9.6). The INTERACTIVE Fetch path adopts the
  cache tee too (the authoring dual path, §9.8): every fetch tees into the cache and
  attempts promotion at completion — this is how authored layouts get SourceRecords.
  Interactive promotion uses the SAME completion hook as import jobs.
- **D6 — Promotion request contents.** `dataset` = the pull's `DataSourceHandle.id`;
  `source_identity` = descriptor identity; `local_path_utf8` = the finalized cache
  path; `loader_plugin_id` = the stock MCAP loader's STABLE MANIFEST ID (ground the
  exact string from pj-official-plugins `data_load_mcap` manifest during
  implementation — never hardcode unverified); `loader_config_json` = the loader's
  minimal valid config (non-empty per the parser_config pin; verify the loader's
  schema); `descriptor_json` = `toSourceDescriptorJson` (canonical + display_name).
  Keep the binding alive while outstanding (job holds the view + the toolbox pins it).
- **D7 — SessionCache DatasetId.** Add `DatasetId dataset_id` to `CachedSession` so
  an in-memory hit can answer "already loaded as dataset X" (spec §13 step 4);
  populated at pull completion; used by query/import only as local metadata.

## Consult verdict (2026-07-31, Codex session `019fb720-2057-7b72-94e3-aa6c5a4693d2`
## via codex-exec) — AMENDMENTS LOCKED; do not implement the pre-amendment shapes

- **D1 amended:** defer-to-getDialog locked as an explicit ONCE-PER-PLUGIN-LIFETIME
  transition (repeated getDialog / rebind must never double-connect — test both).
  bind() is otherwise network-free (verified). Residual: dialog ctor starts an idle
  worker thread pre-bind — not a §6.3 violation, document it.
- **D2 amended:** credentials CANNOT be resolved on the job thread — every
  SettingsView call is main-thread-only (plugin_data_api.hpp:1591). Resolve + copy
  inside start_import() on the main thread; hand the job an immutable connection
  snapshot; hoist the dialog-file-local `resolveCredentials` (mcap_cloud_dialog.cpp:108)
  into a shared unit. Provider + dialog share a per-toolbox-instance **ImportRuntime**:
  SessionFileCache, thread-safe SessionCache, host-write serialization, keyed
  active-materialization registry, and an IN-MEMORY trust set (TrustedOrigins
  currently re-reads the ledger file per call — trusted_origins.cpp:118 — which
  violates §6.3's bounded-query rule; preload/update in memory).
- **D3 amended:** wrong version/kind = `return false` (SDK: query false covers
  malformed OR unsupported — descriptor_import_protocol.h:230), NOT a trust verdict;
  kRefused reserved for genuine policy refusals (v1 emits only trusted /
  needs-confirmation). In-memory SessionCache must never produce is_materialized=true
  — disk-validated cache only.
- **D4 amended (hard blocker):** do NOT drive headless jobs through
  connectAsync+pullTopicsAsync (connectAsync creates a browse backend + records
  credentials — fetch_worker.cpp:71 — and the pull opens a SECOND connection :539;
  requestCancel can't reach the browse connection; the WS-open wait ignores
  cancellation for up to 10 s — backend_connection.cpp:330). Build a DIRECT
  cancellable pull API: `pull(PullRequest{connection snapshot,…})` that publishes its
  sole backend before connect and adds cancellation to the socket-open/Hello/
  OpenSession waits. `include_latched` comes from the descriptor (currently
  hard-coded true, fetch_worker.cpp:611). Shared host-write serialization (each
  worker currently owns a private host_write_mu_ — a provider job could overlap the
  interactive worker on the non-thread-safe host). MaterializeLock busy ⇒ bounded
  cancelable wait + revalidate, NEVER late-attach an interactive fetch's DatasetId
  (would violate on_dataset-before-first-publication); actionable retry failure
  instead. Explicit post-return start gate (callbacks provably cannot fire before
  start_import returns). Byte-ceiling abort must terminal FAILED, not CANCELLED
  (a sink returning false currently classifies as Cancelled —
  backend_connection.cpp:989). EAGER_ONLY only when the eager dataset is genuinely
  usable. **Cross-repo bug found:** PJ4's HeadlessDescriptorProviderSession joins
  jobs BEFORE promotion shutdown() — a job blocking its terminal on an accepted
  promotion callback deadlocks the join; PJ4 must shutdown() promotion (failing
  pending callbacks while the plugin is alive) BEFORE cancel/join. Fix precedes
  provider live integration.
- **D5 amended (hard blocker):** the CACHE IS THE SOLE ENCODER (§9: "cache sole
  encoder; exports byte copies" — canonical:482). Always encode once into the cache
  partial; export destinations receive copies (success: finalize then copy/reflink;
  cancel: copy the readable partial to the export .partial, then DELETE the cache
  partial). Memory-hit rules: memory hit + disk missing ⇒ evict + refetch; memory
  hit + valid disk ⇒ re-promote DatasetId + satisfy export by copy; never re-tee
  from memory. Writer setup moves BEFORE hasDecodable() (currently after —
  fetch_worker.cpp:735/:757). Bounded owned-payload queue/backpressure + pre-opened
  exclusive 0600 sink (SessionMcapWriter still truncating-ofstream). Tee failure
  stays non-fatal to ingest and suppresses promotion. Spec §12's "writer-failure
  cancels fetch" text is STALE (contradicts §9.6) — correct it.
- **D6 locked with values:** `loader_plugin_id = "mcap-loader"`
  (pj-official-plugins data_load_mcap/manifest.json:2); preset
  `{"clamp_large_arrays":true,"max_array_size":500,"selected_topics":[],
  "use_header_timestamp":false,"use_log_time":true}` — use_log_time REQUIRED
  (eager ingest pushes log_time_ns; loader defaults false). No filepath in the
  preset (PJ4 rewrites it). Pin loader-version + catalog-equality tests.
- **D7 amended:** SessionCache becomes toolbox-instance-shared + thread-safe inside
  ImportRuntime, keyed/existence-checked by stable DatasetId (display names collide
  and mutate), carrying cache-file/promotion state for the memory-hit rules.
- **PR strategy amended:** STACKED PRs, not one (canonical §13 build order —
  writer/cache follow-ups precede the provider): PR-1 single-encoder async cache
  tee + export-copy + locks/leases + shared ImportRuntime; PR-2 headless one-shot
  init + shared credential resolver; PR-3 provider ABI/jobs/promotion; PR-4
  live/E2E/docs. The PJ4 promotion-teardown fix precedes PR-3's live integration
  (rides the T7 PR or its own tiny PJ4 PR).
- **New scope from consult:** cross-platform shared cache leases (file_lock.h:4
  leaves them to stage 4; datasets outlive headless sessions); pre-materialization
  cleanup/free-space reserve + under-lock corruption deletion + RAII partial guard
  on every early exit; ABI adversarial test set (short structs, null callbacks,
  no-callback-before-return, on_dataset ordering, terminal exactly-once,
  ceiling-vs-cancel race, destroy-during-connect, re-entrant promotion);
  interactive save+cache matrix; promotion-failure tests (eager data + finalized
  cache remain, no record).

## Tasks (SUPERSEDED by the amended PR stack above — regroup at implementation
## time: PR-1 = old T0/T1 + ImportRuntime + leases; PR-2 = old T3 + credential
## hoist; PR-3 = old T4 + D4's direct pull API + old T2; PR-4 = old T5.
## Original task text below retained for the acceptance criteria it carries.)

- [ ] **T0 — link the descriptor module + worktree/plumbing.** Add
  `source_descriptor.cpp` to `MCAP_CLOUD_SOURCES`; confirm nlohmann linkage; no
  behavior change. (Trivial, fold into T1's commit if preferred.)
- [ ] **T1 — cache tee re-target in FetchWorker (D5, both-path mechanism).**
  Tee-mode param; MaterializeLock acquisition; provenance metadata record;
  ExpectedContent finalize on Complete; delete-on-cancel/failure retention
  (inverse of export — keep the pinned divergence comment true); §9.6 tee-failure
  isolation (ingest completes, promotion skipped, diagnostic). Tests: hermetic tee
  round-trip against the fake ingest host + a corrupted-finalize case + cancel
  deletes partial + export mode byte-identical behavior (regression).
- [ ] **T2 — promotion-at-completion hook (D6) shared by both paths.** On
  EOS-complete + finalize ok: build SourcePromotionRequest, call the view when
  bound, map on_result → outcome/diagnostic; re-entrancy-safe; binding kept alive.
  Interactive Fetch adopts tee+promotion (authoring dual path). SessionCache gains
  DatasetId (D7). Tests: promote-called-with-correct-request (fake promotion host),
  EAGER_ONLY on absent service / rejection / tee failure, re-entrant on_result.
- [ ] **T3 — headless bind mode (D1).** Defer initFromSettings/auto-connect to first
  getDialog(); bind() stays side-effect-free for network. Tests: bind-then-query
  performs zero network (assert no connect attempt via a probe), interactive
  getDialog still auto-connects (existing live behavior preserved).
- [ ] **T4 — the provider extension (D2/D3/D4).** `pluginExtension` override +
  provider module: query thunk (struct_size-covered writes, always identity+path,
  trust matrix, is_materialized via cache.lookup, estimated_bytes rules), start
  thunk (flags fail-closed, out_job-before-worker, job vtable trio), the job
  runner (credentials, lock, pull in cache-tee mode, terminal mapping incl.
  max_transfer_bytes ceiling), wake-on-cancel reachability. Tests: hermetic
  provider matrix (trust/hit/miss/malformed/flags/ceiling/cancel-join-destroy/
  terminal exactly-once/result-string lifetimes) + a live test against the smoke
  server (query→import→promote round trip; cache file mcapdiff-equal to a CLI
  download of the same tuple — spec §12).
- [ ] **T5 — gates + docs.** Full hermetic ctest; `make smoke` (serialize!);
  wasm-compile untouched-set check; spec §9.8 checkbox flip + CATALOG untouched;
  README/plugin docs note the provider.

**PR strategy:** ONE plugin PR (stage-4) — T1–T5 are tightly coupled around the
FetchWorker seam; the milestone gets the team adversarial review (Codex via
codex-exec + Claude) at PR time.

**Cross-repo notes:** loader manifest id grounding (D6) reads the sibling
pj-official-plugins checkout; E2E against the merged PJ4 host lands in stage-5,
not here.
