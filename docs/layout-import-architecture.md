# Layout Import — As-Built Architecture Reference

**Status: LIVE reference (2026-08-01).** The canonical layout import feature is
SHIPPED — merged across three repos and verified end to end by the stage-5
cross-repo live E2E gate. This document describes what EXISTS —
the component map, the runtime flows, and the invariants any future change must
preserve. It is the layout-import analog of `CATALOG_CONTRACT.md`: keep it as-built.

- The **design record** (why it is shaped this way, rejected alternatives, lineage
  v1→v3.6) is `docs/canonical-layout-import.md` — read that for rationale, this for
  reality.
- The **execution records** (per-stage plans, Codex consult verdicts, review arcs)
  are `docs/plans/2026-0[78]-*-layout-*.md` (stages 1–5) — provenance, not
  instructions.
- Merged as: SDK 0.20.0 · plugin stages 1–4 (this repo PRs #4, #11, #14, #15,
  #18–#21) · PJ4 host #470/#464 (pre-existing surfaces) + #490 (foundations),
  #492 (import pipeline), #497 (teardown reorder), #500 (growing-import binder),
  #501 (stage-5: the two headless flags + the live gui-test) · this repo's
  stage-5 companion PR (the `make e2e-layout` gate, the frozen `:8082`
  descriptor vectors, `docs/layout-sharing-runbook.md`).
- The **sharing / operations runbook** (what a shared layout embeds, the trust
  bootstrap, cache purge, the headless flow + diagnostic-id table, the pinned-DSO
  matrix, how to run the gate) is `docs/layout-sharing-runbook.md`.

## 1. What the feature is

A `.pj4.xml` layout can embed, per cloud-sourced dataset, a durable **source
descriptor** (canonical JSON: server URI, s3 keys, time range, topic subset,
`include_latched`) plus a `<materialize>` stanza. Loading that layout — GUI or
`plotjuggler --layout`, zero prompts required — re-creates the exact session:
instantly from a local cache artifact when present (stock file load, zero network),
otherwise by re-downloading through the plugin while plots grow progressively.
Identity is content-addressed: `sha256/128` of the canonical descriptor bytes,
prefixed `mcap-cloud:v1:`; the cache artifact lives at
`$XDG_CACHE_HOME/mcap_cloud/sessions/<sha256-128>.mcap`.

## 2. Cross-repo component map

### SDK (`plotjuggler_sdk`, ≥ 0.20.0)

| Piece | Where | Role |
|---|---|---|
| `pj.descriptor_import.v1` | `pj_base/include/pj_base/descriptor_import_protocol.h` + `sdk/descriptor_import.hpp` | Provider extension: `query_descriptor` (sync, main-thread, strictly bounded) + `start_import` (job with exactly two callbacks). Zero new vtable slots — resolved via the extension hook + `bind()` registry, `struct_size`-gated. |
| `pj.source_promotion.v1` | `pj_base/.../source_promotion` headers | Per-instance host service: `promoteToFileSource` swaps a provisional eager dataset for a stock file-backed load of the cache artifact, in place. |
| Dataset-scoped ingest surface | `create_parser_ingest` tail slot, `DatasetIngestHostView` | The ONLY progress channel for import jobs (there is deliberately no `on_progress` on the job — see invariant I-6). |

### Plugin (`plugin/toolbox_mcap_cloud`, this repo)

| Piece | Where | Role |
|---|---|---|
| `SourceDescriptor` | `src/source_descriptor.{hpp,cpp}` (+ `docs/source-descriptor-vectors.json`) | Parse/canonicalize/identity. Canonical JSON bytes are the identity input — byte stability is load-bearing. |
| `ImportRuntime` | `src/import_runtime.{hpp,cpp}` | ONE per toolbox instance, shared by the dialog path and provider jobs: durable `SessionFileCache`, thread-safe `SessionCache` (DatasetId-keyed, promotion state, generations), the host-write mutex serializing ALL workers, the in-memory trust set, the keyed materialization registry + lease registry, the shared promotion hook. |
| `CacheTee` | `src/import_runtime.{hpp,cpp}` | The single-encoder materialization pipeline: ticket → cleanup → exclusive cross-process lock → 0600 sink + `SessionMcapWriter` + embedded descriptor provenance → bounded writer queue → validated atomic finalize (rename) or partial deletion. |
| `SessionFileCache` | `src/session_file_cache.{hpp,cpp}` + `src/core/file_lock.{h,cpp}` | Artifact store: leases (shared sidecar flocks), materialize locks, LRU/free-space cleanup, metadata preflight validation. |
| Direct pull | `FetchWorker::pull(PullRequest)` in `src/fetch_worker.{hpp,cpp}` | The cancellable one-connection download a provider job drives: fresh `BackendConnection`, eager parser-delegated ingest, optional cache tee, hard session deadline + byte ceilings. |
| Provider | `src/descriptor_import_provider.{hpp,cpp}` | `pj.descriptor_import.v1` implementation: trust/materialized answers from the runtime; job threads; terminal mapping; refusal-while-referenced; per-machine byte cap (`MCAP_CLOUD_IMPORT_MAX_BYTES`). |
| Headless init | `ensureInitFromSettings` latch in `src/mcap_cloud_toolbox.{hpp,cpp}` + `src/credential_resolve.{hpp,cpp}` | A provider job on an instance whose dialog never opened still resolves credentials (main-thread `ConnectionSnapshot`) and the runtime. |
| Trust ledger | `src/trusted_origins.{hpp,cpp}` | Durable origins file under `$XDG_CONFIG_HOME`; in-memory set preloaded at construction. |

### PJ4 host (`~/ws_plotjuggler/PJ4`)

| Piece | Where | Role |
|---|---|---|
| `LayoutImportBatch` | `pj_app/src/LayoutImportBatch.{h,cpp}` | GUI-thread, MainWindow-free transaction owner for one layout's import jobs: sequential job chain, per-job outcomes/diagnostics, rollback ledger, correlation surface (`activeImportDataset()`, `cancelActiveImportJob()`). |
| `HeadlessDescriptorProviderSession` | `pj_app/src/HeadlessDescriptorProviderSession.{h,cpp}` | `launchToolbox` minus the UI: binds the plugin, resolves the provider, queued-only callback marshals, `JobId`-addressed `cancelJob`, the #497 teardown order. Forwards ingest callbacks to `SessionManager` bookkeeping (data half only). |
| `SourcePromotionHost` | `pj_app/src/SourcePromotionHost.{h,cpp}` | The promotion engine over FileLoader's replace transaction; strict in-place `replace_dataset_id`. |
| Rewrite-then-classify + trust gate | `MainWindow` load path + `LayoutXml` | §6.2's seven steps: rewrite maps (keyed on serialized/resolved/cleanPath forms), classify each source cache-HIT vs import job, non-interactive `MissingCurvePolicy::kRetainAndDiagnose`. |
| Growing-import binder (T7) | `MainWindow` + `SessionManager` | Batch-scoped observers on `SessionManager::ingestBegan/Progressed/Ended` filtered by `activeImportDataset()`; title-bar strip under displayed-owner arbitration; per-job keep-partial Stop; mid-import curve binding rides the retention channel (`CatalogModel::itemsAdded`). |
| Ingest lifecycle bookkeeping | `pj_runtime` `SessionManager` (`beginIngest/updateIngest/endIngest` + signals), `ToolboxRuntimeHost` | The #470 surface both interactive and headless imports drive; `updateIngest` publishes rows before the progress signal. |

## 3. Runtime flows

**A. Interactive fetch (authoring).** Dialog → `FetchWorker` download → eager
parser-delegated ingest (plots grow) + `CacheTee` writes the artifact → on
completion, `ImportRuntime::promoteToFileSource` swaps the eager dataset for a
stock `"mcap-loader"` load of the artifact (locked preset, no filepath — the host
rewrites it). Layout save then records a normal `<fileInfo>` + the provider
stanza. A tee failure never aborts the fetch — the dataset stays eager-only.

**B. Layout open, cache HIT.** Rewrite-then-classify finds a valid artifact
(disk-validated, never from in-memory state) → stock lazy file load through
`FileLoader` with a `LoadTicket` → zero network, works with the plugin absent.

**C. Layout open, cache MISS.** Trust gate (untrusted origin ⇒ confirmation /
refusal — never silent) → `LayoutImportBatch` starts ONE
`HeadlessDescriptorProviderSession` job at a time: `query_descriptor` →
`start_import` → provider pulls with the tee; `on_dataset` announces the
provisional dataset (zero-or-one, strictly before any publication); eager ingest
grows it; the binder shows the strip and binds retained curve intents mid-import;
at completion the shared promotion hook swaps to the file-backed dataset
(`SUCCEEDED_PROMOTED`) or the dataset stays eager (`SUCCEEDED_EAGER_ONLY`, with
its diagnostic). Per-job failures never abort the batch; the batch's restore
waiter holds the progressive drain open until its jobs settle.

**D. Stop/cancel.** The strip's Stop routes by displayed owner; for a batch job it
cancels ONLY the active job (keep-partial — published rows stay, the batch
continues). Whole-batch `cancel()` (layout superseded, shutdown) rolls back
produced datasets and restores the prior workspace.

## 4. Invariants (the do-not-break list)

Verify against code, but do not re-litigate — each was locked by consult and most
are test-pinned.

- **I-1 Cache is the sole encoder; exports are byte copies.** One
  `SessionMcapWriter` writes artifacts; the export path copies bytes from the
  cache. Never add a second encoder.
- **I-2 Partials never survive; publish is atomic.** Any non-Complete exit deletes
  the partial; finalize is temp+rename; a lookup()-failing existing file is
  deleted under the exclusive lock only.
- **I-3 Refusal-while-referenced.** Never rematerialize an identity the lease
  registry still references: loaded MCAP datasets lazily re-open the artifact by
  generation-specific chunk offsets, so a logically-equal re-encoding silently
  corrupts reads. The tee refuses (`kArtifactInUseError`); lease drops are
  move-only RAII (`ScopedLeaseDrop`), restore-before-ticket-release, member-order
  guaranteed.
- **I-4 Tee failure never aborts ingest** (spec §9.6): drop the tee, keep the
  download, land `SUCCEEDED_EAGER_ONLY`.
- **I-5 ABI callback contract:** `on_dataset` zero-or-one, strictly before the
  dataset's first publication/progress/promotion; `on_terminal` exactly-once,
  last; no callback before `start_import` returns (start gate); unknown flags
  fail closed. NEVER late-attach a dataset id (the race-loss path returns zero
  `on_dataset` + failure).
- **I-6 No `on_progress` on the job — progress rides the dataset-scoped ingest
  surface.** This is what makes the growing-import binder load-bearing; adding a
  job-level progress callback is an SDK-minor decision, not a patch.
- **I-7 Delivery order is guaranteed, not reconciled.** `on_dataset` reaches the
  GUI thread before that dataset's `ingestBegan` (both are queued from the same
  worker to the same event queue, in ABI order). Host-side correlation is exact
  `activeImportDataset()` equality — no pending/reconcile maps (they would
  misattribute under concurrent interactive imports).
- **I-8 Promotion is strict in-place replacement.** `replace_dataset_id` +
  `require_replacement`; `DatasetId`/`TopicId` survive `beginRefill`; a commit
  that fails to preserve the target id is rejected. **The refill must reuse the
  dataset's existing same-named topic on EVERY ingest route** — the direct-write
  route (`WriteCore::ensureTopic`) and the object route always did; the scalar
  *parser-binding* route (`DataSourceRuntimeHost::cbEnsureParserBinding`)
  unconditionally minted a new topic until PJ4 #501, so a delegated-ingest
  loader (`data_load_mcap`) renumbered every scalar `TopicId` at the replace
  boundary and every bound curve silently vanished. Offline suites missed it
  because their mock loaders use the direct-write API; the stage-5 live E2E
  caught it. `accepted != succeeded`;
  the result callback is exactly-once and may be re-entrant, on any thread.
  PROMOTED means result ok=true; EAGER_ONLY covers everything short of that
  (including failed promotion with a usable dataset) and always emits its
  diagnostic.
- **I-9 Teardown orders are load-bearing.** Session dtor: `cancelAll()` →
  `promotion_host_->shutdown()` → `joinAll()` → registry/handle/hosts (#497 —
  joining before promotion shutdown deadlocks a worker blocked on the promotion
  result). `~ToolboxRuntimeHost` synchronously delivers unfinished ingest
  terminals, so host-side observers MUST be disconnected before the batch is
  destroyed on EVERY path — including `~MainWindow` (structural, not
  declaration-order luck).
- **I-10 Strip displayed ownership.** `{kind, DatasetId}` with kind ∈ {file,
  interactive-toolbox, layout-batch}; file > interactive > layout-batch,
  newest-begin-wins within rank, batch display strictly residual (it never
  displaces what an interactive user sees); Stop routes by displayed owner; an
  ended owner yields to a surviving eligible ingest. Batch Stop = per-job
  keep-partial cancel; NEVER map the shared Stop icon to whole-batch rollback.
- **I-11 Trust is explicit and durable-or-false.** In-memory set preloaded from
  the `TrustedOrigins` ledger; write-through only after a successful INTERACTIVE
  Hello, and only after the durable write succeeded. A layout import never
  silently trusts an origin.
- **I-12 Loader pin.** Promotion and cache-hit loads use manifest id
  `"mcap-loader"` with the locked minimal preset — byte-identical to
  `{"clamp_large_arrays":true,"max_array_size":500,"selected_topics":[],"use_header_timestamp":false,"use_log_time":true}`
  (pinned by `fetch_worker_promotion_test`), and NO filepath (the host rewrites
  it). `use_log_time` is required: the eager ingest pushes log time, so a
  promoted dataset must be identical to what a later import of the artifact
  produces.
- **I-13 Ordinary non-cloud layout loads traverse zero new code.** The batch and
  its observers exist only when a `<materialize>` record is present.
- **I-14 Import jobs are sequential (v1).** `job_active_` is a bare bool; the
  correlation surface holds at most one dataset. Concurrency is a design change,
  not a tweak.
- **I-15 Credentials resolve on the main thread only** — jobs receive an
  immutable `ConnectionSnapshot`. The auth hint is machine-gated
  (AUTH_FAILED + no token source), never string-matched.
- **I-16 Descriptor canonical bytes are frozen.** Identity = sha256/128 over the
  canonical JSON; vectors in `docs/source-descriptor-vectors.json` pin it.
  `include_latched` is part of the session key. No migration shims — a changed
  byte stream is a cache miss, which must remain correct-but-slower, never wrong.

## 5. Where the proof lives

- **Plugin (this repo):** `plugin/toolbox_mcap_cloud/tests/` —
  `descriptor_import_provider_test` (ABI adversarial set), `fetch_worker_direct_pull_test`,
  `fetch_worker_promotion_test`, `fetch_worker_cache_tee_test`, `import_runtime_test`,
  `session_file_cache_test`, `headless_init_test`, `credential_resolve_test`,
  `descriptor_import_live_test` (live leg) — plus `scripts/smoke.sh`'s
  `McapCloudDescriptorImportLive` step and the cache-vs-CLI mcapdiff round trip.
- **PJ4:** `pj_app/tests/` — `layout_import_batch_test`,
  `headless_descriptor_provider_session_test`, `source_promotion_host_test`,
  `file_loader_test`, and five gui suites
  (`main_window_layout_import_{alive,policy,cancel,lifecycle,binder}_test`) over
  the shared fakes `support/fake_import_provider.h` (scripted modes incl. the
  real-surface `progressive`/`progressive-promoted`) and
  `support/layout_import_gui_support.h`. The binder suite's
  `ProgressiveImportBindsCurveMidJobThroughRealHostSurfaces` is the feature's
  headline pin.
- **Cross-repo live E2E (the stage-5 gate):** `scripts/e2e-layout-import.sh`
  (`make e2e-layout`) — its own server on `:8082` over its own `e2e-layout`
  bucket, three REAL DSOs staged with provenance and rebuilt against
  `plugin/SDK_VERSION`, then two halves off that one staged set: PJ4's
  `main_window_layout_import_e2e_test` (`MainWindowLayoutImportE2ETest`, five
  live-gated scenarios — a SKIPPED test FAILS the harness) and three shipped
  `plotjuggler4 --layout --exit-after-layout --dump-diagnostics` legs (cold /
  warm / EAGER). Scenario identities are the frozen `e2e-8082-*` cases in
  `docs/source-descriptor-vectors.json`, consumed byte-verbatim. Operator guide:
  `docs/layout-sharing-runbook.md` (§10 for the harness, §7 for the
  diagnostic-id table).
- **Corpus decodability:** `server/internal/genmcap/genmcap_test.go`'s
  `TestRealRos2Payloads_WireShape` pins the synthetic corpus's ROS 2 CDR
  encoders (XCDR1 encapsulation prefix, per-type golden encoded lengths,
  padding never shrinking a message, full schema-resolver coverage). Every other
  count/round-trip oracle in this repo is payload-AGNOSTIC, so a broken encoder
  was invisible until the real PJ4 parser stack ran against it — these pins are
  the offline net for that blind spot.
- Windows CI note: tests mutate env only through
  `tests/test_support_env.hpp` (`setEnvVar`/`unsetEnvVar`) — raw
  `setenv`/`unsetenv` is a hard MSVC compile error.

## 6. Known gaps and recorded follow-ups

- **Stage-5 cross-repo live E2E — DONE (2026-08-01).** Real app + real plugin +
  real server, both halves off one staged DSO set; the §10 EAGER_ONLY diagnostic
  is shown firing in the shipped binary (step i leg 3) AND in-process (gui-test
  scenario 3). See §5. It found one real production bug on its first full run
  (the delegated-ingest TopicId renumbering fixed in PJ4 #501 — see I-8).
- **PJ4 #501 quality findings (5, non-blocking).** Chiefly: move the two offline
  parser-binding regression pins from
  `pj_runtime/tests/data_source_runtime_host_object_ingest_test.cpp` into
  `StreamParserSwapTest` (`..._stream_swap_test.cpp`), where the sibling
  binding-reuse cases already live, and hoist the deferred two-engine lock in
  `DataSourceRuntimeHost::cbEnsureParserBinding` into a `lockEnginePair` helper
  shared with the direct-write sibling (`WriteCore::ensureTopic` /
  `lockWriteEngines`) so the two paths cannot drift out of lock order.
- **`pj-official-plugins` SDK pin bump.** That checkout's `SDK_VERSION` is still
  `0.18.0` while PJ4 and this repo's plugin are on `0.20.0`. The E2E harness
  temp-edits it, rebuilds `data_load_mcap` + `parser_ros` against the pinned SDK,
  records provenance, and always restores the file — a workaround, not a fix.
  Moving the upstream pin is a separate `pj-official-plugins` PR.
- SDK doc fix: `PJ_toolbox_host_vtable_t` write slots are tagged `[main-thread]`
  but the engine-locked implementation + production worker-thread writes are the
  sanctioned shape — stale tag, not code.
- Dataset lifecycle/rollback ABI (one consolidated design): host deletion
  callback for per-dataset lease release (plugin side) + eager-dataset-with-
  rollback for progressive toolboxes (pj-official-plugins #240's known
  limitation). Design once, not twice.
- Generation-suffixed artifact paths + OFD locks (would lift I-3's cross-process
  refusal into graceful coexistence) — recorded in `core/file_lock.h`.
- Import byte-ceiling preferences UI (the env cap works; the prefs surface
  doesn't exist).
- Fake-provider cleanup nits (epilogue dedup, timer-block dedup, sentinel,
  dual narration) — recorded in the T7 plan's execution record.
