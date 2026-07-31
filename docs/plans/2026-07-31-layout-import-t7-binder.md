# T7 — Growing-Import Binder Implementation Plan (v2, consult-amended)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> Worktree: `~/ws_plotjuggler/PJ4/.worktrees/growing-import-binder`
> (branch `feat/growing-import-binder` on origin/main @ de8c9104; built green).

**Goal:** The last host-side piece of canonical layout import (spec v3.6 §6.2/§8): make a
batch-owned toolbox import's growth *visible and steerable* during layout restore — the
title-bar ingest strip with correct multi-owner arbitration, a per-job Stop, and a
**tested** guarantee that retained curve intents bind progressively while the import
grows, not only at drain.

**Architecture (as amended by the 2026-07-31 Codex consult, session
019fb99d-836c-7ea1-a547-f541f9b79b9c — verdict record in §5):**
`LayoutImportBatch` owns only the *correlation* — `activeImportDataset()` +
per-job cancellation — never presentation. `MainWindow` observes
`SessionManager::ingestBegan/Progressed/Ended` directly with batch-scoped connections
installed at batch handover and torn down before `layout_import_batch_.reset()`,
filtering by exact `activeImportDataset()` equality. The strip gains explicit displayed
ownership `{kind, DatasetId}` (file / interactive toolbox / layout batch) so Stop routes
by owner and an ended owner yields to another eligible ingest.

**Tech stack:** PJ4 `pj_app` (Qt6, C++20), gtest via `add_pj_app_gui_test` +
`layout_import_batch_test`, fake provider harness `pj_app/tests/support/fake_import_provider.h`.

**Status: EXECUTED — PJ4 PR #500 (2026-07-31/08-01). Execution record in §6.**

---

## 0. What is ALREADY merged (do not rebuild)

The two *binding* halves of §6.2's binder shipped in PR-B #492:

- **B3 retention channel**: `pending_items_added_conn_` stays alive past drain under
  `kRetainAndDiagnose` (`MainWindow.cpp:5451-5455`, `:5725-5728`).
- **B4 batch drain signal**: the restore-waiter surface registers the batch's
  `!isFinished()` on `finished()` (`MainWindow.cpp:5456-5466`, `MainWindow.h:1177-1194`).
- **Growth events**: `HeadlessDescriptorProviderSession` forwards ingest callbacks →
  `SessionManager::beginIngest/updateIngest/endIngest`
  (`HeadlessDescriptorProviderSession.cpp:86-95`); `updateIngest` → `samplesIngested` →
  `CatalogModel::rebuildIfChanged` → `itemsAdded` is live.
- **#497 teardown reorder** shipped separately — the stage-4 plan line
  (`2026-07-31-layout-import-stage4-provider.md:273`) is stale; Task 4 fixes it.

**T7's remaining scope:** (a) batch correlation surface + per-job cancel, (b) the
`MainWindow` presentation half — strip ownership arbitration + batch observation + Stop
route (today the strip is `launchToolbox`-only, last-started-wins global state,
`MainWindow.cpp:8584-8645`; `stopAllToolboxImports` `:4248` reaches only owner-guarded
interactive refs), and (c) **pinning mid-import progressive binding with real-surface
tests** (the alive test asserts binding only at drain —
`main_window_layout_import_alive_test.cpp:29-75`).

## 1. LOCKED decisions (Codex-amended; do not re-litigate)

- **D1 (amended) — batch owns correlation only; MainWindow owns observation.**
  `LayoutImportBatch` gains `std::optional<PJ::DatasetId> activeImportDataset() const`,
  set in the `on_dataset` lambda (`LayoutImportBatch.cpp:497-501`, alongside the
  `produced_datasets_` ledger append), cleared at that job's terminal before the
  `startNextJob()` chain. NO presentation `Hooks`, NO batch-side connections to
  `SessionManager` (batch teardown while a session dtor synchronously delivers
  unfinished ingest terminals via `~ToolboxRuntimeHost` (`ToolboxRuntimeHost.cpp:46`)
  would re-enter them — consult blocker 3). MainWindow installs batch-scoped observers
  on the three `SessionManager` signals at batch handover (`MainWindow.cpp:4506`
  region), disconnects them and clears strip ownership BEFORE
  `layout_import_batch_.reset()` (`resetLayoutImportBatch`, `MainWindow.cpp:5801-5805`)
  and in `supersedeActiveRestore` (`:5820`).
- **D2 (rejected race) — exact-id filtering, no reconcile-late map.** The supported
  delivery order is `on_dataset` before `ingestBegan` on the GUI thread: the provider
  fires `datasetCreated` (`fetch_worker.cpp:1829`) and only later `progressStart`
  (`:1855`) from the SAME worker; the session posts `on_dataset` with forced
  `Qt::QueuedConnection` (`HeadlessDescriptorProviderSession.cpp:197`) and the runtime
  host posts begin via `Qt::AutoConnection` from that same off-UI thread
  (`ToolboxRuntimeHost.cpp:288`, `data_source_protocol.h:219`) — same GUI event queue,
  posted in order. Filter every event by `activeImportDataset()` equality; pin the
  ordering with an integration assertion (Task 3). No `unclaimed_begins_` (it would
  also misbehave under #498's multiple folded interactive jobs).
- **D3 (amended) — strip Stop cancels ONLY the active import job, keep-partial.**
  `HeadlessDescriptorProviderSession::startImport` returns a `JobId`; new nonblocking
  `cancelJob(JobId)` invokes that job's `JoinableJob::cancel()` (registry already exists,
  `HeadlessDescriptorProviderSession.h:181`; only `cancelAll` is exposed today). Batch
  stores `{active session, JobId}` and exposes `cancelActiveImportJob()`. A cancelled
  job records `kCancelled` and the batch continues (`LayoutImportBatch.cpp:532` already
  supports this). Whole-batch `cancel()` remains for explicit layout
  cancellation/rollback and shutdown. (Documented fallback if the API stalls:
  non-cancellable batch strip via empty label/icon, `IngestProgressWidget.h:42` — never
  map plain Stop to transaction rollback.)
- **D4 (locked) — promotion preserves `DatasetId`.** Strict in-place replacement:
  `SourcePromotionHost.cpp:252` (`replace_dataset_id` + `require_replacement`),
  `FileLoader.cpp:1231/:1298/:1590`, `SessionManager::beginRefill` keeps ids stable
  (`SessionManager.cpp:504`), and promotion rejects a commit that didn't preserve the
  target id (`SourcePromotionHost.cpp:54`). Binder state and curve keys survive. The
  test MUST run a real promotion transaction (mock_file_source_plugin staged) — a fake
  that merely reports `kSucceededPromoted` is insufficient.
- **D5 (locked) — zero-`on_dataset` degrades silently.** Race-loss/failure jobs
  (`descriptor_import_provider.cpp:176-209`) announce nothing: `activeImportDataset()`
  stays empty, no event matches, no strip, terminal chains normally
  (`LayoutImportBatch.cpp:515`). Focused unit test.
- **D6 (amended) — the fake drives the REAL bound host services; prescribed mechanism:**
  1. `fakeBind` caches `ToolboxHostView` + `ToolboxRuntimeHostView` (+
     `SourcePromotionHostView` for the promotion test) — today it discards the registry
     (`fake_import_provider.h:247`);
  2. the job calls `ToolboxHostView::createDataSource()` and announces that REAL handle
     via `on_dataset` (today's counter id `:304` identifies nothing);
  3. `createDatasetIngest(dataset.id)` + `progressStart`;
  4. create topic/field + append a record through the real write host
     (`plugin_data_api.hpp:1040`);
  5. ONE deterministic `progressUpdate` tick (first tick is immediately
     throttle-eligible; the 50 ms throttle setter lives on the concrete host only,
     `ToolboxRuntimeHost.h:130` — do NOT plan multiple ticks), then block at a test
     gate;
  6. GUI test asserts the curve bound while job + progressive restore are still active;
  7. release gate → `progressFinish` → release ingest → completion notify → terminal.
  The start-return gate invariant must be preserved (no callback before `start_import`
  returns — `LayoutImportBatch.cpp:497`). The tick flushes the write host before
  `updateIngest` (`ToolboxRuntimeHost.cpp:310`) → `samplesIngested` →
  `CatalogModel::itemsAdded` (`CatalogModel.cpp:296/:568/:872`). Out-of-band row
  injection is allowed ONLY as a separate pure binder unit test, never the headline
  integration test.
- **D7 (locked) — plain non-materialize loads traverse zero new code.** The batch
  exists only when a `<materialize>` record is present (`MainWindow.cpp:4400`);
  observers exist only while a batch is active and filter by exact id.
- **D8 (new, consult blocker) — strip displayed ownership.** Current strip state is
  global last-started-wins (`MainWindow.cpp:8630/:8645`). Introduce explicit displayed
  owner `{kind ∈ {file, interactive, layout-batch}, DatasetId}`: Stop routes by
  displayed owner (interactive → existing owner-guarded refs; layout-batch →
  `cancelActiveImportJob()`; never a fake `ToolboxIngestRef` for headless), and when
  the displayed owner ends, the strip switches to another eligible active ingest
  instead of leaving stale text/progress.
- **D9 (new, #498 coexistence) — headless jobs never enter the panel-fold path.**
  Interactive ingest start folds its owning takeover panel (`MainWindow.cpp:8607`);
  batch imports have no panel and must not traverse it. Batch drain must not cancel or
  displace busy pinned panels (`MainWindow.cpp:8950`). Covered by test, not just code
  review.

## 2. File map

- Modify: `pj_app/src/LayoutImportBatch.h/.cpp` — `activeImportDataset()`, active
  `{session, JobId}`, `cancelActiveImportJob()`
- Modify: `pj_app/src/HeadlessDescriptorProviderSession.h/.cpp` — `startImport` returns
  `JobId`; `cancelJob(JobId)`
- Modify: `pj_app/src/MainWindow.h/.cpp` — batch-scoped SessionManager observers, strip
  displayed-ownership arbitration (D8), batch Stop route, teardown/supersede clearing,
  comment truth pass
- Modify: `pj_app/tests/support/fake_import_provider.h` — real-surface progressive mode
  (D6), real promotion mode
- Modify: `pj_app/tests/support/layout_import_gui_support.h` — peer accessors (strip
  owner/visibility, batch active dataset, observer count)
- Modify: `pj_app/tests/layout_import_batch_test.cpp` — correlation + per-job cancel +
  zero-`on_dataset` suites
- Create: `pj_app/tests/main_window_layout_import_binder_test.cpp` — new gui-test target
  `MainWindowLayoutImportBinderTest` (register in `pj_app/CMakeLists.txt` next to
  `:378-383`, with `PJ_MOCK_FILE_SOURCE_PLUGIN_PATH` + `mock_file_source_plugin`
  dependency for the promotion leg)
- Modify (docs, this repo): spec §6.2/§8/§9.3, stage-3 plan `:197-203`, stage-4 plan `:273`

## 3. Tasks

### Task 1 — Correlation + per-job cancel (batch + session)

Non-GUI suite (`layout_import_batch_test`, `headless_descriptor_provider_session_test`).
TDD, red first.

- [ ] **1a (red)**: tests —
  1. `activeImportDataset()` empty before start; set to the announced id while the job
     runs; cleared at terminal before the next job starts (sequential invariant);
  2. zero-`on_dataset` job (fail-mode fake): stays empty end-to-end, batch completes
     (D5);
  3. `startImport` returns a distinct `JobId` per job; `cancelJob(id)` cancels only that
     job (blocked fake job → cancel → `kCancelled` terminal) and is nonblocking;
     `cancelJob` on an unknown/finished id is a safe no-op;
  4. `cancelActiveImportJob()` with a running job → that job's `kCancelled` terminal,
     batch CONTINUES to the next job (`LayoutImportBatch.cpp:532` path), earlier
     produced datasets kept (keep-partial, no rollback);
  5. `cancelActiveImportJob()` with no active job → no-op.
- [ ] **1b**: implement (session `JobId` + `cancelJob`; batch member
  `std::optional<PJ::DatasetId> active_import_dataset_` + `{session*, JobId}` of the
  active job + the two accessors). No SessionManager connections in the batch (D1).
- [ ] **1c**: green — `ctest -R 'LayoutImportBatchTest|HeadlessDescriptorProviderSessionTest'`.
- [ ] **1d**: commit.

### Task 2 — MainWindow: strip displayed ownership + batch observation + Stop route

GUI suite (new `MainWindowLayoutImportBinderTest`).

- [ ] **2a (red)**: peer accessors (displayed strip owner kind+id, strip visible/label,
  observer connection count) + tests —
  1. progressive fake batch job → strip appears with the job's label, shows the tick,
     clears at end; observers exist only while the batch is active (D7);
  2. strip Stop during the batch job → only that job cancelled (`kCancelled`), published
     rows kept, batch continues, restore concludes; NO rollback;
  3. plain cache-hit layout (no `<materialize>`) → zero observers, no strip (D7);
  4. D8 arbitration: batch job + concurrent interactive ingest — displayed owner is
     deterministic, Stop routes to the displayed owner only, and when the displayed
     owner ends the strip switches to the surviving ingest (no stale text);
  5. D9: headless job start does NOT fold any panel; batch drain leaves a busy pinned
     interactive panel running (#498 coexistence);
  6. `resetLayoutImportBatch`/`supersedeActiveRestore` mid-ingest → observers
     disconnected and strip ownership cleared BEFORE the batch dies; no crash, no stale
     Stop route; a surviving interactive ingest is reselected.
- [ ] **2b**: implement — displayed-owner struct on MainWindow; batch-scoped observer
  installation at handover (`MainWindow.cpp:4506` region); Stop routing by owner;
  teardown ordering (disconnect + clear ownership, then reset). All new code behind
  `layoutImportBatchActive()` except the D8 ownership struct refactor of existing strip
  state (interactive-only behavior must remain unchanged — the four existing gui suites
  are the regression net).
- [ ] **2c**: green — `ctest -R 'MainWindowLayoutImport'` (all five suites).
- [ ] **2d**: commit.

### Task 3 — Real-surface progressive fake + the mid-import binding pin

- [ ] **3a (red)**:
  1. **headline pin**: layout references a curve for the topic the progressive fake
     publishes mid-job (D6 mechanism, gate held) → `totalCurveCount == 1` while
     `progressiveInFlight` is still true and the job has NOT terminated — the
     mid-import generalization of `MissImportKeepsRestoreAliveAndBindsAtDrain`, which
     must keep passing unchanged;
  2. D2 ordering pin: instrument (peer) that the batch's `on_dataset` correlation was
     installed before the first `ingestBegan` delivery for that id;
  3. D4: promoted progressive job through the REAL promotion transaction
     (`SourcePromotionHostView` + staged mock_file_source_plugin, the
     `source_promotion_host_test.cpp:180` shape) → after replace, the bound curve still
     resolves (ids stable), `kSucceededPromoted` outcome recorded;
  4. out-of-band pure binder unit test (separate, non-headline): rows injected via
     `pj_test::createDataset`/`addScalarTopic` + driven `SessionManager` lifecycle —
     the cheap regression net for the binder logic alone.
- [ ] **3b**: implement the fake's progressive + real-promotion modes (D6 steps 1-7,
  start-return gate preserved); any minimal production fix the red tests expose.
- [ ] **3c**: green; full `ctest` for `pj_app`.
- [ ] **3d**: commit.

### Task 4 — Docs, comment truth, audit

- [ ] Spec (`docs/canonical-layout-import.md`, this repo): §6.2/§8 binder items →
  as-built wording; §9.3 stage-3 bundle line → T7 DONE.
- [ ] Stage-3 plan `:197-203`: check the T7 box with the PR reference. Stage-4 plan
  `:273`: fix the stale "rides the T7 PR" teardown line (#497 shipped it).
- [ ] PJ4 comment truth pass: `MainWindow.h:727-731`, `:1171-1176`, `:1177-1181`,
  `MainWindow.cpp:5725-5728` — "later — whatever T7 registers" / "T7 drives this" →
  present tense pointing at the real code.
- [ ] Documentation audit per CLAUDE.md (grep docs for touched behaviors), full ctest,
  commit; PR body includes the consult verdict + D1–D9.

## 4. Non-goals (recorded)

- Concurrent import jobs (v1 sequential — `job_active_` bool is the invariant).
- Scene3D TF / playback-focus / Custom-Series per-dataset presentation beyond what the
  drain path already does (only if a red test proves a gap).
- Prewarm/validate import modes (zero-`on_dataset` covered as degradation only).
- A generalized multi-ingest strip UI (D8 is arbitration of the existing single widget,
  not a new list UI).

## 5. Codex consult record (2026-07-31)

Session `019fb99d-836c-7ea1-a547-f541f9b79b9c` (codex-exec, read-only, strong tier).
Verdict on plan v1: **NOT ready** — 5 blockers, all folded into v2 above:
D1 amended (no Hooks; batch = correlation only; MainWindow-owned observation — the
batch-connection variant was teardown-unsafe against `~ToolboxRuntimeHost`'s synchronous
terminal delivery), D2's marshal race REFUTED with a definitive two-path trace (same
worker, same GUI queue, ABI order holds; `unclaimed_begins_` would misattribute under
#498 multi-job), D3 amended to per-job cancel (whole-batch rollback behind the shared
Stop icon rejected; `LayoutImportBatch.cpp:532` already supports per-job kCancelled
continuation), D4/D5/D7 locked (D4 verified through
`SourcePromotionHost.cpp:252`/`FileLoader.cpp:1231-1590`/`SessionManager.cpp:504`),
D6 amended to mandatory real-surface fake (registry views live for the session lifetime;
single deterministic tick — the throttle setter is host-concrete-only), plus new D8
(strip displayed ownership) and D9 (#498 coexistence) with dedicated tests.

## 6. Execution record (2026-07-31/08-01)

Subagent-driven, two-stage review per task (spec then quality), red-first TDD
throughout. **PJ4 PR #500** (`feat/growing-import-binder`, 6 commits on
de8c9104/main):

- **Task 1** `5e6521e3` — correlation + per-job cancel: session `JobId`
  (`Expected<JobId>` startImport) + nonblocking `cancelJob`; batch
  `activeImportDataset()` + `cancelActiveImportJob()` (keep-partial). Fake gained
  the `announce-then-block` mode (spec-adjudicated: minimal necessary — the
  mid-job window is unobservable without a post-announce gate). Spec-compliant;
  quality approved zero findings.
- **Task 2** `909cc81f` + review rounds `489ea86f`, `b3ffc727` — strip displayed
  ownership `{kFile,kInteractiveToolbox,kLayoutBatch}` replacing last-started-wins
  (arbitration: file > interactive > layout-batch, newest-begin-wins within rank,
  batch strictly residual); MainWindow batch-scoped SessionManager observers with
  teardown before every batch reset INCLUDING `~MainWindow` (review round: the
  dtor-side teardown converted an undocumented declaration-order safety accident
  into a structural invariant); per-job Stop route; D9 no-fold/no-displace.
  Quality review's own shuffle verification caught a residual strip-engagement
  leak beyond its first prescription — fixed and re-verified (seeds 1/42/7).
- **Task 3** `9138bda1` — ZERO production changes. Real-surface progressive fake
  (cached bound views; createDataSource → on_dataset(real handle) →
  createDatasetIngest/progressStart → real write-host rows → ONE tick → gate).
  Pins: mid-job curve binding through the real chain (headline); D2
  on_dataset-before-ingestBegan delivery; D4 real promotion transaction with
  DatasetId/TopicId stability (kSucceededPromoted proven via SourceRecord +
  no-eager-only-diagnostic — adjudicated stronger than result() sampling);
  out-of-band pure binder net. Spec reviewer empirically reproduced the red phase
  by reverting the fake. Quality approved (4 nits recorded, below fix bar).
- **Task 4** `ea582c52` — comment truth: every future-tense T7 reference in PJ4
  made present-tense as-built (verified comment-only diff); spec §6.2/§8/§9.3 +
  stage-3 plan closed in this repo alongside the PR.

Full pj_app ctest 31/31 throughout; binder suite (10 scenarios) shuffle-stable.
Recorded follow-ups: SDK doc fix — `PJ_toolbox_host_vtable_t` slots tagged
`[main-thread]` while the engine-locked implementation + production worker-thread
writes are the sanctioned shape (spec-reviewer-confirmed stale tag, not code);
Task-3 quality nits N1–N4 (fake epilogue dedup, timer-block dedup, sentinel
conflation, dual choreography narration) for a future cleanup pass.
