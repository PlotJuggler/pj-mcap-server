# Stage 3 — PJ4 Host Half of Canonical Layout Import — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the PJ4 host half of `docs/canonical-layout-import.md` (spec v3.6 §6.2/§8):
`LoadTicket`, source records + identity resolution, `<materialize>` save/load,
stable-ID loader matching, the `pj.source_promotion.v1` host service, `LayoutImportBatch`
with rewrite-then-classify, and the growing-import binder. SDK 0.20.0 (the ABI) is
released; the plugin side (stages 1–2) is merged in pj-mcap-server.

**Non-goals:** the plugin provider implementation (stage 4), E2E + docs (stage 5),
the generic ImportJob envelope (§11 trigger not met), concurrent provider imports.

**Repo:** `~/ws_plotjuggler/PJ4` (upstream `PlotJuggler/PJ4`, main). Work in a worktree
via the repo's `worktree-new.sh` (the `worktree` skill). **PJ4 rule: NEVER commit
autonomously — surface diffs, get explicit approval per commit.** Windows is a required
target (no POSIX-only APIs). Build gate: `./build.sh` + full `ctest --test-dir build
--output-on-failure`; verify touched `.cpp` actually recompiled.

## 0. Grounding deltas (2026-07-30 recon vs the 2026-07-27 spec citations)

The spec's anchors were re-verified against PJ4 main (~`48712145`). Four findings
CHANGE the plan; the rest moved lines only (fresh anchors inline below).

1. **Desktop layout loads do not enforce the saved plugin.** `LoadHints::
   require_expected_plugin` defaults false and only the WASM browser-replay path sets
   it (`MainWindow.cpp:4884`); desktop `loadLayoutFromPath` (:4268-4421) takes the
   first extension match (`FileLoader.cpp:1135`) and uses `expected_plugin_id` only to
   gate dialog-skip (`:1318`). The spec's stable-ID matching task must ALSO unify this
   desktop/browser divergence (D2 below).
2. **`promptMissingCurves` is already conditional** (`onProgressiveLayoutDrained`
   gates on `!shown.isEmpty()` at `MainWindow.cpp:5539`; `applyWorkspace` on
   `policy == kPrompt`). The batch's non-interactive policy work is therefore about
   *routing a policy into these existing gates* (a `MissingCurvePolicy` already
   exists), not about removing an unconditional call.
3. **Two catalog-empty aborts, not one:** WASM `applyBrowserRestoredLayout`
   (:5123-5128) and desktop `applyRestoredLayout` (:5172-5176). The batch must keep
   the layout alive across a cache-miss import on the DESKTOP path; the WASM guard is
   out of scope (§6.4: WASM emits the unsupported-diagnostic instead).
4. **FileLoader already has a "current load" generation** (`load_generation_`,
   `cancelCurrent(generation, keep_partial)`, `loadGenerationAdvanced`) but queued
   requests carry no id until dequeued. `LoadTicket` layers a per-REQUEST id on top;
   it must compose with, not replace, the generation machinery (D3).

Other refreshed anchors: `LoadedSource{path,prefix,plugin_id,plugin_config_json}`
(`SessionManager.h:295`, vector never pruned — save-time intersection at
`MainWindow.cpp:6364` filters), `resolveDatasetIdentity` 3-tier resolver
(`SessionManager.h:120`), #470 callbacks wired ONLY in `MainWindow` (~:7941-8080,
`toolbox_active_imports_`, strip arbitration `adoptToolboxIngestStrip` :4207 — the
promised lifecycle hoist is still TODO), busy-restore binder =
`beginProgressiveLayoutRestore` (:5255-5295) + `PendingDisplayBinder` +
`CatalogModel::itemsAdded`, replace path `FileLoader.cpp:1188/2132`, save filter
`appendDataSourceElement` :6364, `extractDataSource`/`remapDatasetSourcePaths`
unchanged at `LayoutXml.cpp:77/:546`.

## Decisions to lock (Codex consult before implementation)

- **D1 — identity→DatasetId resolution shape:** extend `SessionManager::
  resolveDatasetIdentity`'s tiering with a source-record tier (provider identity
  match), vs a separate record-keyed map. Lean: extend the resolver (single authority,
  spec pins "resolved against the host's source-record registry to a DatasetId" —
  registry can be the records themselves; rowid-style separate maps invite drift).
- **D2 — plugin-matching unification:** make layout-driven loads (desktop + browser)
  both resolve the loader by STABLE MANIFEST ID with display-name compat fallback,
  and both set `require_expected_plugin` when the layout names a plugin. Risk: legacy
  layouts whose recorded name matches no manifest id — fallback tier covers; missing
  plugin becomes a per-source failure (today desktop silently mis-loads via first
  extension match — arguably a latent bug).
- **D3 — LoadTicket shape:** `LoadRequestId` (uint64) minted at ENQUEUE time, carried
  in `LoadRequest`, mapped to `load_generation_` when dequeued; exactly-once terminal
  signal `loadFinished(ticket, outcome{Loaded|Failed|Cancelled}, effective_path,
  DatasetId)`; `cancel(ticket)` = erase-from-queue (queued) or
  `cancelCurrent(generation)` (active). Existing `loadFile()/queueDrained` signals
  untouched for current callers.
- **D4 — where the promotion service lives:** the `pj.source_promotion.v1`
  implementation needs FileLoader + SessionManager + per-plugin-instance binding.
  Lean: a small `pj_app/src/SourcePromotionHost.{h,cpp}` (shell wiring is pj_app's
  role; pj_runtime must stay Widgets-free and FileLoader lives in pj_app), registered
  into the SDK service registry at toolbox launch where the host already builds
  per-instance callbacks (~`MainWindow.cpp:7941`).
- **D5 — batch policy plumbing:** `LayoutImportBatch` owns
  `interactive: bool` (captured at construction — `startup_auto_reload_` resets on
  return, `MainWindow.cpp:5095-5101`, so it cannot govern late jobs) and passes a
  `MissingCurvePolicy` into the drain path instead of the current implicit prompt.

## Tasks (each = TDD; test conventions per pj_app: standalone gtest for
## Widgets-free logic, `add_pj_app_gui_test` + `friend class …TestPeer` for
## MainWindow-coupled scenarios)

- [ ] **T1 — FileLoader `LoadTicket`** (additive, D3). Mint ids at enqueue; terminal
  exactly-once signal incl. cancel/failure paths + effective path + DatasetId;
  cancel-by-ticket for queued AND active; `loadGenerationAdvanced`-vs-ticket mapping
  exposed for the stop-dialog. Tests: exactly-once on success/failure/cancel(queued)/
  cancel(active), legacy callers unaffected (existing suites stay green).
- [ ] **T2 — SessionManager source records + resolution (D1) + #470 lifecycle hoist.**
  Extend `LoadedSource` with optional `{provider_id, source_identity,
  descriptor_json}`; `attachSourceRecord(DatasetId, …)` (called post-promotion,
  atomic with the catalog notification); invalidate on dataset delete, carry through
  merge (`DatasetMergeActions` currently touches no source state — add it); extend
  `resolveDatasetIdentity` with the record tier. Hoist `toolbox_active_imports_`
  bookkeeping from MainWindow into SessionManager (strip stays in MainWindow).
  Tests: SessionManager unit tests (existing `session_manager_test.cpp` pattern).
- [ ] **T3 — `<materialize>` save/load.** `appendDataSourceElement` emits the
  provider-generic child (provider id, identity attr, canonical-descriptor CDATA) for
  sources carrying records; `extractDataSource` parses it into `DataSourceRef`; WASM
  path detects it and emits the §6.4 "unsupported in browser" diagnostic (never into
  durable browser storage — hard WASM constraint). Round-trip + old-reader-ignore +
  WASM-diagnostic tests (`layout_xml_test` standalone pattern +
  `main_window_source_layout_test` analog).
- [ ] **T4 — stable manifest-ID loader matching (D2).** Manifest-id-first match with
  display-name fallback in `FileLoader`; layout loads set `require_expected_plugin`;
  save path records the manifest id (keep writing the display name for old readers if
  the schema needs it — verify what `<plugin ID=…>` readers exist). Tests: id match,
  name fallback, missing-plugin per-source failure, legacy-layout compat.
- [ ] **T5 — `pj.source_promotion.v1` host service (D4).** `SourcePromotionHost`
  implementing `promote_to_file_source(request, result_cb)`: validate dataset
  generation, drive the stock `replace_dataset_id` load via a T1 ticket (preset
  config, `skip_dialog`, rewrite hint), on terminal result attach the source record
  (T2) + fire `result_cb` exactly once (ok=false on rejection/rollback — EAGER_ONLY
  semantics stay plugin-side). Bind per toolbox instance at launch (host-derived
  provider identity, unspoofable). Tests: fake-plugin-driven promote round trip,
  rejection, vanished-dataset, re-entrant result delivery per the 0.20.0 contract
  (`accepted≠succeeded`, result MAY fire before return).
- [ ] **T6 — `LayoutImportBatch` + rewrite-then-classify (D5).** New
  `pj_app/src/LayoutImportBatch.{h,cpp}`: owns working layout + prior workspace
  (rollback), the §6.2 seven-step ordering (validate descriptor → provider
  `query_descriptor` (strictly bounded) → resolve effective path → rewrite
  `<fileInfo filename>` → `remapDatasetSourcePaths` → rewrite loader preset →
  classify already-loaded/hit/miss), per-job outcomes via T1 tickets + provider job
  handles behind one `start/cancel/join/terminal` interface, sequential jobs (v1),
  cancellation/shutdown joins, `interactive` policy persisted. Desktop
  catalog-empty guard reworked so a pending import keeps the restore alive.
  Trust-gate UI: consolidated confirm prompt (interactive) / fail-with-diagnostic
  (non-interactive). Tests: gui-test per scenario (batch classify matrix, policy
  persistence, per-job failure isolation, cancel, rollback).
- [ ] **T7 — growing-import binder.** Batch-scoped generalization of the
  busy-restore flow: `PendingDisplayBinder` + `CatalogModel::itemsAdded` retries run
  while a batch-owned toolbox import grows (#470 publish ticks); the BATCH provides
  the drain signal for its jobs (not `FileLoader::queueDrained`). Ordinary non-cloud
  layout loads must traverse zero new code. Tests: gui-test with a fake provider
  pushing progressive datasets.

**PR strategy:** two PJ4 PRs — PR-A = T1–T4 (additive foundations, independently
mergeable, no behavior change without a `<materialize>` layout except D2's stricter
plugin matching), PR-B = T5–T7 (the batch + service). Each gets the team adversarial
review; every commit needs explicit user approval (PJ4 rule).

**Cross-repo contract:** descriptor vectors (`docs/source-descriptor-vectors.json`)
consumed byte-identically by T3/T6 tests; the SDK service/extension contracts are
pinned by SDK 0.20.0's own suites — host tests exercise through the REAL SDK headers
(Conan `plotjuggler_sdk/0.20.0`).
