# Plan D — PJ Cloud Connector: PlotJuggler 4 DataSource plugin (`pj_cloud`)

> **Status: DEFERRED — Milestone 2b (M2b).** Planned in full now; built only after the
> M1 PoC is approved. This is the **third client artifact** of the PJ Cloud Connector:
> Plan B ([`2026-05-28-pj-cloud-client-cpp.md`](./2026-05-28-pj-cloud-client-cpp.md))
> ships the Widgets-free `client-core` static lib + `pjcloud-cli`; **Plan D lifts that
> `client-core` UNCHANGED into a PlotJuggler 4 DataSource plugin** and reuses the
> conceptual design of the existing `toolbox_mosaico` plugin
> (`/home/davide/ws_plotjuggler/pj-official-plugins/toolbox_mosaico`).
>
> **Canonical references:** design spec §9 (client design)
> [`2026-05-28-pj-cloud-connector-design.md`](./2026-05-28-pj-cloud-connector-design.md);
> unified plan §3.3 / §3.2 (seams 5 & 6) / §5 (M2b row) / §6 (cache/ingest tests)
> [`2026-06-03-unified-cloud-connector-plan.md`](./2026-06-03-unified-cloud-connector-plan.md);
> Plan B (the `client-core` public surface this plugin links).
>
> **Execution sub-skill (required):** `superpowers:subagent-driven-development` — each task
> below is one TDD section (write the failing gtest first, then the implementation). Follow
> the spec's **§13 phased build order**: Plan D runs only after Plan A (server) and Plan B
> (`client-core`) are green.

---

## 0. Grounding notes — corrections against the design spec (READ FIRST)

These are load-bearing facts verified against the **real PJ4 SDK headers** under
`/home/davide/ws_plotjuggler/PJ4/plotjuggler_sdk/`. Where the design spec names an API that
does not exist in the SDK, the plan uses the closest **real** entry point and flags the gap
as an explicit assumption (per the grounding rule: an explicit assumption is acceptable; an
invented API is not).

1. **Plugin shape = DataSource (`FileSourceBase`) with an embedded dialog — NOT a Toolbox.**
   Mosaico is a `ToolboxPluginBase` (non-modal panel, writes via `PJ::sdk::ToolboxHostView`).
   `pj_cloud` is a **one-shot importer**: `PJ::FileSourceBase` (from
   `pj_base/sdk/data_source_patterns.hpp`) advertising
   `extraCapabilities() == PJ::kCapabilityDelegatedIngest | PJ::kCapabilityHasDialog`, with a
   `PJ::DialogPluginTyped` member returned through `getDialog()` via `PJ::borrowDialog(dialog_)`.
   Both vtables ship from one `.so` (`PJ_DATA_SOURCE_PLUGIN(...)` + `PJ_DIALOG_PLUGIN(...)`),
   exactly as documented in `pj_plugins/docs/data-source-guide.md` §"Dialog Integration"
   (host-side flow: `capabilities() & kCapabilityHasDialog` → `getDialog()` → host shows the
   borrowed dialog → dialog mutates the source's state → `start()` → `importData()`).
   The Mosaico **dialog/state/worker** code lifts cleanly; only the *host write surface*
   changes from `ToolboxHostView` to the DataSource `writeHost()` / `runtimeHost()`.

2. **Raw MCAP forwarding uses delegated ingest — two REAL calls.** The design spec's
   "DatastoreSink forwards (topic, schema, payload, log_time) to the host MessageParser
   family" maps **exactly** onto `DataSourceRuntimeHostView` (from
   `pj_base/sdk/data_source_host_views.hpp`):
   - `runtimeHost().ensureParserBinding(PJ::ParserBindingRequest{topic_name, parser_encoding,
     type_name, schema /*Span<const uint8_t>*/, parser_config_json})` → `Expected<ParserBindingHandle>`.
   - `runtimeHost().pushMessage(handle, host_timestamp_ns, fetch_message_data)` where
     `fetch_message_data` is a **callable returning `sdk::PayloadView` (zero-copy) or
     `std::vector<uint8_t>`**. The host applies its `ObjectIngestPolicy`; the callable MUST
     be **idempotent + thread-safe** and **capture the MCAP chunk by `shared_ptr`** so the
     buffer outlives every deferred pull (the header spells this out at lines 211–301).
   This is the **delegated-ingest** path → capability flag `kCapabilityDelegatedIngest`. The
   plugin ships **no decoders**; ROS/Protobuf/JSON-Schema `MessageParser` plugins decode.

3. **`pjcloud://` URI scheme + `FileSourceBase::launchCustomOpenDialog()` are NOT in the SDK.**
   Searched the whole SDK tree: there is **no** `launchCustomOpenDialog`, no URI-scheme
   registration hook, no `uri_scheme` manifest key. The real importer-open flow is the
   embedded-dialog flow in note (1): the host opens the plugin's dialog, the user picks
   files/topics/time-range, `onAccepted()` snapshots the selection, and `importData()` runs
   the session. **ASSUMPTION A2 (flagged in Task 1 & Task 12):** we model "open a cloud
   session" as the embedded-dialog import flow and stash the chosen target in `saveConfig()`
   JSON (so re-open / session-restore works), with the `pjcloud://` string used only as an
   **opaque config token inside that JSON**, not as a host-registered scheme. If a real
   custom-scheme hook lands in a later SDK, it slots in at `loadConfig()` with no dialog
   changes. Do **not** invent `launchCustomOpenDialog`.

4. **Settings persistence = `PJ::sdk::SettingsView` (`setValue` / `value`)**, acquired from
   the registry as the optional `SettingsStoreService`. Mosaico's `SettingsStore` wrapper
   (`getString/setString/getStringList/setStringList/getInt/...`) lifts unchanged; only the
   key prefix changes `mosaico/` → `pj_cloud/`. **qtkeychain** stores *only* the bearer token
   + TLS material (secrets); non-secret prefs (URI history, last selection, slider range)
   stay in `SettingsView`.

5. **`SessionKey` is provided by Plan B (Task 8a); Plan D provides the cache USE.** Plan B
   ships only the key utility (`computeSessionKey(server_uri, file_ids[], topics[],
   time_range)` with normalized sort + FNV-1a hash). Plan D builds the in-memory
   `SessionCache` (lookup / store / LRU evict) keyed on it.

6. **`writeHost()` and `runtimeHost()` are only valid inside `start()`/`importData()`.** The
   worker thread does host writes from within the `importData()` call stack (the FileSourceBase
   state machine holds the source in `kStarting` for the duration). The dialog phase (catalog
   browse) uses only `CatalogClient` over `client-core` — no host writes.

---

## 1. Goal

Ship `pj_cloud`, a PlotJuggler 4 **DataSource plugin** that lifts Plan B's Widgets-free
`client-core` unchanged and reuses the `toolbox_mosaico` dialog/state/worker design to let a
user browse a PJ-Cloud server's MCAP catalog, select files + topics + a time range, and
stream a reconstructed session into PlotJuggler — forwarding each **raw** MCAP message to the
host's existing `MessageParser` plugins (the connector ships no decoders), with an in-memory
`SessionCache` so a repeated open within the session avoids re-download.

## 2. Architecture

The plugin is a single `.so` exporting two vtables: a `PJ::FileSourceBase` source
(`kCapabilityDelegatedIngest | kCapabilityHasDialog`) and an embedded `PJ::DialogPluginTyped`
returned via `getDialog()`; the dialog drives a Mosaico-style worker thread (command queue +
`onTick`-drained event queue) whose transport member is swapped from Arrow-Flight to a
`BackendConnection` adapter wrapping `client-core` (`CloudConnection`/`MessageDispatcher`/
`CatalogClient`/`SessionClient`), which presents a multi-file selection as **one synthetic
stitched `SequenceRecord`** mapping to a single `OpenFresh{file_ids[], topic_names[],
time_range}`. On accept, `importData()` runs the session: a `RawMcapForwardingDriver`
(implementing `client-core`'s `SessionSink`) forwards each raw record to
`runtimeHost().ensureParserBinding()` + `runtimeHost().pushMessage()` for the host parsers to
decode, while an in-memory `SessionCache` (keyed on Plan B's `SessionKey`) short-circuits a
repeat open. Secrets persist via qtkeychain; non-secret UI state via `PJ::sdk::SettingsView`.

## 3. Tech stack (one line)

C++20, CMake ≥ 3.21 + Conan 2, PJ4 plugin SDK (`plotjuggler_sdk::plugin_sdk`,
`pj_base/sdk` + `pj_plugins/sdk`), Qt 6 Widgets/Gui + Qt 6 Core/Network/WebSockets (the
latter via the lifted `pj_cloud::client-core` from Plan B), `qtkeychain`, embedded Lua/sol2
(lifted from Mosaico), gtest/ctest.

## 4. File structure

```
PJ4/pj_plugins/pj_cloud/
├── CMakeLists.txt                         # links pj_cloud::client-core + Qt6::Widgets + qtkeychain; Widgets-free guard on client-core
├── conanfile.py                           # qtkeychain + (transitively) client-core deps
├── manifest.json                          # id/name/version/category=datasource; declares dialog + delegated-ingest; documents pjcloud token
├── README.md                              # end-user docs: connect, browse, select, import, cache, tags, resume
├── src/
│   ├── pj_cloud_source.cpp                # PJ::FileSourceBase: getDialog(), extraCapabilities(), importData(), save/loadConfig(); PJ_DATA_SOURCE_PLUGIN + PJ_DIALOG_PLUGIN
│   ├── cloud_open_dialog.hpp/.cpp         # PJ::DialogPluginTyped (lifted MosaicoDialog → cloud_connector_); manifest()/ui_content()/widget_data()/onTick + typed handlers
│   ├── dialog_state.hpp                   # DialogState + SequenceRecord (lifted; SequenceRecord.name = file_id; flat metadata)
│   ├── backend_connection.hpp/.cpp        # transport adapter: client-core wrapper; file_ids[] → synthetic stitched SequenceRecord → one OpenFresh
│   ├── fetch_worker.hpp/.cpp              # lifted Mosaico worker (cmd queue + callbacks); transport member = BackendConnection; host writes via RawMcapForwardingDriver
│   ├── raw_mcap_forwarding_driver.hpp/.cpp# SessionSink impl: ensureParserBinding + pushMessage delegated ingest (no decode); host_write_mu_ serialization
│   ├── session_cache.hpp/.cpp             # in-memory LRU cache keyed on client-core SessionKey; COMPLETE-only; memory budget
│   ├── auth_provider.hpp/.cpp             # qtkeychain bearer-token + TLS config per normalized URI; env-var fallback
│   ├── settings_store.hpp/.cpp            # lifted Mosaico wrapper over PJ::sdk::SettingsView (prefix mosaico/ → pj_cloud/)
│   ├── server_history.hpp/.cpp            # lifted (URI history + normalizeServerKey)
│   ├── file_hierarchy_browser.hpp/.cpp    # additive QTreeWidget over GCS prefixes when BackendCapabilities.supports_file_hierarchy
│   ├── tag_editor.hpp/.cpp                # tag-editing UI glue → CatalogClient.updateTags
│   ├── format_utils.h / table_sort.h / name_filter.h / date_filter.h / query_filter.h  # lifted Mosaico utilities (unchanged)
│   └── query/                             # lifted Mosaico Lua query engine (Engine/Lexer/Parser/Completions) — byte-for-byte
├── ui/
│   ├── cloud_open.ui                      # lifted three-panel layout (seq table | topic list | info), + hierarchy tree + range slider + progress
│   └── cert_auth.ui                       # lifted cert/credentials dialog, expanded for qtkeychain-backed token + TLS
└── tests/
    ├── CMakeLists.txt
    ├── backend_connection_test.cpp        # 3 consecutive files → ONE OpenFresh; union time/topics; key reversibility
    ├── session_cache_test.cpp             # HIT (transport counter==0) / key exactness / LRU evict / COMPLETE-only / no cross-restart
    ├── raw_forwarding_driver_test.cpp     # SessionSink→ensureParserBinding/pushMessage dispatch with a fake runtime host
    ├── auth_provider_test.cpp             # qtkeychain round-trip per-URI + env-var fallback (mock keychain)
    └── plugin_load_smoke_test.cpp         # dlopen the .so; resolve both vtables; capabilities flags; getDialog() non-null
```

---

## 5. Ordered task list (TDD; each is one subagent section)

> Build order is dependency-correct: skeleton → lift `client-core` → transport adapter →
> ingest seam → cache → auth → dialog → hierarchy → tags → resume → tests → docs. Tasks 3–6
> are pure logic (gtest-first, no UI). Task 7 (dialog) integrates them. Where a host API is
> uncertain, the task text cites the closest **real** SDK signature and flags the assumption.

### Task 1 — Plugin skeleton: `FileSourceBase` + embedded dialog + CMake + manifest
- **Files:** `CMakeLists.txt`, `conanfile.py`, `manifest.json`, `src/pj_cloud_source.cpp`,
  `src/cloud_open_dialog.hpp` (stub), `tests/plugin_load_smoke_test.cpp`
- **Summary:** Create the `pj_cloud` `.so` target. `PjCloudSource : PJ::FileSourceBase`
  overriding `extraCapabilities()` → `PJ::kCapabilityDelegatedIngest |
  PJ::kCapabilityHasDialog`, `getDialog()` → `PJ::borrowDialog(dialog_)`, and a stub
  `importData()` returning `okStatus()`; `CloudOpenDialog : PJ::DialogPluginTyped` stub with
  `manifest()/ui_content()/widget_data()`. Export with `PJ_DATA_SOURCE_PLUGIN(PjCloudSource,
  ...)` + `PJ_DIALOG_PLUGIN(CloudOpenDialog, ...)` (the one-`.so`-two-vtables pattern from
  `data-source-guide.md`). `manifest.json` sets `category: "datasource"`, `version`, and
  documents the `pjcloud://` config token in `description` (**ASSUMPTION A2: no SDK
  URI-scheme hook exists — the token is opaque config, not a host scheme**). CMake links
  `plotjuggler_sdk::plugin_sdk`. Smoke test: `dlopen` the `.so`, resolve
  `PJ_get_data_source_vtable` + `PJ_get_dialog_vtable`, assert capability flags.
- **Depends:** —

### Task 2 — Lift `client-core` (UNCHANGED) and link it Widgets-free
- **Files:** `CMakeLists.txt`, `conanfile.py`
- **Summary:** Add `pj_cloud::client-core` (the Plan B static lib: `CloudConnection`,
  `MessageDispatcher`, `CatalogClient`, `SessionClient`, `SessionSink`, `Decompression`,
  `SessionKey`) as a CMake dependency and `target_link_libraries(... pj_cloud::client-core
  Qt6::Widgets Qt6::Gui)`. **Zero edits to `client-core`.** Add the build-time guard
  (mirroring Plan B Task 2 and Mosaico's CMake): assert `client-core`'s `LINK_LIBRARIES` does
  **not** contain `Qt6::Widgets` (`FATAL_ERROR` otherwise) — the seam boundary is enforced in
  CI. Widgets/Gui/qtkeychain are added on the **plugin** target only.
- **Depends:** Task 1

### Task 3 — `BackendConnection` adapter: `file_ids[]` → synthetic stitched `SequenceRecord` → one `OpenFresh`
- **Files:** `src/backend_connection.hpp/.cpp`, `src/dialog_state.hpp`,
  `tests/backend_connection_test.cpp`
- **Summary:** Wrap `client-core` (`CloudConnection` + `CatalogClient` + `SessionClient`)
  behind a `BackendConnection` whose virtuals mirror Mosaico's `FetchWorker` callback chain
  (`connectFinished`, `sequencesReady`, `topicsReady`/`topicInfosReady`,
  `topicMetadataReady`, `pullProgress`, `pullFinished`, `allFetchesComplete`,
  `errorOccurred`). `CatalogClient.listFiles` → one `SequenceRecord` per file
  (`SequenceRecord.name = file_id`, flat `metadata` via Plan B `flatMetadata`). **Core M2b
  deliverable:** when the user selects N files {A,B,C}, the adapter merges them into **one
  synthetic stitched `SequenceRecord`** (`time_range = union[min(A),max(C)]`, `topics =
  union`), and `pullTopicsAsync` maps that to a **single**
  `wire::OpenSessionRequest{OpenFresh{file_ids=[A,B,C], topic_names=union, time_range}}`.
  Tests (no network; fake dispatcher): 3 consecutive files → exactly ONE `OpenFresh`;
  union time-range and union topics correct; file_id↔name mapping is 1:1 and reversible;
  reordered selection yields the same request.
- **Depends:** Task 2

### Task 4 — `RawMcapForwardingDriver` (`SessionSink`) → host `MessageParser` via delegated ingest
- **Files:** `src/raw_mcap_forwarding_driver.hpp/.cpp`, `tests/raw_forwarding_driver_test.cpp`
- **Summary:** Implement `client-core`'s `SessionSink` interface (`begin` /
  `writeMessage(topic_id, schema_id, log_time_ns, publish_time_ns, payload)` / `onProgress` /
  `end`). On `begin`, build a `topic_id → ParserBindingHandle` map by calling
  `runtimeHost().ensureParserBinding(PJ::ParserBindingRequest{topic_name, parser_encoding
  /*=schema encoding*/, type_name /*=schema name*/, schema /*Span<const uint8_t>*/,
  parser_config_json="{}"})`. On `writeMessage`, call
  `runtimeHost().pushMessage(handle, log_time_ns, fetch)` where `fetch` is a **lambda
  capturing the payload by `shared_ptr`** and returning `sdk::PayloadView` (zero-copy);
  the lambda must be idempotent + thread-safe (the host may invoke it 0..n times per policy).
  Serialize the whole per-message host-write section under a `host_write_mu_` (lifted from
  Mosaico [C1]) because session batches may arrive on `client-core` worker threads. **Ships
  no decoders.** On `end(COMPLETE)` call `runtimeHost().reportMessage(kInfo, ...)`; the
  FileSourceBase machine flushes on `importData()` return. **ASSUMPTION A1 (flagged):** the
  `ensureParserBinding`/`pushMessage` mapping of MCAP `schema_name/encoding` → PJ4
  `parser_encoding`/`type_name` is verified against a live PJ4 host before M2b coding (the
  signatures are real; the encoding-string convention, e.g. `"ros2msg"` vs `"protobuf"`, is
  the integration point). Tests: a fake `DataSourceRuntimeHostView` records
  `ensureParserBinding` calls (one per topic) and `pushMessage` calls (one per message, with
  correct timestamp + payload bytes when the callable is invoked); assert no decode happens
  in the driver.
- **Depends:** Task 2

### Task 5 — In-memory `SessionCache` (use of Plan B `SessionKey`; LRU; COMPLETE-only)
- **Files:** `src/session_cache.hpp/.cpp`, `tests/session_cache_test.cpp`
- **Summary:** Build the cache the design spec §2/§12 calls in-memory-only. Key =
  `client-core`'s `computeSessionKey(server_uri, file_ids[], topics[], time_range)` (Plan B
  Task 8a) — **exact-tuple match only**, normalized (file_ids + topics sorted) so reordered
  selections collide. Value = the completed reconstruction handle/buffer + `memory_bytes`.
  `lookup(key)` returns a hit only for a **COMPLETE** entry; `store(key, value)` is called
  **only** on `SessionSink::end(EosReason::COMPLETE)` (Cancelled/Error → no entry, no
  half-cached state). LRU eviction by total `memory_bytes` over a budget (default 256 MB;
  configurable). **No disk tier, no cross-restart persistence** (cross-restart is the
  separate Asensus "Task C" item, explicitly out of scope). Tests: HIT returns cached data
  with a **transport-call counter == 0** (no `BackendConnection` invocation); key correctness
  (different file_ids/topics/range → MISS; reordered → HIT); LRU evicts the
  least-recently-used over budget; only COMPLETE entries cached; a fresh cache instance
  (simulating restart) is empty.
- **Depends:** Task 3, Task 4

### Task 6 — `AuthProvider` + qtkeychain (bearer token + TLS config per URI)
- **Files:** `src/auth_provider.hpp/.cpp`, `src/settings_store.hpp/.cpp`,
  `src/server_history.hpp/.cpp`, `tests/auth_provider_test.cpp`, `conanfile.py`,
  `CMakeLists.txt`
- **Summary:** Lift Mosaico's `SettingsStore` (over `PJ::sdk::SettingsView`,
  `setValue`/`value`) and `ServerHistory`/`normalizeServerKey` (prefix `mosaico/` →
  `pj_cloud/`). Add `AuthProvider` wrapping qtkeychain: `saveCredentials(uri, bearer_token,
  tls_cert_path, allow_insecure)` writes **secrets** (token + TLS material) to the OS
  credential store keyed on the normalized URI; non-secret prefs (URI history, last selection,
  slider range) stay in `SettingsView`. `loadCredentials(uri)` retrieves them, with a
  **fallback to env var** `PJCLOUD_API_KEY` for headless use (mirrors Mosaico's
  `MOSAICO_API_KEY`). Feeds `CloudConnection`'s connect settings (token + TLS) — Plan B owns
  the actual TLS/WSS handshake. **ASSUMPTION A3 (flagged):** qtkeychain resolves in Conan and
  links cleanly against PJ4's Qt6; if unavailable on a target platform, `AuthProvider`
  degrades to `SettingsView` storage with a warning. Tests use a mock keychain: per-URI
  round-trip; isolation between URIs; env-var fallback when no stored secret.
- **Depends:** Task 2

### Task 7 — `CloudOpenDialog`: lift Mosaico dialog + worker, swap transport & ingest seams
- **Files:** `src/cloud_open_dialog.hpp/.cpp`, `src/dialog_state.hpp`,
  `src/fetch_worker.hpp/.cpp`, `src/query/**` (lifted), `src/format_utils.h`,
  `src/table_sort.h`, `src/name_filter.h`, `src/date_filter.h`, `src/query_filter.h`,
  `ui/cloud_open.ui`, `ui/cert_auth.ui`
- **Summary:** Lift the Mosaico `DialogState`, `onTick`-drained event queue + command-queue
  worker bridge, `SeqViewCache`, table sort/selection re-mapping, name/date/query filters,
  the **entire Lua query engine** (`query/` byte-for-byte), the three-panel splitter UI, and
  the cert/credentials dialog — renamed `mosaico_` → `cloud_connector_`. **Swap two seams:**
  (a) the worker's transport member becomes `BackendConnection` (Task 3) over `client-core`
  WS+Protobuf instead of `MosaicoClient`/Arrow-Flight; `FetchWorker::pullTopicsAsync` is
  re-parameterized from `sequence_name` to the synthetic-stitched `SequenceRecord` →
  `OpenFresh{file_ids[], topics, time_range}`; (b) host writes go through
  `RawMcapForwardingDriver` (Task 4) instead of `arrow_ingest.cpp`'s `pumpStreamToHost`.
  Wire `SessionCache` (Task 5) into `pullTopicsAsync`: check `lookup` before any transport
  call. `onAccepted()` snapshots `(server_uri, file_ids[], topic_names[], time_range)` into
  `DialogState` for `importData()`; `saveConfig()`/`loadConfig()` persist it (note 3). Use
  `PJ::WidgetData` (from `pj_plugins/sdk/widget_data.hpp`) to build `widget_data()` and the
  typed `DialogPluginTyped` handlers (`onClicked`/`onSelectionChanged`/`onHeaderClicked`/
  `onRangeChanged`/`onDateRangeChanged`/`onCodeChangedWithCursor`). Embed both `.ui` files via
  the `pj_embed_ui` CMake helper. **Topic-detail panel is NOT zero-edit:** Mosaico's
  `topic_meta`/`topic_infos` are Arrow-typed; here they are **flat string metadata**, so
  scope the unchanged reuse to the filter/query/selection fields and rewrite the info-panel
  data model + formatting (drop `formatSchemaFields`/`formatFieldType`; keep the
  indentation/text infra). `importData()` (in `pj_cloud_source.cpp`) reads the snapshot, runs
  the session via `SessionClient` + `RawMcapForwardingDriver`, surfacing progress through
  `runtimeHost().progressStart/progressUpdate/progressFinish`.
- **Depends:** Task 3, Task 4, Task 5, Task 6

### Task 8 — `BackendCapabilities`-driven file-hierarchy browser (additive QTreeWidget)
- **Files:** `src/file_hierarchy_browser.hpp/.cpp`, `src/cloud_open_dialog.hpp/.cpp`,
  `ui/cloud_open.ui`
- **Summary:** Read `CloudConnection::backendCapabilities()` (from `HelloResponse`). When
  `supports_file_hierarchy == true` (Asensus/GCS), show an **additive** `QTreeWidget`
  browsing GCS prefixes, fed by the **same** `ListFiles` results parsed as a tree
  (breadcrumb; click a node → resolve to `file_ids` → existing fetch flow via Task 3). When
  `false` (Dexory/S3), show the flat sequence table unchanged. The two modes are **not
  entangled** — the tree is additive, never replaces the table. `metadata_key_vocabulary`
  from `HelloResponse` populates the Lua query-assist dropdowns (same mechanism Mosaico
  already uses). Tests (component, Task 11): tree built only when flag true; flat table when
  false; tree-node selection resolves to the same `OpenFresh` Task 3 produces.
- **Depends:** Task 7

### Task 9 — Tag editing via `CatalogClient.updateTags`
- **Files:** `src/tag_editor.hpp/.cpp`, `src/cloud_open_dialog.hpp/.cpp`, `ui/cloud_open.ui`
- **Summary:** Add a tag-editing affordance on the selected file/sequence (set/unset
  key→value pairs), wired through a `buttonTag`-style `onClicked` handler to
  `CatalogClient.updateTags(file_id, set_tags, unset_keys)` (Plan B Task 5c). Reuses the
  existing `SettingsStore`/`ServerHistory` infra and the worker command-queue bridge (the
  call is async; the result refreshes the row's flat metadata via the existing
  `topicMetadataReady`/`sequenceInfoReady` path). Surface failures through the dedup'd
  `error_counts` notification. M2b acceptance gate: tag edits persist server-side.
- **Depends:** Task 7

### Task 10 — Reconnect-and-resume on WS drop (`OpenResume`)
- **Files:** `src/backend_connection.hpp/.cpp`, `src/fetch_worker.hpp/.cpp`,
  `src/cloud_open_dialog.hpp/.cpp`
- **Summary:** Surface reconnect-resume in the plugin (resume logic lives **outside**
  `client-core`). On a `CloudConnection::disconnected` mid-session, the dialog re-opens the
  connection (reusing `AuthProvider` credentials) and `BackendConnection` issues
  `wire::OpenSessionRequest{OpenResume{subscription_id, resume_after_seq}}` using the
  last-acked batch from the prior `SessionClient` run (design spec failure/resume model).
  `DialogState` surfaces a "resuming…" hint via `widget_data()`. The `RawMcapForwardingDriver`
  continues appending from `resume_after_seq` without duplicating already-pushed messages.
- **Depends:** Task 7

### Task 11 — Component/integration tests (gtest/ctest)
- **Files:** `tests/CMakeLists.txt`, `tests/backend_connection_test.cpp`,
  `tests/session_cache_test.cpp`, `tests/raw_forwarding_driver_test.cpp`,
  `tests/auth_provider_test.cpp`, `tests/plugin_load_smoke_test.cpp`
- **Summary:** Consolidate and extend the unit tests into the ctest suite and add component
  coverage: (1) `BackendConnection` 3-consecutive-files → ONE `OpenFresh` (union time/topics)
  — the headline use case; (2) `SessionCache` HIT avoids refetch (transport counter == 0),
  key exactness, LRU eviction, COMPLETE-only caching, no cross-restart persistence;
  (3) `RawMcapForwardingDriver` raw-forwarding to a fake host `ensureParserBinding`/
  `pushMessage` dispatch (no decode); (4) `AuthProvider` qtkeychain round-trip + env fallback
  (mock keychain); (5) plugin-load smoke (both vtables resolve, capability flags correct,
  `getDialog()` non-null); (6) file-hierarchy on/off per capability flag. Register all under
  `ctest`. (E2E against a live Go server matrix + real `MessageParser` decode is the unified
  plan §6 L3 leg, run in the integration harness, not duplicated here.)
- **Depends:** Task 3, Task 4, Task 5, Task 6, Task 8

### Task 12 — End-user documentation
- **Files:** `README.md`, `manifest.json`
- **Summary:** Write end-user docs: install/enable the plugin in PJ4, connect to a PJ-Cloud
  server (URI + bearer token + optional TLS cert; how qtkeychain stores secrets and the
  `PJCLOUD_API_KEY` fallback), browse the catalog (flat table vs hierarchy tree per backend),
  Lua metadata query syntax + assist dropdowns, multi-file selection → stitched session,
  topic + time-range selection, import + progress, the in-memory session cache (repeat-open =
  no re-download within the session), tag editing, and reconnect-resume behavior. Document the
  **ASSUMPTION A2** note: how a "cloud open" is modeled as the embedded-dialog import flow
  (no host-registered `pjcloud://` scheme exists in the current SDK) and how the
  `pjcloud://` token is carried as opaque config in `saveConfig()` for re-open/restore.
- **Depends:** Task 7, Task 8, Task 9, Task 10, Task 11
