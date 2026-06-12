# Toolbox Parser-Ingest (Slice 16) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Toolbox plugins host-mediated delegated parsing (the same `ensureParserBinding`/`pushMessage` path DataSources use), then migrate the Dexory Cloud plugin to it — removing ALL in-plugin ROS decoding and making tf/pointcloud topics arrive as real, 3D-draggable AND renderable object topics.

**Architecture:** Two ABI tail slots on `PJ_toolbox_runtime_host_vtable_t` (`create_parser_ingest` / `release_parser_ingest`) return the **standard** `PJ_data_source_runtime_host_t` fat pointer, backed by a per-dataset `DataSourceRuntimeHost` that `ToolboxRuntimeHost` now owns. The entire existing machinery (catalog lookup → `classifySchema` → ObjectStore topic + metadata → SessionManager render-parser registration) runs **unchanged**; we are handing it a toolbox-created dataset instead of a file-created one. This repeats the documented ABI-v5 precedent (toolbox object-topic tail slots, `plotjuggler_sdk/pj_plugins/docs/ARCHITECTURE.md:446-457`). Plugin side: `RosDecodeDriver` (rosx_introspection) is replaced by a thin `ParserIngestDriver` that binds once per topic and pushes raw CDR per message.

**Tech Stack:** C/C++20 (PJ4 host + SDK + plugin), Conan 2 (SDK package `plotjuggler_sdk`), CMake/ctest, gtest, Go 1.23 (genmcap fixtures), Minio.

---

## Ground rules (read before Task 0)

- **Edit ONLY the vendored tree:** `/home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/...` (referred to as `PJ4/` below; the repo root `/home/gn/ws/PJ4_Server_Template/pj-mcap-server` as `<repo>`). The pristine `/home/gn/ws/PJ4` is read-only reference. Every path below exists identically in both trees unless marked **Create**.
- **Two SDK consumers, two mechanisms:** the PJ4 **app** builds the SDK in-tree (`PJ4/plotjuggler_sdk` is a subdirectory of the app build; `PJ4/conanfile.txt` does NOT list plotjuggler_sdk). The **plugins** consume the SDK as Conan package `plotjuggler_sdk/<SDK_VERSION>` (pin file `PJ4/pj-official-plugins/SDK_VERSION`, currently `0.6.0`; every plugin conanfile reads it live). SDK header changes therefore need BOTH an app rebuild AND a `conan create` of the bumped package (Task 7).
- **ABI doctrine:** tail slots only, `struct_size`-gated via `PJ_HAS_TAIL_SLOT`, NO `protocol_version` bump (precedent: ARCHITECTURE.md:452 — "tail slots appended … under ABI v5 (no version bump)").
- **Regression gates that must stay green:** `make smoke` (repo root, server :8081), `make matrix` (server :8082, needs `/home/gn/ws/jkk_dataset02`), plugin ctest in BOTH hermetic and live modes, and the WASM job (`toolbox_dexory_cloud/wasm/` compiles only pure client core — `session_key`/`session_cache`/`stitch_select`/`hierarchy_prefix`/`cli_url_resolve` — none of which this plan touches; verify with the grep in Task 10 Step 4).
- **Grounded facts this plan builds on (all verified 2026-06-09):**
  - `PJ4/pj_runtime/src/ToolboxRuntimeHost.cpp:31-35` — toolbox services today: write + runtime + settings only.
  - `PJ4/pj_runtime/include/pj_runtime/DataSourceRuntimeHost.h:60-70` — ctor `(DataEngine&, ExtensionCatalogService&, DatasetId, PJ_data_source_handle_t, ObjectStore&, std::string source_id, ObjectTopicParserRegistrar, ObjectStore* = nullptr, DataEngine* = nullptr)`; `registerServices`, `flushAll`, `flushPending` public.
  - `PJ4/pj_runtime/src/DataSourceRuntimeHost.cpp:238-246` — the runtime service is just `PJ_data_source_runtime_host_t{.ctx=this, .vtable=&kVtable}`.
  - `PJ4/pj_app/src/FileLoader.cpp:~228-245` — the registrar is `[this](ObjectTopicId id, std::unique_ptr<MessageParserHandle> p){ session_.registerObjectTopicParser(id, std::move(p)); }`; `PJ_data_source_handle_t{uint32(dataset_id)}` — handle id IS the dataset id.
  - `PJ4/pj_app/src/MainWindow.cpp:~3198-3201` — the only ToolboxRuntimeHost construction site (PanelSession block).
  - `PJ4/pj_datastore/src/plugin_data_host.cpp:1237` — toolbox source-handle validation idiom: `engine_.getDataset(source.id) == nullptr`.
  - `PJ4/plotjuggler_sdk/pj_base/include/pj_base/toolbox_protocol.h:68-77` — runtime vtable today: `{u32 protocol_version, u32 struct_size, report_message(8), notify_data_changed(16)}` → new slots land at offsets 24 and 32.
  - `PJ4/pj_runtime/CMakeLists.txt:91-121` — `toolbox_runtime_host_test` target; stub parser plugin `runtime_host_object_parser_plugin` (encoding `"runtime_host_object"`, schemas `mock/image` → kImage, `mock/scalar` → kNone) wired to the data_source ingest test via `PJ_RUNTIME_HOST_OBJECT_PARSER_PATH="$<TARGET_FILE:...>"`.
  - Plugin consumer surface of the old driver (`PJ4/pj-official-plugins/toolbox_dexory_cloud/src/fetch_worker.cpp:383-560`): `driver.bindSession(host, *ds, session_info)`, `driver.decoders()` (fields `.decodable`/`.skip_reason`), `driver.hasDecodable()`, `driver.decode(host, m)`, `driver.decodedCounts()`, `driver.errorCounts()`. The new driver keeps these names so the post-download block compiles with only the two call-shape edits shown in Task 9.
  - Wire data available per topic (`src/backend_types.hpp:120-147`): `SessionTopic{topic_id, topic_name, schema_id, message_encoding}`, `SessionSchema{schema_id, name, encoding, data}`; per message (`src/decoded_message.hpp:20-26`): `DecodedMessage{topic_id, schema_id, log_time_ns, publish_time_ns, payload(std::string)}`.
  - Live-test gating convention: env `DEXORY_CLOUD_LIVE_URL`, `GTEST_SKIP()` when unset (`tests/ros_decode_live_test.cpp:33,85`).
  - genmcap (`<repo>/server/internal/genmcap/genmcap.go`): `TopicSpec{Topic, SchemaName, SchemaEnc, MessageCount}`; `Write` emits schema `Data: []byte(t.SchemaName)` and synthetic payloads — Task 13 adds real-schema/real-payload overrides.

### File structure (what gets created/modified, by responsibility)

```
HOST (vendored PJ4 app + in-tree SDK)
  PJ4/plotjuggler_sdk/pj_base/include/pj_base/toolbox_protocol.h         M  2 tail slots (C ABI)
  PJ4/plotjuggler_sdk/pj_base/tests/abi_layout_sentinels_test.cpp        M  pin offsets 24/32
  PJ4/plotjuggler_sdk/pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp M  ToolboxRuntimeHostView::{create,release}ParserIngest
  PJ4/pj_runtime/include/pj_runtime/DataSourceRuntimeHost.h              M  hostHandle() accessor
  PJ4/pj_runtime/include/pj_runtime/ToolboxRuntimeHost.h                 M  ParserIngestDeps + context map
  PJ4/pj_runtime/src/ToolboxRuntimeHost.cpp                              M  the 2 trampolines
  PJ4/pj_runtime/tests/toolbox_runtime_host_test.cpp                     M  TDD tests (stub parser)
  PJ4/pj_runtime/tests/toolbox_parser_ingest_real_ros_test.cpp           C  env-gated REAL parser_ros tf test
  PJ4/pj_runtime/CMakeLists.txt                                          M  test wiring
  PJ4/pj_app/src/MainWindow.cpp                                          M  deps wiring (catalog + registrar)
SDK PACKAGE
  PJ4/plotjuggler_sdk/conanfile.py                                       M  version 0.6.0 → 0.6.1
  PJ4/pj-official-plugins/SDK_VERSION                                    M  0.6.1
PLUGIN (vendored)
  .../toolbox_dexory_cloud/src/parser_ingest_driver.{hpp,cpp}            C  the new thin driver
  .../toolbox_dexory_cloud/src/fetch_worker.cpp                          M  swap driver
  .../toolbox_dexory_cloud/src/ros_decode_driver.{hpp,cpp}               D  (and rosx + CPM)
  .../toolbox_dexory_cloud/tests/parser_ingest_test_support.hpp          C  fake-host recorder
  .../toolbox_dexory_cloud/tests/parser_ingest_driver_test.cpp           C  hermetic
  .../toolbox_dexory_cloud/tests/parser_ingest_live_test.cpp             C  live (DEXORY_CLOUD_LIVE_URL)
  .../toolbox_dexory_cloud/tests/ros_decode_*.{cpp,hpp}                  D
  .../toolbox_dexory_cloud/CMakeLists.txt                                M
FIXTURES (Go server side)
  <repo>/server/internal/genmcap/genmcap.go                              M  SchemaData/PayloadFn overrides
  <repo>/server/cmd/gen-3d-fixture/main.go                               C  real tf+pointcloud MCAP
```

---

## Phase 0 — Baseline

### Task 0: Prove the vendored tree builds and the gates are green before any change

**Files:** none (build/verify only)

- [ ] **Step 1: Build the vendored PJ4 app (host + in-tree SDK + tests)**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4 && ./build.sh`
Expected: build completes. (If `.qt/6.8.3/gcc_64` is missing in the vendored tree, run `./install_qt6.sh` first or symlink `/home/gn/ws/PJ4/.qt` → `PJ4/.qt`; `.qt` is gitignored.)

- [ ] **Step 2: Run the existing host tests we will extend**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4 && ctest --test-dir build -R "ToolboxRuntimeHost|DataSourceRuntimeHost" --output-on-failure`
Expected: all PASS. (If ctest can't find tests at `build`, locate with `find build -name CTestTestfile.cmake -maxdepth 3` and use that directory for every ctest call in this plan.)

- [ ] **Step 3: Build the plugin + run its ctest (hermetic)**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/pj-official-plugins && ./build.sh toolbox_dexory_cloud && ctest --test-dir build/toolbox_dexory_cloud/Release --output-on-failure`
Expected: 31/31 PASS (live tests SKIP without `DEXORY_CLOUD_LIVE_URL`).

- [ ] **Step 4: Run the repo smoke gate**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server && make smoke`
Expected: final line `SMOKE PASS`.

- [ ] **Step 5: Commit any incidental fixes needed to get green (there should be none)**

```bash
git -C /home/gn/ws/PJ4_Server_Template/pj-mcap-server status   # expect clean (besides this plan file)
```

---

## Phase 1 — Host capability

### Task 1: C ABI — two tail slots on the toolbox runtime host vtable

**Files:**
- Modify: `PJ4/plotjuggler_sdk/pj_base/include/pj_base/toolbox_protocol.h` (vtable at lines 68-77)
- Modify: `PJ4/plotjuggler_sdk/pj_base/tests/abi_layout_sentinels_test.cpp` (existing toolbox pin at line ~178 shows the idiom)

- [ ] **Step 1: Write the failing "test" — the offset sentinels**

In `abi_layout_sentinels_test.cpp`, next to the existing
`static_assert(offsetof(PJ_toolbox_host_vtable_t, register_object_topic) == 72, ...)` add:

```cpp
static_assert(
    offsetof(PJ_toolbox_runtime_host_vtable_t, create_parser_ingest) == 24,
    "toolbox runtime parser-ingest slot pinned");
static_assert(
    offsetof(PJ_toolbox_runtime_host_vtable_t, release_parser_ingest) == 32,
    "toolbox runtime parser-ingest release slot pinned");
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4 && ./build.sh`
Expected: FAIL — `create_parser_ingest` is not a member of `PJ_toolbox_runtime_host_vtable_t`.

- [ ] **Step 3: Add the slots**

In `toolbox_protocol.h`: add the include near the top (with the existing includes):

```c
#include "pj_base/data_source_protocol.h" /* PJ_data_source_runtime_host_t for parser ingest */
```

Inside `PJ_toolbox_runtime_host_vtable_t`, AFTER `notify_data_changed` (line 76), append:

```c
  /* ---- TAIL SLOTS (parser ingest) ------------------------------------
   * Appended for toolbox-delegated parsing; struct_size-gated via
   * PJ_HAS_TAIL_SLOT, no protocol_version bump — the same growth mechanism
   * as the toolbox write host's object-topic slots (ABI v5). */

  /* Create (or return the existing) parser-ingest context bound to a
   * toolbox-created data source. `data_source_id` is the handle id returned
   * by the toolbox write host's create_data_source (== the dataset id).
   * On success fills `out_host` with a standard data-source runtime host
   * fat pointer: ensure_parser_binding / push_message on it behave exactly
   * as they do for file/stream DataSource plugins. The context stays valid
   * until release_parser_ingest or host teardown. [thread-safe] */
  bool (*create_parser_ingest)(
      void* ctx, uint32_t data_source_id, PJ_data_source_runtime_host_t* out_host,
      PJ_error_t* out_error) PJ_NOEXCEPT;

  /* Flush every row written through the context's parser bindings and
   * destroy it. Idempotent: releasing an unknown id succeeds. The fat
   * pointer from create_parser_ingest must not be used afterwards.
   * [thread-safe] */
  bool (*release_parser_ingest)(void* ctx, uint32_t data_source_id, PJ_error_t* out_error) PJ_NOEXCEPT;
```

- [ ] **Step 4: Rebuild — sentinels pass, but ToolboxRuntimeHost.cpp now FAILS**

Run: `./build.sh`
Expected: error in `pj_runtime/src/ToolboxRuntimeHost.cpp` — the positional `runtime_vtable_{...}` initializer is missing the two new members (it zero-fills, but `-Werror=missing-field-initializers` fires if enabled) OR it compiles. If it compiles, fine — Task 4 fills them. If it errors, add two `nullptr,` entries to the initializer now (Task 4 replaces them).

- [ ] **Step 5: Commit**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
git add PJ4/plotjuggler_sdk/pj_base/include/pj_base/toolbox_protocol.h \
        PJ4/plotjuggler_sdk/pj_base/tests/abi_layout_sentinels_test.cpp \
        PJ4/pj_runtime/src/ToolboxRuntimeHost.cpp
git commit -m "sdk: toolbox runtime host gains parser-ingest tail slots (ABI-appendable, offsets 24/32 pinned)"
```

### Task 2: `DataSourceRuntimeHost::hostHandle()` accessor

**Files:**
- Modify: `PJ4/pj_runtime/include/pj_runtime/DataSourceRuntimeHost.h` (public section, right after `registerServices` at line ~85)

- [ ] **Step 1: Add the accessor**

```cpp
  // Fat pointer to this runtime host for handing across the C ABI outside of
  // registerServices() — ToolboxRuntimeHost's parser-ingest slots return it.
  // Valid only while this object lives.
  [[nodiscard]] PJ_data_source_runtime_host_t hostHandle() noexcept {
    return PJ_data_source_runtime_host_t{.ctx = this, .vtable = &kVtable};
  }
```

- [ ] **Step 2: Build**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4 && ./build.sh`
Expected: PASS (it's the same expression `registerServices` uses at DataSourceRuntimeHost.cpp:241-244).

- [ ] **Step 3: Commit**

```bash
git add PJ4/pj_runtime/include/pj_runtime/DataSourceRuntimeHost.h
git commit -m "pj_runtime: expose DataSourceRuntimeHost::hostHandle() for the toolbox parser-ingest path"
```

### Task 3: SDK C++ view — `ToolboxRuntimeHostView::{createParserIngest, releaseParserIngest}`

**Files:**
- Modify: `PJ4/plotjuggler_sdk/pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp` (the `ToolboxRuntimeHostView` class, lines ~43-70)

- [ ] **Step 1: Add the include** (top of file, with the existing `pj_base/toolbox_protocol.h` include)

```cpp
#include "pj_base/sdk/data_source_host_views.hpp"  // DataSourceRuntimeHostView, errorToString
```

- [ ] **Step 2: Add the two methods inside `ToolboxRuntimeHostView`** (after `notifyDataChanged()`)

```cpp
  /// Create (or fetch) the parser-ingest context for a toolbox-created data
  /// source (pass ToolboxHostView::createDataSource's handle `.id`). Returns
  /// the standard delegated-ingest view: ensureParserBinding() once per topic,
  /// pushMessage() per record — exactly like a DataSource plugin. Fails with
  /// an "older host" error when the host predates the tail slot.
  [[nodiscard]] Expected<DataSourceRuntimeHostView> createParserIngest(uint32_t data_source_id) const {
    if (!valid()) {
      return unexpected("toolbox runtime host is not bound");
    }
    if (!PJ_HAS_TAIL_SLOT(PJ_toolbox_runtime_host_vtable_t, host_.vtable, create_parser_ingest)) {
      return unexpected("host does not support toolbox parser ingest (older host)");
    }
    PJ_data_source_runtime_host_t raw{};
    PJ_error_t err{};
    if (!host_.vtable->create_parser_ingest(host_.ctx, data_source_id, &raw, &err)) {
      return unexpected(errorToString(err));
    }
    return DataSourceRuntimeHostView{raw};
  }

  /// Flush + destroy the context. Idempotent. The view returned by
  /// createParserIngest must not be used afterwards.
  [[nodiscard]] Status releaseParserIngest(uint32_t data_source_id) const {
    if (!valid()) {
      return unexpected("toolbox runtime host is not bound");
    }
    if (!PJ_HAS_TAIL_SLOT(PJ_toolbox_runtime_host_vtable_t, host_.vtable, release_parser_ingest)) {
      return unexpected("host does not support toolbox parser ingest (older host)");
    }
    PJ_error_t err{};
    if (!host_.vtable->release_parser_ingest(host_.ctx, data_source_id, &err)) {
      return unexpected(errorToString(err));
    }
    return okStatus();
  }
```

- [ ] **Step 3: Build**

Run: `./build.sh`
Expected: PASS (header-only; `PJ_HAS_TAIL_SLOT` and `errorToString` both come from `data_source_host_views.hpp` / its includes — the same pair `pushMessage` already uses there).

- [ ] **Step 4: Commit**

```bash
git add PJ4/plotjuggler_sdk/pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp
git commit -m "sdk: ToolboxRuntimeHostView createParserIngest/releaseParserIngest wrappers (tail-slot guarded)"
```

### Task 4: `ToolboxRuntimeHost` implementation (TDD with the stub parser plugin)

**Files:**
- Modify: `PJ4/pj_runtime/tests/toolbox_runtime_host_test.cpp`
- Modify: `PJ4/pj_runtime/CMakeLists.txt` (test target at lines 91-99)
- Modify: `PJ4/pj_runtime/include/pj_runtime/ToolboxRuntimeHost.h`
- Modify: `PJ4/pj_runtime/src/ToolboxRuntimeHost.cpp`

- [ ] **Step 1: Write the failing tests**

Append to `toolbox_runtime_host_test.cpp` (add these includes to the existing block):

```cpp
#include <QFileInfo>
#include <QString>
#include "pj_base/sdk/data_source_host_views.hpp"
#include "pj_runtime/ExtensionCatalogService.h"

#ifndef PJ_RUNTIME_HOST_OBJECT_PARSER_PATH
#error "PJ_RUNTIME_HOST_OBJECT_PARSER_PATH must be defined"
#endif
```

and the tests (inside the existing anonymous namespace, using the existing fixture):

```cpp
TEST_F(ToolboxRuntimeHostTest, ParserIngestDelegatesToCatalogParserAndRegistersObjectParser) {
  QFileInfo plugin_file{QString::fromUtf8(PJ_RUNTIME_HOST_OBJECT_PARSER_PATH)};
  PJ::ExtensionCatalogService catalog(plugin_file.absolutePath());
  ASSERT_NE(catalog.findParserByEncoding(QStringLiteral("runtime_host_object")), nullptr);

  std::vector<PJ::ObjectTopicId> registered_object_parsers;
  PJ::ToolboxRuntimeHost::ParserIngestDeps deps;
  deps.catalog = &catalog;
  deps.register_object_parser = [&registered_object_parsers](
                                    PJ::ObjectTopicId id, std::unique_ptr<PJ::MessageParserHandle> parser) {
    EXPECT_NE(parser, nullptr);
    registered_object_parsers.push_back(id);
  };
  host_ = std::make_unique<PJ::ToolboxRuntimeHost>(
      engine_, object_store_, settings_, PJ::ToolboxRuntimeHost::Callbacks{}, std::move(deps));
  auto services = registered();

  auto toolbox_or = services.require<PJ::sdk::ToolboxHostService>();
  ASSERT_TRUE(toolbox_or.has_value());
  auto runtime_or = services.require<PJ::sdk::ToolboxRuntimeHostService>();
  ASSERT_TRUE(runtime_or.has_value());

  auto ds = (*toolbox_or).createDataSource("cloud download");
  ASSERT_TRUE(ds.has_value()) << ds.error();

  auto ingest_or = (*runtime_or).createParserIngest(ds->id);
  ASSERT_TRUE(ingest_or.has_value()) << ingest_or.error();
  PJ::DataSourceRuntimeHostView ingest = *ingest_or;

  auto binding = ingest.ensureParserBinding(PJ::ParserBindingRequest{
      .topic_name = "/camera/image",
      .parser_encoding = "runtime_host_object",
      .type_name = "mock/image",
      .schema = PJ::Span<const uint8_t>{},
      .parser_config_json = "{}",
  });
  ASSERT_TRUE(binding.has_value()) << binding.error();

  auto push = ingest.pushMessage(
      *binding, PJ::Timestamp{100}, []() -> std::vector<uint8_t> { return {1, 2, 3, 4}; });
  ASSERT_TRUE(push.has_value()) << push.error();

  // Release flushes: the stub parser's scalar row (byte_count) becomes readable.
  ASSERT_TRUE((*runtime_or).releaseParserIngest(ds->id).has_value());
  EXPECT_EQ(totalRowCount(ds->id), 1u);

  // mock/image classifies kImage: one object topic, one stored entry, and the
  // registrar received exactly one render-time parser instance.
  const auto object_topics = object_store_.listTopics(static_cast<PJ::DatasetId>(ds->id));
  ASSERT_EQ(object_topics.size(), 1u);
  EXPECT_EQ(object_store_.entryCount(object_topics.front()), 1u);
  EXPECT_EQ(registered_object_parsers.size(), 1u);

  // Idempotent release; recreate-after-release works.
  EXPECT_TRUE((*runtime_or).releaseParserIngest(ds->id).has_value());
  EXPECT_TRUE((*runtime_or).createParserIngest(ds->id).has_value());
}

TEST_F(ToolboxRuntimeHostTest, ParserIngestWithoutDepsFailsCleanly) {
  host_ = std::make_unique<PJ::ToolboxRuntimeHost>(
      engine_, object_store_, settings_, PJ::ToolboxRuntimeHost::Callbacks{});
  auto services = registered();
  auto toolbox_or = services.require<PJ::sdk::ToolboxHostService>();
  ASSERT_TRUE(toolbox_or.has_value());
  auto runtime_or = services.require<PJ::sdk::ToolboxRuntimeHostService>();
  ASSERT_TRUE(runtime_or.has_value());
  auto ds = (*toolbox_or).createDataSource("x");
  ASSERT_TRUE(ds.has_value());
  auto ingest = (*runtime_or).createParserIngest(ds->id);
  ASSERT_FALSE(ingest.has_value());
  EXPECT_NE(ingest.error().find("not configured"), std::string::npos);
}

TEST_F(ToolboxRuntimeHostTest, ParserIngestUnknownDataSourceFails) {
  QFileInfo plugin_file{QString::fromUtf8(PJ_RUNTIME_HOST_OBJECT_PARSER_PATH)};
  PJ::ExtensionCatalogService catalog(plugin_file.absolutePath());
  PJ::ToolboxRuntimeHost::ParserIngestDeps deps;
  deps.catalog = &catalog;
  host_ = std::make_unique<PJ::ToolboxRuntimeHost>(
      engine_, object_store_, settings_, PJ::ToolboxRuntimeHost::Callbacks{}, std::move(deps));
  auto services = registered();
  auto runtime_or = services.require<PJ::sdk::ToolboxRuntimeHostService>();
  ASSERT_TRUE(runtime_or.has_value());
  auto ingest = (*runtime_or).createParserIngest(99999);
  ASSERT_FALSE(ingest.has_value());
  EXPECT_NE(ingest.error().find("not found"), std::string::npos);
}
```

Note: the fixture's existing `totalRowCount(uint32_t)` helper is reused as-is.

- [ ] **Step 2: Wire the stub plugin define into the test target**

In `PJ4/pj_runtime/CMakeLists.txt`, after the existing `add_test(NAME ToolboxRuntimeHostTest ...)` (line 99) add (mirrors lines 110-121 for the data_source test):

```cmake
    target_compile_definitions(toolbox_runtime_host_test PRIVATE
        PJ_RUNTIME_HOST_OBJECT_PARSER_PATH="$<TARGET_FILE:runtime_host_object_parser_plugin>"
    )
    add_dependencies(toolbox_runtime_host_test runtime_host_object_parser_plugin)
```

(The `runtime_host_object_parser_plugin` target is defined later in the same file at line 101 — CMake resolves targets file-globally, but if configure complains about ordering, move this block below line 121.)

- [ ] **Step 3: Run to verify the tests fail**

Run: `./build.sh && ctest --test-dir build -R ToolboxRuntimeHostTest --output-on-failure`
Expected: COMPILE FAIL — `ParserIngestDeps` does not exist / ctor takes 4 args.

- [ ] **Step 4: Implement — header**

`PJ4/pj_runtime/include/pj_runtime/ToolboxRuntimeHost.h`. Add includes:

```cpp
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "pj_datastore/object_store.hpp"  // ObjectTopicId
```

Add forward declarations next to the existing ones (`DataEngine`, `ObjectStore`, `ServiceRegistryBuilder`):

```cpp
class DataSourceRuntimeHost;
class ExtensionCatalogService;
class MessageParserHandle;
```

Inside the class, after the `Callbacks` struct:

```cpp
  // Optional dependencies enabling the parser-ingest tail slots. With a null
  // catalog the slots fail "not configured" — headless tests and embedders
  // that load no parser plugins keep working unchanged.
  struct ParserIngestDeps {
    ExtensionCatalogService* catalog = nullptr;
    // Receives the render-time parser instance for every object topic a
    // parser binding registers — same contract as FileLoader's registrar
    // (forward to SessionManager::registerObjectTopicParser). May be invoked
    // from a toolbox worker thread.
    std::function<void(ObjectTopicId, std::unique_ptr<MessageParserHandle>)> register_object_parser;
  };
```

Change the constructor declaration and add a destructor:

```cpp
  ToolboxRuntimeHost(
      DataEngine& engine, ObjectStore& object_store, sdk::SettingsBackend& settings, Callbacks callbacks,
      ParserIngestDeps parser_ingest = {});
  // Out-of-line: parser_ingests_ holds unique_ptr<DataSourceRuntimeHost>
  // (incomplete here); also flushes any context the toolbox never released.
  ~ToolboxRuntimeHost();
```

In the private section add the trampolines (next to `onReportMessage`/`onNotifyDataChanged`):

```cpp
  static bool onCreateParserIngest(
      void* ctx, uint32_t data_source_id, PJ_data_source_runtime_host_t* out_host,
      PJ_error_t* out_error) noexcept;
  static bool onReleaseParserIngest(void* ctx, uint32_t data_source_id, PJ_error_t* out_error) noexcept;
```

And add members BETWEEN `callbacks_` and `runtime_vtable_` (declaration order must match the ctor init list to keep `-Wreorder` quiet):

```cpp
  DataEngine& engine_;
  ObjectStore& object_store_;
  ParserIngestDeps parser_ingest_deps_;
  // create/release may race (worker thread) with teardown (GUI thread).
  std::mutex parser_ingest_mu_;
  // One delegated-ingest session per toolbox-created dataset, keyed by the
  // data-source handle id (== DatasetId). Reuses the exact machinery file
  // loads use — catalog lookup, classifySchema, ObjectStore registration,
  // render-parser registrar — on a toolbox-created dataset.
  std::unordered_map<uint32_t, std::unique_ptr<DataSourceRuntimeHost>> parser_ingests_;
```

`PJ_data_source_runtime_host_t` is visible via `pj_base/toolbox_protocol.h`, which now includes `data_source_protocol.h` (Task 1).

- [ ] **Step 5: Implement — cpp**

`PJ4/pj_runtime/src/ToolboxRuntimeHost.cpp`. Add includes:

```cpp
#include <cstdio>
#include <string>

#include "pj_runtime/DataSourceRuntimeHost.h"
#include "pj_runtime/ExtensionCatalogService.h"
```

plus the `MessageParserHandle` include — copy the exact `#include` line that `pj_runtime/src/DataSourceRuntimeHost.cpp` uses for it (find it with `grep -n "parser" PJ4/pj_runtime/src/DataSourceRuntimeHost.cpp | grep include`).

Replace the constructor with (note the two added vtable entries and the new members in declaration order):

```cpp
ToolboxRuntimeHost::ToolboxRuntimeHost(
    DataEngine& engine, ObjectStore& object_store, sdk::SettingsBackend& settings, Callbacks callbacks,
    ParserIngestDeps parser_ingest)
    : write_host_(engine, object_store),
      settings_host_(settings),
      callbacks_(std::move(callbacks)),
      engine_(engine),
      object_store_(object_store),
      parser_ingest_deps_(std::move(parser_ingest)),
      runtime_vtable_{
          PJ_TOOLBOX_PLUGIN_PROTOCOL_VERSION,
          sizeof(PJ_toolbox_runtime_host_vtable_t),
          &ToolboxRuntimeHost::onReportMessage,
          &ToolboxRuntimeHost::onNotifyDataChanged,
          &ToolboxRuntimeHost::onCreateParserIngest,
          &ToolboxRuntimeHost::onReleaseParserIngest,
      },
      runtime_{this, &runtime_vtable_} {}

ToolboxRuntimeHost::~ToolboxRuntimeHost() {
  // Flush anything a toolbox left unreleased so rows aren't lost on teardown.
  std::lock_guard lock(parser_ingest_mu_);
  for (auto& [id, host] : parser_ingests_) {
    if (host != nullptr) {
      host->flushAll();
    }
  }
  parser_ingests_.clear();
}
```

Add at the end of the file (before the closing namespace):

```cpp
namespace {
bool parserIngestFail(PJ_error_t* out_error, const char* msg) noexcept {
  if (out_error != nullptr) {
    *out_error = PJ_error_t{};
    std::snprintf(out_error->domain, sizeof(out_error->domain), "%s", "toolbox_runtime_host");
    std::snprintf(out_error->message, sizeof(out_error->message), "%s", msg);
  }
  return false;
}
}  // namespace

bool ToolboxRuntimeHost::onCreateParserIngest(
    void* ctx, uint32_t data_source_id, PJ_data_source_runtime_host_t* out_host,
    PJ_error_t* out_error) noexcept {
  auto* self = static_cast<ToolboxRuntimeHost*>(ctx);
  if (self == nullptr || out_host == nullptr) {
    return parserIngestFail(out_error, "invalid arguments");
  }
  try {
    if (self->parser_ingest_deps_.catalog == nullptr) {
      return parserIngestFail(out_error, "parser ingest is not configured on this host");
    }
    // Same validation the toolbox write host applies to object topics
    // (plugin_data_host.cpp:1237): the id must be a live dataset.
    if (self->engine_.getDataset(static_cast<DatasetId>(data_source_id)) == nullptr) {
      return parserIngestFail(out_error, "data source not found — call createDataSource first");
    }
    std::lock_guard lock(self->parser_ingest_mu_);
    auto& slot = self->parser_ingests_[data_source_id];
    if (slot == nullptr) {
      slot = std::make_unique<DataSourceRuntimeHost>(
          self->engine_, *self->parser_ingest_deps_.catalog, static_cast<DatasetId>(data_source_id),
          PJ_data_source_handle_t{data_source_id}, self->object_store_,
          "toolbox-ingest-" + std::to_string(data_source_id),
          self->parser_ingest_deps_.register_object_parser);
    }
    *out_host = slot->hostHandle();
    return true;
  } catch (const std::exception& e) {
    return parserIngestFail(out_error, e.what());
  } catch (...) {
    return parserIngestFail(out_error, "unknown exception in create_parser_ingest");
  }
}

bool ToolboxRuntimeHost::onReleaseParserIngest(
    void* ctx, uint32_t data_source_id, PJ_error_t* out_error) noexcept {
  auto* self = static_cast<ToolboxRuntimeHost*>(ctx);
  if (self == nullptr) {
    return parserIngestFail(out_error, "invalid arguments");
  }
  try {
    std::unique_ptr<DataSourceRuntimeHost> victim;
    {
      std::lock_guard lock(self->parser_ingest_mu_);
      auto it = self->parser_ingests_.find(data_source_id);
      if (it == self->parser_ingests_.end()) {
        return true;  // idempotent
      }
      victim = std::move(it->second);
      self->parser_ingests_.erase(it);
    }
    // Seal rows BEFORE destruction so the next notify_data_changed/catalog
    // rebuild sees everything the parsers wrote.
    victim->flushAll();
    victim.reset();
    return true;
  } catch (const std::exception& e) {
    return parserIngestFail(out_error, e.what());
  } catch (...) {
    return parserIngestFail(out_error, "unknown exception in release_parser_ingest");
  }
}
```

- [ ] **Step 6: Run the tests**

Run: `./build.sh && ctest --test-dir build -R ToolboxRuntimeHostTest --output-on-failure`
Expected: all PASS (3 new + existing).

- [ ] **Step 7: Run the neighboring suites (no regression)**

Run: `ctest --test-dir build -R "DataSourceRuntimeHost|SessionManager|CatalogModel" --output-on-failure`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add PJ4/pj_runtime/include/pj_runtime/ToolboxRuntimeHost.h \
        PJ4/pj_runtime/src/ToolboxRuntimeHost.cpp \
        PJ4/pj_runtime/tests/toolbox_runtime_host_test.cpp \
        PJ4/pj_runtime/CMakeLists.txt
git commit -m "pj_runtime: ToolboxRuntimeHost owns per-dataset DataSourceRuntimeHost parser-ingest contexts"
```

### Task 5: env-gated integration test with the REAL parser_ros (tf → FrameTransforms)

This is the proof, before any plugin work, that real parser_ros + the toolbox path produces a 3D-capable tf object topic. Gated on `PJ_REAL_ROS_PARSER_DIR`; hermetically SKIPs.

**Files:**
- Create: `PJ4/pj_runtime/tests/toolbox_parser_ingest_real_ros_test.cpp`
- Modify: `PJ4/pj_runtime/CMakeLists.txt`

- [ ] **Step 1: Build parser_ros so the .so exists**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/pj-official-plugins && ./build.sh parser_ros && find build/parser_ros -name "*parser_ros*.so"`
Expected: prints the built plugin path. Note its DIRECTORY — that's the env value used below.

- [ ] **Step 2: Write the test**

```cpp
// Copyright 2026
// SPDX-License-Identifier: MPL-2.0
//
// Integration: ToolboxRuntimeHost parser ingest + the REAL parser_ros plugin.
// Pushes a handcrafted tf2_msgs/msg/TFMessage CDR payload through the toolbox
// parser-ingest path and asserts the 3D-scene contract end to end:
//   - the topic classifies kFrameTransforms (metadata_json drives drag routing,
//     CatalogModel.cpp objectTypeFromMetadata),
//   - the raw bytes land in the ObjectStore,
//   - the render-time parser registrar fires (SessionManager contract).
// Gated: SKIPs unless PJ_REAL_ROS_PARSER_DIR points at the directory holding
// the built parser_ros .so (pj-official-plugins/build/parser_ros/...).

#include <gtest/gtest.h>

#include <QString>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "pj_base/sdk/data_source_host_views.hpp"
#include "pj_base/sdk/plugin_data_api.hpp"
#include "pj_base/sdk/service_registry.hpp"
#include "pj_base/sdk/service_traits.hpp"
#include "pj_base/sdk/settings_store_host.hpp"
#include "pj_base/sdk/toolbox_plugin_base.hpp"
#include "pj_datastore/engine.hpp"
#include "pj_datastore/object_store.hpp"
#include "pj_plugins/host/service_registry_builder.hpp"
#include "pj_runtime/ExtensionCatalogService.h"
#include "pj_runtime/ToolboxRuntimeHost.h"

namespace {

// Minimal XCDR1 little-endian writer. Alignment is relative to the start of
// the body (after the 4-byte encapsulation header) — the rule rosx/FastCDR
// deserializers apply.
struct CdrWriter {
  std::vector<uint8_t> buf{0x00, 0x01, 0x00, 0x00};  // {representation=CDR_LE, options=0}
  [[nodiscard]] size_t body() const { return buf.size() - 4; }
  void align(size_t n) {
    while (body() % n != 0) buf.push_back(0);
  }
  void u32(uint32_t v) {
    align(4);
    for (int i = 0; i < 4; ++i) buf.push_back(static_cast<uint8_t>(v >> (8 * i)));
  }
  void i32(int32_t v) { u32(static_cast<uint32_t>(v)); }
  void f64(double v) {
    align(8);
    uint64_t b = 0;
    std::memcpy(&b, &v, 8);
    for (int i = 0; i < 8; ++i) buf.push_back(static_cast<uint8_t>(b >> (8 * i)));
  }
  void str(std::string_view s) {
    u32(static_cast<uint32_t>(s.size() + 1));
    buf.insert(buf.end(), s.begin(), s.end());
    buf.push_back(0);
  }
};

// One TFMessage with one TransformStamped: map -> base_link, t=(1,2,3), q=identity.
std::vector<uint8_t> makeTfPayload() {
  CdrWriter w;
  w.u32(1);              // transforms sequence length
  w.i32(7);              // header.stamp.sec
  w.u32(500);            // header.stamp.nanosec
  w.str("map");          // header.frame_id
  w.str("base_link");    // child_frame_id
  w.f64(1.0);            // transform.translation.x
  w.f64(2.0);
  w.f64(3.0);
  w.f64(0.0);            // transform.rotation.x
  w.f64(0.0);
  w.f64(0.0);
  w.f64(1.0);            // w
  return w.buf;
}

// The concatenated ros2msg schema text exactly as a rosbag2 MCAP embeds it.
constexpr const char* kTfSchema = R"(geometry_msgs/TransformStamped[] transforms
================================================================================
MSG: geometry_msgs/TransformStamped
std_msgs/Header header
string child_frame_id
Transform transform
================================================================================
MSG: geometry_msgs/Transform
Vector3 translation
Quaternion rotation
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x 0
float64 y 0
float64 z 0
float64 w 1
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
)";

TEST(ToolboxParserIngestRealRos, TfMessageBecomesFrameTransformsObjectTopic) {
  const char* dir = std::getenv("PJ_REAL_ROS_PARSER_DIR");
  if (dir == nullptr || dir[0] == '\0') {
    GTEST_SKIP() << "PJ_REAL_ROS_PARSER_DIR not set (directory containing the built parser_ros .so)";
  }
  PJ::ExtensionCatalogService catalog{QString::fromUtf8(dir)};
  if (catalog.findParserByEncoding(QStringLiteral("ros2msg")) == nullptr) {
    GTEST_SKIP() << "no ros2msg parser found in " << dir;
  }

  PJ::DataEngine engine;
  PJ::ObjectStore object_store;
  PJ::sdk::InMemorySettingsBackend settings;
  PJ::ServiceRegistryBuilder builder;

  std::vector<PJ::ObjectTopicId> registered;
  PJ::ToolboxRuntimeHost::ParserIngestDeps deps;
  deps.catalog = &catalog;
  deps.register_object_parser = [&registered](
                                    PJ::ObjectTopicId id, std::unique_ptr<PJ::MessageParserHandle> parser) {
    EXPECT_NE(parser, nullptr);
    registered.push_back(id);
  };
  PJ::ToolboxRuntimeHost host(engine, object_store, settings, {}, std::move(deps));
  host.registerServices(builder);
  PJ::sdk::ServiceRegistry services(builder.view());

  auto toolbox = services.require<PJ::sdk::ToolboxHostService>();
  ASSERT_TRUE(toolbox.has_value());
  auto runtime = services.require<PJ::sdk::ToolboxRuntimeHostService>();
  ASSERT_TRUE(runtime.has_value());

  auto ds = (*toolbox).createDataSource("tf download");
  ASSERT_TRUE(ds.has_value()) << ds.error();
  auto ingest_or = (*runtime).createParserIngest(ds->id);
  ASSERT_TRUE(ingest_or.has_value()) << ingest_or.error();
  auto ingest = *ingest_or;

  const std::string_view schema{kTfSchema};
  auto binding = ingest.ensureParserBinding(PJ::ParserBindingRequest{
      .topic_name = "/tf",
      .parser_encoding = "ros2msg",
      .type_name = "tf2_msgs/msg/TFMessage",  // verbatim, as the wire/mcap carries it
      .schema = PJ::Span<const uint8_t>(
          reinterpret_cast<const uint8_t*>(schema.data()), schema.size()),
      .parser_config_json = "",
  });
  ASSERT_TRUE(binding.has_value()) << binding.error();

  const auto payload = makeTfPayload();
  auto push = ingest.pushMessage(
      *binding, PJ::Timestamp{1'000'000'000}, [payload]() -> std::vector<uint8_t> { return payload; });
  ASSERT_TRUE(push.has_value()) << push.error();
  ASSERT_TRUE((*runtime).releaseParserIngest(ds->id).has_value());

  // 3D contract: kFrameTransforms metadata (drag routing), bytes stored
  // (render source), render parser registered (SessionManager contract).
  const auto topics = object_store.listTopics(static_cast<PJ::DatasetId>(ds->id));
  ASSERT_EQ(topics.size(), 1u);
  const auto& desc = object_store.descriptor(topics.front());
  EXPECT_NE(desc.metadata_json.find("frame_transforms"), std::string::npos) << desc.metadata_json;
  EXPECT_EQ(object_store.entryCount(topics.front()), 1u);
  EXPECT_EQ(registered.size(), 1u);
}

}  // namespace
```

(If `ObjectStore::descriptor(ObjectTopicId)` has a different name, find the accessor `Media2DDockWidget.cpp:163` uses — `store.descriptor(topic_id).metadata_json` — and match it.)

- [ ] **Step 3: CMake target**

In `PJ4/pj_runtime/CMakeLists.txt`, duplicate the `toolbox_runtime_host_test` block (lines 91-99) verbatim, renaming target/sources/test-name:

```cmake
    add_executable(toolbox_parser_ingest_real_ros_test
        tests/toolbox_parser_ingest_real_ros_test.cpp
    )
    target_compile_options(toolbox_parser_ingest_real_ros_test PRIVATE ${PJ_WARNING_FLAGS})
    # link libraries: copy the target_link_libraries(...) list of
    # toolbox_runtime_host_test (lines 95-98) verbatim.
    add_test(NAME ToolboxParserIngestRealRosTest COMMAND toolbox_parser_ingest_real_ros_test)
```

- [ ] **Step 4: Run — hermetic SKIP, then live PASS**

Run: `./build.sh && ctest --test-dir build -R ToolboxParserIngestRealRosTest --output-on-failure`
Expected: 1 test SKIPPED.

Run (substitute the directory from Step 1):
`PJ_REAL_ROS_PARSER_DIR=<dir-of-parser_ros.so> ctest --test-dir build -R ToolboxParserIngestRealRosTest --output-on-failure`
Expected: PASS. **If `ensureParserBinding` fails on the type name**, retry with `type_name = "tf2_msgs/TFMessage"` and record which form parser_ros accepts — the plugin driver (Task 8) must send the accepted form (the wire carries `tf2_msgs/msg/TFMessage`; `data_load_mcap` forwards mcap schema names verbatim, so verbatim is expected to work).

- [ ] **Step 5: Commit**

```bash
git add PJ4/pj_runtime/tests/toolbox_parser_ingest_real_ros_test.cpp PJ4/pj_runtime/CMakeLists.txt
git commit -m "pj_runtime: env-gated real-parser_ros integration test — toolbox tf ingest yields kFrameTransforms"
```

### Task 6: Wire the deps in MainWindow

**Files:**
- Modify: `PJ4/pj_app/src/MainWindow.cpp` (PanelSession block, host construction at ~3198-3201)

- [ ] **Step 1: Add the deps and pass them**

Directly above the `session->host = std::make_unique<ToolboxRuntimeHost>(...)` call insert:

```cpp
  // Parser-ingest deps: the plugin catalog for ensureParserBinding lookups and
  // the SessionManager registrar for render-time object parsers. The registrar
  // may fire on the toolbox worker thread mid-download — marshal to the GUI
  // thread (same discipline as the host's own callbacks); the queued
  // registration always lands before the later-queued notify_data_changed
  // catalog rebuild. shared_ptr wrapper: std::function requires copyable.
  ToolboxRuntimeHost::ParserIngestDeps ingest_deps;
  ingest_deps.catalog = &session_->extensionCatalog();
  ingest_deps.register_object_parser =
      [this](ObjectTopicId id, std::unique_ptr<MessageParserHandle> parser) {
        auto shared = std::make_shared<std::unique_ptr<MessageParserHandle>>(std::move(parser));
        QMetaObject::invokeMethod(
            this,
            [this, id, shared]() {
              session_->sessionManager().registerObjectTopicParser(id, std::move(*shared));
            },
            Qt::AutoConnection);
      };
```

and change the construction to:

```cpp
  session->host = std::make_unique<ToolboxRuntimeHost>(
      session_->sessionManager().dataEngine(), session_->sessionManager().objectStore(), *session->settings,
      std::move(callbacks), std::move(ingest_deps));
```

If `MessageParserHandle` is not yet a complete type here, add the same `#include` line `pj_runtime/src/DataSourceRuntimeHost.cpp` uses for it (MainWindow.cpp already includes `pj_runtime/ExtensionCatalogService.h` at line 88 and `pj_runtime/ToolboxRuntimeHost.h` at line 93).

- [ ] **Step 2: Build + manual sanity**

Run: `./build.sh`
Expected: PASS.
Run the app with the vendored plugin (pre-migration plugin still uses rosx — this only proves nothing regressed):

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4 && ./run.sh --plugin-dir \
  /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/pj-official-plugins/build/toolbox_dexory_cloud/Release/bin
```

Expected: Dexory Cloud panel opens, browse + fetch against `ws://localhost:8080` behaves exactly as before. (Start the dev server first if needed: `make server-start` at `<repo>`.)

- [ ] **Step 3: Commit**

```bash
git add PJ4/pj_app/src/MainWindow.cpp
git commit -m "pj_app: wire toolbox parser-ingest deps (extension catalog + GUI-marshalled SessionManager registrar)"
```

### Task 7: Repackage the SDK for plugins (0.6.0 → 0.6.1)

**Files:**
- Modify: `PJ4/plotjuggler_sdk/conanfile.py` (line 33: `version = "0.6.0"`)
- Modify: `PJ4/pj-official-plugins/SDK_VERSION` (content: `0.6.0`)

- [ ] **Step 1: Bump both pins to `0.6.1`**

Edit `conanfile.py` line 33 → `version = "0.6.1"`; overwrite `SDK_VERSION` with `0.6.1`.
**Recorded deviation:** `scripts/bump_core_version.py` is NOT used — it also moves the `extern/plotjuggler_core` submodule to an upstream tag `v0.6.1` that does not exist. The package is instead created from the EDITED in-tree SDK (next step), so the submodule is bypassed entirely. Local-fork only; revisit if upstreaming.

- [ ] **Step 2: Create the package from the vendored (edited) SDK**

Run:

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/plotjuggler_sdk
conan create . -s build_type=Release -s compiler.cppstd=20 --build=missing
conan list "plotjuggler_sdk/*"
```

Expected: cache now lists `plotjuggler_sdk/0.6.1`. (If `conan create` needs extra settings, mirror the flags `pj-official-plugins/build.sh:69-73` passes to `conan install`.)

- [ ] **Step 3: Rebuild the plugin against 0.6.1 (still old plugin code — proves resolution + ABI)**

Run: `cd ../pj-official-plugins && ./build.sh toolbox_dexory_cloud && ctest --test-dir build/toolbox_dexory_cloud/Release --output-on-failure`
Expected: builds against 0.6.1; ctest green (hermetic).

- [ ] **Step 4: Commit**

```bash
git add PJ4/plotjuggler_sdk/conanfile.py PJ4/pj-official-plugins/SDK_VERSION
git commit -m "sdk: bump to 0.6.1 (toolbox parser-ingest tail slots) and repoint the plugin pin"
```

---

## Phase 2 — Plugin migration (Dexory Cloud)

All paths below are relative to `PJ4/pj-official-plugins/toolbox_dexory_cloud/`.

### Task 8: `ParserIngestDriver` + hermetic recorder test

**Files:**
- Create: `src/parser_ingest_driver.hpp`, `src/parser_ingest_driver.cpp`
- Create: `tests/parser_ingest_test_support.hpp`, `tests/parser_ingest_driver_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: Write the support header (fake hosts / recorder)**

`tests/parser_ingest_test_support.hpp`:

```cpp
#pragma once
// Fake toolbox-runtime + data-source-runtime hosts for ParserIngestDriver
// tests: records every ensureParserBinding request and every pushed message
// (after invoking the fetcher), implements the release contract. Shared by
// the hermetic driver test and the live worker test.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "pj_base/data_source_protocol.h"
#include "pj_base/toolbox_protocol.h"

namespace pj_ingest_test {

struct RecordedBinding {
  std::string topic_name;
  std::string parser_encoding;
  std::string type_name;
  std::string schema;
  std::string config;
  uint32_t handle = 0;
};

struct RecordedPush {
  uint32_t handle = 0;
  int64_t ts = 0;
  std::vector<uint8_t> bytes;
};

struct FakeIngestHost {
  std::vector<RecordedBinding> bindings;
  std::vector<RecordedPush> pushes;
  std::vector<uint32_t> created;    // data_source_ids passed to create
  std::vector<uint32_t> released;   // data_source_ids passed to release
  bool refuse_create = false;       // simulate an older/unconfigured host
  // Bind requests whose type_name matches this string are refused (per-topic
  // "no parser" simulation).
  std::string refuse_type;

  PJ_data_source_runtime_host_vtable_t ds_vtable{};
  PJ_toolbox_runtime_host_vtable_t tb_vtable{};

  FakeIngestHost() {
    ds_vtable.protocol_version = 1;
    ds_vtable.struct_size = sizeof(PJ_data_source_runtime_host_vtable_t);
    ds_vtable.ensure_parser_binding = &FakeIngestHost::ensureBinding;
    ds_vtable.push_message = &FakeIngestHost::pushMessage;
    tb_vtable.protocol_version = PJ_TOOLBOX_PLUGIN_PROTOCOL_VERSION;
    tb_vtable.struct_size = sizeof(PJ_toolbox_runtime_host_vtable_t);
    tb_vtable.create_parser_ingest = &FakeIngestHost::createIngest;
    tb_vtable.release_parser_ingest = &FakeIngestHost::releaseIngest;
  }

  [[nodiscard]] PJ_toolbox_runtime_host_t toolboxRuntime() {
    return PJ_toolbox_runtime_host_t{this, &tb_vtable};
  }

  static void fill(PJ_error_t* err, const char* msg) {
    if (err != nullptr) {
      *err = PJ_error_t{};
      std::snprintf(err->domain, sizeof(err->domain), "%s", "fake_host");
      std::snprintf(err->message, sizeof(err->message), "%s", msg);
    }
  }
  static std::string sv(PJ_string_view_t s) {
    return s.data != nullptr ? std::string(s.data, s.size) : std::string();
  }

  static bool createIngest(void* ctx, uint32_t id, PJ_data_source_runtime_host_t* out, PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeIngestHost*>(ctx);
    if (self->refuse_create) {
      fill(err, "parser ingest is not configured on this host");
      return false;
    }
    self->created.push_back(id);
    *out = PJ_data_source_runtime_host_t{self, &self->ds_vtable};
    return true;
  }
  static bool releaseIngest(void* ctx, uint32_t id, PJ_error_t* /*err*/) noexcept {
    static_cast<FakeIngestHost*>(ctx)->released.push_back(id);
    return true;
  }
  static bool ensureBinding(
      void* ctx, const PJ_parser_binding_request_t* req, PJ_parser_binding_handle_t* out,
      PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeIngestHost*>(ctx);
    RecordedBinding b;
    b.topic_name = sv(req->topic_name);
    b.parser_encoding = sv(req->parser_encoding);
    b.type_name = sv(req->type_name);
    b.schema.assign(reinterpret_cast<const char*>(req->schema.data), req->schema.size);
    b.config = sv(req->parser_config_json);
    if (!self->refuse_type.empty() && b.type_name == self->refuse_type) {
      fill(err, "no parser found for type");
      return false;
    }
    b.handle = static_cast<uint32_t>(self->bindings.size()) + 1;
    out->id = b.handle;
    self->bindings.push_back(std::move(b));
    return true;
  }
  static bool pushMessage(
      void* ctx, PJ_parser_binding_handle_t handle, int64_t ts, PJ_message_data_fetcher_t fetch,
      PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeIngestHost*>(ctx);
    PJ_payload_t payload{};
    const bool ok = fetch.fetchMessageData != nullptr && fetch.fetchMessageData(fetch.ctx, &payload, err);
    if (ok) {
      RecordedPush p;
      p.handle = handle.id;
      p.ts = ts;
      if (payload.data != nullptr && payload.size > 0) {
        p.bytes.assign(payload.data, payload.data + payload.size);
      }
      self->pushes.push_back(std::move(p));
    }
    if (payload.anchor.release != nullptr) {
      payload.anchor.release(payload.anchor.ctx);  // host releases the anchor
    }
    if (fetch.release != nullptr) {
      fetch.release(fetch.ctx);  // host releases the fetcher closure
    }
    return ok;
  }
};

}  // namespace pj_ingest_test
```

(If the C field names differ — e.g. the fetcher member is not `fetchMessageData`/`release`, or `PJ_payload_t` members are not `data/size/anchor` — open `pj_base/data_source_protocol.h` lines 139-201/328-336 and match them exactly; the SDK-side trampoline in `data_source_host_views.hpp` shows every field in use.)

- [ ] **Step 2: Write the failing driver test**

`tests/parser_ingest_driver_test.cpp`:

```cpp
// Hermetic ParserIngestDriver contract test against the fake recorder host:
// the plugin's WHOLE post-migration parsing responsibility is "bind request
// fields forwarded verbatim, every payload pushed with its log_time, release
// on finalize" — decode correctness is the host's (pj_runtime tests).

#include <gtest/gtest.h>

#include "parser_ingest_driver.hpp"
#include "parser_ingest_test_support.hpp"

#include "pj_base/sdk/toolbox_plugin_base.hpp"

using pj_ingest_test::FakeIngestHost;

namespace {

SessionInfo makeSession() {
  SessionInfo info;
  SessionSchema s1;
  s1.schema_id = 1;
  s1.name = "tf2_msgs/msg/TFMessage";
  s1.encoding = "ros2msg";
  s1.data = "geometry_msgs/TransformStamped[] transforms\n";
  SessionSchema s2;
  s2.schema_id = 2;
  s2.name = "mock/refused";
  s2.encoding = "ros2msg";
  s2.data = "bool x\n";
  info.schemas = {s1, s2};
  SessionTopic t1;
  t1.topic_id = 10;
  t1.topic_name = "/tf";
  t1.schema_id = 1;
  t1.message_encoding = "cdr";
  SessionTopic t2;
  t2.topic_id = 20;
  t2.topic_name = "/refused";
  t2.schema_id = 2;
  t2.message_encoding = "cdr";
  SessionTopic t3;
  t3.topic_id = 30;
  t3.topic_name = "/no_schema";
  t3.schema_id = 99;  // not in dictionary
  t3.message_encoding = "cdr";
  info.topics = {t1, t2, t3};
  return info;
}

DecodedMessage makeMsg(std::uint32_t topic_id, std::int64_t ts, std::string payload) {
  DecodedMessage m;
  m.topic_id = topic_id;
  m.schema_id = 1;
  m.log_time_ns = ts;
  m.publish_time_ns = ts;
  m.payload = std::move(payload);
  return m;
}

TEST(ParserIngestDriver, BindsVerbatimPushesPayloadsAndReleases) {
  FakeIngestHost fake;
  fake.refuse_type = "mock/refused";
  PJ::ToolboxRuntimeHostView runtime{fake.toolboxRuntime()};

  ParserIngestDriver driver;
  const auto info = makeSession();
  auto bind = driver.bindSession(runtime, PJ::sdk::DataSourceHandle{42}, info);

  EXPECT_EQ(bind.decodable, 1u);
  ASSERT_EQ(fake.created.size(), 1u);
  EXPECT_EQ(fake.created[0], 42u);
  ASSERT_EQ(fake.bindings.size(), 1u);  // the refused one never recorded a handle
  EXPECT_EQ(fake.bindings[0].topic_name, "/tf");
  EXPECT_EQ(fake.bindings[0].parser_encoding, "ros2msg");
  EXPECT_EQ(fake.bindings[0].type_name, "tf2_msgs/msg/TFMessage");
  EXPECT_EQ(fake.bindings[0].schema, "geometry_msgs/TransformStamped[] transforms\n");
  EXPECT_EQ(bind.errors.size(), 2u);  // /refused + /no_schema

  ASSERT_TRUE(driver.decoders().count(10));
  EXPECT_TRUE(driver.decoders().at(10).decodable);
  ASSERT_TRUE(driver.decoders().count(20));
  EXPECT_FALSE(driver.decoders().at(20).decodable);
  EXPECT_FALSE(driver.decoders().at(20).skip_reason.empty());
  EXPECT_TRUE(driver.hasDecodable());

  EXPECT_TRUE(driver.decode(makeMsg(10, 111, "\x01\x02\x03")));
  EXPECT_TRUE(driver.decode(makeMsg(10, 222, "\x04")));
  EXPECT_FALSE(driver.decode(makeMsg(20, 333, "x")));  // undecodable topic
  EXPECT_FALSE(driver.decode(makeMsg(77, 444, "x")));  // unknown topic

  ASSERT_EQ(fake.pushes.size(), 2u);
  EXPECT_EQ(fake.pushes[0].ts, 111);
  EXPECT_EQ(fake.pushes[0].bytes, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(fake.pushes[1].ts, 222);

  const auto counts = driver.decodedCounts();
  EXPECT_EQ(counts.at(10), 2u);
  EXPECT_EQ(counts.at(20), 0u);

  driver.finalize();
  ASSERT_EQ(fake.released.size(), 1u);
  EXPECT_EQ(fake.released[0], 42u);
  driver.finalize();  // idempotent
  EXPECT_EQ(fake.released.size(), 1u);
}

TEST(ParserIngestDriver, OlderHostReportsPerTopicReason) {
  FakeIngestHost fake;
  fake.refuse_create = true;
  PJ::ToolboxRuntimeHostView runtime{fake.toolboxRuntime()};

  ParserIngestDriver driver;
  auto bind = driver.bindSession(runtime, PJ::sdk::DataSourceHandle{1}, makeSession());
  EXPECT_EQ(bind.decodable, 0u);
  EXPECT_FALSE(driver.hasDecodable());
  for (const auto& [id, t] : driver.decoders()) {
    EXPECT_FALSE(t.decodable);
    EXPECT_NE(t.skip_reason.find("host parser ingest unavailable"), std::string::npos);
  }
}

}  // namespace
```

(Adjust struct-literal field assignments if `SessionSchema`/`SessionTopic`/`DecodedMessage` initialization differs — the field NAMES are verified at `src/backend_types.hpp:120-147` and `src/decoded_message.hpp:20-26`. If these types live in a namespace, mirror it — check the top of `backend_types.hpp`. `PJ::sdk::DataSourceHandle{42}` — if the handle type doesn't brace-init from one int, construct it the way `fetch_worker.cpp` receives it from `datasetForFetch` and set `.id = 42`.)

- [ ] **Step 3: Run to verify it fails**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/pj-official-plugins && ./build.sh toolbox_dexory_cloud`
Expected: FAIL — `parser_ingest_driver.hpp` not found (after Step 5's CMake edit) / target missing.

- [ ] **Step 4: Implement the driver**

`src/parser_ingest_driver.hpp` (wrap the declarations in the SAME namespace `src/ros_decode_driver.hpp` uses — open it and copy the namespace lines):

```cpp
#pragma once
// ParserIngestDriver (Slice 16) — replaces RosDecodeDriver's decode-in-plugin
// (rosx_introspection) with HOST-delegated parsing: one ensureParserBinding
// per topic, one pushMessage per raw CDR record, through the toolbox
// parser-ingest tail slots (SDK >= 0.6.1). The host's parser plugins
// (parser_ros) write the scalars (specialized Imu/Pose handlers included) and
// classify/store object topics (tf, pointclouds, images, grids) with a
// render-time parser registered — 3D-draggable AND renderable.
//
// On hosts without the tail slots every topic reports skip_reason
// "host parser ingest unavailable: …" — the plugin ships no fallback decoder.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "pj_base/sdk/data_source_host_views.hpp"
#include "pj_base/sdk/plugin_data_api.hpp"
#include "pj_base/sdk/toolbox_plugin_base.hpp"

#include "backend_types.hpp"
#include "decoded_message.hpp"

struct IngestTopic {
  std::uint32_t topic_id = 0;
  std::string topic_name;
  std::string type_name;  // schema name verbatim (e.g. "sensor_msgs/msg/Imu")
  PJ::ParserBindingHandle binding{};
  bool decodable = false;
  std::string skip_reason;          // set when !decodable
  std::uint64_t rows = 0;           // messages pushed to the host
  std::uint64_t decode_errors = 0;  // pushMessage failures
};

struct IngestBindResult {
  std::size_t decodable = 0;
  std::vector<std::string> errors;  // "<topic> (<type>): <reason>" per skipped topic
};

class ParserIngestDriver {
 public:
  ParserIngestDriver() = default;
  ~ParserIngestDriver();  // finalize() if the caller forgot

  ParserIngestDriver(const ParserIngestDriver&) = delete;
  ParserIngestDriver& operator=(const ParserIngestDriver&) = delete;

  // Creates the host parser-ingest context for `ds` and binds one host parser
  // per session topic. Topics without a host parser become !decodable with a
  // per-topic reason.
  IngestBindResult bindSession(
      PJ::ToolboxRuntimeHostView runtime, PJ::sdk::DataSourceHandle ds, const SessionInfo& info);

  // Push one raw message to its topic's binding. Best-effort: failures count
  // into decode_errors and return false.
  bool decode(const DecodedMessage& m);

  // releaseParserIngest (host flushes all parser write hosts). Idempotent.
  // MUST run after the download ends and BEFORE notifyDataChanged so the
  // catalog rebuild sees sealed rows.
  void finalize();

  [[nodiscard]] const std::unordered_map<std::uint32_t, IngestTopic>& decoders() const {
    return topics_;
  }
  [[nodiscard]] bool hasDecodable() const;
  [[nodiscard]] std::unordered_map<std::uint32_t, std::uint64_t> decodedCounts() const;
  [[nodiscard]] std::unordered_map<std::uint32_t, std::uint64_t> errorCounts() const;

 private:
  PJ::ToolboxRuntimeHostView runtime_{};
  PJ::DataSourceRuntimeHostView ingest_{};
  std::uint32_t source_id_ = 0;
  bool active_ = false;
  std::unordered_map<std::uint32_t, IngestTopic> topics_;
};
```

`src/parser_ingest_driver.cpp`:

```cpp
#include "parser_ingest_driver.hpp"

ParserIngestDriver::~ParserIngestDriver() {
  finalize();
}

IngestBindResult ParserIngestDriver::bindSession(
    PJ::ToolboxRuntimeHostView runtime, PJ::sdk::DataSourceHandle ds, const SessionInfo& info) {
  IngestBindResult result;
  runtime_ = runtime;
  source_id_ = ds.id;

  std::unordered_map<std::uint32_t, const SessionSchema*> schema_by_id;
  for (const auto& s : info.schemas) {
    schema_by_id.emplace(s.schema_id, &s);
  }

  auto ingest_or = runtime_.createParserIngest(ds.id);
  if (!ingest_or.has_value()) {
    // Older/unconfigured host: every topic carries the reason; no fallback.
    for (const auto& t : info.topics) {
      IngestTopic it;
      it.topic_id = t.topic_id;
      it.topic_name = t.topic_name;
      it.skip_reason = "host parser ingest unavailable: " + ingest_or.error();
      result.errors.push_back(t.topic_name + ": " + it.skip_reason);
      topics_.emplace(t.topic_id, std::move(it));
    }
    return result;
  }
  ingest_ = *ingest_or;
  active_ = true;

  for (const auto& t : info.topics) {
    IngestTopic it;
    it.topic_id = t.topic_id;
    it.topic_name = t.topic_name;

    const auto sit = schema_by_id.find(t.schema_id);
    if (sit == schema_by_id.end()) {
      it.skip_reason = "schema " + std::to_string(t.schema_id) + " missing from session dictionary";
      result.errors.push_back(t.topic_name + ": " + it.skip_reason);
      topics_.emplace(t.topic_id, std::move(it));
      continue;
    }
    const SessionSchema& schema = *sit->second;
    it.type_name = schema.name;

    // Fields forwarded VERBATIM — same mapping data_load_mcap applies to mcap
    // channel/schema records (mcap_source.cpp:181-318): the wire's
    // SessionSchema mirrors the mcap schema record.
    auto binding = ingest_.ensureParserBinding(PJ::ParserBindingRequest{
        .topic_name = t.topic_name,
        .parser_encoding = schema.encoding,
        .type_name = schema.name,
        .schema = PJ::Span<const uint8_t>(
            reinterpret_cast<const std::uint8_t*>(schema.data.data()), schema.data.size()),
        .parser_config_json = "",
    });
    if (!binding.has_value()) {
      it.skip_reason = binding.error();
      result.errors.push_back(t.topic_name + " (" + schema.name + "): " + it.skip_reason);
      topics_.emplace(t.topic_id, std::move(it));
      continue;
    }
    it.binding = *binding;
    it.decodable = true;
    ++result.decodable;
    topics_.emplace(t.topic_id, std::move(it));
  }
  return result;
}

bool ParserIngestDriver::decode(const DecodedMessage& m) {
  auto it = topics_.find(m.topic_id);
  if (it == topics_.end() || !it->second.decodable || !active_) {
    return false;
  }
  // One owned copy per message; the shared_ptr doubles as the PayloadView
  // anchor, so the host's lazy ObjectStore closures (tf/pointcloud re-reads
  // at render time) stay valid for the dataset's lifetime. pushMessage's
  // contract: the fetcher must be idempotent and thread-safe — a capture-only
  // closure over an immutable shared buffer is both.
  auto owned = std::make_shared<const std::string>(m.payload);
  auto status = ingest_.pushMessage(
      it->second.binding, PJ::Timestamp{m.log_time_ns}, [owned]() -> PJ::sdk::PayloadView {
        return PJ::sdk::PayloadView{
            PJ::Span<const std::uint8_t>(
                reinterpret_cast<const std::uint8_t*>(owned->data()), owned->size()),
            owned};
      });
  if (!status.has_value()) {
    ++it->second.decode_errors;
    return false;
  }
  ++it->second.rows;
  return true;
}

void ParserIngestDriver::finalize() {
  if (!active_) {
    return;
  }
  active_ = false;
  ingest_ = PJ::DataSourceRuntimeHostView{};
  (void)runtime_.releaseParserIngest(source_id_);
}

bool ParserIngestDriver::hasDecodable() const {
  for (const auto& [id, t] : topics_) {
    if (t.decodable) {
      return true;
    }
  }
  return false;
}

std::unordered_map<std::uint32_t, std::uint64_t> ParserIngestDriver::decodedCounts() const {
  std::unordered_map<std::uint32_t, std::uint64_t> out;
  for (const auto& [id, t] : topics_) {
    out.emplace(id, t.rows);
  }
  return out;
}

std::unordered_map<std::uint32_t, std::uint64_t> ParserIngestDriver::errorCounts() const {
  std::unordered_map<std::uint32_t, std::uint64_t> out;
  for (const auto& [id, t] : topics_) {
    out.emplace(id, t.decode_errors);
  }
  return out;
}
```

- [ ] **Step 5: CMake — add the source + the test target**

In `CMakeLists.txt`: add `src/parser_ingest_driver.cpp` to the plugin's source list (next to `src/ros_decode_driver.cpp` at line ~93 — both coexist until Task 10). In the tests section add a target — copy the structure of an existing SDK-linking test target (e.g. the `session_decode_test` block) and set:

```cmake
  add_executable(toolbox_dexory_cloud_parser_ingest_test
    tests/parser_ingest_driver_test.cpp
    src/parser_ingest_driver.cpp
  )
  # link/include lines: copy from the session_decode_test target block verbatim
  # (GTest::gtest_main + the plotjuggler_sdk targets + src/ include dir).
  add_test(NAME DexoryCloudParserIngestTest COMMAND toolbox_dexory_cloud_parser_ingest_test)
```

- [ ] **Step 6: Build + run**

Run: `./build.sh toolbox_dexory_cloud && ctest --test-dir build/toolbox_dexory_cloud/Release -R DexoryCloudParserIngestTest --output-on-failure`
Expected: PASS (both tests).

- [ ] **Step 7: Commit**

```bash
git add PJ4/pj-official-plugins/toolbox_dexory_cloud/src/parser_ingest_driver.hpp \
        PJ4/pj-official-plugins/toolbox_dexory_cloud/src/parser_ingest_driver.cpp \
        PJ4/pj-official-plugins/toolbox_dexory_cloud/tests/parser_ingest_test_support.hpp \
        PJ4/pj-official-plugins/toolbox_dexory_cloud/tests/parser_ingest_driver_test.cpp \
        PJ4/pj-official-plugins/toolbox_dexory_cloud/CMakeLists.txt
git commit -m "plugin: ParserIngestDriver — host-delegated parsing via toolbox parser-ingest slots (hermetic recorder test)"
```

### Task 9: Swap the driver into `fetch_worker`

**Files:**
- Modify: `src/fetch_worker.cpp` (include at line 22; block at lines 383-470)

- [ ] **Step 1: Replace the include**

Line 22: `#include "ros_decode_driver.hpp"` → `#include "parser_ingest_driver.hpp"`.

- [ ] **Step 2: Edit the download block (lines ~383-460) — five surgical changes**

1. `RosDecodeDriver driver;` → `ParserIngestDriver driver;`
2. After `PJ::sdk::ToolboxHostView host = host_provider_();` add:

```cpp
  if (!runtime_host_provider_) {
    finish_all_topics(false, "no runtime host provider");
    finish_all();
    return;
  }
  PJ::ToolboxRuntimeHostView runtime = runtime_host_provider_();
```

3. `RosBindResult bind = driver.bindSession(host, *ds, session_info);` → `IngestBindResult bind = driver.bindSession(runtime, *ds, session_info);`
4. In the download handler: `(void)driver.decode(host, m);` → `(void)driver.decode(m);` (keep the comment).
5. After `downloadSessionResumable(...)` returns and BEFORE `write_lock.unlock();` (i.e. right after the `backend_session_for_cancel_.store(nullptr, ...)` line at ~475) add:

```cpp
  // Seal the host-side parser writes (releaseParserIngest → flushAll) while
  // still inside the host-write critical section, so the GUI-thread
  // notifyDataChanged → catalog rebuild that follows sees every row and
  // every object topic.
  driver.finalize();
```

Everything else in the block (the `decodable_by_id` loop over `driver.decoders()`, `driver.hasDecodable()`, `driver.decodedCounts()`, `driver.errorCounts()`, the SessionCache store) compiles unchanged — `IngestTopic` deliberately keeps the `.decodable`/`.skip_reason`/`.rows`/`.decode_errors` names.

- [ ] **Step 3: Build + hermetic ctest**

Run: `./build.sh toolbox_dexory_cloud && ctest --test-dir build/toolbox_dexory_cloud/Release --output-on-failure`
Expected: build PASS. The old `ros_decode_*` tests still build/pass (the old driver still exists until Task 10); live tests SKIP.

- [ ] **Step 4: Commit**

```bash
git add PJ4/pj-official-plugins/toolbox_dexory_cloud/src/fetch_worker.cpp
git commit -m "plugin: fetch_worker ingests via host parser delegation (ParserIngestDriver); finalize seals rows pre-rebuild"
```

### Task 10: Delete the decode-in-plugin machinery (rosx, RosDecodeDriver, parity tests)

**Files:**
- Delete: `src/ros_decode_driver.hpp`, `src/ros_decode_driver.cpp`
- Delete: `tests/ros_decode_parity_test.cpp`, `tests/ros_decode_ingest_test.cpp`, `tests/ros_decode_live_test.cpp`, `tests/ros_decode_test_support.hpp`
- Modify: `CMakeLists.txt`, `README.md`

- [ ] **Step 1: Confirm nothing else references the dead code**

Run: `grep -rn "ros_decode\|RosDecodeDriver\|rosx" src/ tests/ tools/ wasm/ ../..//toolbox_dexory_cloud/CMakeLists.txt 2>/dev/null | grep -v Binary`
Expected: hits ONLY in the files being deleted + CMakeLists + README + comments. Anything else: fix it first.
Also: `grep -rn "ros_decode\|rosx" /home/gn/ws/PJ4_Server_Template/pj-mcap-server/scripts /home/gn/ws/PJ4_Server_Template/pj-mcap-server/Makefile` — update any smoke/matrix references (none expected; smoke runs ctest wholesale).

- [ ] **Step 2: Delete + clean CMake**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/pj-official-plugins/toolbox_dexory_cloud
git rm src/ros_decode_driver.hpp src/ros_decode_driver.cpp \
       tests/ros_decode_parity_test.cpp tests/ros_decode_ingest_test.cpp \
       tests/ros_decode_live_test.cpp tests/ros_decode_test_support.hpp
```

In `CMakeLists.txt`:
- remove the whole rosx_introspection CPM block (lines ~16-52: `include(.../CPM.cmake)` stays ONLY if something else uses CPM — grep `CPMAddPackage` first; if rosx is the only package, remove the include too),
- remove `src/ros_decode_driver.cpp` from the plugin sources (~line 93),
- remove `rosx_introspection` from `target_link_libraries` (~line 136),
- remove the three `ros_decode_*` test targets and their `add_test` lines,
- rewrite the tests-section header comment (lines 224-237) to name the new gates: `parser_ingest_driver_test` (hermetic contract), `parser_ingest_live_test` (live counts — Task 11), host-side decode correctness owned by `pj_runtime` (`ToolboxRuntimeHostTest`, `ToolboxParserIngestRealRosTest`).
- `tests/fixtures/ros/` — if Step 1's grep shows no remaining references, `git rm -r tests/fixtures/ros`.

- [ ] **Step 3: Update the plugin README** — replace the decode-in-plugin description (rosx_introspection, triple-parity) with: parsing is delegated to the HOST's MessageParser plugins via the toolbox parser-ingest slots (SDK 0.6.1); tf/pointclouds/images arrive as ObjectStore topics with render parsers; the plugin contains zero message decoders.

- [ ] **Step 4: Verify the WASM core is untouched**

Run: `grep -rn "ros_decode\|rosx\|parser_ingest" wasm/`
Expected: no hits (the wasm build compiles only session_key/session_cache/stitch_select/hierarchy_prefix/cli_url_resolve + zstd).

- [ ] **Step 5: Full rebuild + ctest both modes**

Run: `cd .. && ./build.sh toolbox_dexory_cloud && ctest --test-dir build/toolbox_dexory_cloud/Release --output-on-failure`
Expected: PASS, no rosx in the build log, configure no longer downloads CPM/rosx.
Run: `ldd build/toolbox_dexory_cloud/Release/toolbox_dexory_cloud/dexory-cloud-cli | grep -i qt`
Expected: empty (the no-Qt guard test also still enforces this).

- [ ] **Step 6: Commit**

```bash
git add -A PJ4/pj-official-plugins/toolbox_dexory_cloud
git commit -m "plugin: delete decode-in-plugin (rosx_introspection, RosDecodeDriver, parity tests) — parsing is host-delegated"
```

### Task 11: Live worker-level test (counts against the real corpus)

**Files:**
- Create: `tests/parser_ingest_live_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: Write the test** — model it on the deleted `ros_decode_live_test.cpp` (its skeleton: `liveUrl()` from `DEXORY_CLOUD_LIVE_URL`, `GTEST_SKIP()` when unset, FetchWorker wired with a fake toolbox host from `pj_plugins/testing/toolbox_test_store.hpp`). Differences:
  - also call `worker.setRuntimeHostProvider([&fake]{ return PJ::ToolboxRuntimeHostView{fake.toolboxRuntime()}; });` using `pj_ingest_test::FakeIngestHost` from Task 8's support header;
  - pull ALL 6 topics of `nissan_zala_50_zeg_1_0` and assert on the recorder instead of datastore rows:
    - total pushes across all handles == **33670**,
    - the binding for `/nissan/gps/duro/imu` exists with `type_name == "sensor_msgs/msg/Imu"`, `parser_encoding == "ros2msg"`, non-empty schema text, and its handle received exactly **14904** pushes,
    - `fake.released.size() == 1` after the pull completes (finalize ran);
  - keep a cancel leg: start a pull, `requestCancel()`, assert completion callback fires and `fake.released.size() == 1` (finalize on the cancel path too).
  (Ground-truth constants live in `tests/backend_connection_live_test.cpp` — reuse its topic-name constants if exported, else copy the strings.)

- [ ] **Step 2: CMake target** — copy the deleted `ros_decode_live_test` target block from git history (`git show HEAD~1:PJ4/pj-official-plugins/toolbox_dexory_cloud/CMakeLists.txt`) and adapt name/sources to `toolbox_dexory_cloud_parser_ingest_live_test` / `tests/parser_ingest_live_test.cpp` (+ `src/parser_ingest_driver.cpp` and whatever worker/backend sources the old block compiled in).

- [ ] **Step 3: Run hermetic (SKIP), then live**

Run: `./build.sh toolbox_dexory_cloud && ctest --test-dir build/toolbox_dexory_cloud/Release -R ParserIngestLive --output-on-failure`
Expected: SKIPPED.
Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server && make server-start`, then
`DEXORY_CLOUD_LIVE_URL=ws://localhost:8080 ctest --test-dir PJ4/pj-official-plugins/build/toolbox_dexory_cloud/Release -R ParserIngestLive --output-on-failure`
Expected: PASS — 33670 total / imu 14904.

- [ ] **Step 4: Commit**

```bash
git add PJ4/pj-official-plugins/toolbox_dexory_cloud/tests/parser_ingest_live_test.cpp \
        PJ4/pj-official-plugins/toolbox_dexory_cloud/CMakeLists.txt
git commit -m "plugin: live worker test — delegated ingest pushes ground-truth counts (33670 / imu 14904)"
```

### Task 12: Full gates + GUI verification on the real corpus

**Files:** none (verify), Modify: `<repo>/CLAUDE.md` (slice log)

- [ ] **Step 1: `make smoke`** at `<repo>` → `SMOKE PASS`.
- [ ] **Step 2: `make matrix`** at `<repo>` → `MATRIX PASS`.
- [ ] **Step 3: GUI end-to-end** — start the dev server (`make server-start`), run the VENDORED app (its host now has the slots):

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4 && ./run.sh --plugin-dir \
  /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/pj-official-plugins/build/toolbox_dexory_cloud/Release/bin
```

Open Dexory Cloud → connect `ws://localhost:8080` → fetch `nissan_zala_50_zeg_1_0` (all topics). Verify: all 6 topics produce curves; **expected user-visible change**: Imu/PoseStamped now flatten via parser_ros's SPECIALIZED handlers (single stamp double, covariance upper-triangle) instead of the old generic flatten — column names match what PJ4 shows when the same MCAP is opened from disk via `data_load_mcap`. Cross-check by File→Open of `/home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_1_0.mcap` and comparing the tree for the imu topic. This closes the documented Imu/PoseStamped parity gap.

- [ ] **Step 4: Update `<repo>/CLAUDE.md`** — append the Slice 16 entry to the slice log (host parser-ingest tail slots; SDK 0.6.1; plugin sheds rosx; parity gap closed; specialized-handler columns now authoritative).
- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: Slice 16 — toolbox parser delegation live; plugin ships no decoders"
```

---

## Phase 3 — 3D fixtures + the headline demo (tf + pointcloud draggable AND rendering)

### Task 13: genmcap — real-schema/real-payload overrides

**Files:**
- Modify: `<repo>/server/internal/genmcap/genmcap.go`
- Test: existing `<repo>/server/internal/genmcap/genmcap_test.go` (determinism must hold)

- [ ] **Step 1: Extend `TopicSpec`** (after `MessageCount`):

```go
	// SchemaData, when non-nil, is written verbatim as the MCAP schema record
	// data (real concatenated .msg text) instead of the synthetic
	// []byte(SchemaName) — lets fixtures carry parser_ros-decodable schemas.
	SchemaData []byte
	// PayloadFn, when non-nil, supplies each message body (idx = per-topic
	// message index) instead of the synthetic payload(). Must be deterministic.
	PayloadFn func(idx int) []byte
```

- [ ] **Step 2: Use them in `Write`** — in the schema loop replace `Data: []byte(t.SchemaName)` with:

```go
		schemaData := []byte(t.SchemaName) // non-empty, deterministic default
		if t.SchemaData != nil {
			schemaData = t.SchemaData
		}
```

(and pass `Data: schemaData`). In the message loop, replace `Data: payload(ti, c.idx, psize)` with:

```go
			body := payload(ti, c.idx, psize)
			if fn := spec.Topics[ti].PayloadFn; fn != nil {
				body = fn(c.idx)
			}
```

(and pass `Data: body`).

- [ ] **Step 3: Run the genmcap tests (byte-identity of existing fixtures must hold — overrides default to nil)**

Run: `cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/server && go test ./internal/genmcap/ -v`
Expected: all PASS, including `TestDeterministic`.

- [ ] **Step 4: Commit**

```bash
git add server/internal/genmcap/genmcap.go
git commit -m "genmcap: per-topic SchemaData/PayloadFn overrides for real-ROS fixtures (defaults byte-identical)"
```

### Task 14: `gen-3d-fixture`, seed, demo server, GUI acceptance

**Files:**
- Create: `<repo>/server/cmd/gen-3d-fixture/main.go`

- [ ] **Step 1: Write the generator** — one MCAP `synthetic_3d_0.mcap`, topics `/tf` (tf2_msgs/msg/TFMessage, 50 msgs), `/points` (sensor_msgs/msg/PointCloud2, 20 msgs), `/speed` (std_msgs/msg/Float32, 100 msgs):

```go
// Command gen-3d-fixture writes ONE synthetic MCAP whose payloads are REAL
// ROS2 CDR (tf2_msgs/TFMessage + sensor_msgs/PointCloud2 + std_msgs/Float32)
// with real concatenated .msg schema text, so the full pipeline — server →
// toolbox parser-ingest → parser_ros — classifies /tf as frame_transforms and
// /points as point_cloud (3D-scene draggable AND renderable). Deterministic.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"pj-cloud/server/internal/genmcap"
)

// cdr is a minimal XCDR1 little-endian writer; alignment is relative to the
// body start (after the 4-byte encapsulation header).
type cdr struct{ b []byte }

func newCdr() *cdr               { return &cdr{b: []byte{0x00, 0x01, 0x00, 0x00}} }
func (c *cdr) body() int         { return len(c.b) - 4 }
func (c *cdr) align(n int)       { for c.body()%n != 0 { c.b = append(c.b, 0) } }
func (c *cdr) u8(v uint8)        { c.b = append(c.b, v) }
func (c *cdr) u32(v uint32)      { c.align(4); c.b = binary.LittleEndian.AppendUint32(c.b, v) }
func (c *cdr) i32(v int32)       { c.u32(uint32(v)) }
func (c *cdr) f32(v float32)     { c.u32(math.Float32bits(v)) }
func (c *cdr) f64(v float64)     { c.align(8); c.b = binary.LittleEndian.AppendUint64(c.b, math.Float64bits(v)) }
func (c *cdr) str(s string)      { c.u32(uint32(len(s) + 1)); c.b = append(c.b, s...); c.b = append(c.b, 0) }
func (c *cdr) bytes(p []byte)    { c.u32(uint32(len(p))); c.b = append(c.b, p...) }
func (c *cdr) header(sec int32, nsec uint32, frame string) { c.i32(sec); c.u32(nsec); c.str(frame) }

func tfPayload(idx int) []byte {
	w := newCdr()
	w.u32(1) // transforms[]
	w.header(int32(idx), 0, "map")
	w.str("base_link")
	w.f64(float64(idx) * 0.1) // a robot driving in x
	w.f64(0)
	w.f64(0)
	w.f64(0) // identity quaternion
	w.f64(0)
	w.f64(0)
	w.f64(1)
	return w.b
}

// pointCloudPayload: 64 points on a spinning ring, fields x/y/z float32,
// point_step 12, height 1.
func pointCloudPayload(idx int) []byte {
	const n = 64
	data := make([]byte, 0, n*12)
	for i := 0; i < n; i++ {
		ang := float64(i)/n*2*math.Pi + float64(idx)*0.1
		for _, v := range []float64{2 * math.Cos(ang), 2 * math.Sin(ang), 0.2 * float64(idx%10)} {
			data = binary.LittleEndian.AppendUint32(data, math.Float32bits(float32(v)))
		}
	}
	w := newCdr()
	w.header(int32(idx), 0, "base_link")
	w.u32(1)         // height
	w.u32(n)         // width
	w.u32(3)         // fields[]
	for fi, name := range []string{"x", "y", "z"} {
		w.str(name)
		w.u32(uint32(fi * 4)) // offset
		w.u8(7)               // datatype FLOAT32
		w.u32(1)              // count
	}
	w.u8(0)          // is_bigendian
	w.u32(12)        // point_step
	w.u32(12 * n)    // row_step
	w.bytes(data)    // data[]
	w.u8(1)          // is_dense
	return w.b
}

func float32Payload(idx int) []byte {
	w := newCdr()
	w.f32(float32(idx) * 0.5)
	return w.b
}

const tfSchema = `geometry_msgs/TransformStamped[] transforms
================================================================================
MSG: geometry_msgs/TransformStamped
std_msgs/Header header
string child_frame_id
Transform transform
================================================================================
MSG: geometry_msgs/Transform
Vector3 translation
Quaternion rotation
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x 0
float64 y 0
float64 z 0
float64 w 1
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
`

const pointCloud2Schema = `std_msgs/Header header
uint32 height
uint32 width
PointField[] fields
bool is_bigendian
uint32 point_step
uint32 row_step
uint8[] data
bool is_dense
================================================================================
MSG: sensor_msgs/PointField
uint8 INT8=1
uint8 UINT8=2
uint8 INT16=3
uint8 UINT16=4
uint8 INT32=5
uint8 UINT32=6
uint8 FLOAT32=7
uint8 FLOAT64=8
string name
uint32 offset
uint8 datatype
uint32 count
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
`

const float32Schema = "float32 data\n"

func main() {
	out := flag.String("out", "", "output directory (required)")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "gen-3d-fixture: -out <dir> is required")
		os.Exit(2)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	spec := genmcap.FileSpec{
		Key:     "synthetic_3d_0.mcap",
		StartNs: 1_700_000_000_000_000_000,
		StepNs:  100_000_000, // 10 Hz
		Topics: []genmcap.TopicSpec{
			{Topic: "/tf", SchemaName: "tf2_msgs/msg/TFMessage", SchemaEnc: "ros2msg",
				MessageCount: 50, SchemaData: []byte(tfSchema), PayloadFn: tfPayload},
			{Topic: "/points", SchemaName: "sensor_msgs/msg/PointCloud2", SchemaEnc: "ros2msg",
				MessageCount: 20, SchemaData: []byte(pointCloud2Schema), PayloadFn: pointCloudPayload},
			{Topic: "/speed", SchemaName: "std_msgs/msg/Float32", SchemaEnc: "ros2msg",
				MessageCount: 100, SchemaData: []byte(float32Schema), PayloadFn: float32Payload},
		},
	}
	path := filepath.Join(*out, spec.Key)
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := genmcap.Write(f, spec); err != nil {
		_ = f.Close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = f.Close()
	fmt.Printf("wrote %s (170 messages)\n", path)
}
```

- [ ] **Step 2: Generate + sanity-check with the repo's own tool**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/server
go run ./cmd/gen-3d-fixture -out /tmp/pj-3d
go run ./cmd/mcaptopics /tmp/pj-3d/synthetic_3d_0.mcap
```

Expected: lists `/tf` (50), `/points` (20), `/speed` (100). **Decode sanity before going further:** run the Task 5 host test feeding `tfPayload(0)`'s byte layout — or simpler, File→Open `/tmp/pj-3d/synthetic_3d_0.mcap` in the VENDORED PJ4 app (data_load_mcap → parser_ros): `/tf` must appear as a draggable frame_transforms object topic and `/points` as a pointcloud. If parser_ros rejects a payload, fix the CDR writer here until file-open works — file-open is the reference decoder for these fixtures.

- [ ] **Step 3: Seed a dedicated bucket + demo server (do NOT touch `recordings` — smoke ground truth is pinned to 8 files)**

Create bucket `recordings-3d` and upload (mirror the `docker run … mc` idiom from `scripts/smoke.sh:185-187`, network `minio_default`, alias `local`):

```bash
docker run --rm --network minio_default -v /tmp/pj-3d:/data --entrypoint sh \
  minio/mc -c "mc alias set local http://minio:9000 admin password123 && \
               mc mb -p local/recordings-3d && mc cp /data/synthetic_3d_0.mcap local/recordings-3d/"
```

Start a demo server on **:8083** against that bucket with a fresh DB: copy the exact server start invocation from `scripts/smoke.sh` (search for `pj-cloud-server` after the build at line 202), changing: listen port → `:8083`, bucket → `recordings-3d`, db → `/tmp/pj-cloud-3d.db`.

- [ ] **Step 4: THE HEADLINE ACCEPTANCE (GUI, manual)**

Run the vendored app with the vendored plugin (Task 12 Step 3 command). In Dexory Cloud connect to `ws://localhost:8083`, fetch `synthetic_3d_0` with all topics. Verify and record in the slice log:
1. `/speed` plots as a normal curve.
2. `/tf` and `/points` appear in the topic list as **object topics** (3D-draggable — `CurveListPanel` marks them).
3. Dragging `/points` into a 3D view renders the spinning ring; dragging `/tf` makes the `map→base_link` transform available (the TransformService picks it up for the dataset).
4. Repeat Fetch (cache hit) re-ingests without error; cancel mid-fetch leaves the app stable.

- [ ] **Step 5: Commit**

```bash
git add server/cmd/gen-3d-fixture
git commit -m "fixtures: gen-3d-fixture — real-CDR tf + pointcloud MCAP for the 3D-scene acceptance demo"
```

---

## Risks / decisions already made (do not re-litigate during execution)

1. **API shape**: 2 tail slots returning the standard `PJ_data_source_runtime_host_t` — chosen over a 4-slot bespoke binding API to reuse the SDK view + trampolines wholesale. Slot keying by `uint32_t` (== dataset id) avoids cross-header C-type dependencies.
2. **Registrar threading**: marshalled to the GUI thread in MainWindow (shared_ptr-wrapped move-only payload); queued registration provably precedes the queued catalog rebuild.
3. **Payload lifetime**: one owned copy per message in `ParserIngestDriver::decode`; the shared_ptr is the anchor — satisfies pushMessage's idempotent/thread-safe fetcher contract and keeps ObjectStore lazy closures valid for the dataset's life.
4. **SDK version**: `0.6.1` created from the EDITED in-tree `PJ4/plotjuggler_sdk` (bypasses `extern/plotjuggler_core`; `bump_core_version.py` deliberately not used — recorded deviation).
5. **User-visible change**: Imu/PoseStamped columns switch from the generic flatten to parser_ros's specialized handlers — this *closes* the documented parity gap; file-open and cloud-fetch now produce identical trees.
6. **No plugin-side fallback decoder**: on hosts without the slots, topics report "host parser ingest unavailable" — the connector ships no decoders, per the original unified-plan rule (the Slice-3/5 amendment that allowed rosx is hereby retired).
7. **Type-name form**: wire-verbatim (`tf2_msgs/msg/TFMessage`); Task 5 Step 4 empirically pins what parser_ros accepts BEFORE the plugin depends on it.

## Self-review checklist (run after implementation, before declaring the slice done)

- [ ] All 9 verify gates: app build, `ctest -R "ToolboxRuntimeHost|DataSourceRuntimeHost|SessionManager|CatalogModel"`, real-ros test (gated), plugin ctest hermetic, plugin ctest live, `make smoke`, `make matrix`, GUI corpus check (Task 12), GUI 3D acceptance (Task 14).
- [ ] `grep -rn "rosx\|ros_decode" PJ4/pj-official-plugins/toolbox_dexory_cloud` → no functional hits.
- [ ] Sentinel asserts pin 24/32; plugin built against SDK 0.6.1; old-host path returns clean per-topic errors (hermetic test covers it).
- [ ] CLAUDE.md slice log updated.
