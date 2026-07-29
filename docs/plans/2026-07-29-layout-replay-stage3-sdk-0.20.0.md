# SDK 0.20.0 — Descriptor-Replay v1 ABI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the v3.5 descriptor-replay ABI (spec: `docs/canonical-layout-replay.md` §8 in the mcap_server repo) as PlotJuggler SDK **0.20.0** — the plugin extension `pj.descriptor_replay.v1`, the host service `pj.materialized_source.v1`, and the generic `DatasetIngestHostView` lifecycle facade — with **zero new vtable slots**, and open the PR against `PlotJuggler/plotjuggler_sdk` main.

**Architecture:** One new family-neutral installed C header (`pj_base/include/pj_base/descriptor_replay_protocol.h`) carries all new ABI structs (struct_size-versioned, zero-init growth contract, FORCE_INT32 enums, fail-closed unknowns). One new C++ SDK header (`pj_base/include/pj_base/sdk/descriptor_replay.hpp`) carries the typed views + RAII job + service trait. `DatasetIngestHostView` widens the existing facade over the **same** `PJ_data_source_runtime_host_t` fat pointer that `create_parser_ingest` already returns — no C ABI change at all for that piece. All new structs get ABI-layout sentinel pins (precedent-setting: extension structs were previously unpinned).

**Tech Stack:** C++20, Conan 2 + CMake, gtest/ctest, `-Wall -Wextra -Wshadow -Wold-style-cast -Wcast-qual -Wconversion -Werror`, clang-format v22.1.0 (Google, 2-space, 120 col).

**Working directory for ALL tasks:** `/home/davide/ws_plotjuggler/plotjuggler_sdk-cloud/.worktrees/feat-descriptor-replay-v1` (branch `feat/descriptor-replay-v1`, based on `upstream/main` = `dcdbcee`, v0.19.0). The shell cwd resets between Bash calls — always use absolute paths or `git -C`.

**Design authority:** the fifth-consult amended C declarations (recorded below task-by-task) are FINAL — do not redesign field order, names, or semantics. The two Codex consult records (sessions `019fa9d6-34dd-7232-9f41-a5bd71a32554`, `019faca5-155d-73a0-8c39-27f02544a3dc`) and spec §8 are the source.

**Build/test commands** (from the worktree root):

```bash
./build.sh --debug          # Debug+ASAN into build/debug_asan
./test.sh                   # ctest in all discovered build dirs
# single test binary after a build:
ctest --test-dir build/debug_asan --output-on-failure -R <test_name>
```

First build in the worktree takes minutes (conan install + full build); later builds are incremental.

---

## File structure

| File | Action | Responsibility |
|---|---|---|
| `pj_base/include/pj_base/descriptor_replay_protocol.h` | Create | ALL new C ABI: extension + service structs, enums, macros, growth/lifetime/threading contract text |
| `pj_base/include/pj_base/sdk/descriptor_replay.hpp` | Create | C++ layer: `DescriptorReplayProviderView`, `JoinableJob`, `MaterializedSourceHostView`, `PJ::sdk::MaterializedSourceHostService` trait |
| `pj_base/include/pj_base/sdk/data_source_host_views.hpp` | Modify | `DatasetIngestHostView` beside `ParserIngestHostView` (~line 357) + `DataSourceRuntimeHostView::datasetIngest()` accessor |
| `pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp` | Modify | `ToolboxRuntimeHostView::createDatasetIngest()/releaseDatasetIngest()` declarations |
| `pj_base/src/toolbox_plugin_base.cpp` | Modify | their implementations (mirror `createParserIngest` at :8-21) |
| `pj_base/tests/abi_layout_sentinels_test.cpp` | Modify | pin offsets/sizes of every new struct (nothing existing changes) |
| `pj_base/tests/descriptor_replay_extension_test.cpp` | Create | extension round-trip, flags fail-closed, growth contract, JoinableJob, null-extension |
| `pj_base/tests/materialized_source_service_test.cpp` | Create | service trait + registry wiring + adopt exactly-once + reject path |
| `pj_base/tests/dataset_ingest_view_test.cpp` | Create | facade forwarding, cooperative stop, parser access, older-host slot-missing |
| `pj_base/CMakeLists.txt` | Modify | add the three test files to the `PJ_BASE_TESTS` list (~:70-118) |
| `CHANGELOG.md` | Modify | `## [0.20.0]` entry (top, after header) |
| `conanfile.py` | Modify | `version = "0.20.0"` (:33) + docstring example tag (:9) |
| `CMakeLists.txt` | Modify | `set(PJ_PACKAGE_VERSION "0.20.0")` (:131) |
| `recipe.yaml` | Modify | `context: version: "0.20.0"` (:3-4) |
| `pj_plugins/docs/toolbox-guide.md` | Modify | new "Descriptor replay & materialized-source adoption" section |
| `pj_plugins/docs/ARCHITECTURE.md` | Modify | one-line inventory additions (extension id, service id, new headers) |

Headers install wholesale via `install(DIRECTORY include/ ...)` (`pj_base/CMakeLists.txt:66`) — header-only additions need **no** CMake edit; only the tests do.

---

### Task 1: The C ABI header + layout sentinels

**Files:**
- Create: `pj_base/include/pj_base/descriptor_replay_protocol.h`
- Modify: `pj_base/tests/abi_layout_sentinels_test.cpp` (append a new section; do NOT touch existing asserts)

Before writing, read `pj_base/include/pj_base/plugin_data_api.h:1-120` and `pj_base/include/pj_base/data_source_protocol.h:80-110` to mirror the file's exact conventions: header guard style (`#pragma once` vs guards), `extern "C"` usage, the FORCE_INT32 comment idiom (canonical rationale at `data_source_protocol.h:83-85`), and the `)PJ_NOEXCEPT;` spelling (no space before `PJ_NOEXCEPT` — clang-format artifact; copy whatever the surrounding files do and let clang-format settle it).

- [ ] **Step 1: Write the failing test** — append to `pj_base/tests/abi_layout_sentinels_test.cpp` (new section at the end of the file, before any final namespace close, following the file's exact `static_assert(offsetof(...))` idiom used at :148-208). Add `#include "pj_base/descriptor_replay_protocol.h"` next to the existing protocol includes at the top.

```cpp
// ---- descriptor_replay_protocol.h (new in 0.20.0) --------------------------
// First ABI pins for extension/service structs. Same maintenance rule as
// above: tails may grow (update sizeof deliberately); existing offsets never
// move.

static_assert(sizeof(PJ_descriptor_trust_t) == 4);
static_assert(sizeof(PJ_descriptor_replay_outcome_t) == 4);
static_assert(sizeof(PJ_descriptor_replay_start_flags_t) == 8);

static_assert(offsetof(PJ_descriptor_query_result_v1_t, struct_size) == 0);
static_assert(offsetof(PJ_descriptor_query_result_v1_t, reserved0) == 4);
static_assert(offsetof(PJ_descriptor_query_result_v1_t, trust) == 8);
static_assert(offsetof(PJ_descriptor_query_result_v1_t, is_materialized) == 12);
static_assert(offsetof(PJ_descriptor_query_result_v1_t, source_identity) == 16);
static_assert(offsetof(PJ_descriptor_query_result_v1_t, local_path_utf8) == 32);
static_assert(offsetof(PJ_descriptor_query_result_v1_t, message) == 48);
static_assert(offsetof(PJ_descriptor_query_result_v1_t, estimated_bytes) == 64);
static_assert(sizeof(PJ_descriptor_query_result_v1_t) == 72);

static_assert(offsetof(PJ_descriptor_replay_start_request_v1_t, struct_size) == 0);
static_assert(offsetof(PJ_descriptor_replay_start_request_v1_t, reserved0) == 4);
static_assert(offsetof(PJ_descriptor_replay_start_request_v1_t, descriptor_json) == 8);
static_assert(offsetof(PJ_descriptor_replay_start_request_v1_t, flags) == 24);
static_assert(offsetof(PJ_descriptor_replay_start_request_v1_t, max_transfer_bytes) == 32);
static_assert(sizeof(PJ_descriptor_replay_start_request_v1_t) == 40);

static_assert(offsetof(PJ_descriptor_replay_callbacks_v1_t, struct_size) == 0);
static_assert(offsetof(PJ_descriptor_replay_callbacks_v1_t, reserved0) == 4);
static_assert(offsetof(PJ_descriptor_replay_callbacks_v1_t, on_dataset) == 8);
static_assert(offsetof(PJ_descriptor_replay_callbacks_v1_t, on_terminal) == 16);
static_assert(sizeof(PJ_descriptor_replay_callbacks_v1_t) == 24);

static_assert(offsetof(PJ_joinable_job_vtable_t, struct_size) == 0);
static_assert(offsetof(PJ_joinable_job_vtable_t, reserved0) == 4);
static_assert(offsetof(PJ_joinable_job_vtable_t, cancel) == 8);
static_assert(offsetof(PJ_joinable_job_vtable_t, join) == 16);
static_assert(offsetof(PJ_joinable_job_vtable_t, destroy) == 24);
static_assert(sizeof(PJ_joinable_job_vtable_t) == 32);
static_assert(sizeof(PJ_joinable_job_t) == 16);

static_assert(offsetof(PJ_descriptor_replay_provider_v1_t, struct_size) == 0);
static_assert(offsetof(PJ_descriptor_replay_provider_v1_t, reserved0) == 4);
static_assert(offsetof(PJ_descriptor_replay_provider_v1_t, query_descriptor) == 8);
static_assert(offsetof(PJ_descriptor_replay_provider_v1_t, start_replay) == 16);
static_assert(sizeof(PJ_descriptor_replay_provider_v1_t) == 24);

static_assert(offsetof(PJ_materialized_source_adopt_request_v1_t, struct_size) == 0);
static_assert(offsetof(PJ_materialized_source_adopt_request_v1_t, dataset) == 4);
static_assert(offsetof(PJ_materialized_source_adopt_request_v1_t, source_identity) == 8);
static_assert(offsetof(PJ_materialized_source_adopt_request_v1_t, local_path_utf8) == 24);
static_assert(offsetof(PJ_materialized_source_adopt_request_v1_t, loader_plugin_id) == 40);
static_assert(offsetof(PJ_materialized_source_adopt_request_v1_t, loader_config_json) == 56);
static_assert(offsetof(PJ_materialized_source_adopt_request_v1_t, descriptor_json) == 72);
static_assert(sizeof(PJ_materialized_source_adopt_request_v1_t) == 88);

static_assert(offsetof(PJ_materialized_source_host_vtable_t, protocol_version) == 0);
static_assert(offsetof(PJ_materialized_source_host_vtable_t, struct_size) == 4);
static_assert(offsetof(PJ_materialized_source_host_vtable_t, adopt) == 8);
static_assert(sizeof(PJ_materialized_source_host_vtable_t) == 16);
static_assert(sizeof(PJ_materialized_source_host_t) == 16);
```

- [ ] **Step 2: Run to verify it fails** — `cmake --build build/debug_asan --target abi_layout_sentinels_test 2>&1 | tail -20` (if `build/debug_asan` does not exist yet, run `./build.sh --debug` once first — it will fail at this file). Expected: FAIL, `descriptor_replay_protocol.h: No such file or directory`.

- [ ] **Step 3: Write the header** — `pj_base/include/pj_base/descriptor_replay_protocol.h`. This is the FINAL ABI (fifth-consult amended); reproduce exactly, adapting only the boilerplate (guard, extern-C, FORCE_INT32 comment idiom) to match `plugin_data_api.h`:

```c
#pragma once

/**
 * Descriptor replay v1 — a FAMILY-NEUTRAL plugin extension + host service pair
 * for replaying a persisted "source descriptor" (an opaque, provider-defined
 * JSON document, typically stored in a layout file) back into a loaded
 * dataset, and for adopting the provider's materialized artifact as a stock
 * file-backed source.
 *
 *  - Plugin side: "pj.descriptor_replay.v1" (PJ_descriptor_replay_provider_v1_t),
 *    returned from ANY plugin family's get_plugin_extension hook (see
 *    PJ_data_source_vtable_t::get_plugin_extension for the hook contract).
 *    plugin_ctx is the originating plugin-family instance context — the same
 *    ctx get_plugin_extension was called with, never the extension-table
 *    pointer.
 *  - Host side: "pj.materialized_source.v1" (PJ_materialized_source_host_t),
 *    an optional service acquired from the bind() registry. Absence means the
 *    host has no adoption support. The host registers it PER PLUGIN INSTANCE
 *    and derives the provider's manifest id from that binding itself — the
 *    plugin never supplies its own identity, so it cannot be spoofed.
 *
 * Growth contract (every struct below, both directions): the struct's owner
 * zero-initializes its complete allocation, then sets struct_size; the other
 * side reads/writes only fields wholly covered by that size. Field-appends to
 * v1 structs are therefore absent-as-zero on older peers. Semantic changes
 * (as opposed to additions) get a side-by-side ".v2" id instead.
 *
 * Encoding, lifetime and threading rules:
 *  - All text and JSON is UTF-8. Paths are absolute filesystem paths encoded
 *    as UTF-8; on Windows the host converts them to native wide paths.
 *  - query_descriptor inputs live for the call; views in out_result live
 *    until the NEXT query_descriptor call on the same plugin instance — the
 *    caller copies immediately.
 *  - start_replay copies the request contents and the callback function
 *    pointers before returning; no job callback may occur before it returns
 *    true.
 *  - Job callbacks are serialized but may arrive off the main thread; the
 *    host marshals. on_dataset is zero-or-one and precedes the dataset's
 *    progress_start, first publication, adoption, and on_terminal.
 *    on_terminal is exactly-once and last.
 *  - join returns only after the terminal callback has returned. destroy
 *    cancels and joins when necessary. Never call join/destroy from a job
 *    callback. The plugin instance and its DSO must stay alive until every
 *    job obtained from it has been destroyed.
 */

#include <stdint.h>

#include "pj_base/plugin_data_api.h"

#define PJ_DESCRIPTOR_REPLAY_EXTENSION_V1 "pj.descriptor_replay.v1"
#define PJ_MATERIALIZED_SOURCE_HOST_SERVICE_V1 "pj.materialized_source.v1"

/** Unknown/future trust values fail closed: treat as REFUSED. */
typedef enum PJ_descriptor_trust_t {
  PJ_DESCRIPTOR_TRUST_REFUSED = 0,
  PJ_DESCRIPTOR_TRUST_NEEDS_CONFIRMATION = 1,
  PJ_DESCRIPTOR_TRUST_TRUSTED = 2,
  /* Forces a stable 4-byte width across compilers. Not a real state. */
  PJ_DESCRIPTOR_TRUST_FORCE_INT32 = 0x7FFFFFFF
} PJ_descriptor_trust_t;

/** Unknown/future outcome values fail closed: treat as FAILED. */
typedef enum PJ_descriptor_replay_outcome_t {
  PJ_DESCRIPTOR_REPLAY_FAILED = 0,
  PJ_DESCRIPTOR_REPLAY_CANCELLED = 1,
  /* Replay produced a usable eager dataset but no adoptable artifact. */
  PJ_DESCRIPTOR_REPLAY_SUCCEEDED_UNMATERIALIZED = 2,
  PJ_DESCRIPTOR_REPLAY_SUCCEEDED_MATERIALIZED = 3,
  /* Forces a stable 4-byte width across compilers. Not a real state. */
  PJ_DESCRIPTOR_REPLAY_OUTCOME_FORCE_INT32 = 0x7FFFFFFF
} PJ_descriptor_replay_outcome_t;

typedef uint64_t PJ_descriptor_replay_start_flags_t;

/* V1 defines no optional modes. Added flag bits require an SDK MINOR. */
#define PJ_DESCRIPTOR_REPLAY_START_FLAG_NONE UINT64_C(0)
#define PJ_DESCRIPTOR_REPLAY_START_FLAGS_V1_MASK UINT64_C(0)

/*
 * Caller zero-initializes its complete available capacity, then sets
 * struct_size. The provider writes only fields wholly covered by that size.
 *
 * On success source_identity and local_path_utf8 are always present, whether
 * is_materialized is zero or one. Returned views remain valid until the next
 * query_descriptor call on the same plugin instance.
 */
typedef struct PJ_descriptor_query_result_v1_t {
  uint32_t struct_size;
  uint32_t reserved0; /* must be zero */

  PJ_descriptor_trust_t trust;
  uint32_t is_materialized; /* exactly 0 or 1 */

  PJ_string_view_t source_identity;
  PJ_string_view_t local_path_utf8;
  PJ_string_view_t message; /* optional refusal/confirmation explanation */

  /*
   * Best estimate of provider payload bytes that replay would transfer,
   * measured consistently with max_transfer_bytes. Zero means unknown.
   * Derived from the descriptor or local metadata — never the network.
   */
  uint64_t estimated_bytes;
} PJ_descriptor_query_result_v1_t;

/*
 * Caller-owned and caller-sized. The caller zero-initializes the complete
 * allocation, then sets struct_size.
 *
 * descriptor_json is valid for the call; start_replay copies it before
 * returning successfully. Runtime options live HERE, never in the descriptor
 * (the descriptor is the persisted identity artifact).
 */
typedef struct PJ_descriptor_replay_start_request_v1_t {
  uint32_t struct_size;
  uint32_t reserved0; /* must be zero */

  PJ_string_view_t descriptor_json;
  PJ_descriptor_replay_start_flags_t flags;

  /*
   * Maximum provider payload bytes that may be transferred by this replay.
   * Zero means no additional caller-imposed ceiling; provider-configured
   * hard resource limits still apply.
   */
  uint64_t max_transfer_bytes;
} PJ_descriptor_replay_start_request_v1_t;

typedef struct PJ_joinable_job_vtable_t {
  uint32_t struct_size;
  uint32_t reserved0; /* must be zero */

  /** [thread-safe] Non-blocking, idempotent, best-effort cancellation. */
  void (*cancel)(void* ctx) PJ_NOEXCEPT;

  /**
   * [blocking, not-callback-thread] Idempotent. Returns after all callbacks,
   * including on_terminal, have returned.
   */
  void (*join)(void* ctx) PJ_NOEXCEPT;

  /**
   * [blocking, not-callback-thread] Invalidates ctx. Cancels and joins first
   * when necessary.
   */
  void (*destroy)(void* ctx) PJ_NOEXCEPT;
} PJ_joinable_job_vtable_t;

/* ABI-FROZEN: fat pointer layout permanent. */
typedef struct PJ_joinable_job_t {
  void* ctx;
  const PJ_joinable_job_vtable_t* vtable;
} PJ_joinable_job_t;

typedef struct PJ_descriptor_replay_callbacks_v1_t {
  uint32_t struct_size;
  uint32_t reserved0; /* must be zero */

  /**
   * [job-callback-thread, serialized] Zero or one call. Must precede the
   * dataset's progress_start, first publication, adoption, and on_terminal.
   */
  void (*on_dataset)(void* callback_ctx, PJ_data_source_handle_t dataset) PJ_NOEXCEPT;

  /**
   * [job-callback-thread, serialized] Exactly once and last. message is valid
   * only for the duration of the callback.
   */
  void (*on_terminal)(void* callback_ctx, PJ_descriptor_replay_outcome_t outcome,
                      PJ_string_view_t message) PJ_NOEXCEPT;
} PJ_descriptor_replay_callbacks_v1_t;

/*
 * Returned through any plugin family's get_plugin_extension() for
 * PJ_DESCRIPTOR_REPLAY_EXTENSION_V1. The plugin owns this struct; it must
 * stay valid for the plugin instance lifetime. plugin_ctx is the same
 * originating plugin-instance context passed to get_plugin_extension(); it is
 * never the extension-table pointer.
 */
typedef struct PJ_descriptor_replay_provider_v1_t {
  uint32_t struct_size;
  uint32_t reserved0; /* must be zero */

  /**
   * [main-thread, strictly bounded] No network, credential resolution,
   * blocking lock acquisition, or full-file scan. False means a
   * malformed/unsupported descriptor; out_result is then unspecified and
   * out_error carries the reason.
   */
  bool (*query_descriptor)(void* plugin_ctx, PJ_string_view_t descriptor_json,
                           PJ_descriptor_query_result_v1_t* out_result, PJ_error_t* out_error) PJ_NOEXCEPT;

  /**
   * [main-thread] Copies the request and callback pointers before returning.
   * Unknown flag bits are rejected synchronously: false, out_error populated,
   * no callbacks, and out_job untouched. On true: out_job is valid and
   * on_terminal will occur exactly once.
   */
  bool (*start_replay)(void* plugin_ctx, const PJ_descriptor_replay_start_request_v1_t* request,
                       const PJ_descriptor_replay_callbacks_v1_t* callbacks, void* callback_ctx,
                       PJ_joinable_job_t* out_job, PJ_error_t* out_error) PJ_NOEXCEPT;
} PJ_descriptor_replay_provider_v1_t;

/*
 * Adoption request. All views are valid for the duration of the adopt() call;
 * the host copies before returning. loader_plugin_id + loader_config_json are
 * provider-supplied because a non-MCAP artifact needs its own companion
 * loader; dataset is the provisional dataset announced via on_dataset.
 */
typedef struct PJ_materialized_source_adopt_request_v1_t {
  uint32_t struct_size;
  PJ_data_source_handle_t dataset;

  PJ_string_view_t source_identity;
  PJ_string_view_t local_path_utf8;
  PJ_string_view_t loader_plugin_id;
  PJ_string_view_t loader_config_json;
  PJ_string_view_t descriptor_json;
} PJ_materialized_source_adopt_request_v1_t;

/**
 * [host-callback-thread] Exactly once after a successfully accepted adoption.
 * message is valid only for the duration of the callback.
 */
typedef void (*PJ_materialized_source_adopt_result_fn)(void* callback_ctx, bool ok,
                                                       PJ_string_view_t message) PJ_NOEXCEPT;

/**
 * Host adoption service ("pj.materialized_source.v1", protocol_version 1).
 * Bound per plugin instance — the service ctx identifies the provider.
 */
typedef struct PJ_materialized_source_host_vtable_t {
  uint32_t protocol_version; /* = 1 */
  uint32_t struct_size;

  /**
   * [thread-safe, asynchronous] Copies the complete request before returning
   * and marshals the stock loader transaction to the host thread.
   *
   * false: request rejected synchronously; result_cb will not run.
   * true: result_cb runs exactly once.
   *
   * On success the host has transactionally replaced the named dataset,
   * captured the loader's accepted configuration, and attached
   * {provider manifest id, source_identity, descriptor_json} before result_cb.
   */
  bool (*adopt)(void* ctx, const PJ_materialized_source_adopt_request_v1_t* request,
                PJ_materialized_source_adopt_result_fn result_cb, void* callback_ctx,
                PJ_error_t* out_error) PJ_NOEXCEPT;
} PJ_materialized_source_host_vtable_t;

/* ABI-FROZEN: fat pointer layout permanent. */
typedef struct PJ_materialized_source_host_t {
  void* ctx;
  const PJ_materialized_source_host_vtable_t* vtable;
} PJ_materialized_source_host_t;
```

- [ ] **Step 4: Run to verify it passes** — rebuild `abi_layout_sentinels_test` and run it: `ctest --test-dir build/debug_asan --output-on-failure -R abi_layout_sentinels_test`. Expected: PASS. If an `offsetof` assert fails, the HEADER is wrong (field order) — fix the header, never the pinned number.

- [ ] **Step 5: Commit**

```bash
git -C /home/davide/ws_plotjuggler/plotjuggler_sdk-cloud/.worktrees/feat-descriptor-replay-v1 add pj_base/include/pj_base/descriptor_replay_protocol.h pj_base/tests/abi_layout_sentinels_test.cpp
git -C /home/davide/ws_plotjuggler/plotjuggler_sdk-cloud/.worktrees/feat-descriptor-replay-v1 commit -m "feat(abi): descriptor-replay v1 protocol header — extension + adoption service, zero vtable growth"
```

---

### Task 2: C++ provider view + RAII job (`sdk/descriptor_replay.hpp`, extension half)

**Files:**
- Create: `pj_base/include/pj_base/sdk/descriptor_replay.hpp` (header-only)
- Create: `pj_base/tests/descriptor_replay_extension_test.cpp`
- Modify: `pj_base/CMakeLists.txt` (add the test to `PJ_BASE_TESTS`)

Before writing, read: `pj_base/tests/notify_available_topics_test.cpp:130-217` (the ExtensionSource pattern — copy its shape), `pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp:82-165` (trait placement + `pluginExtension` virtual at :139), `pj_base/include/pj_base/sdk/plugin_data_api.hpp` (the `toAbiString`/`fillError`/`errorToString` helpers and how views construct `Expected` errors — copy the exact error-construction idiom), and `pj_base/include/pj_base/expected.hpp`.

**Design (final):**
- `namespace PJ`: `enum class DescriptorTrust`, `enum class ReplayOutcome`, `struct DescriptorQueryResult`, `struct ReplayStartRequest`, `class JoinableJob`, `class DescriptorReplayProviderView`. Trait goes in `PJ::sdk` (Task 3).
- All C-enum→C++-enum mapping is **fail-closed**: unknown trust → `Refused`, unknown outcome → `Failed`.
- Extension capacity checks use the explicit `offsetof + sizeof` idiom from `data_source_handle.hpp:183-207` (NOT `PJ_HAS_TAIL_SLOT`).
- `JoinableJob` owns both the C fat pointer and the heap `CallbackContext` (the two `std::function`s). Destructor order: `vtable->destroy(ctx)` FIRST (destroy cancels+joins, so after it returns no callback can be running), THEN free the context. Move-only.
- Thunks are `noexcept` and swallow exceptions (mirror `toolbox_trampolines.cpp:104-112`).

- [ ] **Step 1: Write the failing test** — `pj_base/tests/descriptor_replay_extension_test.cpp`:

```cpp
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "pj_base/descriptor_replay_protocol.h"
#include "pj_base/sdk/descriptor_replay.hpp"
#include "pj_base/sdk/toolbox_plugin_base.hpp"

namespace {

// A toolbox plugin advertising pj.descriptor_replay.v1, modeled on
// notify_available_topics_test.cpp's ExtensionSource.
class FakeReplayToolbox : public PJ::ToolboxPluginBase {
 public:
  const void* pluginExtension(std::string_view id) override {
    if (id == PJ_DESCRIPTOR_REPLAY_EXTENSION_V1) {
      return &ext_;
    }
    return nullptr;
  }

  // ToolboxPluginBase pure-virtuals: minimal no-op bodies (copy the set the
  // mock toolbox at pj_plugins/examples/mock_toolbox.cpp implements).

 private:
  struct JobState {
    std::thread worker;
    std::atomic<bool> cancelled{false};
  };

  static bool queryThunk(void* /*plugin_ctx*/, PJ_string_view_t descriptor_json,
                         PJ_descriptor_query_result_v1_t* out, PJ_error_t* /*err*/) noexcept {
    // Growth contract: write only fields wholly covered by out->struct_size.
    auto covered = [out](size_t off, size_t sz) { return out->struct_size >= off + sz; };
    static const std::string identity = "fake:v1:sha256/128:00";
    static const std::string path = "/tmp/fake.mcap";
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, trust), sizeof(out->trust))) {
      out->trust = descriptor_json.size > 0 ? PJ_DESCRIPTOR_TRUST_TRUSTED : PJ_DESCRIPTOR_TRUST_REFUSED;
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, is_materialized), sizeof(out->is_materialized))) {
      out->is_materialized = 0;
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, source_identity), sizeof(out->source_identity))) {
      out->source_identity = PJ_string_view_t{identity.data(), identity.size()};
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, local_path_utf8), sizeof(out->local_path_utf8))) {
      out->local_path_utf8 = PJ_string_view_t{path.data(), path.size()};
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, estimated_bytes), sizeof(out->estimated_bytes))) {
      out->estimated_bytes = 12345;
    }
    return true;
  }

  static bool startThunk(void* /*plugin_ctx*/, const PJ_descriptor_replay_start_request_v1_t* request,
                         const PJ_descriptor_replay_callbacks_v1_t* callbacks, void* callback_ctx,
                         PJ_joinable_job_t* out_job, PJ_error_t* err) noexcept {
    if ((request->flags & ~PJ_DESCRIPTOR_REPLAY_START_FLAGS_V1_MASK) != 0) {
      if (err != nullptr) {
        err->code = 1;  // populate via the header's PJ_error_t fields; message optional
      }
      return false;  // fail closed: no callbacks, out_job untouched
    }
    auto on_dataset = callbacks->on_dataset;
    auto on_terminal = callbacks->on_terminal;
    auto* state = new JobState();
    state->worker = std::thread([state, on_dataset, on_terminal, callback_ctx] {
      if (on_dataset != nullptr) {
        on_dataset(callback_ctx, PJ_data_source_handle_t{7});
      }
      const char* msg = state->cancelled.load() ? "cancelled" : "done";
      auto outcome = state->cancelled.load() ? PJ_DESCRIPTOR_REPLAY_CANCELLED
                                             : PJ_DESCRIPTOR_REPLAY_SUCCEEDED_UNMATERIALIZED;
      on_terminal(callback_ctx, outcome, PJ_string_view_t{msg, std::char_traits<char>::length(msg)});
    });
    out_job->ctx = state;
    out_job->vtable = &kJobVtable;
    return true;
  }

  static void jobCancel(void* ctx) noexcept { static_cast<JobState*>(ctx)->cancelled.store(true); }
  static void jobJoin(void* ctx) noexcept {
    auto* state = static_cast<JobState*>(ctx);
    if (state->worker.joinable()) {
      state->worker.join();
    }
  }
  static void jobDestroy(void* ctx) noexcept {
    jobCancel(ctx);
    jobJoin(ctx);
    delete static_cast<JobState*>(ctx);
  }

  static constexpr PJ_joinable_job_vtable_t kJobVtable{sizeof(PJ_joinable_job_vtable_t), 0, &FakeReplayToolbox::jobCancel,
                                                       &FakeReplayToolbox::jobJoin, &FakeReplayToolbox::jobDestroy};

  PJ_descriptor_replay_provider_v1_t ext_{sizeof(PJ_descriptor_replay_provider_v1_t), 0,
                                          &FakeReplayToolbox::queryThunk, &FakeReplayToolbox::startThunk};
};

TEST(DescriptorReplayExtension, QueryRoundTripsThroughView) {
  FakeReplayToolbox plugin;
  PJ::DescriptorReplayProviderView view(plugin.pluginExtension(PJ_DESCRIPTOR_REPLAY_EXTENSION_V1), &plugin);
  ASSERT_TRUE(view.valid());
  auto result = view.queryDescriptor(R"({"v":1})");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->trust, PJ::DescriptorTrust::Trusted);
  EXPECT_FALSE(result->is_materialized);
  EXPECT_EQ(result->source_identity, "fake:v1:sha256/128:00");
  EXPECT_EQ(result->local_path_utf8, "/tmp/fake.mcap");
  EXPECT_EQ(result->estimated_bytes, 12345u);
}

TEST(DescriptorReplayExtension, UnknownFlagBitsFailClosed) {
  FakeReplayToolbox plugin;
  PJ::DescriptorReplayProviderView view(plugin.pluginExtension(PJ_DESCRIPTOR_REPLAY_EXTENSION_V1), &plugin);
  PJ::ReplayStartRequest request;
  request.descriptor_json = R"({"v":1})";
  request.flags = UINT64_C(1) << 63;  // not in the v1 mask
  bool dataset_seen = false;
  bool terminal_seen = false;
  auto job = view.startReplay(
      request, [&](PJ::DatasetId) { dataset_seen = true; },
      [&](PJ::ReplayOutcome, std::string) { terminal_seen = true; });
  EXPECT_FALSE(job.has_value());
  EXPECT_FALSE(dataset_seen);
  EXPECT_FALSE(terminal_seen);
}

TEST(DescriptorReplayExtension, StartReplayDeliversDatasetThenTerminalExactlyOnce) {
  FakeReplayToolbox plugin;
  PJ::DescriptorReplayProviderView view(plugin.pluginExtension(PJ_DESCRIPTOR_REPLAY_EXTENSION_V1), &plugin);
  PJ::ReplayStartRequest request;
  request.descriptor_json = R"({"v":1})";
  std::vector<std::string> order;
  int terminals = 0;
  PJ::ReplayOutcome outcome = PJ::ReplayOutcome::Failed;
  {
    auto job = view.startReplay(
        request, [&](PJ::DatasetId id) { order.push_back("dataset:" + std::to_string(id)); },
        [&](PJ::ReplayOutcome o, std::string) {
          order.push_back("terminal");
          outcome = o;
          ++terminals;
        });
    ASSERT_TRUE(job.has_value());
    job->join();  // returns only after on_terminal returned
    EXPECT_EQ(terminals, 1);
  }  // ~JoinableJob: destroy is idempotent after join
  ASSERT_EQ(order.size(), 2u);
  EXPECT_EQ(order[0], "dataset:7");
  EXPECT_EQ(order[1], "terminal");
  EXPECT_EQ(outcome, PJ::ReplayOutcome::SucceededUnmaterialized);
}

TEST(DescriptorReplayExtension, DestroyWithoutJoinCancelsAndJoins) {
  FakeReplayToolbox plugin;
  PJ::DescriptorReplayProviderView view(plugin.pluginExtension(PJ_DESCRIPTOR_REPLAY_EXTENSION_V1), &plugin);
  PJ::ReplayStartRequest request;
  request.descriptor_json = R"({"v":1})";
  std::atomic<int> terminals{0};
  {
    auto job = view.startReplay(request, nullptr,
                                [&](PJ::ReplayOutcome, std::string) { terminals.fetch_add(1); });
    ASSERT_TRUE(job.has_value());
    // no join: the destructor must destroy (cancel+join) safely
  }
  EXPECT_EQ(terminals.load(), 1);
}

TEST(DescriptorReplayExtension, GrowthContractSmallerCallerCapacityGetsOnlyCoveredFields) {
  // Simulate an OLD caller: capacity ends before estimated_bytes.
  FakeReplayToolbox plugin;
  const auto* ext = static_cast<const PJ_descriptor_replay_provider_v1_t*>(
      plugin.pluginExtension(PJ_DESCRIPTOR_REPLAY_EXTENSION_V1));
  ASSERT_NE(ext, nullptr);
  PJ_descriptor_query_result_v1_t result{};
  result.struct_size = offsetof(PJ_descriptor_query_result_v1_t, estimated_bytes);
  result.estimated_bytes = 999;  // sentinel: provider must NOT touch it
  PJ_string_view_t json{"{}", 2};
  ASSERT_TRUE(ext->query_descriptor(&plugin, json, &result, nullptr));
  EXPECT_EQ(result.estimated_bytes, 999u);
  EXPECT_EQ(result.trust, PJ_DESCRIPTOR_TRUST_TRUSTED);
}

TEST(DescriptorReplayExtension, PluginWithoutExtensionYieldsInvalidView) {
  class PlainToolbox : public PJ::ToolboxPluginBase { /* same minimal no-op bodies */ };
  PlainToolbox plugin;
  PJ::DescriptorReplayProviderView view(plugin.pluginExtension(PJ_DESCRIPTOR_REPLAY_EXTENSION_V1), &plugin);
  EXPECT_FALSE(view.valid());
  EXPECT_FALSE(view.queryDescriptor("{}").has_value());
}

}  // namespace
```

(Adapt the ToolboxPluginBase pure-virtual no-op bodies from `pj_plugins/examples/mock_toolbox.cpp` — whatever set it overrides, both fakes here need the same. If `ToolboxPluginBase` cannot be instantiated standalone without bind, drop the base class and implement `pluginExtension` shape directly on a plain struct — the extension contract, not the plugin lifecycle, is under test.)

- [ ] **Step 2: Register the test** — add `tests/descriptor_replay_extension_test.cpp` to the `PJ_BASE_TESTS` list in `pj_base/CMakeLists.txt` (alphabetical position within the list ~:70-118).

- [ ] **Step 3: Run to verify it fails** — build: expected FAIL, `pj_base/sdk/descriptor_replay.hpp: No such file or directory`.

- [ ] **Step 4: Implement `pj_base/include/pj_base/sdk/descriptor_replay.hpp`** (extension half now; the service half is Task 3 — same file, so write the full skeleton with the Task 3 classes stubbed OUT ENTIRELY, i.e. absent, not empty):

```cpp
#pragma once

/// C++ wrappers for the descriptor-replay v1 extension and the
/// materialized-source adoption service. See
/// pj_base/descriptor_replay_protocol.h for the ABI contract.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "pj_base/descriptor_replay_protocol.h"
#include "pj_base/expected.hpp"
#include "pj_base/sdk/plugin_data_api.hpp"
#include "pj_base/types.hpp"

namespace PJ {

/// Fail-closed C++ mirror of PJ_descriptor_trust_t.
enum class DescriptorTrust : int32_t { Refused = 0, NeedsConfirmation = 1, Trusted = 2 };

/// Fail-closed C++ mirror of PJ_descriptor_replay_outcome_t.
enum class ReplayOutcome : int32_t {
  Failed = 0,
  Cancelled = 1,
  SucceededUnmaterialized = 2,
  SucceededMaterialized = 3,
};

/// queryDescriptor result, copied immediately (the C views are only valid
/// until the next query on the same plugin instance).
struct DescriptorQueryResult {
  DescriptorTrust trust = DescriptorTrust::Refused;
  bool is_materialized = false;
  std::string source_identity;
  std::string local_path_utf8;
  std::string message;
  uint64_t estimated_bytes = 0;  ///< 0 = unknown
};

struct ReplayStartRequest {
  std::string descriptor_json;
  uint64_t flags = PJ_DESCRIPTOR_REPLAY_START_FLAG_NONE;
  uint64_t max_transfer_bytes = 0;  ///< 0 = no caller ceiling
};

/// RAII owner of a PJ_joinable_job_t plus the callback closures backing it.
/// Move-only. The destructor destroys the job (cancel+join if needed) BEFORE
/// releasing the closures, so a late callback can never touch freed state.
class JoinableJob {
 public:
  JoinableJob() = default;
  JoinableJob(const JoinableJob&) = delete;
  JoinableJob& operator=(const JoinableJob&) = delete;
  JoinableJob(JoinableJob&& other) noexcept
      : job_(other.job_), callback_ctx_(std::move(other.callback_ctx_)) {
    other.job_ = PJ_joinable_job_t{};
  }
  JoinableJob& operator=(JoinableJob&& other) noexcept {
    if (this != &other) {
      reset();
      job_ = other.job_;
      callback_ctx_ = std::move(other.callback_ctx_);
      other.job_ = PJ_joinable_job_t{};
    }
    return *this;
  }
  ~JoinableJob() { reset(); }

  [[nodiscard]] bool valid() const noexcept { return job_.vtable != nullptr; }

  /// [thread-safe] Non-blocking, idempotent.
  void cancel() const {
    if (valid() && job_.vtable->cancel != nullptr) {
      job_.vtable->cancel(job_.ctx);
    }
  }

  /// [blocking] Returns after on_terminal has returned. Never call from a
  /// job callback.
  void join() const {
    if (valid() && job_.vtable->join != nullptr) {
      job_.vtable->join(job_.ctx);
    }
  }

 private:
  friend class DescriptorReplayProviderView;

  struct CallbackContext {
    std::function<void(DatasetId)> on_dataset;
    std::function<void(ReplayOutcome, std::string)> on_terminal;
  };

  JoinableJob(PJ_joinable_job_t job, std::unique_ptr<CallbackContext> ctx)
      : job_(job), callback_ctx_(std::move(ctx)) {}

  void reset() {
    if (valid() && job_.vtable->destroy != nullptr) {
      job_.vtable->destroy(job_.ctx);
    }
    job_ = PJ_joinable_job_t{};
    callback_ctx_.reset();
  }

  PJ_joinable_job_t job_{};
  std::unique_ptr<CallbackContext> callback_ctx_;
};

/// Typed consumer of the "pj.descriptor_replay.v1" extension. Construct from
/// the raw pointer returned by get_plugin_extension / pluginExtension plus
/// the SAME plugin-instance ctx that hook was called with.
class DescriptorReplayProviderView {
 public:
  DescriptorReplayProviderView() = default;
  DescriptorReplayProviderView(const void* extension, void* plugin_ctx)
      : ext_(static_cast<const PJ_descriptor_replay_provider_v1_t*>(extension)), plugin_ctx_(plugin_ctx) {}

  [[nodiscard]] bool valid() const noexcept {
    return ext_ != nullptr &&
           ext_->struct_size >=
               offsetof(PJ_descriptor_replay_provider_v1_t, start_replay) + sizeof(ext_->start_replay) &&
           ext_->query_descriptor != nullptr && ext_->start_replay != nullptr;
  }

  [[nodiscard]] Expected<DescriptorQueryResult> queryDescriptor(std::string_view descriptor_json) const {
    // invalid view -> error Expected (use the SDK's standard error idiom).
    // Zero-init a full-size PJ_descriptor_query_result_v1_t, set struct_size,
    // call, map fail-closed, copy all strings, return.
  }

  [[nodiscard]] Expected<JoinableJob> startReplay(const ReplayStartRequest& request,
                                                  std::function<void(DatasetId)> on_dataset,
                                                  std::function<void(ReplayOutcome, std::string)> on_terminal) const {
    // invalid view -> error. Build zero-init C request + callbacks structs
    // (struct_size = sizeof), heap-allocate CallbackContext, call
    // start_replay; on false free the context and return the PJ_error_t as an
    // error; on true wrap into JoinableJob.
  }

 private:
  static void onDatasetThunk(void* callback_ctx, PJ_data_source_handle_t dataset) noexcept {
    auto* ctx = static_cast<JoinableJob::CallbackContext*>(callback_ctx);
    try {
      if (ctx->on_dataset) {
        ctx->on_dataset(static_cast<DatasetId>(dataset.id));
      }
    } catch (...) {
    }
  }
  static void onTerminalThunk(void* callback_ctx, PJ_descriptor_replay_outcome_t outcome,
                              PJ_string_view_t message) noexcept {
    auto* ctx = static_cast<JoinableJob::CallbackContext*>(callback_ctx);
    try {
      if (ctx->on_terminal) {
        ctx->on_terminal(mapOutcome(outcome),
                         message.data == nullptr ? std::string{} : std::string(message.data, message.size));
      }
    } catch (...) {
    }
  }
  static ReplayOutcome mapOutcome(PJ_descriptor_replay_outcome_t outcome) noexcept {
    switch (outcome) {
      case PJ_DESCRIPTOR_REPLAY_CANCELLED:
        return ReplayOutcome::Cancelled;
      case PJ_DESCRIPTOR_REPLAY_SUCCEEDED_UNMATERIALIZED:
        return ReplayOutcome::SucceededUnmaterialized;
      case PJ_DESCRIPTOR_REPLAY_SUCCEEDED_MATERIALIZED:
        return ReplayOutcome::SucceededMaterialized;
      default:
        return ReplayOutcome::Failed;  // fail closed, incl. unknown values
    }
  }

  const PJ_descriptor_replay_provider_v1_t* ext_ = nullptr;
  void* plugin_ctx_ = nullptr;
};

}  // namespace PJ
```

Fill the two commented method bodies completely (they're prose here only to keep this plan readable; the shapes are fully constrained): zero-init + `struct_size` + fail-closed mapping in `queryDescriptor`; and in `startReplay` the C structs are stack-locals (the ABI guarantees start_replay copies), `callback_ctx` is the heap `CallbackContext`. Copy the error-construction idiom (`Expected` error from `PJ_error_t` / from a literal) from an existing view in `sdk/plugin_data_api.hpp` — match it exactly, do not invent a new one. Mind `-Wconversion`: `PJ_string_view_t.size` is `uint64_t`, `std::string` wants `size_t` — cast explicitly.

- [ ] **Step 5: Run to verify it passes** — `ctest --test-dir build/debug_asan --output-on-failure -R descriptor_replay_extension_test`. Expected: PASS (ASAN build also proves the destroy-order/lifetime design).

- [ ] **Step 6: Commit**

```bash
git -C <worktree> add pj_base/include/pj_base/sdk/descriptor_replay.hpp pj_base/tests/descriptor_replay_extension_test.cpp pj_base/CMakeLists.txt
git -C <worktree> commit -m "feat(sdk): DescriptorReplayProviderView + RAII JoinableJob over pj.descriptor_replay.v1"
```

---

### Task 3: Adoption service view + trait (service half of `sdk/descriptor_replay.hpp`)

**Files:**
- Modify: `pj_base/include/pj_base/sdk/descriptor_replay.hpp` (append the service classes)
- Create: `pj_base/tests/materialized_source_service_test.cpp`
- Modify: `pj_base/CMakeLists.txt` (register test)

Before writing, read: `pj_base/include/pj_base/sdk/service_traits.hpp:15-108` (trait shape + `detail::isValidServiceName`), `pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp:82-95` (the precedent for a trait living NEXT TO its view instead of in service_traits.hpp — include the `static_assert` that precedent forgot), `pj_plugins/include/pj_plugins/host/service_registry_builder.hpp`, `pj_base/tests/data_processors_api_test.cpp` (fake-host-vtable service test pattern), `pj_base/include/pj_base/sdk/service_registry.hpp:40-74` and `:111-117` (`makeView` requires `explicit View(Raw)`).

- [ ] **Step 1: Write the failing test** — `pj_base/tests/materialized_source_service_test.cpp`:

```cpp
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "pj_base/descriptor_replay_protocol.h"
#include "pj_base/sdk/descriptor_replay.hpp"
#include "pj_base/sdk/service_registry.hpp"
#include "pj_plugins/host/service_registry_builder.hpp"

namespace {

// Fake host for the pj.materialized_source.v1 service, modeled on
// data_processors_api_test.cpp's fake host.
struct FakeAdoptionHost {
  struct Captured {
    uint32_t dataset = 0;
    std::string source_identity;
    std::string local_path;
    std::string loader_plugin_id;
    std::string loader_config_json;
    std::string descriptor_json;
  };
  std::vector<Captured> requests;
  bool accept = true;   // false => synchronous rejection, callback must not run
  bool succeed = true;  // outcome delivered through the callback

  static bool adoptThunk(void* ctx, const PJ_materialized_source_adopt_request_v1_t* request,
                         PJ_materialized_source_adopt_result_fn result_cb, void* callback_ctx,
                         PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeAdoptionHost*>(ctx);
    if (!self->accept) {
      if (err != nullptr) {
        err->code = 42;
      }
      return false;
    }
    Captured c;
    c.dataset = request->dataset.id;
    auto s = [](PJ_string_view_t v) { return v.data ? std::string(v.data, v.size) : std::string{}; };
    c.source_identity = s(request->source_identity);
    c.local_path = s(request->local_path_utf8);
    c.loader_plugin_id = s(request->loader_plugin_id);
    c.loader_config_json = s(request->loader_config_json);
    c.descriptor_json = s(request->descriptor_json);
    self->requests.push_back(c);
    const char* msg = self->succeed ? "adopted" : "generation mismatch";
    result_cb(callback_ctx, self->succeed, PJ_string_view_t{msg, std::char_traits<char>::length(msg)});
    return true;
  }

  [[nodiscard]] PJ_materialized_source_host_t view() {
    static constexpr PJ_materialized_source_host_vtable_t kVtable{1, sizeof(PJ_materialized_source_host_vtable_t),
                                                                  &FakeAdoptionHost::adoptThunk};
    return PJ_materialized_source_host_t{this, &kVtable};
  }
};

PJ::AdoptRequest makeRequest() {
  PJ::AdoptRequest r;
  r.dataset = 7;
  r.source_identity = "mcap-cloud:v1:sha256/128:aa";
  r.local_path_utf8 = "/cache/aa.mcap";
  r.loader_plugin_id = "data_load_mcap";
  r.loader_config_json = R"({"use_log_time":true})";
  r.descriptor_json = R"({"v":1})";
  return r;
}

TEST(MaterializedSourceService, RegistryLookupAndAdoptExactlyOnce) {
  FakeAdoptionHost host;
  PJ::ServiceRegistryBuilder builder;
  builder.registerService<PJ::sdk::MaterializedSourceHostService>(host.view());
  PJ::sdk::ServiceRegistry registry(builder.view());

  auto view = registry.get<PJ::sdk::MaterializedSourceHostService>();
  ASSERT_TRUE(view.has_value());

  int calls = 0;
  bool ok = false;
  std::string message;
  auto status = view->adopt(makeRequest(), [&](bool result_ok, std::string result_message) {
    ++calls;
    ok = result_ok;
    message = std::move(result_message);
  });
  EXPECT_TRUE(status);  // adapt to the SDK's Status truthiness idiom
  EXPECT_EQ(calls, 1);
  EXPECT_TRUE(ok);
  EXPECT_EQ(message, "adopted");
  ASSERT_EQ(host.requests.size(), 1u);
  EXPECT_EQ(host.requests[0].dataset, 7u);
  EXPECT_EQ(host.requests[0].loader_plugin_id, "data_load_mcap");
  EXPECT_EQ(host.requests[0].descriptor_json, R"({"v":1})");
}

TEST(MaterializedSourceService, SynchronousRejectionNeverRunsCallback) {
  FakeAdoptionHost host;
  host.accept = false;
  PJ::MaterializedSourceHostView view(host.view());
  int calls = 0;
  auto status = view.adopt(makeRequest(), [&](bool, std::string) { ++calls; });
  EXPECT_FALSE(status);
  EXPECT_EQ(calls, 0);
  EXPECT_TRUE(host.requests.empty());
}

TEST(MaterializedSourceService, AbsentServiceIsNullopt) {
  PJ::ServiceRegistryBuilder builder;  // nothing registered
  PJ::sdk::ServiceRegistry registry(builder.view());
  EXPECT_FALSE(registry.get<PJ::sdk::MaterializedSourceHostService>().has_value());
}

}  // namespace
```

(Adapt the two construction idioms to reality after reading the headers: how `PJ::sdk::ServiceRegistry` is constructed from a `PJ_service_registry_t` — see how the plugin bases store it — and the exact `Status` truthiness. `pj_base` tests can include `pj_plugins` headers only if the test links them; if `pj_plugins/host/...` is not visible from `pj_base/tests`, register the fake service by hand-building a tiny `PJ_service_registry_vtable_t` fake instead — `notify_available_topics_test.cpp` and `data_processors_api_test.cpp` show which pattern the tree prefers; use the pattern that keeps this a `pj_base` test. If neither works cleanly, place the test in `pj_plugins/tests/` with an explicit `add_test` instead — follow `pj_plugins/CMakeLists.txt:264-338`.)

- [ ] **Step 2: Run to verify it fails** — expected FAIL: `MaterializedSourceHostService`/`MaterializedSourceHostView`/`AdoptRequest` not declared.

- [ ] **Step 3: Implement** — append to `pj_base/include/pj_base/sdk/descriptor_replay.hpp`:

```cpp
namespace PJ {

/// Adoption request (C++ mirror). All strings copied by the host during adopt().
struct AdoptRequest {
  DatasetId dataset{};
  std::string source_identity;
  std::string local_path_utf8;
  std::string loader_plugin_id;
  std::string loader_config_json;
  std::string descriptor_json;
};

/// View over the host's "pj.materialized_source.v1" service.
class MaterializedSourceHostView {
 public:
  MaterializedSourceHostView() = default;
  explicit MaterializedSourceHostView(PJ_materialized_source_host_t host) : host_(host) {}

  [[nodiscard]] bool valid() const noexcept {
    return host_.ctx != nullptr && host_.vtable != nullptr &&
           host_.vtable->struct_size >=
               offsetof(PJ_materialized_source_host_vtable_t, adopt) + sizeof(host_.vtable->adopt) &&
           host_.vtable->adopt != nullptr;
  }

  /// Async adopt. On acceptance @p on_result runs exactly once (host thread,
  /// possibly re-entrantly before this returns). A synchronous rejection
  /// returns an error Status and @p on_result never runs.
  [[nodiscard]] Status adopt(const AdoptRequest& request, std::function<void(bool, std::string)> on_result) const {
    // invalid view -> error Status. Build zero-init C request (struct_size =
    // sizeof, views into the strings). Heap-allocate the std::function as the
    // callback_ctx; the thunk invokes it then deletes it (exactly-once). On
    // synchronous false: delete the context and return the PJ_error_t as an
    // error Status.
  }

 private:
  static void resultThunk(void* callback_ctx, bool ok, PJ_string_view_t message) noexcept {
    auto* fn = static_cast<std::function<void(bool, std::string)>*>(callback_ctx);
    try {
      (*fn)(ok, message.data == nullptr ? std::string{} : std::string(message.data, message.size));
    } catch (...) {
    }
    delete fn;
  }
  PJ_materialized_source_host_t host_{};
};

namespace sdk {

/// Service trait for the optional per-plugin-instance adoption service.
struct MaterializedSourceHostService {
  static constexpr const char* kName = PJ_MATERIALIZED_SOURCE_HOST_SERVICE_V1;
  static constexpr uint32_t kMinVersion = 1;
  using Raw = PJ_materialized_source_host_t;
  using Vtable = PJ_materialized_source_host_vtable_t;
  using View = MaterializedSourceHostView;
  static_assert(detail::isValidServiceName(kName), "kName must match the pj naming rule");
};

}  // namespace sdk
}  // namespace PJ
```

Add `#include "pj_base/sdk/service_traits.hpp"` to the header's include list (for `detail::isValidServiceName`); fix namespace qualification to match how `service_traits.hpp` scopes `detail` (traits live in `PJ::sdk`, views in `PJ` — mirror `ToolboxRuntimeHostService`/`ToolboxRuntimeHostView` exactly). Fill the `adopt` body completely per its comment.

- [ ] **Step 4: Run to verify it passes** — `ctest --test-dir build/debug_asan --output-on-failure -R materialized_source_service_test`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add pj_base/include/pj_base/sdk/descriptor_replay.hpp pj_base/tests/materialized_source_service_test.cpp pj_base/CMakeLists.txt
git -C <worktree> commit -m "feat(sdk): MaterializedSourceHostView + service trait for pj.materialized_source.v1"
```

---

### Task 4: `DatasetIngestHostView` + toolbox accessors (zero C ABI change)

**Files:**
- Modify: `pj_base/include/pj_base/sdk/data_source_host_views.hpp` (add class after `ParserIngestHostView`, ~line 357, plus the inline accessor)
- Modify: `pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp` (declarations next to `createParserIngest` at :66-70)
- Modify: `pj_base/src/toolbox_plugin_base.cpp` (implementations mirroring :8-35)
- Create: `pj_base/tests/dataset_ingest_view_test.cpp`
- Modify: `pj_base/CMakeLists.txt` (register test)

**Design (final, from the fourth Codex consult):** the raw context returned by `create_parser_ingest` is a complete `PJ_data_source_runtime_host_t` including progress and cooperative stop; `DatasetIngestHostView` is a WIDER facade over that same fat pointer — `ParserIngestHostView` ∪ {reportMessage, progressStart, progressUpdate, progressFinish, isStopRequested}. `createDatasetIngest()` is a second C++ accessor over the SAME `create_parser_ingest` slot; `releaseDatasetIngest()` forwards to `releaseParserIngest()`. Contract text: this is the canonical dataset-scoped ingest lifecycle for BOTH delegated parsing and direct toolbox writes (a direct-writing toolbox acquires it purely for progress/stop and never binds a parser).

Before writing, read `pj_base/tests/push_message_test.cpp` (fake runtime-host vtable pattern) and `pj_base/src/toolbox_plugin_base.cpp:8-35` (tail-slot gating + exact error strings).

- [ ] **Step 1: Write the failing test** — `pj_base/tests/dataset_ingest_view_test.cpp`:

```cpp
#include <cstring>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "pj_base/data_source_protocol.h"
#include "pj_base/sdk/data_source_host_views.hpp"
#include "pj_base/sdk/toolbox_plugin_base.hpp"
#include "pj_base/toolbox_protocol.h"

namespace {

// Fake data-source runtime host recording lifecycle calls (modeled on
// push_message_test.cpp's fake host).
struct FakeRuntimeHost {
  std::vector<std::string> calls;
  bool stop_requested = false;

  static bool progressStartThunk(void* ctx, PJ_string_view_t label, uint64_t total, bool cancellable,
                                 PJ_error_t*) noexcept {
    auto* self = static_cast<FakeRuntimeHost*>(ctx);
    self->calls.push_back("start:" + std::string(label.data, label.size) + ":" + std::to_string(total) +
                          (cancellable ? ":c" : ""));
    return true;
  }
  static bool progressUpdateThunk(void* ctx, uint64_t step) noexcept {
    static_cast<FakeRuntimeHost*>(ctx)->calls.push_back("update:" + std::to_string(step));
    return true;
  }
  static void progressFinishThunk(void* ctx) noexcept {
    static_cast<FakeRuntimeHost*>(ctx)->calls.push_back("finish");
  }
  static bool isStopRequestedThunk(void* ctx) noexcept {
    return static_cast<FakeRuntimeHost*>(ctx)->stop_requested;
  }

  // Fill a complete vtable: copy the slot set + signatures from
  // data_source_protocol.h:236-361, providing no-op bodies for slots this
  // test does not exercise (report_message, notify_state, ensure_parser_binding, ...).
  PJ_data_source_runtime_host_vtable_t vtable{};
  FakeRuntimeHost() {
    std::memset(&vtable, 0, sizeof(vtable));
    vtable.protocol_version = PJ_DATA_SOURCE_PROTOCOL_VERSION;  // use the real macro name
    vtable.struct_size = sizeof(vtable);
    vtable.progress_start = &FakeRuntimeHost::progressStartThunk;
    vtable.progress_update = &FakeRuntimeHost::progressUpdateThunk;
    vtable.progress_finish = &FakeRuntimeHost::progressFinishThunk;
    vtable.is_stop_requested = &FakeRuntimeHost::isStopRequestedThunk;
    // + minimal no-op report_message etc. as required by the view's calls
  }
  [[nodiscard]] PJ_data_source_runtime_host_t view() { return PJ_data_source_runtime_host_t{this, &vtable}; }
};

// Fake toolbox runtime host whose create_parser_ingest returns the fake
// runtime host above.
struct FakeToolboxRuntimeHost {
  FakeRuntimeHost ingest_host;
  std::vector<uint32_t> created;
  std::vector<uint32_t> released;

  static bool createThunk(void* ctx, uint32_t id, PJ_data_source_runtime_host_t* out, PJ_error_t*) noexcept {
    auto* self = static_cast<FakeToolboxRuntimeHost*>(ctx);
    self->created.push_back(id);
    *out = self->ingest_host.view();
    return true;
  }
  static bool releaseThunk(void* ctx, uint32_t id, PJ_error_t*) noexcept {
    static_cast<FakeToolboxRuntimeHost*>(ctx)->released.push_back(id);
    return true;
  }
  static void reportThunk(void*, PJ_toolbox_message_level_t, PJ_string_view_t) noexcept {}
  static void notifyThunk(void*) noexcept {}

  PJ_toolbox_runtime_host_vtable_t vtable{};
  FakeToolboxRuntimeHost() {
    std::memset(&vtable, 0, sizeof(vtable));
    vtable.protocol_version = 1;  // match the real macro if one exists
    vtable.struct_size = sizeof(vtable);
    vtable.report_message = &FakeToolboxRuntimeHost::reportThunk;
    vtable.notify_data_changed = &FakeToolboxRuntimeHost::notifyThunk;
    vtable.create_parser_ingest = &FakeToolboxRuntimeHost::createThunk;
    vtable.release_parser_ingest = &FakeToolboxRuntimeHost::releaseThunk;
  }
  [[nodiscard]] PJ_toolbox_runtime_host_t view() { return PJ_toolbox_runtime_host_t{this, &vtable}; }
};

TEST(DatasetIngestView, CreateForwardsLifecycleCallsToTheSameContext) {
  FakeToolboxRuntimeHost host;
  PJ::ToolboxRuntimeHostView runtime(host.view());
  auto ingest = runtime.createDatasetIngest(42);
  ASSERT_TRUE(ingest.has_value());
  ASSERT_TRUE(ingest->valid());
  EXPECT_EQ(host.created, std::vector<uint32_t>{42u});

  ASSERT_TRUE(ingest->progressStart("replay", 100, true));
  EXPECT_TRUE(ingest->progressUpdate(50));
  host.ingest_host.stop_requested = true;
  EXPECT_TRUE(ingest->isStopRequested());
  ingest->progressFinish();

  EXPECT_EQ(host.ingest_host.calls,
            (std::vector<std::string>{"start:replay:100:c", "update:50", "finish"}));

  EXPECT_TRUE(runtime.releaseDatasetIngest(42));
  EXPECT_EQ(host.released, std::vector<uint32_t>{42u});
}

TEST(DatasetIngestView, ParserAccessSharesTheContext) {
  FakeToolboxRuntimeHost host;
  PJ::ToolboxRuntimeHostView runtime(host.view());
  auto ingest = runtime.createDatasetIngest(1);
  ASSERT_TRUE(ingest.has_value());
  auto parser_view = ingest->parserIngest();
  EXPECT_TRUE(parser_view.valid());
}

TEST(DatasetIngestView, OlderHostWithoutTailSlotErrors) {
  FakeToolboxRuntimeHost host;
  // Simulate a pre-parser-ingest host: struct_size ends before the tail.
  host.vtable.struct_size = offsetof(PJ_toolbox_runtime_host_vtable_t, create_parser_ingest);
  PJ::ToolboxRuntimeHostView runtime(host.view());
  EXPECT_FALSE(runtime.createDatasetIngest(1).has_value());
  EXPECT_FALSE(runtime.releaseDatasetIngest(1));
}

}  // namespace
```

(Slot names in the fakes — `progress_start` etc. — must be corrected to the REAL member names from `data_source_protocol.h:236-361` while implementing; the sentinel test at `abi_layout_sentinels_test.cpp:148-159` lists them. `EXPECT_TRUE(status)`-style checks adapt to the Status idiom.)

- [ ] **Step 2: Run to verify it fails** — expected FAIL: `createDatasetIngest` is not a member of `ToolboxRuntimeHostView`.

- [ ] **Step 3: Implement**

In `data_source_host_views.hpp`, after `ParserIngestHostView` (~:357), plus a forward declaration next to the existing one at :115 and an accessor declaration in `DataSourceRuntimeHostView` next to `parserIngest()` at :134:

```cpp
/// Canonical dataset-scoped ingest lifecycle facade — the ONE surface for
/// both delegated parsing and direct toolbox writes. Wraps the same runtime
/// host that ParserIngestHostView wraps, but exposes the lifecycle verbs
/// (#470's progressive-import surface) that the parser-only facade hides:
/// progress start/update/finish and cooperative stop. A toolbox that writes
/// directly (e.g. Arrow batches through ToolboxHostView) acquires this view
/// for its dataset purely for lifecycle, and never binds a parser.
/// Obtain via ToolboxRuntimeHostView::createDatasetIngest() (toolboxes) or
/// DataSourceRuntimeHostView::datasetIngest() (data sources).
class DatasetIngestHostView {
 public:
  DatasetIngestHostView() = default;
  explicit DatasetIngestHostView(PJ_data_source_runtime_host_t host) : host_(host) {}

  [[nodiscard]] bool valid() const noexcept { return host_.valid(); }

  void reportMessage(DataSourceMessageLevel level, std::string_view message) const {
    host_.reportMessage(level, message);
  }
  [[nodiscard]] Status progressStart(std::string_view label, uint64_t total_steps, bool cancellable) const {
    return host_.progressStart(label, total_steps, cancellable);
  }
  [[nodiscard]] bool progressUpdate(uint64_t current_step) const { return host_.progressUpdate(current_step); }
  void progressFinish() const { host_.progressFinish(); }
  [[nodiscard]] bool isStopRequested() const { return host_.isStopRequested(); }

  [[nodiscard]] Expected<ParserBindingHandle> ensureParserBinding(const ParserBindingRequest& request) const {
    return host_.ensureParserBinding(request);
  }
  template <typename FetchMessageData>
  [[nodiscard]] Status pushMessage(ParserBindingHandle handle, Timestamp host_timestamp_ns,
                                   FetchMessageData&& fetch_message_data) const {
    return host_.pushMessage(handle, host_timestamp_ns, std::forward<FetchMessageData>(fetch_message_data));
  }

  /// Narrow parser-only facade over the same context.
  [[nodiscard]] ParserIngestHostView parserIngest() const noexcept { return ParserIngestHostView{host_.raw()}; }

 private:
  DataSourceRuntimeHostView host_{};
};

inline DatasetIngestHostView DataSourceRuntimeHostView::datasetIngest() const noexcept {
  return DatasetIngestHostView{host_};
}
```

In `toolbox_plugin_base.hpp`, after `releaseParserIngest` (:70):

```cpp
  /// Generic alias over the same context as createParserIngest: the canonical
  /// dataset-scoped ingest lifecycle for BOTH delegated parsing and direct
  /// toolbox writes (progress/stop + optional parser access). Same threading
  /// rule: drive the returned view from a single worker thread.
  [[nodiscard]] Expected<DatasetIngestHostView> createDatasetIngest(uint32_t data_source_id) const;

  /// Flush + destroy the context (same slot as releaseParserIngest). Idempotent.
  [[nodiscard]] Status releaseDatasetIngest(uint32_t data_source_id) const;
```

In `toolbox_plugin_base.cpp` (mirror :8-35 — same `PJ_HAS_TAIL_SLOT` gating, error text `"toolbox runtime host does not support create_parser_ingest (older host)"` verbatim so old-host behavior reads identically):

```cpp
Expected<DatasetIngestHostView> ToolboxRuntimeHostView::createDatasetIngest(uint32_t data_source_id) const {
  // identical body to createParserIngest except the success line returns
  // DatasetIngestHostView{ingest_host} instead of ParserIngestHostView{...}.
}

Status ToolboxRuntimeHostView::releaseDatasetIngest(uint32_t data_source_id) const {
  return releaseParserIngest(data_source_id);
}
```

(Write the `createDatasetIngest` body in full by copying `createParserIngest`'s 14 lines and changing the return type — do not refactor the existing function.)

- [ ] **Step 4: Run to verify it passes** — `ctest --test-dir build/debug_asan --output-on-failure -R dataset_ingest_view_test`, then the FULL suite `./test.sh` (the header edits touch widely-included files). Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add pj_base/include/pj_base/sdk/data_source_host_views.hpp pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp pj_base/src/toolbox_plugin_base.cpp pj_base/tests/dataset_ingest_view_test.cpp pj_base/CMakeLists.txt
git -C <worktree> commit -m "feat(sdk): DatasetIngestHostView — canonical dataset lifecycle for delegated parsing AND direct writes"
```

---

### Task 5: Version bump + CHANGELOG + docs

**Files:**
- Modify: `conanfile.py:33` (`version = "0.20.0"`) and `:9` (docstring `plotjuggler_sdk/0.20.0`)
- Modify: `CMakeLists.txt:131` (`set(PJ_PACKAGE_VERSION "0.20.0")`)
- Modify: `recipe.yaml:4` (`version: "0.20.0"`)
- Modify: `CHANGELOG.md` (insert after the 4-line header, before `## [0.19.0]`)
- Modify: `pj_plugins/docs/toolbox-guide.md` (new section at the end)
- Modify: `pj_plugins/docs/ARCHITECTURE.md` (inventory line for the new extension/service/headers — read the file first and add where extensions/services are listed)

- [ ] **Step 1: Bump the version in all four places** (the conda-release workflow hard-fails if any of conanfile.py / CMakeLists.txt / recipe.yaml disagree — the docstring is the CLAUDE.md-required fourth).

- [ ] **Step 2: CHANGELOG entry** (model: `## [0.15.0]` — the release that added an extension; end with the explicit ABI-impact statement):

```markdown
## [0.20.0]

### Feature: descriptor replay v1 — replay a persisted source descriptor, adopt the materialized artifact (MINOR)

A provider plugin (any family) can now advertise "pj.descriptor_replay.v1"
through the existing `get_plugin_extension` hook, and a host can offer the
optional "pj.materialized_source.v1" adoption service through the `bind()`
registry — zero new family-vtable slots, no capability bit (presence =
capability). This is the SDK half of the canonical-layout-replay design: a
layout stores an opaque provider descriptor; on load the host queries the
provider (trust + identity + planned artifact path + `estimated_bytes`),
optionally starts a replay job, and the provider asks the host to adopt its
materialized artifact as a stock file-backed source.

- New family-neutral installed C header `pj_base/descriptor_replay_protocol.h`:
  `PJ_descriptor_replay_provider_v1_t` with `query_descriptor` (sync, strictly
  bounded — no network; always returns provider `source_identity` + planned
  `local_path_utf8`; `estimated_bytes`, 0 = unknown) and `start_replay` taking
  a caller-sized `PJ_descriptor_replay_start_request_v1_t{descriptor_json,
  flags, max_transfer_bytes}` (v1 flags mask = 0 — unknown bits fail closed)
  with exactly two serialized callbacks: `on_dataset` (zero-or-one, precedes
  the dataset's progress/publication/adoption) and `on_terminal` (exactly-once,
  last: SUCCEEDED_MATERIALIZED / SUCCEEDED_UNMATERIALIZED / FAILED /
  CANCELLED), returning a joinable-job fat pointer (cancel / join / destroy).
  The adoption request carries provider-supplied `loader_plugin_id` +
  `loader_config_json` so a non-MCAP artifact adopts through its own companion
  loader; the service is bound per plugin instance so the host derives the
  provider identity itself. Every new struct is struct_size-versioned under an
  explicit growth contract (owner zero-initializes, peer touches only fields
  wholly covered); enums are FORCE_INT32-pinned with fail-closed unknowns.
- C++ wrappers in `pj_base/sdk/descriptor_replay.hpp`:
  `DescriptorReplayProviderView` (typed extension consumer, fail-closed enum
  mapping), RAII `JoinableJob` (owns the callback closures; destroy-before-
  release ordering), `MaterializedSourceHostView` +
  `PJ::sdk::MaterializedSourceHostService` trait.
- Generic dataset-ingest lifecycle: `DatasetIngestHostView` (progress
  start/update/finish, cooperative stop, report, parser access) obtained via
  new `ToolboxRuntimeHostView::createDatasetIngest()` /
  `releaseDatasetIngest()` — C++ aliases over the EXISTING
  `create_parser_ingest`/`release_parser_ingest` slots — and
  `DataSourceRuntimeHostView::datasetIngest()`. This makes the dataset-scoped
  lifecycle canonical for both delegated parsing and direct toolbox writes
  (previously `ParserIngestHostView` hid it and direct Arrow writers could not
  reach the progressive-import surface).
- ABI-layout sentinels now pin every new struct (the first pins for extension
  structs).

No vtable grows anywhere: `PJ_ABI_VERSION` (5), every `PJ_*_PROTOCOL_VERSION`,
every `PJ_*_MIN_VTABLE_SIZE`, and `abi/baseline.abi` unchanged (additions
only: one new installed C header, header-only C++ additions, and two
out-of-line `ToolboxRuntimeHostView` methods).
```

- [ ] **Step 3: toolbox-guide.md section** — append (adjust heading level to the file's structure):

```markdown
## Descriptor replay and materialized-source adoption (0.20.0)

A toolbox (or any plugin family) that can re-create a dataset from a persisted
descriptor — a cloud session, a database query — advertises
`pj.descriptor_replay.v1` from `pluginExtension()` by returning a static
`PJ_descriptor_replay_provider_v1_t` (`pj_base/descriptor_replay_protocol.h`):

- `query_descriptor` is synchronous and strictly bounded (no network, no
  credential resolution, no blocking locks): it classifies trust
  (refused / needs-confirmation / trusted), reports whether the artifact is
  already materialized locally, and ALWAYS returns the provider's canonical
  `source_identity`, the planned `local_path_utf8`, and `estimated_bytes`
  (0 = unknown).
- `start_replay` launches the asynchronous replay job (caller-sized request:
  descriptor + flags + `max_transfer_bytes` ceiling). Exactly two serialized
  callbacks: `on_dataset` (zero-or-one — announce the provisional dataset
  BEFORE any progress/publication/adoption) and `on_terminal` (exactly-once,
  last). Progress, publish ticks and cooperative stop do NOT ride the job:
  they ride the dataset-scoped ingest lifecycle below.

During the replay the provider drives the standard ingest lifecycle through
`ToolboxRuntimeHostView::createDatasetIngest(dataset_id)` — the canonical
dataset-scoped surface for BOTH delegated parsing (`ensureParserBinding` /
`pushMessage`) and direct writes (Arrow or scalar appends through
`ToolboxHostView`, using the view only for progress/stop). When the artifact
file is complete, the provider asks the host to adopt it as a stock
file-backed source through the optional per-instance
`pj.materialized_source.v1` service
(`PJ::sdk::MaterializedSourceHostService`): the request names the dataset,
the artifact path, the provider's `source_identity`, the descriptor, and the
loader (`loader_plugin_id` + `loader_config_json`) that can re-ingest the
artifact with eager-path-identical semantics. A provider that cannot yet
produce such an artifact reports `SUCCEEDED_UNMATERIALIZED` instead.

C++ consumers: `PJ::DescriptorReplayProviderView`, `PJ::JoinableJob`,
`PJ::MaterializedSourceHostView` in `pj_base/sdk/descriptor_replay.hpp`.
```

- [ ] **Step 4: ARCHITECTURE.md** — read it; add the extension id + service id + the two new headers wherever it inventories extensions/services/protocol headers (keep to a few lines, matching its style).

- [ ] **Step 5: Full verification** — `./build.sh --debug && ./test.sh` AND a release-config build `./build.sh`. Expected: all ctest suites PASS in both trees, zero warnings.

- [ ] **Step 6: Commit**

```bash
git -C <worktree> add conanfile.py CMakeLists.txt recipe.yaml CHANGELOG.md pj_plugins/docs/toolbox-guide.md pj_plugins/docs/ARCHITECTURE.md
git -C <worktree> commit -m "chore(release): 0.20.0 — descriptor replay v1 (CHANGELOG, version sources, guides)"
```

---

### Task 6: Push + PR

- [ ] **Step 1: Final review pass** — `git -C <worktree> log --oneline upstream/main..HEAD` (expect 5 commits), `git -C <worktree> diff upstream/main --stat`. Confirm NO existing pinned numbers in `abi_layout_sentinels_test.cpp` changed and no family `*_protocol.h` was touched except… none should be touched at all.

- [ ] **Step 2: Push** — `git -C <worktree> push -u upstream feat/descriptor-replay-v1` (feature branches live on the upstream repo, like `feat/toolbox-parser-ingest-rangeslider-markers`).

- [ ] **Step 3: Open the PR** against `PlotJuggler/plotjuggler_sdk` `main` with `gh pr create`. Title: `feat: descriptor replay v1 — pj.descriptor_replay.v1 extension + pj.materialized_source.v1 service + DatasetIngestHostView (0.20.0)`. Body: summary of the four deliverables, the zero-vtable-growth ABI statement, the test inventory, a pointer to the design record (canonical-layout-replay spec §8, external repo), and the standard Claude Code footer.

**NOT in this PR (deliberate):** tagging/publishing the release (explicitly-authorized separate step per SDK CLAUDE.md), the PJ4 host work (LayoutImportBatch/LoadTicket/etc.), and the plugin `SDK_VERSION` pin bump 0.11.0→0.20.0 (happens in the plugin repo once the package is published).

---

## Self-review notes

- Spec coverage: extension (§8 bullet 1) → Tasks 1-2; service (§8 bullet 2) → Tasks 1, 3; DatasetIngest co-batch (§8 bullet 3) → Task 4; "ABI-layout + old-host/null-extension tests" → Tasks 1, 2, 4; MINOR 0.20.0 + no protocol bumps → Task 5; "stable manifest-ID selection in FileLoader" is PJ4-host scope, NOT SDK — excluded on purpose.
- The two prose-bodied methods (`queryDescriptor`/`startReplay`/`adopt` interiors) are fully constrained by the ABI + the named reference idioms; everything else is complete code.
- Offsets in Task 1 were hand-computed for x86-64 SysV (`PJ_string_view_t` = 16/8-align, enums = 4, `PJ_data_source_handle_t` = 4) — if the compiler disagrees, the header field ORDER wins and the plan's number was wrong; re-derive, don't reorder.
