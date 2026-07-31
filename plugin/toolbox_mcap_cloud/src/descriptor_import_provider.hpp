// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// DescriptorImportProvider — the pj.descriptor_import.v1 implementation
// (stage-4 PR-3, D2/D3/D4-as-amended; spec docs/canonical-layout-import.md
// §6.3/§8/§9.8). Owned by McapCloudToolbox, which exposes it through its
// pluginExtension() thunks (the ABI's plugin_ctx is the TOOLBOX instance —
// never this object or the extension table).
//
// Surfaces, both operating directly on the raw C structs so the growth
// contract (struct_size-covered reads/writes) lives in exactly one place:
//   - queryDescriptor: [main-thread, strictly bounded] — descriptor parse,
//     in-memory trust lookup, DISK-validated cache lookup (bounded I/O),
//     file-size estimate. NO network, NO credential resolution, NO blocking
//     lock acquisition. Result strings are owned by this instance and stay
//     valid until the NEXT query on it (the ABI lifetime rule).
//   - startImport: [main-thread] — flags fail closed FIRST; required
//     callbacks validated; descriptor parsed; credentials resolved + the
//     ConnectionSnapshot built ON THIS THREAD (every SettingsView call is
//     main-thread-only — the PR-2 contract); out_job populated BEFORE the
//     worker thread starts; an explicit post-return START GATE guarantees no
//     callback can run before start_import returns.
//
// The job (one worker thread + one FetchWorker per job, D4): bounded
// cancelable in-process materialization gate -> the direct cancellable pull
// (cache-tee mode, ticket adopted) -> promotion wait (cancel-aware, DETACH
// on cancel) -> the exactly-once terminal. cancel() is idempotent and
// non-blocking; join() returns after on_terminal returned; destroy() is
// cancel+join+free. NEVER call join/destroy from a job callback (ABI rule —
// a defensive self-join guard documents it).
//
// NOTHING here touches the dialog/Qt: the provider works on an instance
// whose getDialog() was never called (the PR-2 headless-init latch stays
// cold).
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <pj_base/descriptor_import_protocol.h>
#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/toolbox_plugin_base.hpp>

#include "import_runtime.hpp"

namespace mcap_cloud {

class CredentialStore;

class DescriptorImportProvider {
 public:
  /// The host access a job's pull needs (the toolbox's toolboxHost() /
  /// runtimeHost(), wrapped as providers exactly like the dialog's worker).
  struct HostBindings {
    std::function<PJ::sdk::ToolboxHostView()> host_provider;
    std::function<PJ::ToolboxRuntimeHostView()> runtime_host_provider;
  };

  explicit DescriptorImportProvider(ImportRuntime& runtime);
  ~DescriptorImportProvider();

  DescriptorImportProvider(const DescriptorImportProvider&) = delete;
  DescriptorImportProvider& operator=(const DescriptorImportProvider&) = delete;

  /// Main-thread wiring at bind() (re-bind swaps the views; no network).
  void bind(PJ::sdk::SettingsView settings, HostBindings bindings);

  /// The raw extension surface (see the file header). The toolbox's
  /// PJ_descriptor_import_provider_v1_t thunks forward here.
  bool queryDescriptor(PJ_string_view_t descriptor_json, PJ_descriptor_query_result_v1_t* out_result,
                       PJ_error_t* out_error);
  bool startImport(const PJ_descriptor_import_start_request_v1_t* request,
                   const PJ_descriptor_import_callbacks_v1_t* callbacks, void* callback_ctx,
                   PJ_joinable_job_t* out_job, PJ_error_t* out_error);

  // ---- test seams (main-thread, set before start_import) -------------------
  /// Runs inside start_import AFTER out_job is populated and the (gated)
  /// worker exists, BEFORE the start gate is released — the
  /// no-callback-before-return pin.
  void setStartGateProbeForTest(std::function<void()> probe) { start_gate_probe_ = std::move(probe); }
  /// Shrink the bounded in-process lock wait (poll interval + total budget).
  void setLockWaitForTest(std::chrono::milliseconds poll, std::chrono::milliseconds timeout) {
    lock_poll_ = poll;
    lock_timeout_ = timeout;
  }
  /// Runs on the job thread after the pull returned, BEFORE the terminal
  /// mapping — lets a test interleave a cancel deterministically against the
  /// ceiling flag (the ceiling-vs-cancel precedence pin).
  void setPreTerminalHookForTest(std::function<void()> hook) { pre_terminal_hook_ = std::move(hook); }

 private:
  struct JobState;

  static void jobCancel(void* ctx) noexcept;
  static void jobJoin(void* ctx) noexcept;
  static void jobDestroy(void* ctx) noexcept;
  static constexpr PJ_joinable_job_vtable_t kJobVtable{
      sizeof(PJ_joinable_job_vtable_t), 0, &DescriptorImportProvider::jobCancel,
      &DescriptorImportProvider::jobJoin, &DescriptorImportProvider::jobDestroy};

  CredentialStore& credentialStore();

  ImportRuntime& runtime_;
  PJ::sdk::SettingsView settings_{};
  HostBindings bindings_{};
  std::unique_ptr<CredentialStore> credentials_;  // lazily constructed (config root)

  // query-result string storage: owned by the provider, valid until the NEXT
  // query on this instance (ABI lifetime rule). Main-thread only, like the
  // query itself.
  std::string query_identity_;
  std::string query_path_;
  std::string query_message_;

  // test seams
  std::function<void()> start_gate_probe_;
  std::function<void()> pre_terminal_hook_;
  std::chrono::milliseconds lock_poll_{50};
  std::chrono::milliseconds lock_timeout_{60000};
};

}  // namespace mcap_cloud
