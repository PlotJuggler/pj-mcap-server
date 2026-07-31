// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// McapCloudToolbox implementation + the plugin exports. Both vtables
// (toolbox + dialog) are exported from this one .so. The dialog is
// borrowed (DialogPresenter keys on HAS_DIALOG + getDialog()); the toolbox
// binds the toolbox write host + runtime host and hands them to the dialog as
// providers, so the in-dialog FetchWorker can ingest on fetch completion and
// notifyDataChanged() so the catalog tree rebuilds.

#include "mcap_cloud_toolbox.hpp"

#include <pj_base/sdk/platform.hpp>
#include <pj_plugins/sdk/dialog_plugin_typed.hpp>

#include "mcap_cloud_panel_manifest.hpp"

namespace mcap_cloud {

McapCloudToolbox::McapCloudToolbox() {
  // ONE ImportRuntime per toolbox instance (stage-4 PR-1): the dialog's
  // FetchWorker and the future descriptor-import provider (PR-3) share its
  // SessionFileCache, thread-safe SessionCache, host-write mutex, in-memory
  // trust set and materialization registry. Wired in the constructor —
  // before bind()/setSettings can enqueue any worker command.
  //
  // Residual (§6.3-reviewed, not a violation): constructing the dialog here
  // starts its command-pump worker THREAD, which then idles — no network,
  // no credential access, no settings read happens until a command is
  // queued, and nothing queues one before the first getDialog() runs the
  // one-shot interactive init.
  dialog_.setImportRuntime(&import_runtime_);
}

uint64_t McapCloudToolbox::capabilities() const {
  // Non-modal panel (the Mosaico shape): HAS_DIALOG + NON_MODAL_DIALOG.
  return PJ::kToolboxCapabilityHasDialog | PJ::kToolboxCapabilityNonModalDialog;
}

PJ::Status McapCloudToolbox::bind(PJ::sdk::ServiceRegistry services) {
  auto status = ToolboxPluginBase::bind(services);
  if (!status) {
    return status;
  }
  // Once the toolbox host is bound, hand a host-view provider to the dialog so
  // the worker can ingest decoded scalars on fetch completion.
  dialog_.setHostProvider([this]() { return toolboxHost(); });
  // Runtime host carries notifyDataChanged — the dialog calls it after a
  // successful import so the app flushes pending writer chunks and rebuilds
  // the catalog tree. Without it, ingested data stays buffered and the
  // dataset panel never sees the new topics.
  dialog_.setRuntimeHostProvider([this]() { return runtimeHost(); });
  // Store the optional pj.settings.v1 view (QSettings-like persistence). An
  // absent service yields an unbound view that reads defaults. STORE-ONLY:
  // the persisted-UI restore + auto-connect run at the first getDialog()
  // (spec §6.3 — a headless provider bind must stay network-free), so a
  // re-bind after initialization swaps the view without reconnecting.
  dialog_.setSettings(services.get<PJ::sdk::SettingsStoreService>().value_or(PJ::sdk::SettingsView{}));
  // OPTIONAL pj.source_promotion.v1 (D6): get<>, never require<> — absence
  // means a host without promotion support, and every completed materialize
  // is then EAGER_ONLY-equivalent. Bound per plugin instance (the host
  // derives the provider manifest id from the binding); this instance owns
  // both the runtime and the bound registry scope, so the view stays alive
  // while any promotion is outstanding.
  import_runtime_.setPromotionHost(services.get<PJ::sdk::SourcePromotionHostService>());
  // The descriptor-import provider (PR-3): same settings view as the dialog
  // (credential resolution at IMPORT time, main-thread), plus the host
  // providers its per-job pulls ingest through. Network-free — the provider
  // touches the network only inside an authorized start_import (spec §6.3).
  provider_.bind(services.get<PJ::sdk::SettingsStoreService>().value_or(PJ::sdk::SettingsView{}),
                 {[this]() { return toolboxHost(); }, [this]() { return runtimeHost(); }});
  return PJ::okStatus();
}

const void* McapCloudToolbox::pluginExtension(std::string_view id) {
  if (id == PJ_DESCRIPTOR_IMPORT_EXTENSION_V1) {
    return &descriptor_import_ext_;
  }
  return nullptr;
}

bool McapCloudToolbox::descriptorQueryThunk(void* plugin_ctx, PJ_string_view_t descriptor_json,
                                            PJ_descriptor_query_result_v1_t* out_result,
                                            PJ_error_t* out_error) noexcept {
  if (plugin_ctx == nullptr) {
    PJ::sdk::fillError(out_error, 1, "mcap_cloud", "null plugin_ctx");
    return false;
  }
  // plugin_ctx is the toolbox instance the host called get_plugin_extension
  // with (the PJ_TOOLBOX_PLUGIN create_fn returns `new McapCloudToolbox()`
  // as void*, so this cast is exact). The provider walls exceptions itself.
  return static_cast<McapCloudToolbox*>(plugin_ctx)
      ->provider_.queryDescriptor(descriptor_json, out_result, out_error);
}

bool McapCloudToolbox::descriptorStartThunk(void* plugin_ctx,
                                            const PJ_descriptor_import_start_request_v1_t* request,
                                            const PJ_descriptor_import_callbacks_v1_t* callbacks,
                                            void* callback_ctx, PJ_joinable_job_t* out_job,
                                            PJ_error_t* out_error) noexcept {
  if (plugin_ctx == nullptr) {
    PJ::sdk::fillError(out_error, 1, "mcap_cloud", "null plugin_ctx");
    return false;
  }
  return static_cast<McapCloudToolbox*>(plugin_ctx)
      ->provider_.startImport(request, callbacks, callback_ctx, out_job, out_error);
}

PJ_borrowed_dialog_t McapCloudToolbox::getDialog() {
  // The FIRST call runs the one-shot interactive init (persisted-state
  // restore + auto-connect) — PJ4 reaches getDialog() only when the panel is
  // actually presented, so a headless provider bind never triggers it (spec
  // §6.3). initFromSettings sets connecting=true BEFORE queueing the
  // connectAsync command, so the panel renders the connect-in-progress state
  // from its very first widget_data(). Latched once per plugin lifetime:
  // repeated calls (and re-binds) are plain borrows.
  dialog_.ensureInitFromSettings();
  return PJ::borrowDialog(dialog_);
}

}  // namespace mcap_cloud

PJ_TOOLBOX_PLUGIN(mcap_cloud::McapCloudToolbox, kMcapCloudPanelManifest)
PJ_DIALOG_PLUGIN(mcap_cloud::McapCloudDialog, kMcapCloudPanelManifest)
