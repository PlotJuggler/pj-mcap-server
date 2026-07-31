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
  return PJ::okStatus();
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
