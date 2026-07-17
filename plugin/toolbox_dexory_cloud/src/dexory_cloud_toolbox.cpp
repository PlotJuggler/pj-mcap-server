// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// DexoryCloudToolbox — the cloud TOOLBOX entry (Slice 5 restore).
//
// The cloud connector IS a cloud TOOLBOX (the Mosaico-style non-modal panel: browse
// catalog, Lua filter, select sequence+topics+time-range, Fetch downloads right
// there). It does NOT appear in the Streaming combo. This REPLACES the Slice-4
// PjCloudSource : StreamSourceBase shape (pj_cloud_source.cpp, deleted).
//
// Ingest path: HOST-DELEGATED parsing via ParserIngestDriver (Slice 16).
// The toolbox parser-ingest tail slots (create_parser_ingest /
// release_parser_ingest + the data-source runtime ensure_parser_binding /
// push_message) decode every ROS2/CDR topic through the host's registered
// MessageParser plugins — the SDK 0.6.1 toolbox parser-ingest path. The plugin
// ships ZERO message decoders; tf/pointclouds/images arrive as ObjectStore
// object topics with render-time parsers registered by the host.
//
// Both vtables (toolbox + dialog) are exported from this one .so. The dialog is
// borrowed (DialogPresenter keys on HAS_DIALOG + getDialog()); the toolbox binds
// the toolbox write host + runtime host and hands them to the dialog as
// providers, so the in-dialog FetchWorker can ingest on fetch completion and
// notifyDataChanged() so the catalog tree rebuilds.

#include <pj_base/sdk/platform.hpp>
#include <pj_base/sdk/toolbox_plugin_base.hpp>
#include <pj_plugins/sdk/dialog_plugin_typed.hpp>

#include "dexory_cloud_dialog.hpp"
#include "dexory_cloud_panel_manifest.hpp"

namespace dexory_cloud {

class DexoryCloudToolbox : public PJ::ToolboxPluginBase {
 public:
  uint64_t capabilities() const override {
    // Non-modal panel (the Mosaico shape): HAS_DIALOG + NON_MODAL_DIALOG.
    return PJ::kToolboxCapabilityHasDialog | PJ::kToolboxCapabilityNonModalDialog;
  }

  PJ::Status bind(PJ::sdk::ServiceRegistry services) override {
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
    // Bind the optional pj.settings.v1 store (QSettings-like persistence) and
    // restore persisted UI state + auto-connect. An absent service yields an
    // unbound view that reads defaults.
    dialog_.setSettings(services.get<PJ::sdk::SettingsStoreService>().value_or(PJ::sdk::SettingsView{}));
    return PJ::okStatus();
  }

  PJ_borrowed_dialog_t getDialog() override {
    return PJ::borrowDialog(dialog_);
  }

 private:
  DexoryCloudDialog dialog_;
};

}  // namespace dexory_cloud

PJ_TOOLBOX_PLUGIN(dexory_cloud::DexoryCloudToolbox, kDexoryCloudPanelManifest)
PJ_DIALOG_PLUGIN(dexory_cloud::DexoryCloudDialog, kDexoryCloudPanelManifest)
