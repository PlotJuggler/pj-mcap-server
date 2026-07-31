// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// McapCloudToolbox — the cloud TOOLBOX entry (Slice 5 restore).
//
// The cloud connector IS a cloud TOOLBOX (the Mosaico-style non-modal panel:
// browse catalog, Lua filter, select sequence+topics+time-range, Fetch
// downloads right there). It does NOT appear in the Streaming combo. This
// REPLACES the Slice-4 PjCloudSource : StreamSourceBase shape
// (pj_cloud_source.cpp, deleted).
//
// Ingest path: HOST-DELEGATED parsing via ParserIngestDriver (Slice 16).
// The toolbox parser-ingest tail slots (create_parser_ingest /
// release_parser_ingest + the data-source runtime ensure_parser_binding /
// push_message) decode every ROS2/CDR topic through the host's registered
// MessageParser plugins — the SDK 0.6.1 toolbox parser-ingest path. The plugin
// ships ZERO message decoders; tf/pointclouds/images arrive as ObjectStore
// object topics with render-time parsers registered by the host.
//
// Split out of mcap_cloud_toolbox.cpp (stage-4 PR-2) so the headless-init
// tests can drive the REAL class through a fake service registry; the .cpp
// keeps the PJ_TOOLBOX_PLUGIN / PJ_DIALOG_PLUGIN exports.
#pragma once

#include <pj_base/sdk/toolbox_plugin_base.hpp>

#include "import_runtime.hpp"
#include "mcap_cloud_dialog.hpp"

namespace mcap_cloud {

class McapCloudToolbox : public PJ::ToolboxPluginBase {
 public:
  McapCloudToolbox();

  uint64_t capabilities() const override;

  // Wires the dialog's host/runtime providers + the settings view. Network-
  // free by contract (spec docs/canonical-layout-import.md §6.3: a provider
  // bind must not run the interactive dialog initialization / auto-connect);
  // the interactive init runs at the first getDialog() instead.
  PJ::Status bind(PJ::sdk::ServiceRegistry services) override;

  // Borrow the embedded dialog. The FIRST call runs the one-shot interactive
  // initialization (persisted-state restore + auto-connect) — see
  // McapCloudDialog::ensureInitFromSettings for the once-per-plugin-lifetime
  // latch semantics.
  PJ_borrowed_dialog_t getDialog() override;

 private:
  // Declared BEFORE dialog_ so the runtime outlives the dialog (and its
  // worker thread) on destruction. Standard roots: the user cache dir
  // (MCAP_CLOUD_CACHE_DIR || XDG) + the trusted-origins ledger under the
  // config root — construction reads only the ledger, no network.
  ImportRuntime import_runtime_{SessionFileCache::standard(nullptr), TrustedOrigins::standard()};
  McapCloudDialog dialog_;
};

}  // namespace mcap_cloud
