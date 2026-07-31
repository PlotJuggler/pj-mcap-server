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

#include <string_view>

#include <pj_base/descriptor_import_protocol.h>
#include <pj_base/sdk/toolbox_plugin_base.hpp>

#include "descriptor_import_provider.hpp"
#include "import_runtime.hpp"
#include "mcap_cloud_dialog.hpp"

namespace mcap_cloud {

class McapCloudToolbox : public PJ::ToolboxPluginBase {
 public:
  McapCloudToolbox();

  uint64_t capabilities() const override;

  // pj.descriptor_import.v1 (stage-4 PR-3): returns the provider extension
  // table for PJ_DESCRIPTOR_IMPORT_EXTENSION_V1, nullptr for anything else.
  // The ABI's plugin_ctx at the thunks is THIS toolbox instance (the same
  // ctx get_plugin_extension was called with — never the extension table).
  const void* pluginExtension(std::string_view id) override;

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

  // The per-instance ImportRuntime (PR-1) — shared by the dialog worker and
  // the descriptor-import provider; the promotion host (D6) binds onto it.
  // Exposed for the provider glue and the hermetic wiring tests.
  [[nodiscard]] ImportRuntime& importRuntime() { return import_runtime_; }
  [[nodiscard]] DescriptorImportProvider& descriptorImportProvider() { return provider_; }

 private:
  // The C thunks behind descriptor_import_ext_: plugin_ctx is THIS toolbox
  // instance; they forward to provider_ (exception-walled at the boundary).
  static bool descriptorQueryThunk(void* plugin_ctx, PJ_string_view_t descriptor_json,
                                   PJ_descriptor_query_result_v1_t* out_result,
                                   PJ_error_t* out_error) noexcept;
  static bool descriptorStartThunk(void* plugin_ctx,
                                   const PJ_descriptor_import_start_request_v1_t* request,
                                   const PJ_descriptor_import_callbacks_v1_t* callbacks,
                                   void* callback_ctx, PJ_joinable_job_t* out_job,
                                   PJ_error_t* out_error) noexcept;

  // Declared BEFORE dialog_ so the runtime outlives the dialog (and its
  // worker thread) on destruction — and BEFORE provider_ (the provider
  // borrows the runtime). Standard roots: the user cache dir
  // (MCAP_CLOUD_CACHE_DIR || XDG) + the trusted-origins ledger under the
  // config root — construction reads only the ledger, no network.
  // NOTE (ABI): the host must destroy every started import job before
  // destroying this plugin instance — reverse member order then tears down
  // dialog_, provider_, import_runtime_ with no job threads left.
  ImportRuntime import_runtime_{SessionFileCache::standard(nullptr), TrustedOrigins::standard()};
  DescriptorImportProvider provider_{import_runtime_};
  // The plugin-owned extension table (must stay valid for the instance
  // lifetime — ABI rule); the thunks resolve the provider via plugin_ctx.
  PJ_descriptor_import_provider_v1_t descriptor_import_ext_{
      sizeof(PJ_descriptor_import_provider_v1_t), 0, &McapCloudToolbox::descriptorQueryThunk,
      &McapCloudToolbox::descriptorStartThunk};
  McapCloudDialog dialog_;
};

}  // namespace mcap_cloud
