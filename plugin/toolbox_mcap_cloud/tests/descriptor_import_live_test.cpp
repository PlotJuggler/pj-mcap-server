// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// LIVE descriptor-import round trip (stage-4 PR-3 commit 4): query (miss) ->
// start_import -> terminal -> query (materialized hit), against a real
// pj-cloud server. Self-skips without MCAP_CLOUD_LIVE_URL (the
// parser_ingest_live_test gating pattern); the smoke server runs
// -allow-anonymous, so credential resolution's dev-anonymous fallback is the
// live shape. The smoke-integrated §12 round trip (PR-4): when
// MCAP_CLOUD_LIVE_CACHE_COPY names a destination path, the finalized cache
// file is copied there so smoke.sh can mcapdiff it against a direct
// mcap-cloud-cli download of the SAME tuple (key + /imu + full range +
// --latched) — the cache-is-the-sole-encoder equality gate.
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <system_error>
#include <vector>

#include <pj_base/descriptor_import_protocol.h>
#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/settings_store_host.hpp>

#include "descriptor_import_provider.hpp"
#include "fake_toolbox_host.hpp"
#include "import_runtime.hpp"
#include "parser_ingest_test_support.hpp"
#include "scoped_job.hpp"
#include "session_file_cache.hpp"
#include "source_descriptor.hpp"
#include "test_support_fs.hpp"
#include "trusted_origins.hpp"

namespace {

namespace fs = std::filesystem;

const char* liveUrl() { return std::getenv("MCAP_CLOUD_LIVE_URL"); }

// Ground truth pinned in lockstep with smoke.sh + backend_connection_live_test.
constexpr const char* kSeq =
    "customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-24/ci_synth_big.mcap";
constexpr const char* kImuTopic = "/imu";
constexpr std::uint64_t kImuMessages = 2000;

struct TempRoot : mcap_cloud_test::ScopedTempDir {
  explicit TempRoot(const std::string& name) : ScopedTempDir("mcap-cloud-import-live-" + name) {}
};

struct LiveRecorder {
  std::mutex mu;
  std::condition_variable cv;
  std::vector<std::uint32_t> datasets;
  std::vector<std::pair<PJ_descriptor_import_outcome_t, std::string>> terminals;

  static void onDataset(void* ctx, PJ_data_source_handle_t dataset) noexcept {
    auto* self = static_cast<LiveRecorder*>(ctx);
    const std::lock_guard<std::mutex> lock(self->mu);
    self->datasets.push_back(dataset.id);
  }
  static void onTerminal(void* ctx, PJ_descriptor_import_outcome_t outcome,
                         PJ_string_view_t message) noexcept {
    auto* self = static_cast<LiveRecorder*>(ctx);
    {
      const std::lock_guard<std::mutex> lock(self->mu);
      self->terminals.emplace_back(
          outcome, std::string(message.data == nullptr ? "" : message.data, message.size));
    }
    self->cv.notify_all();
  }
  bool waitTerminal(std::chrono::seconds timeout) {
    std::unique_lock<std::mutex> lock(mu);
    return cv.wait_for(lock, timeout, [this] { return !terminals.empty(); });
  }
};

}  // namespace

TEST(McapCloudDescriptorImportLive, QueryImportTerminalThenMaterializedHit) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live descriptor-import test skipped";
  }

  TempRoot cache_root("cache");
  TempRoot config_root("config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));
  mcap_cloud::testsupport::FakeToolboxHost host;
  pj_ingest_test::FakeIngestHost ingest(/*with_progress_slots=*/true);
  PJ::sdk::InMemorySettingsBackend backend;
  PJ::sdk::SettingsStoreHost settings_host{backend};

  mcap_cloud::DescriptorImportProvider provider(rt);
  provider.bind(PJ::sdk::SettingsView{settings_host.view()},
                {[&host]() { return host.view(); },
                 [&ingest]() { return PJ::ToolboxRuntimeHostView{ingest.toolboxRuntime()}; }});

  mcap_cloud::SourceDescriptor d;
  d.version = 1;
  d.kind = "mcap-cloud-session";
  d.server_uri = url;
  d.s3_keys = {kSeq};
  d.topics = {kImuTopic};
  d.display_name = "live-import";
  const std::string json = mcap_cloud::toSourceDescriptorJson(d);
  const std::string identity = mcap_cloud::descriptorIdentity(d);

  // 1) query: a miss (fresh cache root), identity + planned path returned.
  PJ_descriptor_query_result_v1_t out{};
  out.struct_size = sizeof(out);
  PJ_error_t err{};
  ASSERT_TRUE(provider.queryDescriptor(PJ_string_view_t{json.data(), json.size()}, &out, &err))
      << err.message;
  EXPECT_EQ(out.is_materialized, 0u);
  EXPECT_EQ(std::string(out.source_identity.data, out.source_identity.size), identity);

  // 2) import: full round trip to the exactly-once terminal. No promotion
  //    host is bound -> SUCCEEDED_EAGER_ONLY is the correct live terminal.
  //    ScopedJob (IMPORTANT-4): declared AFTER provider/rt/hosts so a failing
  //    ASSERT below destroys (cancel+join) the worker BEFORE they unwind.
  LiveRecorder recorder;
  PJ_descriptor_import_start_request_v1_t request{};
  request.struct_size = sizeof(request);
  request.descriptor_json = PJ_string_view_t{json.data(), json.size()};
  PJ_descriptor_import_callbacks_v1_t cbs{};
  cbs.struct_size = sizeof(cbs);
  cbs.on_dataset = &LiveRecorder::onDataset;
  cbs.on_terminal = &LiveRecorder::onTerminal;
  mcap_cloud_test::ScopedJob job;
  ASSERT_TRUE(provider.startImport(&request, &cbs, &recorder, &job.job, &err)) << err.message;
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(120)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminals.size(), 1u);
  EXPECT_EQ(recorder.terminals[0].first, PJ_DESCRIPTOR_IMPORT_SUCCEEDED_EAGER_ONLY)
      << recorder.terminals[0].second;
  ASSERT_EQ(recorder.datasets.size(), 1u) << "on_dataset exactly once";

  // The eager ingest decoded the pinned ground truth through the fake host.
  EXPECT_EQ(ingest.pushes.size(), kImuMessages);

  // 3) the cache file finalized + a re-query answers materialized with the
  //    file size as the estimate.
  fs::path cache_file;
  ASSERT_TRUE(rt.fileCache().lookup(identity, &cache_file));
  out = PJ_descriptor_query_result_v1_t{};
  out.struct_size = sizeof(out);
  ASSERT_TRUE(provider.queryDescriptor(PJ_string_view_t{json.data(), json.size()}, &out, &err));
  EXPECT_EQ(out.is_materialized, 1u);
  EXPECT_GT(out.estimated_bytes, 0u);

  // 4) smoke's §12 round-trip hook: export the finalized cache file (the
  //    TempRoot above is destroyed with this scope) so the harness of record
  //    can compare it against a direct CLI download of the same tuple.
  //    Env-gated and test-only; unset (hermetic ctest, plain live runs) it is
  //    inert.
  const char* copy_dest = std::getenv("MCAP_CLOUD_LIVE_CACHE_COPY");
  if (copy_dest != nullptr && *copy_dest != '\0') {
    std::error_code copy_ec;
    fs::copy_file(cache_file, fs::path(copy_dest), fs::copy_options::overwrite_existing, copy_ec);
    ASSERT_FALSE(copy_ec) << "cache-file export to " << copy_dest
                          << " failed: " << copy_ec.message();
  }
  // ScopedJob destroys (cancel+join+free) on scope exit.
}
