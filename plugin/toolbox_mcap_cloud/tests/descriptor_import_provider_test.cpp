// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC pj.descriptor_import.v1 provider matrix (stage-4 PR-3 commit 3,
// D2/D3/D4-as-amended), driving DescriptorImportProvider's raw C surface the
// way a host would. Pins:
//   query (main-thread, strictly bounded):
//     - malformed OR unsupported descriptor -> false + error (a CONTRACT
//       failure, never a trust verdict);
//     - source_identity + local_path_utf8 ALWAYS returned (hit or miss),
//       offsetof-covered writes only;
//     - trust: in-memory set only (isTrusted -> trusted, else
//       needs_confirmation; kRefused unused in v1);
//     - is_materialized: DISK-validated cache lookup ONLY — an in-memory
//       SessionCache entry must never flip it;
//     - estimated_bytes = file size when materialized, else 0.
//   start_import:
//     - unknown flag bits fail closed FIRST (false + error, no callbacks,
//       out_job untouched — canary-checked);
//     - null/short callbacks rejected (on_terminal is required);
//     - malformed descriptor rejected;
//     - the post-return START GATE: no callback can run before start_import
//       returns (probe seam);
//   the job runner:
//     - full round trip against the fake streaming server: on_dataset
//       exactly once BEFORE the exactly-once terminal; SUCCEEDED_PROMOTED
//       with a promotion host, SUCCEEDED_EAGER_ONLY without; cache file
//       finalized;
//     - cancel -> CANCELLED, partial deleted; ceiling -> FAILED naming the
//       byte cause; lock contention -> bounded wait + actionable-retry
//       FAILED; the concurrent-materialize race -> FAILED
//       "materialized concurrently" (the locked v1 decision — see the
//       provider's runToTerminal comment);
//     - cancel/join/destroy: idempotent cancel, join-after-terminal,
//       destroy-immediately-after-start safe.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <pj_base/descriptor_import_protocol.h>
#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/settings_store_host.hpp>

#include "descriptor_import_provider.hpp"
#include "fake_promotion_host.hpp"
#include "fake_streaming_server.hpp"
#include "fake_toolbox_host.hpp"
#include "import_runtime.hpp"
#include "parser_ingest_test_support.hpp"
#include "scoped_job.hpp"
#include "session_file_cache.hpp"
#include "source_descriptor.hpp"
#include "test_support_env.hpp"
#include "test_support_fs.hpp"
#include "trusted_origins.hpp"

namespace {

namespace fs = std::filesystem;
using mcap_cloud_test::FakeStreamingServer;
using mcap_cloud_test::cacheRootHasPartial;
using mcap_cloud_test::descriptorFor;
using mcap_cloud_test::identityFor;

struct TempRoot : mcap_cloud_test::ScopedTempDir {
  explicit TempRoot(const std::string& name) : ScopedTempDir("mcap-cloud-provider-" + name) {}
};

// Records the exactly-two ABI callbacks with ordering + counting.
struct JobRecorder {
  std::mutex mu;
  std::condition_variable cv;
  std::vector<std::uint32_t> datasets;
  std::vector<std::pair<PJ_descriptor_import_outcome_t, std::string>> terminals;
  int datasets_before_terminal = -1;

  static void onDataset(void* ctx, PJ_data_source_handle_t dataset) noexcept {
    auto* self = static_cast<JobRecorder*>(ctx);
    const std::lock_guard<std::mutex> lock(self->mu);
    self->datasets.push_back(dataset.id);
  }
  static void onTerminal(void* ctx, PJ_descriptor_import_outcome_t outcome,
                         PJ_string_view_t message) noexcept {
    auto* self = static_cast<JobRecorder*>(ctx);
    {
      const std::lock_guard<std::mutex> lock(self->mu);
      self->datasets_before_terminal = static_cast<int>(self->datasets.size());
      self->terminals.emplace_back(
          outcome, std::string(message.data == nullptr ? "" : message.data, message.size));
    }
    self->cv.notify_all();
  }

  [[nodiscard]] PJ_descriptor_import_callbacks_v1_t callbacks() {
    PJ_descriptor_import_callbacks_v1_t cbs{};
    cbs.struct_size = sizeof(cbs);
    cbs.on_dataset = &JobRecorder::onDataset;
    cbs.on_terminal = &JobRecorder::onTerminal;
    return cbs;
  }

  bool waitTerminal(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu);
    return cv.wait_for(lock, timeout, [this] { return !terminals.empty(); });
  }
  [[nodiscard]] std::size_t terminalCount() {
    const std::lock_guard<std::mutex> lock(mu);
    return terminals.size();
  }
  [[nodiscard]] std::pair<PJ_descriptor_import_outcome_t, std::string> terminal(std::size_t i) {
    const std::lock_guard<std::mutex> lock(mu);
    return terminals.at(i);
  }
  [[nodiscard]] std::size_t datasetCount() {
    const std::lock_guard<std::mutex> lock(mu);
    return datasets.size();
  }
};

// RAII job handle hoisted to scoped_job.hpp (shared with the live suite).
using mcap_cloud_test::ScopedJob;

// The wired provider harness: runtime over temp roots, in-memory settings,
// fake toolbox + ingest hosts. NO dialog, NO getDialog — the provider must
// work on a fully cold interactive path.
struct ProviderHarness {
  explicit ProviderHarness(const std::string& name)
      : env("mcap-cloud-provider-" + name),
        cache_root(name + "-cacheroot"),
        config_root(name + "-configroot"),
        rt(mcap_cloud::SessionFileCache(cache_root.path), mcap_cloud::TrustedOrigins(config_root.path)),
        ingest(/*with_progress_slots=*/true),
        provider(rt) {
    provider.bind(PJ::sdk::SettingsView{settings_host.view()},
                  {[this]() { return host.view(); },
                   [this]() { return PJ::ToolboxRuntimeHostView{ingest.toolboxRuntime()}; }});
  }

  [[nodiscard]] static std::string descriptorJson(const std::string& uri) {
    return mcap_cloud::toSourceDescriptorJson(descriptorFor(uri));
  }

  // Convenience start over the provider's raw C surface.
  bool start(const std::string& descriptor_json, JobRecorder& recorder, ScopedJob& out,
             std::uint64_t max_transfer_bytes = 0, std::string* error_out = nullptr) {
    PJ_descriptor_import_start_request_v1_t request{};
    request.struct_size = sizeof(request);
    request.descriptor_json = PJ_string_view_t{descriptor_json.data(), descriptor_json.size()};
    request.flags = PJ_DESCRIPTOR_IMPORT_START_FLAG_NONE;
    request.max_transfer_bytes = max_transfer_bytes;
    const PJ_descriptor_import_callbacks_v1_t cbs = recorder.callbacks();
    PJ_error_t err{};
    const bool ok = provider.startImport(&request, &cbs, &recorder, &out.job, &err);
    if (!ok && error_out != nullptr) {
      *error_out = err.message;
    }
    return ok;
  }

  mcap_cloud_test::HermeticEnv env;
  TempRoot cache_root;
  TempRoot config_root;
  mcap_cloud::ImportRuntime rt;
  mcap_cloud::testsupport::FakeToolboxHost host;
  pj_ingest_test::FakeIngestHost ingest;
  PJ::sdk::InMemorySettingsBackend backend;
  PJ::sdk::SettingsStoreHost settings_host{backend};
  mcap_cloud::DescriptorImportProvider provider;
};

// The re-entrant accepting/failing promotion fake (see the promotion suite).
struct FakePromotionHost {
  bool succeed = true;
  std::atomic<int> calls{0};
  static bool promoteThunk(void* ctx, const PJ_source_promotion_request_v1_t*,
                           PJ_source_promotion_result_fn result_cb, void* callback_ctx,
                           PJ_error_t*) noexcept {
    auto* self = static_cast<FakePromotionHost*>(ctx);
    self->calls.fetch_add(1);
    const char* msg = self->succeed ? "promoted" : "rolled back";
    result_cb(callback_ctx, self->succeed, PJ_string_view_t{msg, std::char_traits<char>::length(msg)});
    return true;
  }
  [[nodiscard]] PJ_source_promotion_host_t view() {
    static constexpr PJ_source_promotion_host_vtable_t kVtable{
        1, sizeof(PJ_source_promotion_host_vtable_t), &FakePromotionHost::promoteThunk};
    return PJ_source_promotion_host_t{this, &kVtable};
  }
};

std::string queryJson(const std::string& uri) { return ProviderHarness::descriptorJson(uri); }

bool runQuery(mcap_cloud::DescriptorImportProvider& provider, const std::string& json,
              PJ_descriptor_query_result_v1_t* out, std::string* error_out = nullptr) {
  PJ_error_t err{};
  out->struct_size = sizeof(*out);
  const bool ok = provider.queryDescriptor(PJ_string_view_t{json.data(), json.size()}, out, &err);
  if (!ok && error_out != nullptr) {
    *error_out = err.message;
  }
  return ok;
}

}  // namespace

// ---------------------------------------------------------------------------
// query_descriptor
// ---------------------------------------------------------------------------

TEST(McapCloudProviderQuery, MalformedOrUnsupportedDescriptorFailsAsContract) {
  ProviderHarness h("query-malformed");
  PJ_descriptor_query_result_v1_t out{};
  std::string error;
  EXPECT_FALSE(runQuery(h.provider, "not json at all", &out, &error));
  EXPECT_FALSE(error.empty());

  // Unsupported version: query false (malformed OR unsupported — the SDK's
  // one contract-failure channel), NEVER a kRefused trust verdict.
  std::string v2 = queryJson("ws://127.0.0.1:9");
  const auto pos = v2.find("\"v\":1");
  ASSERT_NE(pos, std::string::npos);
  v2.replace(pos, 5, "\"v\":2");
  EXPECT_FALSE(runQuery(h.provider, v2, &out, &error));
  EXPECT_FALSE(error.empty());
}

TEST(McapCloudProviderQuery, AlwaysReturnsIdentityAndPathWithTrustMatrix) {
  ProviderHarness h("query-matrix");
  const std::string uri = "ws://127.0.0.1:9";
  const std::string json = queryJson(uri);
  const std::string identity = identityFor(uri);

  PJ_descriptor_query_result_v1_t out{};
  ASSERT_TRUE(runQuery(h.provider, json, &out));
  EXPECT_EQ(out.trust, PJ_DESCRIPTOR_TRUST_NEEDS_CONFIRMATION) << "untrusted origin";
  EXPECT_EQ(out.is_materialized, 0u);
  EXPECT_EQ(std::string(out.source_identity.data, out.source_identity.size), identity);
  EXPECT_EQ(std::string(out.local_path_utf8.data, out.local_path_utf8.size),
            h.rt.fileCache().pathFor(identity).string());
  EXPECT_EQ(out.estimated_bytes, 0u);
  EXPECT_GT(out.message.size, 0u) << "needs-confirmation carries a diagnostic";

  // Trust flips through the in-memory set (recordSuccessfulHello).
  h.rt.recordSuccessfulHello(uri);
  ASSERT_TRUE(runQuery(h.provider, json, &out));
  EXPECT_EQ(out.trust, PJ_DESCRIPTOR_TRUST_TRUSTED);
}

TEST(McapCloudProviderQuery, InMemorySessionCacheNeverAnswersMaterialized) {
  ProviderHarness h("query-memnever");
  const std::string uri = "ws://127.0.0.1:9";
  // Plant an in-memory entry claiming a cache identity — with NO disk file.
  const PJ::cloud::SessionKey key =
      PJ::cloud::computeSessionKey(uri, {"a.mcap"}, {"/one"}, {0, 0}, true);
  mcap_cloud::CachedSession entry;
  entry.display_name = "a.mcap";
  entry.server_uri = uri;
  entry.dataset_id = 1;
  entry.cache_identity = identityFor(uri);
  entry.tee_outcome = mcap_cloud::TeeOutcome::kFinalized;
  h.rt.sessionCache().store(key, std::move(entry));

  PJ_descriptor_query_result_v1_t out{};
  ASSERT_TRUE(runQuery(h.provider, queryJson(uri), &out));
  EXPECT_EQ(out.is_materialized, 0u)
      << "is_materialized must come from the DISK-validated cache only (D3-as-amended)";
}

TEST(McapCloudProviderQuery, MaterializedAfterImportWithFileSizeEstimate) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("query-materialized");

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);

  PJ_descriptor_query_result_v1_t out{};
  ASSERT_TRUE(runQuery(h.provider, queryJson(server.uri()), &out));
  EXPECT_EQ(out.is_materialized, 1u);
  fs::path cache_file;
  ASSERT_TRUE(h.rt.fileCache().lookup(identityFor(server.uri()), &cache_file));
  std::error_code ec;
  EXPECT_EQ(out.estimated_bytes, static_cast<std::uint64_t>(fs::file_size(cache_file, ec)));
}

// ---------------------------------------------------------------------------
// start_import — synchronous rejections
// ---------------------------------------------------------------------------

TEST(McapCloudProviderStart, UnknownFlagBitsFailClosedFirst) {
  ProviderHarness h("start-flags");
  const std::string json = queryJson("ws://127.0.0.1:9");
  JobRecorder recorder;

  PJ_descriptor_import_start_request_v1_t request{};
  request.struct_size = sizeof(request);
  request.descriptor_json = PJ_string_view_t{json.data(), json.size()};
  request.flags = std::uint64_t{1} << 63;  // not in the v1 mask
  const PJ_descriptor_import_callbacks_v1_t cbs = recorder.callbacks();

  // out_job canary: pre-fill with sentinel bytes; a fail-closed rejection
  // must leave it byte-identical (out_job untouched).
  PJ_joinable_job_t job;
  std::memset(&job, 0xAB, sizeof(job));
  PJ_joinable_job_t canary;
  std::memcpy(&canary, &job, sizeof(job));

  PJ_error_t err{};
  EXPECT_FALSE(h.provider.startImport(&request, &cbs, &recorder, &job, &err));
  EXPECT_NE(std::string(err.message).find("flag"), std::string::npos) << err.message;
  EXPECT_EQ(std::memcmp(&job, &canary, sizeof(job)), 0) << "out_job must be untouched";
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(recorder.terminalCount(), 0u) << "no callbacks after a rejected start";
  EXPECT_EQ(recorder.datasetCount(), 0u);
}

TEST(McapCloudProviderStart, NullOrShortCallbacksRejected) {
  ProviderHarness h("start-callbacks");
  const std::string json = queryJson("ws://127.0.0.1:9");
  PJ_descriptor_import_start_request_v1_t request{};
  request.struct_size = sizeof(request);
  request.descriptor_json = PJ_string_view_t{json.data(), json.size()};
  PJ_joinable_job_t job{};
  PJ_error_t err{};

  // Null callbacks pointer.
  EXPECT_FALSE(h.provider.startImport(&request, nullptr, nullptr, &job, &err));

  // Null on_terminal.
  PJ_descriptor_import_callbacks_v1_t cbs{};
  cbs.struct_size = sizeof(cbs);
  cbs.on_dataset = &JobRecorder::onDataset;
  cbs.on_terminal = nullptr;
  EXPECT_FALSE(h.provider.startImport(&request, &cbs, nullptr, &job, &err));

  // struct_size too short to cover on_terminal.
  cbs.on_terminal = &JobRecorder::onTerminal;
  cbs.struct_size = offsetof(PJ_descriptor_import_callbacks_v1_t, on_terminal);
  EXPECT_FALSE(h.provider.startImport(&request, &cbs, nullptr, &job, &err));
}

TEST(McapCloudProviderStart, MalformedDescriptorRejected) {
  ProviderHarness h("start-malformed");
  JobRecorder recorder;
  ScopedJob job;
  std::string error;
  EXPECT_FALSE(h.start("{\"v\":1}", recorder, job, 0, &error));
  EXPECT_FALSE(error.empty());
  EXPECT_EQ(job.job.vtable, nullptr);
}

TEST(McapCloudProviderStart, NoCallbackBeforeStartReturns) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("start-gate");
  JobRecorder recorder;

  // The probe runs inside start_import AFTER out_job is populated and the
  // worker thread exists, but BEFORE the start gate is released: give a
  // rogue ungated worker generous time to fire a callback, then assert none
  // did — the ABI forbids any callback before start_import returns.
  std::atomic<bool> probed{false};
  h.provider.setStartGateProbeForTest([&recorder, &probed]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(recorder.terminalCount(), 0u) << "callback before start_import returned";
    EXPECT_EQ(recorder.datasetCount(), 0u) << "callback before start_import returned";
    probed.store(true);
  });

  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  EXPECT_TRUE(probed.load());
  EXPECT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);
}

// ---------------------------------------------------------------------------
// the job runner — terminals
// ---------------------------------------------------------------------------

TEST(McapCloudProviderJob, RoundTripPromotedWithDatasetBeforeTerminal) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-promoted");
  FakePromotionHost promo;
  h.rt.setPromotionHost(PJ::SourcePromotionHostView(promo.view()));

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_SUCCEEDED_PROMOTED)
      << recorder.terminal(0).second;
  ASSERT_EQ(recorder.datasetCount(), 1u) << "on_dataset exactly once";
  EXPECT_EQ(recorder.datasets_before_terminal, 1) << "on_dataset precedes on_terminal";
  EXPECT_EQ(promo.calls.load(), 1);

  fs::path cache_file;
  EXPECT_TRUE(h.rt.fileCache().lookup(identityFor(server.uri()), &cache_file));
}

TEST(McapCloudProviderJob, RoundTripEagerOnlyWithoutPromotionService) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-eager");

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_SUCCEEDED_EAGER_ONLY)
      << recorder.terminal(0).second;
  fs::path cache_file;
  EXPECT_TRUE(h.rt.fileCache().lookup(identityFor(server.uri()), &cache_file))
      << "the finalized cache file exists even in the EAGER_ONLY end";
}

TEST(McapCloudProviderJob, CancelMidDownloadIsCancelledAndDeletesPartial) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kStallAfterTwoBatches);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-cancel");

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  // Wait for the dataset (the download is live), then cancel.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (recorder.datasetCount() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_GT(recorder.datasetCount(), 0u);
  job.job.vtable->cancel(job.job.ctx);
  job.job.vtable->cancel(job.job.ctx);  // idempotent
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u) << "terminal exactly once under cancel";
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_CANCELLED);
  EXPECT_FALSE(cacheRootHasPartial(h.cache_root.path)) << "partials never survive";
  fs::path unused;
  EXPECT_FALSE(h.rt.fileCache().lookup(identityFor(server.uri()), &unused));
}

TEST(McapCloudProviderJob, ByteCeilingFailsNamingTheByteCause) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-ceiling");

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job, /*max_transfer_bytes=*/1));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_FAILED)
      << "a ceiling abort must be FAILED, never CANCELLED";
  EXPECT_NE(recorder.terminal(0).second.find("byte"), std::string::npos)
      << recorder.terminal(0).second;
}

TEST(McapCloudProviderJob, LockContentionFailsActionablyAfterBoundedWait) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-lockbusy");
  h.provider.setLockWaitForTest(std::chrono::milliseconds(20), std::chrono::milliseconds(200));

  // Hold the in-process registry slot for the whole test.
  auto ticket = h.rt.tryBeginMaterialize(identityFor(server.uri()));
  ASSERT_TRUE(ticket.has_value());

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(10)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_FAILED);
  EXPECT_NE(recorder.terminal(0).second.find("in progress"), std::string::npos)
      << "the contention failure must be actionable: " << recorder.terminal(0).second;
  EXPECT_EQ(recorder.datasetCount(), 0u) << "zero on_dataset — no eager ingest ran";
}

TEST(McapCloudProviderJob, ConcurrentMaterializeRaceFailsAsRetryClassification) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-lockrace");
  h.provider.setLockWaitForTest(std::chrono::milliseconds(20), std::chrono::seconds(10));

  // 1) Materialize the session (a completed import) so the cache is VALID.
  {
    JobRecorder recorder;
    ScopedJob job;
    ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
    ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
    job.job.vtable->join(job.job.ctx);
  }
  fs::path cache_file;
  ASSERT_TRUE(h.rt.fileCache().lookup(identityFor(server.uri()), &cache_file));

  // 2) Hold the registry slot (the "concurrent materialization"), start the
  //    job (it enters the bounded wait), then release: the job acquires the
  //    slot AFTER waiting, revalidates, finds the fresh valid file — the
  //    locked v1 decision: FAIL with the retry-classification message (a
  //    reload classifies hit; on_dataset must never fire — there was no
  //    eager ingest to announce).
  auto ticket = h.rt.tryBeginMaterialize(identityFor(server.uri()));
  ASSERT_TRUE(ticket.has_value());
  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  std::this_thread::sleep_for(std::chrono::milliseconds(150));  // job is waiting
  EXPECT_EQ(recorder.terminalCount(), 0u);
  ticket.reset();  // the concurrent materialization "completes"
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(10)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_FAILED);
  EXPECT_NE(recorder.terminal(0).second.find("materialized concurrently"), std::string::npos)
      << recorder.terminal(0).second;
  EXPECT_EQ(recorder.datasetCount(), 0u) << "zero on_dataset in the race end";
}

// ---------------------------------------------------------------------------
// ABI adversarial set (PR-3 commit 4)
// ---------------------------------------------------------------------------

TEST(McapCloudProviderAbi, ShortQueryResultStructSizeLeavesUncoveredBytesUntouched) {
  ProviderHarness h("abi-shortquery");
  const std::string json = queryJson("ws://127.0.0.1:9");

  // Full allocation filled with a canary; struct_size covers only through
  // is_materialized. The provider must write trust+is_materialized and leave
  // every byte beyond the declared size EXACTLY as it found it (growth
  // contract: read/write only fields wholly covered by struct_size).
  PJ_descriptor_query_result_v1_t out;
  std::memset(&out, 0xAB, sizeof(out));
  out.struct_size = offsetof(PJ_descriptor_query_result_v1_t, source_identity);
  out.reserved0 = 0;
  PJ_error_t err{};
  ASSERT_TRUE(h.provider.queryDescriptor(PJ_string_view_t{json.data(), json.size()}, &out, &err));
  EXPECT_EQ(out.trust, PJ_DESCRIPTOR_TRUST_NEEDS_CONFIRMATION);
  EXPECT_EQ(out.is_materialized, 0u);

  const auto* bytes = reinterpret_cast<const unsigned char*>(&out);
  for (std::size_t i = out.struct_size; i < sizeof(out); ++i) {
    ASSERT_EQ(bytes[i], 0xAB) << "byte " << i << " beyond struct_size was written";
  }
}

TEST(McapCloudProviderAbi, QueryStringLifetimeCopiesSurviveTheNextQuery) {
  ProviderHarness h("abi-strlife");
  const std::string uri_a = "ws://127.0.0.1:9";
  const std::string uri_b = "ws://127.0.0.1:10";

  PJ_descriptor_query_result_v1_t out{};
  ASSERT_TRUE(runQuery(h.provider, queryJson(uri_a), &out));
  // The ABI-correct consumption pattern: COPY the views immediately (they
  // are only valid until the NEXT query on this instance). ASAN-friendly:
  // the stale views are never dereferenced after the second query.
  const std::string identity_a(out.source_identity.data, out.source_identity.size);
  const std::string path_a(out.local_path_utf8.data, out.local_path_utf8.size);

  ASSERT_TRUE(runQuery(h.provider, queryJson(uri_b), &out));
  const std::string identity_b(out.source_identity.data, out.source_identity.size);

  EXPECT_EQ(identity_a, identityFor(uri_a)) << "the copies keep the FIRST query's values";
  EXPECT_EQ(path_a, h.rt.fileCache().pathFor(identityFor(uri_a)).string());
  EXPECT_EQ(identity_b, identityFor(uri_b)) << "the second query serves its own values";
  EXPECT_NE(identity_a, identity_b);
}

TEST(McapCloudProviderAbi, CeilingVsCancelRaceProducesOneCancelledTerminal) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("abi-race");

  // Deterministic interleave via the pre-terminal seam: the pull returns
  // with the byte ceiling latched, THEN the job is cancelled before the
  // terminal mapping runs — both flags set. The documented precedence:
  // CANCEL WINS (an explicit caller cancel outranks the resource
  // classification; the partial is deleted either way) — and there is
  // EXACTLY ONE terminal.
  JobRecorder recorder;
  ScopedJob job;
  std::atomic<bool> hook_ran{false};
  h.provider.setPreTerminalHookForTest([&]() {
    if (job.job.vtable != nullptr) {
      job.job.vtable->cancel(job.job.ctx);
    }
    hook_ran.store(true);
  });
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job, /*max_transfer_bytes=*/1));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);

  EXPECT_TRUE(hook_ran.load());
  ASSERT_EQ(recorder.terminalCount(), 1u) << "both flags set must still yield ONE terminal";
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_CANCELLED)
      << "documented precedence: cancel wins over the byte ceiling — got: "
      << recorder.terminal(0).second;
}

TEST(McapCloudProviderAbi, DestroyDuringConnectUnblocksFast) {
  mcap_cloud_test::SilentTcpServer server;
  ASSERT_TRUE(server.ok());
  ProviderHarness h("abi-destroyconnect");

  JobRecorder recorder;
  PJ_joinable_job_t job{};
  const std::string json = queryJson(server.uri());
  PJ_descriptor_import_start_request_v1_t request{};
  request.struct_size = sizeof(request);
  request.descriptor_json = PJ_string_view_t{json.data(), json.size()};
  const PJ_descriptor_import_callbacks_v1_t cbs = recorder.callbacks();
  PJ_error_t err{};
  ASSERT_TRUE(h.provider.startImport(&request, &cbs, &recorder, &job, &err));

  // Let the job reach the (silent) WebSocket-open wait, then destroy: the
  // commit-1 cancellable open wait must unblock it well under the 10 s
  // handshake timeout.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  const auto destroy_at = std::chrono::steady_clock::now();
  job.vtable->destroy(job.ctx);
  const auto destroy_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - destroy_at);

  EXPECT_LT(destroy_ms.count(), 2000) << "destroy must cancel-wake the connect wait promptly";
  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_CANCELLED);
}

TEST(McapCloudProviderJob, DestroyImmediatelyAfterStartIsSafeAndTerminalsOnce) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kStallAfterTwoBatches);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-destroyfast");

  JobRecorder recorder;
  PJ_joinable_job_t job{};
  const std::string json = queryJson(server.uri());
  PJ_descriptor_import_start_request_v1_t request{};
  request.struct_size = sizeof(request);
  request.descriptor_json = PJ_string_view_t{json.data(), json.size()};
  const PJ_descriptor_import_callbacks_v1_t cbs = recorder.callbacks();
  PJ_error_t err{};
  ASSERT_TRUE(h.provider.startImport(&request, &cbs, &recorder, &job, &err));
  job.vtable->destroy(job.ctx);  // cancel+join+free, immediately

  ASSERT_EQ(recorder.terminalCount(), 1u) << "destroy implies the terminal already fired";
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_CANCELLED)
      << recorder.terminal(0).second;
}

// ---------------------------------------------------------------------------
// PR-3 quality-review set (IMPORTANT-1/2/3)
// ---------------------------------------------------------------------------

// IMPORTANT-1: the interactive runtime-mode pull holds the SHARED host-write
// mutex across its ENTIRE download — a provider job blocking on that mutex
// must still honor cancel promptly (poll-try_lock, mirroring the
// materialize-gate discipline), or cancel/join/destroy stall for minutes.
TEST(McapCloudProviderJob, CancelWhileHostWriteMutexHeldUnblocksFast) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-hostwrite");

  // The "interactive worker mid-download": hold the shared host-write mutex.
  std::unique_lock<std::mutex> host_write_hold(h.rt.hostWriteMutex());

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  // Let the job connect + open the session and reach the host-write wait.
  const auto open_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (server.openSessions() == 0 && std::chrono::steady_clock::now() < open_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_GT(server.openSessions(), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));  // firmly in the lock wait

  const auto cancel_at = std::chrono::steady_clock::now();
  job.job.vtable->cancel(job.job.ctx);
  const bool got_terminal = recorder.waitTerminal(std::chrono::seconds(2));
  const auto unblock_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - cancel_at);
  host_write_hold.unlock();  // let a non-cancellable (red) pull run to its end
  job.job.vtable->join(job.job.ctx);

  EXPECT_TRUE(got_terminal) << "cancel must wake the host-write lock wait";
  EXPECT_LT(unblock_ms.count(), 2000);
  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_CANCELLED)
      << recorder.terminal(0).second;
}

// IMPORTANT-2: a cancel landing AFTER a kComplete pull (tee finalized, entry
// stored, promotion settled) must NOT rewrite history — the job reports its
// TRUTHFUL terminal (here: PROMOTED). Deterministic via the pre-terminal
// seam. Mid-download/non-complete cancels keep cancel-wins (the ceiling
// race test above).
TEST(McapCloudProviderAbi, PostCompleteCancelReportsTruthfulTerminal) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("abi-postcomplete");
  FakePromotionHost promo;  // re-entrant: settled by the time the pull returns
  h.rt.setPromotionHost(PJ::SourcePromotionHostView(promo.view()));

  JobRecorder recorder;
  ScopedJob job;
  h.provider.setPreTerminalHookForTest([&]() {
    if (job.job.vtable != nullptr) {
      job.job.vtable->cancel(job.job.ctx);  // the post-complete cancel
    }
  });
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_SUCCEEDED_PROMOTED)
      << "a completed pull must report its truthful terminal even if a cancel raced in "
         "post-completion — got: "
      << recorder.terminal(0).second;
  EXPECT_EQ(promo.calls.load(), 1);
}

// IMPORTANT-3: an all-decode-errors session (every message received, zero
// rows decoded) is NOT a usable eager dataset — FAILED, not EAGER_ONLY.
TEST(McapCloudProviderJob, AllDecodeErrorsSessionFails) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-decodeerrors");
  h.ingest.refuse_push = true;  // bindings succeed; every push fails -> decode errors only

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_FAILED)
      << "zero decoded rows out of a non-empty stream must FAIL, never EAGER_ONLY — got: "
      << recorder.terminal(0).second;
  EXPECT_NE(recorder.terminal(0).second.find("decoded"), std::string::npos)
      << recorder.terminal(0).second;
}

// IMPORTANT-3 (the other side): a genuinely EMPTY time window — a real plan
// whose selection holds zero messages, Eos{COMPLETE} — stays EAGER_ONLY
// (nothing failed; there was simply nothing to decode).
TEST(McapCloudProviderJob, EmptyWindowCompleteIsEagerOnly) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kCompleteEmpty);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("job-emptywindow");

  JobRecorder recorder;
  ScopedJob job;
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(20)));
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_SUCCEEDED_EAGER_ONLY)
      << recorder.terminal(0).second;
}

// The DETACH branch (runToTerminal's !settled arm — the join-unblockability
// guarantee): a promotion ACCEPTED but still OUTSTANDING when the cancel
// lands must NOT block the terminal on on_result — exactly one CANCELLED
// with the detach wording, delivered promptly. The shared result state
// outlives the job: releasing the deferred host afterwards settles it
// cleanly (observable as the SessionCache entry flipping to kPromoted long
// after the job terminated). Distinct from PostCompleteCancelReportsTruthful-
// Terminal above, which pins the SETTLED side of the same interleave.
TEST(McapCloudProviderAbi, OutstandingPromotionAtCancelDetachesCancelled) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  ProviderHarness h("abi-detach");
  mcap_cloud_test::DeferredPromotionHost promo;  // accepted; never settles until release()
  h.rt.setPromotionHost(PJ::SourcePromotionHostView(promo.view()));

  JobRecorder recorder;
  ScopedJob job;
  // The pull returns kComplete with the promotion accepted-but-unsettled;
  // the pre-terminal cancel then forces the waitSettled predicate false.
  h.provider.setPreTerminalHookForTest([&]() {
    if (job.job.vtable != nullptr) {
      job.job.vtable->cancel(job.job.ctx);
    }
  });
  ASSERT_TRUE(h.start(queryJson(server.uri()), recorder, job));
  ASSERT_TRUE(recorder.waitTerminal(std::chrono::seconds(5)))
      << "the terminal must not wait for the never-settling on_result (detach)";
  job.job.vtable->join(job.job.ctx);

  ASSERT_EQ(recorder.terminalCount(), 1u);
  EXPECT_EQ(recorder.terminal(0).first, PJ_DESCRIPTOR_IMPORT_CANCELLED)
      << recorder.terminal(0).second;
  EXPECT_NE(recorder.terminal(0).second.find("detached"), std::string::npos)
      << recorder.terminal(0).second;

  // Settle the shared state cleanly before teardown (ASAN-quiet), and pin
  // the detached-state-outlives-the-job property: the late ok=true still
  // lands on the SessionCache entry.
  promo.release();
  promo.joinWorker();
  const PJ::cloud::SessionKey key =
      PJ::cloud::computeSessionKey(server.uri(), {"a.mcap"}, {"/one"}, {0, 0}, true);
  auto entry = h.rt.sessionCache().lookup(key, [](const mcap_cloud::CachedSession&) { return true; });
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->promotion_state, mcap_cloud::PromotionState::kPromoted)
      << "the detached shared state must settle the entry after the job is gone";
}

// Adversarial F3 at the provider surface: an empty-s3_keys descriptor is a
// CONTRACT failure at both entry points — query false + error, start false
// with no job and no callbacks (previously it started a worker that
// dereferenced s3_keys.front(): UB, potentially no terminal at all).
TEST(McapCloudProviderAbi, EmptyKeyDescriptorRejectedAtQueryAndStart) {
  ProviderHarness h("abi-emptykeys");
  const std::string json =
      R"({"v":1,"kind":"mcap-cloud-session","server_uri":"ws://127.0.0.1:9","s3_keys":[],)"
      R"("topics":[],"start_ns":"0","end_ns":"0","include_latched":true})";

  PJ_descriptor_query_result_v1_t out{};
  std::string query_error;
  EXPECT_FALSE(runQuery(h.provider, json, &out, &query_error));
  EXPECT_FALSE(query_error.empty());

  JobRecorder recorder;
  ScopedJob job;
  std::string start_error;
  EXPECT_FALSE(h.start(json, recorder, job, 0, &start_error));
  EXPECT_FALSE(start_error.empty());
  EXPECT_EQ(job.job.vtable, nullptr) << "no job may exist for an empty selection";
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(recorder.terminalCount(), 0u);
  EXPECT_EQ(recorder.datasetCount(), 0u);
}
