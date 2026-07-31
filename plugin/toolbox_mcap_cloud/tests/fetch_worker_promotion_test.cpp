// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC promotion-at-completion matrix (stage-4 PR-3 commit 2, D6): on
// EOS-complete + cache finalize ok (TeeOutcome::kFinalized) BOTH pull paths
// build the SourcePromotionRequest from the locked D6 values and call the
// pj.source_promotion.v1 host view through the shared
// ImportRuntime::promoteToFileSource hook. Pins:
//   - the request contents EXACTLY (dataset id, identity, finalized cache
//     path, loader id "mcap-loader", the locked preset json BYTE-COMPARED,
//     descriptor_json = toSourceDescriptorJson);
//   - EAGER_ONLY-equivalence on absent service / synchronous rejection /
//     async failure — the eager dataset stays, the finalized cache file
//     stays valid, and NO promoted record is assumed;
//   - kPromoted on ok=true (recorded on the shared SessionCache entry);
//   - a re-entrant on_result (fired inside promote_to_file_source) is safe;
//   - a DEFERRED on_result (the ABI's normal async shape, settling after
//     the pull returned) still lands on the entry via the shared state;
//   - memory-hit + valid disk (kExistingValid) re-promotes with the CACHED
//     dataset id — unless the entry is already kPromoted (idempotency).
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <pj_base/descriptor_import_protocol.h>
#include <pj_base/sdk/descriptor_import.hpp>
#include <pj_base/sdk/plugin_data_api.hpp>

#include "fake_promotion_host.hpp"
#include "fake_streaming_server.hpp"
#include "fake_toolbox_host.hpp"
#include "fetch_worker.hpp"
#include "import_runtime.hpp"
#include "parser_ingest_test_support.hpp"
#include "session_file_cache.hpp"
#include "session_key.hpp"
#include "test_support_fs.hpp"
#include "trusted_origins.hpp"

namespace {

namespace fs = std::filesystem;
using mcap_cloud_test::FakeStreamingServer;
using mcap_cloud_test::descriptorFor;
using mcap_cloud_test::identityFor;

struct TempRoot : mcap_cloud_test::ScopedTempDir {
  explicit TempRoot(const std::string& name) : ScopedTempDir("mcap-cloud-promotion-" + name) {}
};

// Fake pj.source_promotion.v1 host mirroring the SDK's
// source_promotion_service_test FakePromotionHost: captures the full request,
// then delivers result_cb RE-ENTRANTLY (inside promote_to_file_source) —
// which doubles as the re-entrancy-safety pin for every test using it.
struct FakePromotionHost {
  struct Captured {
    std::uint32_t dataset = 0;
    std::string source_identity;
    std::string local_path;
    std::string loader_plugin_id;
    std::string loader_config_json;
    std::string descriptor_json;
  };
  std::mutex mu;
  std::vector<Captured> requests;
  bool accept = true;   // false => synchronous rejection, callback never runs
  bool succeed = true;  // outcome delivered through the callback

  static bool promoteThunk(void* ctx, const PJ_source_promotion_request_v1_t* request,
                           PJ_source_promotion_result_fn result_cb, void* callback_ctx,
                           PJ_error_t* err) noexcept {
    auto* self = static_cast<FakePromotionHost*>(ctx);
    if (!self->accept) {
      PJ::sdk::fillError(err, 1, "source_promotion", "rejected");
      return false;
    }
    {
      const std::lock_guard<std::mutex> lock(self->mu);
      Captured c;
      c.dataset = request->dataset.id;
      c.source_identity = std::string(PJ::sdk::toStringView(request->source_identity));
      c.local_path = std::string(PJ::sdk::toStringView(request->local_path_utf8));
      c.loader_plugin_id = std::string(PJ::sdk::toStringView(request->loader_plugin_id));
      c.loader_config_json = std::string(PJ::sdk::toStringView(request->loader_config_json));
      c.descriptor_json = std::string(PJ::sdk::toStringView(request->descriptor_json));
      self->requests.push_back(std::move(c));
    }
    const char* msg = self->succeed ? "promoted" : "generation mismatch";
    result_cb(callback_ctx, self->succeed, PJ_string_view_t{msg, std::char_traits<char>::length(msg)});
    return true;
  }

  [[nodiscard]] std::size_t requestCount() {
    const std::lock_guard<std::mutex> lock(mu);
    return requests.size();
  }
  [[nodiscard]] Captured request(std::size_t i) {
    const std::lock_guard<std::mutex> lock(mu);
    return requests.at(i);
  }

  [[nodiscard]] PJ_source_promotion_host_t view() {
    static constexpr PJ_source_promotion_host_vtable_t kVtable{
        1, sizeof(PJ_source_promotion_host_vtable_t), &FakePromotionHost::promoteThunk};
    return PJ_source_promotion_host_t{this, &kVtable};
  }
};

// Deferred-result host hoisted to fake_promotion_host.hpp (shared with the
// provider suite's outstanding-promotion detach pin).
using mcap_cloud_test::DeferredPromotionHost;

// One fully-wired headless worker over a given runtime (the direct-pull
// harness shape; the interactive-path pins ride the cache-tee suite).
struct Harness {
  Harness() : ingest(/*with_progress_slots=*/true) {
    worker.setHostProvider([this]() { return host.view(); });
    worker.setRuntimeHostProvider([this]() { return PJ::ToolboxRuntimeHostView{ingest.toolboxRuntime()}; });
  }

  [[nodiscard]] mcap_cloud::PullRequest request(mcap_cloud::ImportRuntime& rt, const std::string& uri) {
    const mcap_cloud::SourceDescriptor d = descriptorFor(uri);
    mcap_cloud::PullRequest r;
    r.connection.uri = uri;
    r.sequence_names = d.s3_keys;
    r.group_name = d.display_name;
    r.topic_names = d.topics;
    r.start_ns = d.start_ns;
    r.end_ns = d.end_ns;
    r.include_latched = d.include_latched;
    r.runtime = &rt;
    r.canonical_descriptor_json = mcap_cloud::canonicalSourceDescriptorJson(d);
    r.descriptor_json = mcap_cloud::toSourceDescriptorJson(d);
    r.identity = mcap_cloud::descriptorIdentity(d);
    return r;
  }

  mcap_cloud::testsupport::FakeToolboxHost host;
  pj_ingest_test::FakeIngestHost ingest;
  mcap_cloud::FetchWorker worker;
};

// The locked D6 preset (byte-compared — use_log_time REQUIRED, no filepath).
constexpr const char* kExpectedPreset =
    "{\"clamp_large_arrays\":true,\"max_array_size\":500,\"selected_topics\":[],"
    "\"use_header_timestamp\":false,\"use_log_time\":true}";

mcap_cloud::PromotionState entryPromotionState(mcap_cloud::ImportRuntime& rt, const std::string& uri) {
  const PJ::cloud::SessionKey key =
      PJ::cloud::computeSessionKey(uri, {"a.mcap"}, {"/one"}, {0, 0});
  auto entry = rt.sessionCache().lookup(key, [](const mcap_cloud::CachedSession&) { return true; });
  return entry.has_value() ? entry->promotion_state : mcap_cloud::PromotionState::kNone;
}

}  // namespace

TEST(McapCloudPromotion, CompletePullPromotesWithTheExactD6Request) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("exact-cache");
  TempRoot config_root("exact-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));
  FakePromotionHost promo;
  rt.setPromotionHost(PJ::SourcePromotionHostView(promo.view()));
  ASSERT_TRUE(rt.hasPromotionHost());

  Harness h;
  const mcap_cloud::PullResult result = h.worker.pull(h.request(rt, server.uri()));
  ASSERT_EQ(result.terminal, mcap_cloud::PullTerminal::kComplete) << result.error;
  ASSERT_EQ(result.tee_outcome, mcap_cloud::TeeOutcome::kFinalized) << result.tee_error;

  // The pull surfaces the shared promotion result; the re-entrant fake has
  // already settled it ok.
  ASSERT_NE(result.promotion, nullptr);
  ASSERT_TRUE(result.promotion->ok().has_value());
  EXPECT_TRUE(*result.promotion->ok()) << result.promotion->message();

  // Request contents EXACT (D6 locked values).
  const std::string identity = identityFor(server.uri());
  fs::path cache_file;
  ASSERT_TRUE(rt.fileCache().lookup(identity, &cache_file));
  ASSERT_EQ(promo.requestCount(), 1u);
  const auto req = promo.request(0);
  EXPECT_EQ(req.dataset, 1u) << "dataset = the pull's DataSourceHandle.id";
  EXPECT_EQ(req.source_identity, identity);
  EXPECT_EQ(req.local_path, cache_file.string()) << "the finalized cache path";
  EXPECT_EQ(req.loader_plugin_id, "mcap-loader");
  EXPECT_EQ(req.loader_config_json, kExpectedPreset) << "the locked preset, byte-identical";
  EXPECT_EQ(req.descriptor_json,
            mcap_cloud::toSourceDescriptorJson(descriptorFor(server.uri())));

  EXPECT_EQ(entryPromotionState(rt, server.uri()), mcap_cloud::PromotionState::kPromoted);
}

TEST(McapCloudPromotion, AbsentServiceIsEagerOnlyEquivalent) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("absent-cache");
  TempRoot config_root("absent-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));
  ASSERT_FALSE(rt.hasPromotionHost());

  Harness h;
  const mcap_cloud::PullResult result = h.worker.pull(h.request(rt, server.uri()));
  ASSERT_EQ(result.terminal, mcap_cloud::PullTerminal::kComplete) << result.error;
  ASSERT_EQ(result.tee_outcome, mcap_cloud::TeeOutcome::kFinalized) << result.tee_error;

  // EAGER_ONLY-equivalent: settled false, eager dataset + cache file intact.
  ASSERT_NE(result.promotion, nullptr);
  ASSERT_TRUE(result.promotion->ok().has_value());
  EXPECT_FALSE(*result.promotion->ok());
  fs::path cache_file;
  EXPECT_TRUE(rt.fileCache().lookup(identityFor(server.uri()), &cache_file));
  EXPECT_EQ(h.host.createDataSourceCalls(), 1) << "the eager dataset stays";
  EXPECT_EQ(entryPromotionState(rt, server.uri()), mcap_cloud::PromotionState::kEagerOnly);
}

TEST(McapCloudPromotion, SynchronousRejectionKeepsEagerAndCache) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("reject-cache");
  TempRoot config_root("reject-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));
  FakePromotionHost promo;
  promo.accept = false;
  rt.setPromotionHost(PJ::SourcePromotionHostView(promo.view()));

  Harness h;
  const mcap_cloud::PullResult result = h.worker.pull(h.request(rt, server.uri()));
  ASSERT_EQ(result.terminal, mcap_cloud::PullTerminal::kComplete) << result.error;
  ASSERT_NE(result.promotion, nullptr);
  ASSERT_TRUE(result.promotion->ok().has_value());
  EXPECT_FALSE(*result.promotion->ok());
  EXPECT_FALSE(result.promotion->message().empty());

  fs::path cache_file;
  EXPECT_TRUE(rt.fileCache().lookup(identityFor(server.uri()), &cache_file))
      << "the finalized cache file must survive a rejected promotion";
  EXPECT_EQ(entryPromotionState(rt, server.uri()), mcap_cloud::PromotionState::kEagerOnly);
}

TEST(McapCloudPromotion, AsyncFailureKeepsEagerAndCache) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("asyncfail-cache");
  TempRoot config_root("asyncfail-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));
  FakePromotionHost promo;
  promo.succeed = false;
  rt.setPromotionHost(PJ::SourcePromotionHostView(promo.view()));

  Harness h;
  const mcap_cloud::PullResult result = h.worker.pull(h.request(rt, server.uri()));
  ASSERT_EQ(result.terminal, mcap_cloud::PullTerminal::kComplete) << result.error;
  ASSERT_NE(result.promotion, nullptr);
  ASSERT_TRUE(result.promotion->ok().has_value());
  EXPECT_FALSE(*result.promotion->ok());

  fs::path cache_file;
  EXPECT_TRUE(rt.fileCache().lookup(identityFor(server.uri()), &cache_file));
  EXPECT_EQ(entryPromotionState(rt, server.uri()), mcap_cloud::PromotionState::kEagerOnly);
}

TEST(McapCloudPromotion, DeferredResultSettlesTheEntryAfterThePullReturned) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("deferred-cache");
  TempRoot config_root("deferred-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));
  DeferredPromotionHost promo;
  rt.setPromotionHost(PJ::SourcePromotionHostView(promo.view()));

  Harness h;
  const mcap_cloud::PullResult result = h.worker.pull(h.request(rt, server.uri()));
  ASSERT_EQ(result.terminal, mcap_cloud::PullTerminal::kComplete) << result.error;
  ASSERT_NE(result.promotion, nullptr);
  EXPECT_FALSE(result.promotion->ok().has_value()) << "still pending after the pull returned";
  EXPECT_EQ(entryPromotionState(rt, server.uri()), mcap_cloud::PromotionState::kPending);

  promo.release();
  promo.joinWorker();
  ASSERT_TRUE(result.promotion->ok().has_value());
  EXPECT_TRUE(*result.promotion->ok());
  EXPECT_EQ(entryPromotionState(rt, server.uri()), mcap_cloud::PromotionState::kPromoted);
}

TEST(McapCloudPromotion, MemoryHitWithValidDiskRePromotesWithTheCachedDatasetId) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("rehit-cache");
  TempRoot config_root("rehit-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));
  // First pull with a FAILING promotion -> entry lands kEagerOnly.
  FakePromotionHost promo;
  promo.succeed = false;
  rt.setPromotionHost(PJ::SourcePromotionHostView(promo.view()));

  // Interactive-shaped worker (the memory-hit path lives in pullTopicsAsync).
  Harness h;
  h.worker.setImportRuntime(&rt);
  h.worker.setDatasetExistsForTest([](const mcap_cloud::CachedSession&) { return true; });
  bool connected = false;
  h.worker.connectFinished = [&](bool ok, std::string, std::string, std::string) { connected = ok; };
  h.worker.connectAsync(server.uri(), "", "", false);
  ASSERT_TRUE(connected);
  h.worker.pullTopicsAsync({"a.mcap"}, "a.mcap", {"/one"}, 0, 0);
  ASSERT_EQ(promo.requestCount(), 1u);
  ASSERT_EQ(entryPromotionState(rt, server.uri()), mcap_cloud::PromotionState::kEagerOnly);

  // Now the promotion host recovers; a memory hit with a valid disk file
  // must RE-promote with the CACHED dataset id (spec §6.1).
  promo.succeed = true;
  h.worker.pullTopicsAsync({"a.mcap"}, "a.mcap", {"/one"}, 0, 0);
  ASSERT_EQ(server.openSessions(), 1) << "the second pull must be a zero-transport memory hit";
  ASSERT_EQ(promo.requestCount(), 2u) << "kExistingValid must re-promote";
  const auto req = promo.request(1);
  EXPECT_EQ(req.dataset, 1u);
  EXPECT_EQ(req.source_identity, identityFor(server.uri()));
  EXPECT_EQ(entryPromotionState(rt, server.uri()), mcap_cloud::PromotionState::kPromoted);

  // Idempotency: a third hit on an already-kPromoted entry does NOT
  // re-promote again.
  h.worker.pullTopicsAsync({"a.mcap"}, "a.mcap", {"/one"}, 0, 0);
  EXPECT_EQ(promo.requestCount(), 2u) << "an already-promoted entry must not re-promote";
}
