// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC direct cancellable pull matrix (stage-4 PR-3 commit 1, D4-as-
// amended): FetchWorker::pull(PullRequest) is the headless import entry —
// it never goes through connectAsync (no browse backend, no credential
// recording), owns exactly ONE session BackendConnection published for
// cancel BEFORE connect, and always tees into the SessionFileCache (the
// single-encoder rule). Pins:
//   - round trip: Complete terminal, tee finalized + valid cache file,
//     dataset created exactly once and surfaced via datasetCreated,
//     COMPLETE-only SessionCache entry stored with the dataset id;
//   - cancel during the WebSocket-open wait unblocks FAST (< 2 s): the
//     open wait joins the cancel predicate (backend_connection.cpp — the
//     10 s uncancellable window the consult flagged as the D4 hard blocker);
//   - cancel during the Hello wait unblocks fast (wake_on_cancel already
//     covers it — regression pin);
//   - cancel mid-download: kCancelled terminal, cache partial deleted;
//   - max_transfer_bytes ceiling: a DISTINCT kFailed cause naming the byte
//     ceiling — NEVER kCancelled (a sink-false classifies as
//     SessionEos::Cancelled on the wire; the pull must not leak that as a
//     cancel) — and the cache partial never survives.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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
using mcap_cloud_test::cacheRootHasPartial;
using mcap_cloud_test::descriptorFor;
using mcap_cloud_test::identityFor;

struct TempRoot : mcap_cloud_test::ScopedTempDir {
  explicit TempRoot(const std::string& name) : ScopedTempDir("mcap-cloud-direct-pull-" + name) {}
};

// SilentTcpServer / SilentHelloServer hoisted VERBATIM into
// fake_streaming_server.hpp (shared with the provider ABI suite).
using mcap_cloud_test::SilentHelloServer;
using mcap_cloud_test::SilentTcpServer;

// A fully-wired headless worker (NO connectAsync — the direct-pull contract).
struct PullHarness {
  PullHarness() : ingest(/*with_progress_slots=*/true) {
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
    r.datasetCreated = [this](PJ::sdk::DataSourceHandle handle) {
      dataset_ids.push_back(handle.id);
    };
    r.onProgress = [this](std::uint64_t messages) { progress_messages.store(messages); };
    return r;
  }

  mcap_cloud::testsupport::FakeToolboxHost host;
  pj_ingest_test::FakeIngestHost ingest;
  mcap_cloud::FetchWorker worker;
  std::vector<std::uint32_t> dataset_ids;
  std::atomic<std::uint64_t> progress_messages{0};
};

}  // namespace

TEST(McapCloudDirectPull, CompleteRoundTripFinalizesCacheAndStoresEntry) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("complete-cache");
  TempRoot config_root("complete-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  const mcap_cloud::PullResult result = h.worker.pull(h.request(rt, server.uri()));

  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kComplete) << result.error;
  EXPECT_FALSE(result.byte_ceiling_exceeded);
  EXPECT_EQ(result.stats.messages_received,
            static_cast<std::uint64_t>(mcap_cloud_test::kFakeBatches * mcap_cloud_test::kFakeMessagesPerBatch));
  EXPECT_TRUE(result.any_decodable);

  // Exactly one dataset, surfaced through datasetCreated (the PR-3 job's
  // on_dataset source) BEFORE the pull returned.
  ASSERT_EQ(h.dataset_ids.size(), 1u);
  EXPECT_EQ(h.dataset_ids[0], 1u);
  ASSERT_TRUE(result.dataset.has_value());
  EXPECT_EQ(result.dataset->id, 1u);
  EXPECT_EQ(h.host.createDataSourceCalls(), 1);

  // Cache finalized + valid; no partial left.
  const std::string identity = identityFor(server.uri());
  EXPECT_EQ(result.tee_outcome, mcap_cloud::TeeOutcome::kFinalized) << result.tee_error;
  fs::path cache_file;
  ASSERT_TRUE(rt.fileCache().lookup(identity, &cache_file));
  EXPECT_FALSE(cacheRootHasPartial(cache_root.path));

  // COMPLETE-only shared SessionCache entry with the stable dataset id (D7).
  const PJ::cloud::SessionKey key =
      PJ::cloud::computeSessionKey(server.uri(), {"a.mcap"}, {"/one"}, {0, 0}, true);
  auto entry = rt.sessionCache().lookup(key, [](const mcap_cloud::CachedSession&) { return true; });
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->dataset_id, 1u);
  EXPECT_EQ(entry->tee_outcome, mcap_cloud::TeeOutcome::kFinalized);
  EXPECT_EQ(entry->cache_identity, identity);
}

TEST(McapCloudDirectPull, CancelDuringSocketOpenUnblocksFast) {
  SilentTcpServer server;
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("openwait-cache");
  TempRoot config_root("openwait-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  mcap_cloud::PullResult result;
  std::thread pull([&] { result = h.worker.pull(h.request(rt, server.uri())); });
  // Give the pull time to reach the socket-open wait, then cancel and require
  // a FAST unblock: without the cancel predicate on that wait the pull sits
  // out the full 10 s open timeout (the D4 hard-blocker window).
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  const auto cancel_at = std::chrono::steady_clock::now();
  h.worker.requestCancel();
  pull.join();
  const auto unblock_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - cancel_at);

  EXPECT_LT(unblock_ms.count(), 2000) << "cancel must wake the WebSocket-open wait promptly";
  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kCancelled) << result.error;
  EXPECT_FALSE(cacheRootHasPartial(cache_root.path));
}

TEST(McapCloudDirectPull, CancelDuringHelloUnblocksFast) {
  SilentHelloServer server;
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("hello-cache");
  TempRoot config_root("hello-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  mcap_cloud::PullResult result;
  std::thread pull([&] { result = h.worker.pull(h.request(rt, server.uri())); });
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  const auto cancel_at = std::chrono::steady_clock::now();
  h.worker.requestCancel();
  pull.join();
  const auto unblock_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - cancel_at);

  EXPECT_LT(unblock_ms.count(), 2000) << "cancel must wake the Hello wait promptly (wake_on_cancel)";
  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kCancelled) << result.error;
}

TEST(McapCloudDirectPull, CancelMidDownloadDeletesCachePartial) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kStallAfterTwoBatches);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("cancel-cache");
  TempRoot config_root("cancel-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  mcap_cloud::PullResult result;
  std::thread pull([&] { result = h.worker.pull(h.request(rt, server.uri())); });
  struct PullJoinGuard {
    mcap_cloud::FetchWorker& worker;
    std::thread& thread;
    ~PullJoinGuard() {
      if (thread.joinable()) {
        worker.requestCancel();
        thread.join();
      }
    }
  } pull_join_guard{h.worker, pull};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (h.progress_messages.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_GT(h.progress_messages.load(), 0u) << "messages must flow before the cancel";
  h.worker.requestCancel();
  pull.join();

  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kCancelled);
  EXPECT_EQ(result.tee_outcome, mcap_cloud::TeeOutcome::kAborted);
  EXPECT_FALSE(cacheRootHasPartial(cache_root.path)) << "cache partials never survive (spec §10)";
  fs::path unused;
  EXPECT_FALSE(rt.fileCache().lookup(identityFor(server.uri()), &unused));
  EXPECT_EQ(rt.sessionCache().size(), 0u) << "no half-cached entry";
}

TEST(McapCloudDirectPull, ByteCeilingExceededFailsWithByteCauseNeverCancelled) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("ceiling-cache");
  TempRoot config_root("ceiling-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  mcap_cloud::PullRequest request = h.request(rt, server.uri());
  request.max_transfer_bytes = 1;  // the first wire batch exceeds this
  const mcap_cloud::PullResult result = h.worker.pull(std::move(request));

  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kFailed)
      << "a ceiling abort must terminal FAILED, never CANCELLED (the sink-false "
         "SessionEos::Cancelled classification must not leak through)";
  EXPECT_TRUE(result.byte_ceiling_exceeded);
  EXPECT_NE(result.error.find("byte"), std::string::npos) << result.error;
  EXPECT_FALSE(cacheRootHasPartial(cache_root.path));
  fs::path unused;
  EXPECT_FALSE(rt.fileCache().lookup(identityFor(server.uri()), &unused));
  EXPECT_EQ(rt.sessionCache().size(), 0u);
}

TEST(McapCloudDirectPull, RequiresRuntimeAndHosts) {
  TempRoot cache_root("guards-cache");
  TempRoot config_root("guards-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  // No runtime -> immediate kFailed, no crash.
  {
    PullHarness h;
    mcap_cloud::PullRequest r = h.request(rt, "ws://127.0.0.1:1");
    r.runtime = nullptr;
    const auto result = h.worker.pull(std::move(r));
    EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kFailed);
    EXPECT_FALSE(result.error.empty());
  }
  // No host providers -> immediate kFailed.
  {
    mcap_cloud::FetchWorker bare;
    PullHarness h;
    const auto result = bare.pull(h.request(rt, "ws://127.0.0.1:1"));
    EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kFailed);
    EXPECT_FALSE(result.error.empty());
  }
}

// Adversarial F1 at the pull level: two direct pulls differing ONLY in
// include_latched are DIFFERENT sessions — the second must never be served
// from the first's memory entry (it opens its own wire session).
TEST(McapCloudDirectPull, IncludeLatchedVariantsNeverAliasInTheCache) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("latchalias-cache");
  TempRoot config_root("latchalias-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  auto request_with_latched = [&](bool include_latched, PullHarness& h) {
    mcap_cloud::SourceDescriptor d = mcap_cloud_test::descriptorFor(server.uri());
    d.include_latched = include_latched;
    mcap_cloud::PullRequest r;
    r.connection.uri = server.uri();
    r.sequence_names = d.s3_keys;
    r.group_name = d.display_name;
    r.topic_names = d.topics;
    r.include_latched = d.include_latched;
    r.runtime = &rt;
    r.canonical_descriptor_json = mcap_cloud::canonicalSourceDescriptorJson(d);
    r.descriptor_json = mcap_cloud::toSourceDescriptorJson(d);
    r.identity = mcap_cloud::descriptorIdentity(d);
    return r;
  };

  PullHarness h1;
  const auto r1 = h1.worker.pull(request_with_latched(false, h1));
  ASSERT_EQ(r1.terminal, mcap_cloud::PullTerminal::kComplete) << r1.error;
  ASSERT_EQ(server.openSessions(), 1);

  PullHarness h2;
  const auto r2 = h2.worker.pull(request_with_latched(true, h2));
  ASSERT_EQ(r2.terminal, mcap_cloud::PullTerminal::kComplete) << r2.error;
  EXPECT_EQ(server.openSessions(), 2)
      << "a true-latched request must never be served from the false-latched entry (F1)";
}

// Adversarial F6 (control-only): with ZERO messages (a real plan, empty
// window) every wire byte is control traffic — no per-message check ever
// runs, so only the FINAL cumulative check can enforce the ceiling. Must be
// kFailed with the byte cause, nothing finalized, nothing stored.
TEST(McapCloudDirectPull, ControlOnlyBytesTripTheCeiling) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kCompleteEmpty);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("ctlceiling-cache");
  TempRoot config_root("ctlceiling-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  mcap_cloud::PullRequest request = h.request(rt, server.uri());
  request.max_transfer_bytes = 1;  // the Eos alone exceeds this
  const auto result = h.worker.pull(std::move(request));

  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kFailed)
      << "control/EOS bytes above the ceiling must FAIL, never finalize";
  EXPECT_TRUE(result.byte_ceiling_exceeded);
  EXPECT_NE(result.error.find("byte"), std::string::npos) << result.error;
  fs::path unused;
  EXPECT_FALSE(rt.fileCache().lookup(identityFor(server.uri()), &unused))
      << "an over-budget session must not finalize the cache";
  EXPECT_EQ(rt.sessionCache().size(), 0u) << "no entry stored above budget";
  EXPECT_EQ(result.promotion, nullptr) << "no promotion above budget";
}

// Adversarial F6 (final frames): a Progress flood BETWEEN the last message
// and the Eos pushes the cumulative wire bytes over a ceiling the payload
// bytes were comfortably under — the overage must fail the pull whether a
// late per-message check or the final check observes it.
TEST(McapCloudDirectPull, FramesBeyondTheLastMessageTripTheCeiling) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kCompleteWithProgressFlood);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("floodceiling-cache");
  TempRoot config_root("floodceiling-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  mcap_cloud::PullRequest request = h.request(rt, server.uri());
  // Above the batches+Eos total (~13.3 KiB incl. framing), far below the
  // ~2000-frame progress flood on top.
  request.max_transfer_bytes = 14 * 1024;
  const auto result = h.worker.pull(std::move(request));

  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kFailed) << result.error;
  EXPECT_TRUE(result.byte_ceiling_exceeded);
  fs::path unused;
  EXPECT_FALSE(rt.fileCache().lookup(identityFor(server.uri()), &unused));
}

// Adversarial F8 (watchdog half): host-stop-watchdog spawn failure degrades
// to no-watchdog — the pull completes normally (in-loop stop checks remain).
TEST(McapCloudDirectPull, WatchdogSpawnFailureIsNonfatal) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("wdspawn-cache");
  TempRoot config_root("wdspawn-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  h.worker.setWatchdogThreadFactoryForTest([](std::function<void()>) -> std::thread {
    throw std::system_error(std::make_error_code(std::errc::resource_unavailable_try_again),
                            "injected watchdog spawn failure");
  });
  const auto result = h.worker.pull(h.request(rt, server.uri()));
  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kComplete)
      << "watchdog spawn failure must be nonfatal: " << result.error;
  EXPECT_EQ(result.tee_outcome, mcap_cloud::TeeOutcome::kFinalized) << result.tee_error;
}

// Adversarial F12: the transfer-DURATION ceiling — same distinct-FAILED
// semantics as bytes (the flood pull reliably takes >> 50 ms).
TEST(McapCloudDirectPull, DurationCeilingFailsWithTheDurationCause) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kCompleteWithProgressFlood);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("duration-cache");
  TempRoot config_root("duration-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  PullHarness h;
  mcap_cloud::PullRequest request = h.request(rt, server.uri());
  request.max_transfer_duration = std::chrono::milliseconds(50);
  const auto result = h.worker.pull(std::move(request));

  EXPECT_EQ(result.terminal, mcap_cloud::PullTerminal::kFailed) << result.error;
  EXPECT_TRUE(result.duration_ceiling_exceeded);
  EXPECT_NE(result.error.find("duration"), std::string::npos) << result.error;
  fs::path unused;
  EXPECT_FALSE(rt.fileCache().lookup(identityFor(server.uri()), &unused));
}
