// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// LIVE worker-level HOST-DELEGATED ingest test (Slice 16, gated on
// MCAP_CLOUD_LIVE_URL). Drives the REAL FetchWorker::pullTopicsAsync over the
// REAL BackendConnection against a running pj-cloud server with BOTH providers
// wired: FakeToolboxHost (datasets, setHostProvider) + the FakeIngestHost
// parser-ingest recorder (setRuntimeHostProvider). Decoded scalars NO LONGER
// flow through the toolbox write API — the worker pushes raw payloads to the
// (fake) ingest host via ParserIngestDriver, so correctness here is binding
// fidelity + push counts against the pinned synthetic ci_synth_big ground
// truth (catalog-migration corpus, gen-ci-fixtures' bigSpec(), `-hive-big`):
//   - all 3 topics report pullFinished ok; exactly ONE parser-ingest context is
//     created AND released (finalize ran);
//   - exactly 3 recorded bindings; the imu binding carries the verbatim ABI
//     fields (type "sensor_msgs/msg/Imu", encoding "ros2msg", non-empty schema
//     text, the load-bearing parser_config_json "{}");
//   - total pushes across all handles == 3000; the imu handle == 2000;
//   - every push carries non-empty bytes. Per-handle timestamp ordering is NOT
//     asserted — wire batches interleave, the wire does not guarantee it;
//   - a cancel-mid-pull still releases the context (finalize on the cancel
//     path) and stops short of the full count (a rare full-completion race is
//     tolerated via <= and logged).
// pullTopicsAsync is synchronous (blocking) and decodes inline on the calling
// thread, so the recorder vectors need no locking; the cancel leg's background
// thread reads ONLY the atomic push_events counter, never the vectors.
// Self-skips unless MCAP_CLOUD_LIVE_URL is set.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "fake_toolbox_host.hpp"
#include "fetch_worker.hpp"
#include "parser_ingest_test_support.hpp"

using namespace mcap_cloud;
using mcap_cloud::testsupport::FakeToolboxHost;
using pj_ingest_test::FakeIngestHost;

namespace {

const char* liveUrl() { return std::getenv("MCAP_CLOUD_LIVE_URL"); }

// Ground truth pinned in lockstep with smoke.sh + backend_connection_live_test.
constexpr const char* kSeq =
    "customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-24/ci_synth_big.mcap";
constexpr const char* kImuTopic = "/imu";
constexpr std::uint64_t kTotalMessages = 3000;
constexpr std::uint64_t kImuMessages = 2000;

const std::vector<std::string>& allTopics() {
  static const std::vector<std::string> kTopics = {"/clock", "/imu", "/odom"};
  return kTopics;
}

// Build a connected FetchWorker with BOTH providers wired (datasets +
// parser-ingest recorder), ready for pullTopicsAsync.
class LiveWorker {
 public:
  LiveWorker(FakeToolboxHost& host, FakeIngestHost& fake) {
    worker_.setHostProvider([&host]() { return host.view(); });
    worker_.setRuntimeHostProvider([&fake]() { return PJ::ToolboxRuntimeHostView{fake.toolboxRuntime()}; });
  }

  bool connect(const std::string& url) {
    bool done = false;
    bool ok = false;
    worker_.connectFinished = [&](bool success, std::string, std::string, std::string err) {
      ok = success;
      if (!success) {
        last_error_ = std::move(err);
      }
      done = true;
    };
    worker_.connectAsync(url, "", "", false);
    return done && ok;
  }

  FetchWorker& worker() { return worker_; }
  const std::string& lastError() const { return last_error_; }

 private:
  FetchWorker worker_;
  std::string last_error_;
};

TEST(McapCloudParserIngestLive, FullPullPushesGroundTruthCounts) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live ingest test skipped";
  }

  FakeToolboxHost host;
  FakeIngestHost fake;
  LiveWorker lw(host, fake);
  ASSERT_TRUE(lw.connect(url)) << "connect failed: " << lw.lastError();

  const std::vector<std::string>& topics = allTopics();

  int finished = 0;
  int failed = 0;
  bool all_complete = false;
  std::vector<std::string> errors;
  lw.worker().pullFinished = [&](std::string, std::string topic, bool ok, std::string err) {
    ++finished;
    if (!ok) {
      ++failed;
      errors.push_back(topic + ": " + err);
    }
  };
  lw.worker().allFetchesComplete = [&](std::string) { all_complete = true; };

  lw.worker().resetCancel();
  lw.worker().pullTopicsAsync({kSeq}, kSeq, topics, 0, 0);  // synchronous

  EXPECT_TRUE(all_complete);
  EXPECT_EQ(finished, static_cast<int>(topics.size()));
  EXPECT_EQ(failed, 0) << (errors.empty() ? "" : errors.front());

  // Exactly ONE parser-ingest context for the whole pull, and finalize ran
  // (releaseParserIngest seals host parser writes before notifyDataChanged).
  ASSERT_EQ(fake.created.size(), 1u);
  EXPECT_EQ(fake.released.size(), 1u) << "finalize did not release the ingest context";

  // One ensureParserBinding per topic, fields verbatim over the ABI.
  ASSERT_EQ(fake.bindings.size(), topics.size());
  const auto* imu = pj_ingest_test::findBinding(fake, kImuTopic);
  ASSERT_NE(imu, nullptr) << "imu topic never bound";
  EXPECT_EQ(imu->type_name, "sensor_msgs/msg/Imu");
  EXPECT_EQ(imu->parser_encoding, "ros2msg");
  EXPECT_FALSE(imu->schema.empty()) << "imu binding carried no schema text";
  EXPECT_EQ(imu->config, "{}") << "parser_config_json must be the load-bearing non-empty {}";

  // Ground-truth push counts: every message of the sequence reached the host
  // exactly once (3000 total; 2000 on the imu handle).
  EXPECT_EQ(fake.pushes.size(), kTotalMessages);
  EXPECT_EQ(pj_ingest_test::pushesForTopic(fake, kImuTopic), kImuMessages);

  // Every push must carry non-empty bytes. (No per-handle timestamp-ordering
  // assertion: wire batches interleave; ordering is not a wire guarantee.)
  std::size_t empty_pushes = 0;
  for (const auto& p : fake.pushes) {
    if (p.bytes.empty()) {
      ++empty_pushes;
    }
  }
  EXPECT_EQ(empty_pushes, 0u) << empty_pushes << " pushes carried empty payload bytes";
}

TEST(McapCloudParserIngestLive, CancelMidPullReleasesContext) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live ingest test skipped";
  }

  FakeToolboxHost host;
  FakeIngestHost fake;
  LiveWorker lw(host, fake);
  ASSERT_TRUE(lw.connect(url)) << "connect failed: " << lw.lastError();

  bool all_complete = false;
  lw.worker().allFetchesComplete = [&](std::string) { all_complete = true; };

  // Cancel from a background thread shortly after the FIRST pushes arrive, then
  // run the (synchronous) pull on this thread — mirrors the deleted
  // ros_decode_live_test cancel leg, but keyed on the recorder's atomic
  // push_events counter (the only recorder state safe to read cross-thread) so
  // the cancel provably lands mid-stream. Bounded poll; requestCancel fires
  // unconditionally afterwards (a too-late cancel degrades to the tolerated
  // full-completion race below).
  std::atomic<bool> pull_running{true};
  std::thread canceller([&]() {
    for (int i = 0; i < 5000 && pull_running.load(); ++i) {
      if (fake.push_events.load(std::memory_order_relaxed) > 0) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    lw.worker().requestCancel();
  });

  lw.worker().resetCancel();
  lw.worker().pullTopicsAsync({kSeq}, kSeq, allTopics(), 0, 0);  // synchronous
  pull_running.store(false);
  canceller.join();

  EXPECT_TRUE(all_complete);  // completion always fires, even on cancel

  // finalize MUST run on the cancel path too: the one created ingest context
  // was released (host flushAll — no leaked half-open parser writes).
  ASSERT_EQ(fake.created.size(), 1u);
  EXPECT_EQ(fake.released.size(), 1u) << "cancel path did not release the ingest context";

  // Genuinely cancelled mid-stream: fewer pushes than the full sequence. The
  // rare race where the download completes before the cancel lands is tolerated
  // (<=) and logged rather than failed.
  EXPECT_LE(fake.pushes.size(), kTotalMessages);
  if (fake.pushes.size() >= kTotalMessages) {
    GTEST_LOG_(WARNING) << "cancel raced with full completion (" << fake.pushes.size()
                        << " pushes) — pull finished before the cancel landed";
  }
}

// ── Stage 2: the #470 progress surface — live verification ──
// These four cases are the "still-missing live verification" PJ4 #470 was
// merged without: a REAL worker pull against the smoke server, with the fake
// runtime host recording the plugin side of the progress contract.

// The pull announces itself to the host: exactly one progress_start carrying
// the session total (exact on MessageIndex-bearing fixtures) + cancellable,
// monotonic updates ending in the final authoritative sample, exactly one
// progress_finish on the completed path.
TEST(McapCloudParserIngestLive, ProgressSurfaceReportsHostVisibleImport) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live ingest test skipped";
  }

  FakeToolboxHost host;
  FakeIngestHost fake;
  LiveWorker lw(host, fake);
  ASSERT_TRUE(lw.connect(url)) << "connect failed: " << lw.lastError();

  int failed = 0;
  lw.worker().pullFinished = [&](std::string, std::string, bool ok, std::string) {
    failed += ok ? 0 : 1;
  };
  lw.worker().resetCancel();
  lw.worker().pullTopicsAsync({kSeq}, kSeq, allTopics(), 0, 0);  // synchronous
  EXPECT_EQ(failed, 0);

  ASSERT_EQ(fake.progress_starts.size(), 1u) << "the pull must announce exactly one progress sequence";
  EXPECT_NE(fake.progress_starts[0].label.find("MCAP Cloud"), std::string::npos);
  EXPECT_NE(fake.progress_starts[0].label.find(kSeq), std::string::npos)
      << "label must carry the group name, got: " << fake.progress_starts[0].label;
  EXPECT_EQ(fake.progress_starts[0].total, kTotalMessages)
      << "total must be the session estimate (exact with MessageIndex)";
  EXPECT_TRUE(fake.progress_starts[0].cancellable);

  ASSERT_FALSE(fake.progress_updates.empty()) << "at least the final sample must fire";
  for (std::size_t i = 1; i < fake.progress_updates.size(); ++i) {
    EXPECT_GE(fake.progress_updates[i], fake.progress_updates[i - 1]) << "updates must be monotonic";
  }
  EXPECT_EQ(fake.progress_updates.back(), kTotalMessages)
      << "the completed path must end with the final authoritative sample";
  EXPECT_EQ(fake.progress_finish_events.load(), 1u);
}

// A host stop request (the strip's stop button) cancels the pull
// cooperatively. Pre-latched so the FIRST throttled tick observes it
// deterministically (the throttle stamp starts at epoch — the first message
// always ticks), independent of how fast a localhost pull completes.
TEST(McapCloudParserIngestLive, HostStopRequestCancelsPull) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live ingest test skipped";
  }

  FakeToolboxHost host;
  FakeIngestHost fake;
  fake.stop_requested.store(true);
  LiveWorker lw(host, fake);
  ASSERT_TRUE(lw.connect(url)) << "connect failed: " << lw.lastError();

  bool all_complete = false;
  lw.worker().allFetchesComplete = [&](std::string) { all_complete = true; };
  lw.worker().resetCancel();
  lw.worker().pullTopicsAsync({kSeq}, kSeq, allTopics(), 0, 0);  // synchronous

  EXPECT_TRUE(all_complete);
  ASSERT_EQ(fake.created.size(), 1u);
  EXPECT_EQ(fake.released.size(), 1u) << "host-stop path did not release the ingest context";
  EXPECT_LT(fake.pushes.size(), kTotalMessages)
      << "a pre-latched host stop must cancel on the first tick, not run to completion";
  EXPECT_EQ(fake.progress_finish_events.load(), 0u)
      << "an aborted pull must not paint a completed progress sequence";
}

// progress_update returning false is the host's OTHER cancel channel (spec:
// user cancelled, not an error) — same cooperative-cancel outcome.
TEST(McapCloudParserIngestLive, ProgressUpdateFalseCancelsPull) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live ingest test skipped";
  }

  FakeToolboxHost host;
  FakeIngestHost fake;
  fake.progress_update_continue.store(false);
  LiveWorker lw(host, fake);
  ASSERT_TRUE(lw.connect(url)) << "connect failed: " << lw.lastError();

  lw.worker().resetCancel();
  lw.worker().pullTopicsAsync({kSeq}, kSeq, allTopics(), 0, 0);  // synchronous

  ASSERT_EQ(fake.created.size(), 1u);
  EXPECT_EQ(fake.released.size(), 1u);
  EXPECT_LT(fake.pushes.size(), kTotalMessages);
  EXPECT_EQ(fake.progress_finish_events.load(), 0u);
}

// Pre-#470 host (no progress slots): progressStart is refused, and the
// plugin must treat every subsequent false progressUpdate return as
// "unsupported" — NEVER as a cancel. The pull completes in full. This is the
// regression pin for the host_progress_active gate: without it, every
// download on an old host would instantly self-cancel.
TEST(McapCloudParserIngestLive, PreProgressHostCompletesWithoutSpuriousCancel) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live ingest test skipped";
  }

  FakeToolboxHost host;
  FakeIngestHost fake(/*with_progress_slots=*/false);
  LiveWorker lw(host, fake);
  ASSERT_TRUE(lw.connect(url)) << "connect failed: " << lw.lastError();

  int failed = 0;
  lw.worker().pullFinished = [&](std::string, std::string, bool ok, std::string) {
    failed += ok ? 0 : 1;
  };
  lw.worker().resetCancel();
  lw.worker().pullTopicsAsync({kSeq}, kSeq, allTopics(), 0, 0);  // synchronous

  EXPECT_EQ(failed, 0) << "an old host without progress slots must not fail the pull";
  EXPECT_EQ(fake.pushes.size(), kTotalMessages)
      << "the pull must run to completion — no spurious cancel from the refused surface";
  EXPECT_TRUE(fake.progress_starts.empty());
  EXPECT_EQ(fake.progress_finish_events.load(), 0u);
}

}  // namespace
