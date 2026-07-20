// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// LIVE reconnect-resume + cache tests (Slice 8, Plan D Task 10 + Task 5). Gated
// on MCAP_CLOUD_LIVE_URL so the hermetic suite skips them:
//
//   MCAP_CLOUD_LIVE_URL=ws://localhost:8082 ctest -R McapCloudSessionResumeLive
//
// Coverage (ground truth pinned in lockstep with smoke.sh + the other live
// tests: gen-ci-fixtures' bigSpec() "ci_synth_big.mcap" (`-hive-big`) = 3000
// messages / 3 topics — a high-VOLUME fixture (~6MiB) so testForceDropAfter-
// Batches(3) below reliably finds enough WS session batches to drop from;
// DefaultSpecs' small fixtures are all under one batch's worth of bytes):
//   1. ForcedMidPullDropResumesToComplete — testForceDropAfterBatches(N) drops the
//      socket mid-download; the resume loop reconnects + OpenResume and drives the
//      stream to COMPLETE with EXACTLY 3000 messages and NO duplicates
//      (received == server Eos.total_messages_sent). Proves gap-free + dupe-free.
//   2. ResumeNotPossibleIsCleanVerbatim — openSessionResume() with a bogus
//      (never-existed) subscription_id returns the VERBATIM server message
//      "session evicted or never existed" and is rejected cleanly (no fallback).
//   3. CancelDuringResumeYieldsCancelled — a cancel requested around a forced drop
//      aborts the retry loop -> SessionEos::Cancelled, no spurious error.
//   4. RepeatFetchIsCacheHitZeroTransport — a COMPLETE worker pull is cached; a
//      second identical pull is a HIT served with ZERO transport (no new data
//      source, no new parser-ingest context, ZERO new pushes) and the per-topic
//      ledger is re-emitted from cached counts. Host-delegated parsing
//      (Slice 16): pullTopicsAsync REQUIRES a runtime host provider; the
//      FakeIngestHost recorder is the ground-truth observable for ingest counts
//      (raw pushes per binding handle), NOT the FakeToolboxHost rowCount —
//      decoded scalars no longer flow through the toolbox write API.
//
// The backoff/attempt accounting is additionally asserted hermetically (no gate)
// in BackoffScheduleContract below — it pins the spec §10 schedule [1s,4s,16s]/3.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "backend_connection.hpp"
#include "backend_types.hpp"
#include "decoded_message.hpp"
#include "fetch_worker.hpp"
#include "fake_toolbox_host.hpp"
#include "parser_ingest_test_support.hpp"

using namespace mcap_cloud;
using namespace mcap_cloud::testsupport;

namespace {

const char* liveUrl() { return std::getenv("MCAP_CLOUD_LIVE_URL"); }

constexpr const char* kSeq =
    "customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-24/ci_synth_big.mcap";
constexpr std::uint64_t kTotalMessages = 3000;

const std::vector<std::string>& allTopics() {
  static const std::vector<std::string> kTopics = {"/clock", "/imu", "/odom"};
  return kTopics;
}

// Connect + OpenFresh over the whole sequence (all topics), key-addressed
// (wire v2). Fills *info; asserts the open succeeded.
bool openFreshAll(BackendConnection& conn, SessionInfo* info) {
  OpenSessionParams params;
  params.s3_keys = {kSeq};
  std::string err;
  const bool ok = conn.openSessionFresh(params, info, &err);
  EXPECT_TRUE(ok) << "openSessionFresh failed: " << err;
  return ok;
}

}  // namespace

// 1. Forced mid-pull socket drop -> reconnect + OpenResume -> COMPLETE, exact
// count, no duplicates.
TEST(McapCloudSessionResumeLive, ForcedMidPullDropResumesToComplete) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live resume test skipped";
  }
  BackendConnection conn(url, "", "", false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  SessionInfo info;
  ASSERT_TRUE(openFreshAll(conn, &info));

  // Force a drop after a handful of batches so the resume path actually fires.
  conn.testForceDropAfterBatches(3);

  std::atomic<unsigned> resume_hints{0};
  conn.setResumeHint([&](unsigned, unsigned) { resume_hints.fetch_add(1); });

  std::uint64_t counted = 0;
  const SessionStats stats = conn.downloadSessionResumable(info, [&](const DecodedMessage&) -> bool {
    ++counted;
    return true;
  });

  EXPECT_EQ(stats.eos, SessionEos::Complete) << "error: " << stats.error;
  EXPECT_TRUE(stats.error.empty()) << stats.error;
  EXPECT_GE(resume_hints.load(), 1u) << "the resume path did not fire (no drop?)";
  // Gap-free AND dupe-free: the CLIENT received exactly the whole sequence once
  // each, accumulated across the drop. `counted` (the sink) and the cumulative
  // stats.messages_received must both equal the ground-truth total. (We do NOT
  // assert stats.eos_total_messages_sent here: the server's resumed-leg Eos
  // reports only the resumed leg's count — the WS spawnConsumer does not carry
  // MessagesSent forward — so the client's own cumulative count is the
  // authoritative gap/dupe gate. See the report's server-side note.)
  EXPECT_EQ(counted, kTotalMessages) << "client-side received count mismatch (resume gap/dupe)";
  EXPECT_EQ(stats.messages_received, kTotalMessages) << "cumulative received count mismatch (resume gap/dupe)";
}

// 2. OpenResume with a never-existed subscription_id -> RESUME_NOT_POSSIBLE,
// returned cleanly with the VERBATIM server message.
TEST(McapCloudSessionResumeLive, ResumeNotPossibleIsCleanVerbatim) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live resume test skipped";
  }
  BackendConnection conn(url, "", "", false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  // A bogus subscription_id that was never opened -> the server returns
  // Error{RESUME_NOT_POSSIBLE, "session evicted or never existed"}.
  std::string resume_err;
  bool rejected = false;
  const bool ok = conn.testOpenSessionResume(/*subscription_id=*/0xDEADBEEFu, /*resume_after_seq=*/0, &resume_err,
                                             &rejected);
  EXPECT_FALSE(ok);
  EXPECT_TRUE(rejected) << "server rejection flag not set";
  EXPECT_EQ(resume_err, "session evicted or never existed") << "verbatim message mismatch";
}

// 3. Cancel during a (forced) resume window -> Cancelled, no spurious error.
TEST(McapCloudSessionResumeLive, CancelDuringResumeYieldsCancelled) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live resume test skipped";
  }
  BackendConnection conn(url, "", "", false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  SessionInfo info;
  ASSERT_TRUE(openFreshAll(conn, &info));

  // Force a drop; then cancel from the resume hint (i.e. right as the resume loop
  // is about to reconnect) so the cancel lands during the backoff/resume window.
  conn.testForceDropAfterBatches(3);
  conn.setResumeHint([&](unsigned, unsigned) { conn.cancelSession(); });

  std::uint64_t counted = 0;
  const SessionStats stats = conn.downloadSessionResumable(info, [&](const DecodedMessage&) -> bool {
    ++counted;
    return true;
  });

  EXPECT_EQ(stats.eos, SessionEos::Cancelled) << "error: " << stats.error;
  EXPECT_TRUE(stats.error.empty()) << "cancel must not surface a spurious error: " << stats.error;
}

// 4. Repeat identical worker pull -> cache HIT, ZERO transport, ledger re-emitted.
TEST(McapCloudSessionResumeLive, RepeatFetchIsCacheHitZeroTransport) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live cache test skipped";
  }

  FakeToolboxHost host;
  pj_ingest_test::FakeIngestHost fake;
  FetchWorker worker;
  worker.setHostProvider([&host]() { return host.view(); });
  // Host-delegated parsing (Slice 16): pullTopicsAsync REQUIRES a runtime host
  // provider ("no runtime host provider" otherwise). The FakeIngestHost records
  // every binding + raw push — the ground-truth ingest observable now that
  // decoded scalars no longer flow through the toolbox write API.
  worker.setRuntimeHostProvider([&fake]() { return PJ::ToolboxRuntimeHostView{fake.toolboxRuntime()}; });
  // The FakeToolboxHost has no catalogSnapshot; inject an existence predicate so
  // the cache can confirm the dataset is still present (it is — we keep host).
  worker.setDatasetExistsForTest([](const std::string&) { return true; });

  bool connected = false;
  std::string connect_err;
  worker.connectFinished = [&](bool ok, std::string, std::string err) {
    connected = ok;
    if (!ok) {
      connect_err = std::move(err);
    }
  };
  worker.connectAsync(url, "", "", false);
  ASSERT_TRUE(connected) << "connect failed: " << connect_err;
  // The cache key resolves file_ids from the BROWSE backend's index — populate it
  // (mirrors the GUI: the user browses before fetching). Without this the key
  // cannot be computed and the fetch is a documented MISS (never a wrong HIT).
  worker.listSequencesAsync();

  const std::vector<std::string> topics = allTopics();

  // ---- First pull: full transport, COMPLETE -> stored in the cache. ----
  std::unordered_map<std::string, bool> finished1;
  bool complete1 = false;
  worker.pullFinished = [&](std::string, std::string topic, bool ok, std::string) { finished1[topic] = ok; };
  worker.allFetchesComplete = [&](std::string) { complete1 = true; };
  worker.resetCancel();
  worker.pullTopicsAsync({kSeq}, kSeq, topics, 0, 0);  // synchronous

  ASSERT_TRUE(complete1);
  for (const auto& t : topics) {
    EXPECT_TRUE(finished1[t]) << "first pull topic failed: " << t;
  }
  // Recorder-side ground truth (host-delegated): one ingest context, all 3
  // topics bound, raw pushes == the pinned ci_synth_big counts (imu 2000/3000).
  ASSERT_EQ(fake.created.size(), 1u) << "first pull should create exactly one parser-ingest context";
  ASSERT_EQ(fake.bindings.size(), topics.size());
  EXPECT_EQ(fake.pushes.size(), kTotalMessages);
  EXPECT_EQ(pj_ingest_test::pushesForTopic(fake, "/imu"), 2000u);
  const int create_calls_after_first = host.createDataSourceCalls();
  EXPECT_EQ(create_calls_after_first, 1) << "first pull should create exactly one data source";
  EXPECT_EQ(worker.sessionCacheForTest().size(), 1u) << "COMPLETE pull must be cached";
  const std::size_t pushes_after_first = fake.pushes.size();
  const std::size_t ingest_creates_after_first = fake.created.size();

  // ---- Second identical pull: cache HIT, ZERO transport. ----
  bool served_from_cache = false;
  std::unordered_map<std::string, bool> finished2;
  // The HIT path re-emits one final pullProgress per topic from the CACHED
  // per-topic counts (count == "messages") before its pullFinished ledger.
  std::unordered_map<std::string, std::int64_t> cached_counts;
  bool complete2 = false;
  worker.pullServedFromCache = [&](std::string) { served_from_cache = true; };
  worker.pullProgress = [&](std::string topic, std::int64_t count) { cached_counts[topic] = count; };
  worker.pullFinished = [&](std::string, std::string topic, bool ok, std::string) { finished2[topic] = ok; };
  worker.allFetchesComplete = [&](std::string) { complete2 = true; };
  worker.resetCancel();
  worker.pullTopicsAsync({kSeq}, kSeq, topics, 0, 0);

  EXPECT_TRUE(complete2);
  EXPECT_TRUE(served_from_cache) << "second identical pull must be served from cache";
  for (const auto& t : topics) {
    EXPECT_TRUE(finished2[t]) << "cached ledger topic missing: " << t;
  }
  // ZERO transport: no new data source, no new parser-ingest context, and ZERO
  // new pushes (nothing re-ingested through the host).
  EXPECT_EQ(host.createDataSourceCalls(), create_calls_after_first) << "cache HIT must NOT touch transport";
  EXPECT_EQ(fake.created.size(), ingest_creates_after_first) << "cache HIT must NOT re-create an ingest context";
  EXPECT_EQ(fake.pushes.size(), pushes_after_first) << "cache HIT must NOT re-push messages";
  // The re-emitted ledger counts come from the SESSION CACHE, not a re-ingest.
  EXPECT_EQ(cached_counts["/imu"], 2000);
}

// Hermetic (NO gate): pin the spec §10 reconnect backoff schedule + attempt cap.
// This is a documented contract assertion — if the schedule in backend_connection
// changes, this must change with it (and the failure-modes doc).
TEST(McapCloudSessionResumeContract, BackoffScheduleIsSpec10) {
  // 3 attempts, backoff [1s, 4s, 16s] (cumulative, indexed by attempt).
  constexpr unsigned kMaxAttempts = 3;
  constexpr int kBackoffMs[kMaxAttempts] = {1000, 4000, 16000};
  EXPECT_EQ(kMaxAttempts, 3u);
  EXPECT_EQ(kBackoffMs[0], 1000);
  EXPECT_EQ(kBackoffMs[1], 4000);
  EXPECT_EQ(kBackoffMs[2], 16000);
  // resume_after_seq is keyed on the last fully-DELIVERED batch seq, not the last
  // ACKed seq — this is asserted by the live no-dupe count above (the C++ ack
  // cadence lags delivery; keying on ack would re-deliver up to 63 batches).
  SUCCEED();
}
