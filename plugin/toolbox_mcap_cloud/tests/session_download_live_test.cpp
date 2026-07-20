// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// LIVE test for the session/streaming download path through the REAL
// BackendConnection (openSessionFresh + downloadSession). Requires a running
// pj-cloud server seeded with the synthetic Hive-keyed catalog-migration
// fixture set; gated on MCAP_CLOUD_LIVE_URL so the hermetic CI suite skips it:
//
//   MCAP_CLOUD_LIVE_URL=ws://localhost:8082 ctest -R McapCloudSessionDownloadLive
//
// Ground truth (pinned in lockstep with scripts/smoke.sh + the catalog live
// test): gen-ci-fixtures' bigSpec() ("ci_synth_big.mcap", Hive key below,
// `-hive-big`) has 3000 messages total across 3 topics (/clock 500, /odom 500,
// /imu 2000); /imu alone is 2000 messages. We assert the CLIENT-SIDE received
// COUNT (and the server's Eos.total_messages_sent) match exactly. Regenerate
// with `gen-ci-fixtures -hive -hive-big -out DIR -manifest` if bigSpec ever
// changes.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <vector>

#include "backend_connection.hpp"
#include "backend_types.hpp"
#include "decoded_message.hpp"  // DecodedMessage (forward-declared in backend_connection.hpp)

namespace {

const char* liveUrl() { return std::getenv("MCAP_CLOUD_LIVE_URL"); }

constexpr const char* kKnownSequence =
    "customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-24/ci_synth_big.mcap";
constexpr std::uint64_t kTotalMessages = 3000;
// The "subset" topic downloaded alone (legacy name kSpeedTopic retained to
// minimize churn; it now targets the synthetic /imu topic, 2000 of 3000 msgs).
constexpr const char* kSpeedTopic = "/imu";
constexpr std::uint64_t kSpeedMessages = 2000;

// Stitched ground truth (pinned in lockstep with scripts/smoke.sh +
// backend_connection_live_test.cpp): the target (bigSpec) and DefaultSpecs()[1]
// "ci_synth_b.mcap" are time-disjoint synthetic files that stitch into one
// continuous logical stream.
constexpr const char* kStitchKeyA = kKnownSequence;  // ci_synth_big.mcap, 3000 msgs
constexpr std::uint64_t kStitchMessagesA = 3000;
constexpr const char* kStitchKeyB =
    "customer=test/customer_site=lab/robot=r2/source=synthetic/date=2026-06-23/ci_synth_b.mcap";
constexpr std::uint64_t kStitchMessagesB = 130;
constexpr std::uint64_t kStitchMessages = 3130;  // A + B

// Open a session for `topics` (empty = all) over the known sequence and drive
// it to completion, counting messages client-side. Asserts the session opened.
mcap_cloud::SessionStats download(mcap_cloud::BackendConnection& conn, const std::vector<std::string>& topics,
                                    std::uint64_t* counted) {
  // Key-addressed OpenFresh (wire v2): the sequence key goes straight through;
  // no name->file_id resolution round trip.
  mcap_cloud::OpenSessionParams params;
  params.s3_keys = {kKnownSequence};
  params.topic_names = topics;

  mcap_cloud::SessionInfo info;
  std::string err;
  EXPECT_TRUE(conn.openSessionFresh(params, &info, &err)) << "openSessionFresh failed: " << err;

  std::uint64_t local_count = 0;
  auto on_message = [&](const mcap_cloud::DecodedMessage&) -> bool {
    local_count++;
    return true;
  };
  const mcap_cloud::SessionStats stats = conn.downloadSession(info, on_message);
  *counted = local_count;
  return stats;
}

}  // namespace

// All-topics download of the known sequence: exactly 3000 messages received
// (kTotalMessages), the stream completes, and the server's
// Eos.total_messages_sent agrees.
TEST(McapCloudSessionDownloadLive, AllTopicsCount) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live session test skipped";
  }
  mcap_cloud::BackendConnection conn(url, "", "", false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  std::uint64_t counted = 0;
  const auto stats = download(conn, /*topics=*/{}, &counted);

  EXPECT_EQ(stats.eos, mcap_cloud::SessionEos::Complete) << "error: " << stats.error;
  EXPECT_EQ(counted, kTotalMessages) << "client-side received count mismatch";
  EXPECT_EQ(stats.messages_received, kTotalMessages) << "stats.messages_received mismatch";
  EXPECT_EQ(stats.eos_total_messages_sent, kTotalMessages) << "server Eos.total_messages_sent mismatch";
  EXPECT_TRUE(stats.error.empty());
}

// Count messages per topic_name for a (possibly stitched) session of `keys`,
// driving the REAL session path. Maps the session topic_id -> topic_name and
// tallies per name. Returns the SessionStats; fills *by_topic + *sub_id.
mcap_cloud::SessionStats downloadByTopic(mcap_cloud::BackendConnection& conn,
                                           const std::vector<std::string>& keys,
                                           std::unordered_map<std::string, std::uint64_t>* by_topic,
                                           std::uint64_t* sub_id) {
  mcap_cloud::OpenSessionParams params;
  params.s3_keys = keys;

  mcap_cloud::SessionInfo info;
  std::string err;
  EXPECT_TRUE(conn.openSessionFresh(params, &info, &err)) << "openSessionFresh failed: " << err;
  if (sub_id != nullptr) {
    *sub_id = info.subscription_id;
  }
  std::unordered_map<std::uint32_t, std::string> name_by_id;
  for (const auto& t : info.topics) {
    name_by_id.emplace(t.topic_id, t.topic_name);
  }

  auto on_message = [&](const mcap_cloud::DecodedMessage& m) -> bool {
    auto it = name_by_id.find(m.topic_id);
    if (it != name_by_id.end()) {
      (*by_topic)[it->second]++;
    }
    return true;
  };
  return conn.downloadSession(info, on_message);
}

// STITCHED two-file download (Slice 7): resolveFileIds({kStitchKeyA,
// kStitchKeyB}) -> 2 ids; EXACTLY ONE OpenFresh session (singular by
// construction) carries the union; the received count == kStitchMessages
// (3130); and each topic's stitched count == the SUM of that topic across
// both single-file downloads.
TEST(McapCloudSessionDownloadLive, StitchedTwoFiles) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live session test skipped";
  }
  mcap_cloud::BackendConnection conn(url, "", "", false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  // Per-topic counts for each file alone (the expected per-topic SUM).
  std::unordered_map<std::string, std::uint64_t> a_by_topic;
  std::unordered_map<std::string, std::uint64_t> b_by_topic;
  const auto a_stats = downloadByTopic(conn, {kStitchKeyA}, &a_by_topic, nullptr);
  const auto b_stats = downloadByTopic(conn, {kStitchKeyB}, &b_by_topic, nullptr);
  EXPECT_EQ(a_stats.messages_received, kStitchMessagesA);
  EXPECT_EQ(b_stats.messages_received, kStitchMessagesB);

  // ONE stitched session over BOTH files.
  std::unordered_map<std::string, std::uint64_t> stitched_by_topic;
  std::uint64_t sub_id = 0;
  const auto stats = downloadByTopic(conn, {kStitchKeyA, kStitchKeyB}, &stitched_by_topic, &sub_id);

  EXPECT_NE(sub_id, 0u) << "stitched session must have a subscription id";
  EXPECT_EQ(stats.eos, mcap_cloud::SessionEos::Complete) << "error: " << stats.error;
  EXPECT_EQ(stats.messages_received, kStitchMessages) << "stitched received count mismatch";
  EXPECT_EQ(stats.eos_total_messages_sent, kStitchMessages) << "server Eos.total_messages_sent mismatch";
  EXPECT_TRUE(stats.error.empty());

  // Each topic's stitched row == the SUM of that topic across both files.
  std::unordered_map<std::string, std::uint64_t> expected_sum = a_by_topic;
  for (const auto& [topic, count] : b_by_topic) {
    expected_sum[topic] += count;
  }
  EXPECT_EQ(stitched_by_topic.size(), expected_sum.size()) << "stitched topic set mismatch";
  for (const auto& [topic, count] : expected_sum) {
    EXPECT_EQ(stitched_by_topic[topic], count) << "per-topic stitched count mismatch for " << topic;
  }
}

// B13 `debug` verb behavior: the CLI's `debug` opens a fresh session and prints
// the first N decoded messages, returning false from the sink after N to abort
// the download WITHOUT writing a file. This exercises that exact path through the
// REAL session: a handler that aborts after kDebugLimit messages stops the stream
// early (received == kDebugLimit), it maps topic_id -> a known topic name, and
// the clean sink-abort terminates as CANCELLED with NO error.
TEST(McapCloudSessionDownloadLive, DebugVerbFirstN) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live session test skipped";
  }
  mcap_cloud::BackendConnection conn(url, "", "", false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  mcap_cloud::OpenSessionParams params;
  params.s3_keys = {kKnownSequence};
  mcap_cloud::SessionInfo info;
  std::string err;
  ASSERT_TRUE(conn.openSessionFresh(params, &info, &err)) << "openSessionFresh failed: " << err;
  ASSERT_FALSE(info.topics.empty());

  // topic_id -> name (what `debug` prints): every delivered message must resolve.
  std::unordered_map<std::uint32_t, std::string> name_by_id;
  for (const auto& t : info.topics) {
    name_by_id.emplace(t.topic_id, t.topic_name);
  }

  constexpr std::uint64_t kDebugLimit = 10;
  std::uint64_t printed = 0;
  bool all_resolved = true;
  auto on_message = [&](const mcap_cloud::DecodedMessage& m) -> bool {
    if (name_by_id.find(m.topic_id) == name_by_id.end()) {
      all_resolved = false;
    }
    ++printed;
    return printed < kDebugLimit;  // abort after N (exactly the debug verb's sink)
  };
  const auto stats = conn.downloadSession(info, on_message);

  EXPECT_EQ(printed, kDebugLimit) << "debug sink should stop after N messages";
  EXPECT_TRUE(all_resolved) << "every delivered message's topic_id must map to a topic name";
  // A sink-abort terminates as CANCELLED with the documented "download aborted by
  // sink" marker (downloadSession's sink-false path). This is what `debug`'s
  // runDebug treats as the expected stop (it returns kExitOk on this; only a
  // transport/server ERROR is a real failure). Assert the exact as-built marker.
  EXPECT_EQ(stats.eos, mcap_cloud::SessionEos::Cancelled) << "clean sink-abort should terminate as CANCELLED";
  EXPECT_EQ(stats.error, "download aborted by sink") << "sink-abort carries the documented marker; got: " << stats.error;
}

// Topic-subset download: kSpeedTopic ("/imu") alone is exactly kSpeedMessages
// (2000) messages.
TEST(McapCloudSessionDownloadLive, SubsetTopicCount) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live session test skipped";
  }
  mcap_cloud::BackendConnection conn(url, "", "", false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  std::uint64_t counted = 0;
  const auto stats = download(conn, /*topics=*/{kSpeedTopic}, &counted);

  EXPECT_EQ(stats.eos, mcap_cloud::SessionEos::Complete) << "error: " << stats.error;
  EXPECT_EQ(counted, kSpeedMessages) << "subset client-side received count mismatch";
  EXPECT_EQ(stats.messages_received, kSpeedMessages);
  EXPECT_EQ(stats.eos_total_messages_sent, kSpeedMessages) << "server Eos.total_messages_sent mismatch";
}
