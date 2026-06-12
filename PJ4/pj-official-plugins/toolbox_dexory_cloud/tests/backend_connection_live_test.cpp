// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// LIVE test for BackendConnection::connect() + listSequences()/listTopics().
// Requires a running pj-cloud server; gated on DEXORY_CLOUD_LIVE_URL so the
// hermetic CI suite skips it:
//
//   DEXORY_CLOUD_LIVE_URL=ws://localhost:8080 ctest -R DexoryCloudBackendLive
//
// Regression context: connect() originally polled ix::WebSocket::getReadyState()
// right after the asynchronous start(); a fresh ix::WebSocket rests in
// ReadyState::Closed until its background thread begins connecting, so the very
// first poll misread the INITIAL Closed as a terminal failure and reported
// "could not open WebSocket ..." without ever dialing the server. This test
// exercises the real connect path end-to-end so that class of bug cannot ship
// unexercised again.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <vector>

#include "backend_connection.hpp"
#include "backend_types.hpp"

namespace {

const char* liveUrl() {
  return std::getenv("DEXORY_CLOUD_LIVE_URL");
}

// Ground truth for the Minio fixture set (see CLAUDE.md / harness brief).
constexpr int kExpectedSequenceCount = 8;
constexpr const char* kKnownSequence = "nissan_zala_50_zeg_1_0.mcap";
constexpr std::int64_t kImuMessageCount = 14904;
constexpr const char* kImuTopic = "/nissan/gps/duro/imu";

// Slice 7 stitched ground truth (pinned in lockstep with scripts/smoke.sh +
// session_download_live_test.cpp): two consecutive, time-disjoint nissan files
// stitch into one continuous logical stream of 65032 messages.
constexpr const char* kStitchKeyA = "nissan_zala_50_zeg_2_0.mcap";
constexpr std::int64_t kStitchMessagesA = 43301;
constexpr const char* kStitchKeyB = "nissan_zala_50_zeg_3_0.mcap";
constexpr std::int64_t kStitchMessagesB = 21731;
constexpr std::int64_t kStitchMessages = 65032;  // A + B

const std::vector<std::string> kKnownTopics = {
    "/nissan/gps/duro/current_pose", "/nissan/gps/duro/imu",   "/nissan/gps/duro/mag",
    "/nissan/gps/duro/status_string", "/nissan/vehicle_speed", "/nissan/vehicle_steering",
};

// B3 ground truth (Slice 14): the server's Hello handler now DERIVES
// BackendCapabilities from the catalog (was hardcoded). Pinned in lockstep with
// server/internal/catalog/caps.go:
//   - supports_file_hierarchy = (any s3_key contains '/'). The Dexory nissan
//     corpus is FLAT (object keys like "nissan_zala_50_zeg_1_0.mcap", no '/'),
//     so this stays false.
//   - metadata_key_vocabulary = the 8 constant DERIVED keys (caps.go
//     derivedMetadataKeys) UNION the distinct tags_effective keys, sorted +
//     de-duplicated. On a clean nissan bucket (no embedded MCAP tags) the
//     vocabulary is exactly the 8 derived keys; during the smoke tag-flow (step
//     h adds then removes a transient override tag) it can momentarily carry an
//     extra key. So we assert the DERIVED-KEY SUBSET is present + the list is
//     sorted (resilient to transient tags), NOT exact equality.
constexpr bool kExpectFileHierarchy = false;
// caps.go derivedMetadataKeys, sorted — the stable floor of the vocabulary.
const std::vector<std::string> kExpectDerivedVocabKeys = {
    "chunk_count", "duration_ns", "end_ns", "message_count", "s3_key", "size_bytes", "start_ns", "topic_count",
};

}  // namespace

TEST(DexoryCloudBackendLive, ConnectListSequencesAndTopics) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "DEXORY_CLOUD_LIVE_URL not set — live server test skipped";
  }

  dexory_cloud::BackendConnection conn(url, /*cert_path=*/"", /*api_key=*/"",
                                       /*allow_insecure=*/false);

  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;
  ASSERT_TRUE(conn.version().has_value());

  const auto sequences = conn.listSequences();
  ASSERT_FALSE(sequences.empty()) << "server returned no sequences";

  // Every sequence must resolve to a non-empty topic list (each MCAP has
  // at least one channel); exercise the first one.
  const auto topics = conn.listTopics(sequences.front().name);
  EXPECT_FALSE(topics.empty()) << "no topics for sequence " << sequences.front().name;
}

// B3: the HelloResponse.backend (BackendCapabilities) the server advertises is
// parsed and exposed via backendCapabilities(). The server now DERIVES these
// from the catalog (Slice 14, caps.go), so assert the derived contract: flat
// catalog (no hierarchy) for the Dexory nissan corpus, and a metadata
// vocabulary that (a) is sorted and (b) contains the constant derived-key set.
// The subset+sorted form is resilient to the smoke tag-flow transiently adding
// an override-tag key to the vocabulary (step h sets then unsets a tag).
TEST(DexoryCloudBackendLive, BackendCapabilities) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "DEXORY_CLOUD_LIVE_URL not set — live server test skipped";
  }

  dexory_cloud::BackendConnection conn(url, /*cert_path=*/"", /*api_key=*/"",
                                       /*allow_insecure=*/false);

  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  const auto caps = conn.backendCapabilities();
  ASSERT_TRUE(caps.has_value()) << "server advertised no BackendCapabilities (has_backend()==false)";

  // Flat nissan corpus: no object key contains '/', so no hierarchy.
  EXPECT_EQ(caps->supports_file_hierarchy, kExpectFileHierarchy)
      << "supports_file_hierarchy mismatch — derived from s3_key LIKE '%/%' (caps.go)";

  const auto& vocab = caps->metadata_key_vocabulary;

  // (a) The vocabulary is sorted + de-duplicated (caps.go sorts the union).
  EXPECT_TRUE(std::is_sorted(vocab.begin(), vocab.end())) << "metadata_key_vocabulary is not sorted";
  EXPECT_EQ(std::adjacent_find(vocab.begin(), vocab.end()), vocab.end())
      << "metadata_key_vocabulary has duplicate keys";

  // (b) Every constant derived key is present (the stable floor). Extra keys
  // (transient override tags during the smoke tag-flow) are tolerated.
  for (const auto& key : kExpectDerivedVocabKeys) {
    EXPECT_NE(std::find(vocab.begin(), vocab.end(), key), vocab.end())
        << "metadata_key_vocabulary missing derived key '" << key << "' (lockstep with caps.go)";
  }
}

// Ground-truth assertions against the known Minio fixture set: exact sequence
// count, the known nissan_zala_50 MCAP with its exact 6 topics, the imu topic's
// exact message count, and a non-empty sequence metadata map carrying a
// "message_count" key.
TEST(DexoryCloudBackendLive, GroundTruthCatalog) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "DEXORY_CLOUD_LIVE_URL not set — live server test skipped";
  }

  dexory_cloud::BackendConnection conn(url, /*cert_path=*/"", /*api_key=*/"",
                                       /*allow_insecure=*/false);

  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  const auto sequences = conn.listSequences();
  EXPECT_EQ(sequences.size(), static_cast<std::size_t>(kExpectedSequenceCount))
      << "expected exactly " << kExpectedSequenceCount << " sequences";

  // Locate the known sequence by name.
  const dexory_cloud::SequenceInfo* known = nullptr;
  for (const auto& seq : sequences) {
    if (seq.name == kKnownSequence) {
      known = &seq;
      break;
    }
  }
  ASSERT_NE(known, nullptr) << "sequence '" << kKnownSequence << "' not present";

  // Its metadata map must be non-empty and carry a "message_count" key.
  EXPECT_FALSE(known->user_metadata.empty()) << "sequence metadata map is empty";
  EXPECT_NE(known->user_metadata.find("message_count"), known->user_metadata.end())
      << "sequence metadata missing 'message_count' key";

  // Exactly the 6 known topics, by name (order-independent).
  const auto topics = conn.listTopics(kKnownSequence);
  ASSERT_EQ(topics.size(), kKnownTopics.size())
      << "expected exactly " << kKnownTopics.size() << " topics in " << kKnownSequence;

  std::vector<std::string> topic_names;
  topic_names.reserve(topics.size());
  for (const auto& topic : topics) {
    topic_names.push_back(topic.topic_name);
  }
  std::sort(topic_names.begin(), topic_names.end());
  std::vector<std::string> expected = kKnownTopics;
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(topic_names, expected) << "topic name set mismatch for " << kKnownSequence;

  // The imu topic's message_count is exact ground truth.
  const dexory_cloud::TopicInfo* imu = nullptr;
  for (const auto& topic : topics) {
    if (topic.topic_name == kImuTopic) {
      imu = &topic;
      break;
    }
  }
  ASSERT_NE(imu, nullptr) << "imu topic '" << kImuTopic << "' not present";
  EXPECT_EQ(imu->message_count, kImuMessageCount) << "imu message_count mismatch";
}

// Slice 7 stitched ground truth: the two consecutive nissan files used by the
// stitched download legs are present, time-disjoint and orderable, and carry the
// exact per-file message counts (43301 + 21731 = 65032) in their metadata. These
// constants are pinned in lockstep with scripts/smoke.sh + the session-download
// live test; this asserts the catalog still matches before a stitched pull runs.
TEST(DexoryCloudBackendLive, StitchedGroundTruth) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "DEXORY_CLOUD_LIVE_URL not set — live server test skipped";
  }

  dexory_cloud::BackendConnection conn(url, /*cert_path=*/"", /*api_key=*/"",
                                       /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  const auto sequences = conn.listSequences();
  auto find = [&](const char* name) -> const dexory_cloud::SequenceInfo* {
    for (const auto& seq : sequences) {
      if (seq.name == name) {
        return &seq;
      }
    }
    return nullptr;
  };
  auto msgCount = [](const dexory_cloud::SequenceInfo& s) -> std::int64_t {
    auto it = s.user_metadata.find("message_count");
    if (it == s.user_metadata.end()) {
      return -1;
    }
    return static_cast<std::int64_t>(std::stoll(it->second));
  };

  const dexory_cloud::SequenceInfo* a = find(kStitchKeyA);
  const dexory_cloud::SequenceInfo* b = find(kStitchKeyB);
  ASSERT_NE(a, nullptr) << "stitch key A '" << kStitchKeyA << "' not present";
  ASSERT_NE(b, nullptr) << "stitch key B '" << kStitchKeyB << "' not present";

  EXPECT_EQ(msgCount(*a), kStitchMessagesA) << "zeg_2 message_count mismatch";
  EXPECT_EQ(msgCount(*b), kStitchMessagesB) << "zeg_3 message_count mismatch";
  EXPECT_EQ(kStitchMessagesA + kStitchMessagesB, kStitchMessages);

  // Time-disjoint + orderable: one ends at or before the other starts (the
  // stitched pull requires pairwise non-overlapping, orderable ranges).
  const bool disjoint = (a->max_ts_ns <= b->min_ts_ns) || (b->max_ts_ns <= a->min_ts_ns);
  EXPECT_TRUE(disjoint) << "zeg_2 / zeg_3 time ranges overlap — stitching would be rejected";

  // Both resolve to file_ids in the order requested.
  ASSERT_FALSE(sequences.empty());
  std::vector<std::string> missing;
  const auto ids = conn.resolveFileIds({kStitchKeyA, kStitchKeyB}, &missing);
  EXPECT_TRUE(missing.empty());
  EXPECT_EQ(ids.size(), 2u);
}

// UpdateTags round-trip (Slice 6): set an override tag on the known sequence,
// confirm it surfaces in BOTH the flat user_metadata (the Lua-filter view) and
// the per-tag TagRow override view, then unset it and confirm it is gone from
// both. Cleans up after itself so the corpus is left untagged.
TEST(DexoryCloudBackendLive, UpdateTagsSetThenUnset) {
  const char* url = liveUrl();
  if (url == nullptr || *url == '\0') {
    GTEST_SKIP() << "DEXORY_CLOUD_LIVE_URL not set — live server test skipped";
  }

  dexory_cloud::BackendConnection conn(url, /*cert_path=*/"", /*api_key=*/"",
                                       /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;

  // listSequences() builds the name->file_id index updateTags() resolves against.
  ASSERT_FALSE(conn.listSequences().empty());

  const std::string kKey = "dexory_live_test_tag";
  const std::string kValue = "set-by-backend-live-test";

  // Helper: re-list and return the metadata + TagRow view for the known seq.
  auto findKnown = [&](std::vector<dexory_cloud::TagRow>* tags_out,
                       std::unordered_map<std::string, std::string>* meta_out) -> bool {
    for (const auto& seq : conn.listSequences()) {
      if (seq.name == kKnownSequence) {
        if (tags_out != nullptr) {
          *tags_out = seq.tags;
        }
        if (meta_out != nullptr) {
          *meta_out = seq.user_metadata;
        }
        return true;
      }
    }
    return false;
  };

  // --- set ---
  std::vector<dexory_cloud::TagRow> effective;
  ASSERT_TRUE(conn.updateTags(kKnownSequence, {{kKey, kValue}}, /*unset_keys=*/{}, &effective, &error))
      << "updateTags(set) failed: " << error;
  // The post-update effective view echoes the override tag.
  bool eff_has = false;
  for (const auto& t : effective) {
    if (t.key == kKey) {
      EXPECT_EQ(t.value, kValue);
      EXPECT_TRUE(t.is_override) << "set tag must be an override";
      eff_has = true;
    }
  }
  EXPECT_TRUE(eff_has) << "UpdateTagsResponse.effective_tags missing the set tag";

  // A fresh listSequences() shows it in BOTH views (the Lua filter relies on the
  // flat map; the editor relies on the TagRow override bit).
  {
    std::vector<dexory_cloud::TagRow> tags;
    std::unordered_map<std::string, std::string> meta;
    ASSERT_TRUE(findKnown(&tags, &meta));
    ASSERT_NE(meta.find(kKey), meta.end()) << "flat metadata missing the set tag";
    EXPECT_EQ(meta.at(kKey), kValue);
    bool row_has = false;
    for (const auto& t : tags) {
      if (t.key == kKey) {
        EXPECT_EQ(t.value, kValue);
        EXPECT_TRUE(t.is_override);
        row_has = true;
      }
    }
    EXPECT_TRUE(row_has) << "FileSummary.tags missing the set override tag";
  }

  // --- unset (cleanup) ---
  ASSERT_TRUE(conn.updateTags(kKnownSequence, /*set_tags=*/{}, {kKey}, /*effective_out=*/nullptr, &error))
      << "updateTags(unset) failed: " << error;
  {
    std::vector<dexory_cloud::TagRow> tags;
    std::unordered_map<std::string, std::string> meta;
    ASSERT_TRUE(findKnown(&tags, &meta));
    EXPECT_EQ(meta.find(kKey), meta.end()) << "flat metadata still has the unset tag";
    for (const auto& t : tags) {
      EXPECT_NE(t.key, kKey) << "FileSummary.tags still has the unset tag";
    }
  }

  // Unknown sequence is a clean failure, not a crash.
  EXPECT_FALSE(conn.updateTags("definitely-not-a-real-sequence.mcap", {{"k", "v"}}, {}, nullptr, &error));
  EXPECT_FALSE(error.empty());
}
