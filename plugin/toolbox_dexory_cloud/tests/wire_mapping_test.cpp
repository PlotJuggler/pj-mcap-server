// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Socket-free protobuf round-trip tests for the wire->local mapping. We build
// canonical pj_cloud.v1 messages, serialize + parse them (exercising the real
// generated codec), then assert the pure mapping onto the dialog's
// SequenceInfo / TopicInfo structs. No live server is needed.

#include "wire_mapping.hpp"

#include <gtest/gtest.h>

#include <string>

#include "pj_cloud.pb.h"

namespace {

using dexory_cloud::mapGetFileResponseTopics;
using dexory_cloud::mapListFilesResponse;
using dexory_cloud::mapTags;

// Serialize then parse, so the test runs through the actual protobuf codec the
// BackendConnection uses on the wire — not just the in-memory message object.
template <typename T>
T roundTrip(const T& message) {
  std::string bytes;
  EXPECT_TRUE(message.SerializeToString(&bytes));
  T parsed;
  EXPECT_TRUE(parsed.ParseFromString(bytes));
  return parsed;
}

TEST(WireMapping, ListFilesResponseMapsFileSummaryToSequenceInfo) {
  pj_cloud::v1::ListFilesResponse response;

  auto* file = response.add_files();
  file->set_id(42);
  file->set_s3_key("nissan_zala_2024-01-01.mcap");
  file->set_size_bytes(123456);
  file->set_topic_count(3);
  file->set_message_count(9999);
  auto* recorded = file->mutable_recorded();
  recorded->set_start_ns(1000);
  recorded->set_end_ns(5000);

  // Flat metadata keyed by file id as a DECIMAL STRING (the client-ingest
  // contract). Maps verbatim into SequenceInfo.user_metadata.
  auto& meta = (*response.mutable_metadata())["42"];
  (*meta.mutable_entries())["robot_id"] = "zala-7";
  (*meta.mutable_entries())["operator"] = "alice";

  const auto mapped = mapListFilesResponse(roundTrip(response));

  ASSERT_EQ(mapped.size(), 1u);
  EXPECT_EQ(mapped[0].file_id, 42u);
  // name == s3_key (human-readable filename), not the numeric id.
  EXPECT_EQ(mapped[0].info.name, "nissan_zala_2024-01-01.mcap");
  EXPECT_EQ(mapped[0].info.min_ts_ns, 1000);
  EXPECT_EQ(mapped[0].info.max_ts_ns, 5000);
  EXPECT_EQ(mapped[0].info.total_size_bytes, 123456);
  ASSERT_EQ(mapped[0].info.user_metadata.size(), 2u);
  EXPECT_EQ(mapped[0].info.user_metadata.at("robot_id"), "zala-7");
  EXPECT_EQ(mapped[0].info.user_metadata.at("operator"), "alice");
}

TEST(WireMapping, ListFilesResponsePreservesOrderAndHandlesMissingMetadata) {
  pj_cloud::v1::ListFilesResponse response;

  auto* a = response.add_files();
  a->set_id(1);
  a->set_s3_key("a.mcap");
  auto* b = response.add_files();
  b->set_id(2);
  b->set_s3_key("b.mcap");

  // Only file 2 has metadata; file 1 should map to an empty user_metadata.
  auto& meta = (*response.mutable_metadata())["2"];
  (*meta.mutable_entries())["site"] = "warehouse-3";

  const auto mapped = mapListFilesResponse(roundTrip(response));

  ASSERT_EQ(mapped.size(), 2u);
  EXPECT_EQ(mapped[0].info.name, "a.mcap");
  EXPECT_TRUE(mapped[0].info.user_metadata.empty());
  EXPECT_EQ(mapped[1].info.name, "b.mcap");
  ASSERT_EQ(mapped[1].info.user_metadata.size(), 1u);
  EXPECT_EQ(mapped[1].info.user_metadata.at("site"), "warehouse-3");
}

TEST(WireMapping, GetFileResponseMapsTopicInfo) {
  pj_cloud::v1::GetFileResponse response;

  auto* t = response.add_topics();
  t->set_name("/nissan/gps/duro/imu");
  t->set_schema_name("sensor_msgs/msg/Imu");
  t->set_schema_encoding("ros2msg");
  t->set_message_count(4242);

  const auto topics = mapGetFileResponseTopics(roundTrip(response));

  ASSERT_EQ(topics.size(), 1u);
  EXPECT_EQ(topics[0].topic_name, "/nissan/gps/duro/imu");
  EXPECT_EQ(topics[0].message_count, 4242);

  // schema name/encoding/count ride into schema_fields so the Arrow-free
  // "Fields (N):" renderer surfaces them.
  bool has_schema = false;
  bool has_encoding = false;
  for (const auto& [name, value] : topics[0].schema_fields) {
    if (name == "schema" && value == "sensor_msgs/msg/Imu") {
      has_schema = true;
    }
    if (name == "encoding" && value == "ros2msg") {
      has_encoding = true;
    }
  }
  EXPECT_TRUE(has_schema);
  EXPECT_TRUE(has_encoding);
}

// ---- Tags (Slice 6) ---------------------------------------------------------

TEST(WireMapping, FileSummaryTagsMapToTagRowsWithOverrideBit) {
  pj_cloud::v1::ListFilesResponse response;

  auto* file = response.add_files();
  file->set_id(7);
  file->set_s3_key("tagged.mcap");
  // The flat metadata view (Lua filter) and the richer per-tag view
  // (FileSummary.tags, carries is_override) describe the SAME effective set.
  auto& meta = (*response.mutable_metadata())["7"];
  (*meta.mutable_entries())["verified"] = "yes";
  (*meta.mutable_entries())["site"] = "warehouse-3";

  auto* embedded = file->add_tags();
  embedded->set_key("site");
  embedded->set_value("warehouse-3");
  embedded->set_is_override(false);
  auto* override_tag = file->add_tags();
  override_tag->set_key("verified");
  override_tag->set_value("yes");
  override_tag->set_is_override(true);

  const auto mapped = mapListFilesResponse(roundTrip(response));

  ASSERT_EQ(mapped.size(), 1u);
  // The flat map is unchanged (Lua-filter contract).
  EXPECT_EQ(mapped[0].info.user_metadata.at("verified"), "yes");
  EXPECT_EQ(mapped[0].info.user_metadata.at("site"), "warehouse-3");
  // The parallel TagRow vector carries the per-tag override bit.
  ASSERT_EQ(mapped[0].info.tags.size(), 2u);
  EXPECT_EQ(mapped[0].info.tags[0].key, "site");
  EXPECT_EQ(mapped[0].info.tags[0].value, "warehouse-3");
  EXPECT_FALSE(mapped[0].info.tags[0].is_override);
  EXPECT_EQ(mapped[0].info.tags[1].key, "verified");
  EXPECT_EQ(mapped[0].info.tags[1].value, "yes");
  EXPECT_TRUE(mapped[0].info.tags[1].is_override);
}

TEST(WireMapping, FileSummaryWithNoTagsMapsToEmptyTagRows) {
  pj_cloud::v1::ListFilesResponse response;
  auto* file = response.add_files();
  file->set_id(1);
  file->set_s3_key("untagged.mcap");

  const auto mapped = mapListFilesResponse(roundTrip(response));
  ASSERT_EQ(mapped.size(), 1u);
  EXPECT_TRUE(mapped[0].info.tags.empty());
}

TEST(WireMapping, UpdateTagsResponseEffectiveTagsMapToTagRows) {
  // The exact field BackendConnection::updateTags() reads on success.
  pj_cloud::v1::UpdateTagsResponse response;
  auto* a = response.add_effective_tags();
  a->set_key("verified");
  a->set_value("yes");
  a->set_is_override(true);
  auto* b = response.add_effective_tags();
  b->set_key("recorder");
  b->set_value("zala-7");
  b->set_is_override(false);

  const auto parsed = roundTrip(response);
  const auto rows = mapTags(parsed.effective_tags());

  ASSERT_EQ(rows.size(), 2u);
  EXPECT_EQ(rows[0].key, "verified");
  EXPECT_EQ(rows[0].value, "yes");
  EXPECT_TRUE(rows[0].is_override);
  EXPECT_EQ(rows[1].key, "recorder");
  EXPECT_EQ(rows[1].value, "zala-7");
  EXPECT_FALSE(rows[1].is_override);
}

}  // namespace
