// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "session_mcap_writer.hpp"

#include <gtest/gtest.h>
#include <mcap/reader.hpp>

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

namespace {

namespace fs = std::filesystem;

struct TempMcap {
  TempMcap() {
    path = fs::temp_directory_path() / "session-mcap-writer-test.mcap";
    std::error_code ec;
    fs::remove(path, ec);
  }
  ~TempMcap() {
    std::error_code ec;
    fs::remove(path, ec);
  }
  fs::path path;
};

mcap_cloud::SessionInfo sessionInfo() {
  mcap_cloud::SessionInfo info;
  info.schemas = {
      {.schema_id = 5, .name = "demo/msg/One", .encoding = "ros2msg", .data = "int32 value"},
      {.schema_id = 8, .name = "demo/msg/Two", .encoding = "ros2msg", .data = "string value"},
  };
  info.topics = {
      {.topic_id = 11, .topic_name = "/one", .schema_id = 5, .message_encoding = "cdr"},
      {.topic_id = 22, .topic_name = "/two", .schema_id = 8, .message_encoding = "cdr"},
  };
  return info;
}

}  // namespace

TEST(SessionMcapWriter, RoundTripsSchemasChannelsTimesPayloadsAndSequences) {
  TempMcap temp;
  mcap_cloud::SessionMcapWriter writer;
  std::string error;
  const std::string compressible_payload(4096, 'A');
  ASSERT_TRUE(writer.open(temp.path.string(), sessionInfo(), &error)) << error;
  ASSERT_TRUE(writer.write(
      {.topic_id = 11, .schema_id = 5, .log_time_ns = 100, .publish_time_ns = 90,
       .payload = compressible_payload},
      &error))
      << error;
  ASSERT_TRUE(writer.write(
      {.topic_id = 22, .schema_id = 8, .log_time_ns = 200, .publish_time_ns = 180, .payload = "two"},
      &error))
      << error;
  ASSERT_TRUE(writer.write(
      {.topic_id = 11, .schema_id = 5, .log_time_ns = 300, .publish_time_ns = 280, .payload = "one-b"},
      &error))
      << error;
  ASSERT_TRUE(writer.close(&error)) << error;

  mcap::McapReader reader;
  const mcap::Status open_status = reader.open(temp.path.string());
  ASSERT_TRUE(open_status.ok()) << open_status.message;
  const mcap::Status summary_status = reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan);
  ASSERT_TRUE(summary_status.ok()) << summary_status.message;

  ASSERT_EQ(reader.schemas().size(), 2u);
  ASSERT_EQ(reader.channels().size(), 2u);
  ASSERT_EQ(reader.chunkIndexes().size(), 1u);
  EXPECT_EQ(reader.chunkIndexes().front().compression, "zstd");
  // Statistics presence is load-bearing: the server-side FormatCodec (and any
  // future cache-adoption load) REJECTS unsummarized/statistics-less files —
  // an options tweak dropping them must fail here, not in production.
  ASSERT_TRUE(reader.statistics().has_value());
  EXPECT_EQ(reader.statistics()->messageCount, 3u);
  EXPECT_EQ(reader.statistics()->channelCount, 2u);

  struct Seen {
    std::string topic;
    std::string schema;
    std::uint32_t sequence;
    std::uint64_t log_time;
    std::uint64_t publish_time;
    std::string payload;
  };
  std::vector<Seen> seen;
  for (const mcap::MessageView& view : reader.readMessages()) {
    seen.push_back({
        .topic = view.channel->topic,
        .schema = view.schema ? view.schema->name : std::string{},
        .sequence = view.message.sequence,
        .log_time = view.message.logTime,
        .publish_time = view.message.publishTime,
        .payload = std::string(
            reinterpret_cast<const char*>(view.message.data),
            static_cast<std::size_t>(view.message.dataSize)),
    });
  }
  reader.close();

  ASSERT_EQ(seen.size(), 3u);
  EXPECT_EQ(seen[0].topic, "/one");
  EXPECT_EQ(seen[0].schema, "demo/msg/One");
  EXPECT_EQ(seen[0].sequence, 0u);
  EXPECT_EQ(seen[0].log_time, 100u);
  EXPECT_EQ(seen[0].publish_time, 90u);
  EXPECT_EQ(seen[0].payload, compressible_payload);
  EXPECT_EQ(seen[1].topic, "/two");
  EXPECT_EQ(seen[1].sequence, 0u);
  EXPECT_EQ(seen[2].topic, "/one");
  EXPECT_EQ(seen[2].sequence, 1u);
  EXPECT_EQ(seen[2].payload, "one-b");
}

TEST(SessionMcapWriter, SkipsUnknownTopicAndRejectsBadOutputPath) {
  TempMcap temp;
  mcap_cloud::SessionMcapWriter writer;
  std::string error;
  ASSERT_TRUE(writer.open(temp.path.string(), sessionInfo(), &error)) << error;
  // An out-of-dictionary topic_id is a violated server invariant: skipped
  // defensively with a count — one stray record must never abort (or un-save)
  // a multi-GiB stream.
  EXPECT_TRUE(writer.write({.topic_id = 999, .payload = "bad"}, &error)) << error;
  EXPECT_EQ(writer.skippedUnknownTopics(), 1u);
  ASSERT_TRUE(writer.close(&error)) << error;

  mcap_cloud::SessionMcapWriter bad_writer;
  EXPECT_FALSE(bad_writer.open(
      (temp.path / "missing-parent" / "out.mcap").string(), sessionInfo(), &error));
  EXPECT_NE(error.find("could not open output MCAP"), std::string::npos);
}
