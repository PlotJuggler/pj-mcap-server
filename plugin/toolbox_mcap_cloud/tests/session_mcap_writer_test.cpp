// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "session_mcap_writer.hpp"

#include <gtest/gtest.h>
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>

#include <cstddef>
#include <filesystem>
#include <fstream>
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

// Minimal caller-owned sink for the open(IWritable&) overload: a bare
// std::ofstream wrapper with none of CheckedFileWriter's policy — creation
// policy (exclusive create, permissions, fsync) is exactly what the seam
// moves OUT of the writer.
class OfstreamSink final : public mcap::IWritable {
 public:
  explicit OfstreamSink(const fs::path& path) : stream_(path, std::ios::binary | std::ios::trunc) {}

  void handleWrite(const std::byte* data, std::uint64_t size) override {
    stream_.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
    size_ += size;
  }
  void end() override {
    stream_.flush();
    stream_.close();
  }
  void flush() override {
    stream_.flush();
  }
  std::uint64_t size() const override {
    return size_;
  }

 private:
  std::ofstream stream_;
  std::uint64_t size_ = 0;
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

// The caller-owned-sink overload (the future cache tee's entry point) writes
// the same valid, summarized MCAP as the path overload — one init path for
// the schema/channel dictionaries.
TEST(SessionMcapWriter, SinkOverloadRoundTrips) {
  TempMcap temp;
  std::string error;
  {
    OfstreamSink sink(temp.path);
    mcap_cloud::SessionMcapWriter writer;
    ASSERT_TRUE(writer.open(sink, sessionInfo(), &error)) << error;
    ASSERT_TRUE(writer.write(
        {.topic_id = 11, .schema_id = 5, .log_time_ns = 100, .publish_time_ns = 90, .payload = "one-a"},
        &error))
        << error;
    ASSERT_TRUE(writer.write(
        {.topic_id = 22, .schema_id = 8, .log_time_ns = 200, .publish_time_ns = 180, .payload = "two"},
        &error))
        << error;
    ASSERT_TRUE(writer.close(&error)) << error;
    // `sink` outlives close() (the overload's lifetime contract) and is only
    // destroyed here, after the footer has gone through it.
  }

  mcap::McapReader reader;
  const mcap::Status open_status = reader.open(temp.path.string());
  ASSERT_TRUE(open_status.ok()) << open_status.message;
  const mcap::Status summary_status = reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan);
  ASSERT_TRUE(summary_status.ok()) << summary_status.message;
  ASSERT_EQ(reader.schemas().size(), 2u);
  ASSERT_EQ(reader.channels().size(), 2u);
  ASSERT_TRUE(reader.statistics().has_value());
  EXPECT_EQ(reader.statistics()->messageCount, 2u);

  std::vector<std::string> payloads;
  for (const mcap::MessageView& view : reader.readMessages()) {
    payloads.emplace_back(reinterpret_cast<const char*>(view.message.data),
                          static_cast<std::size_t>(view.message.dataSize));
  }
  reader.close();
  ASSERT_EQ(payloads.size(), 2u);
  EXPECT_EQ(payloads[0], "one-a");
  EXPECT_EQ(payloads[1], "two");
}

// writeMetadata embeds a named MCAP Metadata record (the source-descriptor
// provenance hook) that lands in the summary's metadata index and reads back
// verbatim. mcap 2.1.1 has no readMetadata() convenience: the read path is
// readSummary -> metadataIndexes() -> ReadRecord + ParseMetadata.
TEST(SessionMcapWriter, WriteMetadataAppearsInMetadataIndex) {
  TempMcap temp;
  mcap_cloud::SessionMcapWriter writer;
  std::string error;
  const std::string kName = "mcap_cloud/source_descriptor";
  const std::string kJson = "{\"v\":1}";
  ASSERT_TRUE(writer.open(temp.path, sessionInfo(), &error)) << error;
  ASSERT_TRUE(writer.writeMetadata(kName, kJson, &error)) << error;
  ASSERT_TRUE(writer.write(
      {.topic_id = 11, .schema_id = 5, .log_time_ns = 100, .publish_time_ns = 90, .payload = "one-a"},
      &error))
      << error;
  ASSERT_TRUE(writer.close(&error)) << error;

  mcap::McapReader reader;
  const mcap::Status open_status = reader.open(temp.path.string());
  ASSERT_TRUE(open_status.ok()) << open_status.message;
  const mcap::Status summary_status = reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan);
  ASSERT_TRUE(summary_status.ok()) << summary_status.message;

  ASSERT_TRUE(reader.statistics().has_value());
  EXPECT_EQ(reader.statistics()->metadataCount, 1u);
  ASSERT_EQ(reader.metadataIndexes().count(kName), 1u);
  const mcap::MetadataIndex& index = reader.metadataIndexes().find(kName)->second;

  mcap::Record record;
  const mcap::Status read_status = mcap::McapReader::ReadRecord(*reader.dataSource(), index.offset, &record);
  ASSERT_TRUE(read_status.ok()) << read_status.message;
  mcap::Metadata metadata;
  const mcap::Status parse_status = mcap::McapReader::ParseMetadata(record, &metadata);
  ASSERT_TRUE(parse_status.ok()) << parse_status.message;
  reader.close();

  EXPECT_EQ(metadata.name, kName);
  ASSERT_EQ(metadata.metadata.size(), 1u);
  ASSERT_TRUE(metadata.metadata.count("json"));
  EXPECT_EQ(metadata.metadata.at("json"), kJson);
}

// writeMetadata is a between-open-and-first-write hook: after the first
// write() it fails (a Metadata record mid-stream would split the open chunk)
// and before open() it fails too.
TEST(SessionMcapWriter, WriteMetadataRejectedAfterFirstWriteAndBeforeOpen) {
  TempMcap temp;
  mcap_cloud::SessionMcapWriter writer;
  std::string error;
  EXPECT_FALSE(writer.writeMetadata("mcap_cloud/source_descriptor", "{}", &error));
  EXPECT_NE(error.find("not open"), std::string::npos) << error;

  ASSERT_TRUE(writer.open(temp.path, sessionInfo(), &error)) << error;
  ASSERT_TRUE(writer.write(
      {.topic_id = 11, .schema_id = 5, .log_time_ns = 100, .publish_time_ns = 90, .payload = "one-a"},
      &error))
      << error;
  EXPECT_FALSE(writer.writeMetadata("mcap_cloud/source_descriptor", "{}", &error));
  EXPECT_NE(error.find("first"), std::string::npos) << error;
  ASSERT_TRUE(writer.close(&error)) << error;
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
