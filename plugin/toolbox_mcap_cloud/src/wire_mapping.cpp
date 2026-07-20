// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT

#include "wire_mapping.hpp"

#include <string>
#include <utility>

namespace mcap_cloud {

MappedSequence mapFileSummary(const pj_cloud::v1::FileSummary& summary, const pj_cloud::v1::FlatMetadata* metadata) {
  MappedSequence mapped;
  mapped.file_id = summary.id();

  SequenceInfo info;
  // The s3_key is the human-readable filename — that's what the table shows.
  info.name = summary.s3_key();
  if (summary.has_recorded()) {
    info.min_ts_ns = summary.recorded().start_ns();
    info.max_ts_ns = summary.recorded().end_ns();
  }
  info.total_size_bytes = static_cast<std::int64_t>(summary.size_bytes());

  // Flat tags_effective view -> user_metadata, copied verbatim (the Lua filter
  // consumes this 1:1). Absent metadata (no entry for this file id) is fine —
  // the map is simply left empty.
  if (metadata != nullptr) {
    for (const auto& kv : metadata->entries()) {
      info.user_metadata.emplace(kv.first, kv.second);
    }
  }

  // FileSummary.tags carries the per-tag is_override bit (same effective set as
  // the flat map). The tag editor reads this to tint override rows.
  info.tags = mapTags(summary.tags());

  mapped.info = std::move(info);
  return mapped;
}

std::vector<TagRow> mapTags(const ::google::protobuf::RepeatedPtrField<pj_cloud::v1::Tag>& tags) {
  std::vector<TagRow> out;
  out.reserve(static_cast<std::size_t>(tags.size()));
  for (const auto& t : tags) {
    out.push_back(TagRow{t.key(), t.value(), t.is_override()});
  }
  return out;
}

std::vector<MappedSequence> mapListFilesResponse(const pj_cloud::v1::ListFilesResponse& response) {
  std::vector<MappedSequence> out;
  out.reserve(static_cast<std::size_t>(response.files_size()));
  const auto& metadata_map = response.metadata();
  for (const auto& summary : response.files()) {
    // metadata is keyed by file id as a DECIMAL STRING (the client-ingest
    // contract); look up this file's flat tags by that key.
    const std::string key = std::to_string(summary.id());
    const pj_cloud::v1::FlatMetadata* meta = nullptr;
    if (auto it = metadata_map.find(key); it != metadata_map.end()) {
      meta = &it->second;
    }
    out.push_back(mapFileSummary(summary, meta));
  }
  return out;
}

TopicInfo mapTopicInfo(const pj_cloud::v1::TopicInfo& topic) {
  TopicInfo info;
  info.topic_name = topic.name();
  info.message_count = static_cast<std::int64_t>(topic.message_count());
  // The wire TopicInfo has no per-topic byte size (only message_count); leave
  // total_size_bytes at 0 so the topic table's Size column stays blank rather
  // than showing a misleading value.
  //
  // Surface schema name/encoding through the Arrow-free schema_fields renderer.
  // The Info panel's "Fields (N):" block draws (name, type) pairs; we reuse it
  // for the schema descriptor since the MCAP wire carries no field-level schema.
  if (!topic.schema_name().empty()) {
    info.schema_fields.emplace_back("schema", topic.schema_name());
  }
  if (!topic.schema_encoding().empty()) {
    info.schema_fields.emplace_back("encoding", topic.schema_encoding());
  }
  if (topic.message_count() > 0) {
    info.schema_fields.emplace_back("messages", std::to_string(topic.message_count()));
  }
  return info;
}

std::vector<TopicInfo> mapGetFileResponseTopics(const pj_cloud::v1::GetFileResponse& response) {
  std::vector<TopicInfo> out;
  out.reserve(static_cast<std::size_t>(response.topics_size()));
  for (const auto& topic : response.topics()) {
    out.push_back(mapTopicInfo(topic));
  }
  return out;
}

}  // namespace mcap_cloud
