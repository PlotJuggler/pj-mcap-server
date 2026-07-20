// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// This TU owns the single MCAP_IMPLEMENTATION definition for the CLI: it pulls
// in the vendored Foxglove mcap writer implementation (writer.inl) exactly once.

#include "session_download.hpp"

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "session_decode.hpp"  // full DecodedMessage definition

#define MCAP_IMPLEMENTATION
#include <mcap/writer.hpp>

namespace mcap_cloud {

SessionStats downloadToMcap(BackendConnection& conn, const OpenSessionParams& params, const std::string& out_path,
                            SessionInfo* info_out, std::string* error_out) {
  SessionStats stats;
  auto set_error = [&](const std::string& msg) {
    stats.error = msg;
    if (error_out != nullptr) {
      *error_out = msg;
    }
  };

  SessionInfo info;
  std::string open_err;
  if (!conn.openSessionFresh(params, &info, &open_err)) {
    set_error(open_err.empty() ? "openSessionFresh failed" : open_err);
    stats.eos = SessionEos::Error;
    return stats;
  }
  if (info_out != nullptr) {
    *info_out = info;
  }

  // Open the output MCAP. CHUNKED + ZSTD, matching the Go reference downloader so
  // both stacks produce equivalent reconstructions.
  mcap::McapWriter writer;
  mcap::McapWriterOptions options("");  // empty profile
  options.compression = mcap::Compression::Zstd;
  options.compressionLevel = mcap::CompressionLevel::Default;
  options.chunkSize = 4 * 1024 * 1024;
  const mcap::Status open_status = writer.open(out_path, options);
  if (!open_status.ok()) {
    set_error("could not open output MCAP '" + out_path + "': " + open_status.message);
    stats.eos = SessionEos::Error;
    return stats;
  }

  // Register schemas (schema_id 0 == "no schema", skip). The writer assigns its
  // own ids on addSchema, so keep wire schema_id -> writer SchemaId mapping.
  std::unordered_map<std::uint32_t, mcap::SchemaId> schema_id_map;
  for (const auto& s : info.schemas) {
    if (s.schema_id == 0) {
      continue;
    }
    mcap::Schema schema(s.name, s.encoding, s.data);
    writer.addSchema(schema);  // fills schema.id
    schema_id_map[s.schema_id] = schema.id;
  }

  // Register channels: wire topic_id -> writer ChannelId. The channel references
  // the writer's SchemaId (0 when the wire schema_id was 0 / unmapped).
  std::unordered_map<std::uint32_t, mcap::ChannelId> channel_id_map;
  for (const auto& t : info.topics) {
    mcap::SchemaId mapped_schema = 0;
    if (auto it = schema_id_map.find(t.schema_id); it != schema_id_map.end()) {
      mapped_schema = it->second;
    }
    mcap::Channel channel(t.topic_name, t.message_encoding, mapped_schema);
    writer.addChannel(channel);  // fills channel.id
    channel_id_map[t.topic_id] = channel.id;
  }

  // Per-channel monotonic sequence (matches the Go reference downloader).
  std::unordered_map<mcap::ChannelId, std::uint32_t> seq_by_channel;

  auto on_message = [&](const DecodedMessage& m) -> bool {
    auto it = channel_id_map.find(m.topic_id);
    if (it == channel_id_map.end()) {
      // A message for a topic_id not in the dictionary should never happen; skip
      // defensively rather than crash. (The session contract guarantees every
      // emitted topic_id is in topic_id_map.)
      return true;
    }
    const mcap::ChannelId chan = it->second;
    mcap::Message rec;
    rec.channelId = chan;
    rec.sequence = seq_by_channel[chan]++;
    rec.logTime = static_cast<mcap::Timestamp>(m.log_time_ns);
    rec.publishTime = static_cast<mcap::Timestamp>(m.publish_time_ns);
    rec.dataSize = m.payload.size();
    rec.data = reinterpret_cast<const std::byte*>(m.payload.data());
    const mcap::Status ws = writer.write(rec);
    if (!ws.ok()) {
      set_error("mcap write failed: " + ws.message);
      return false;
    }
    return true;
  };

  stats = conn.downloadSession(info, on_message);

  // Always close the writer (flush footer + index) so even a partial file is a
  // readable MCAP. close() is safe regardless of how the stream ended.
  writer.close();

  if (!stats.error.empty() && error_out != nullptr) {
    *error_out = stats.error;
  }
  return stats;
}

}  // namespace mcap_cloud
