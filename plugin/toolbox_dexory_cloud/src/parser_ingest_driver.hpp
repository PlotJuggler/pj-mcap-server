// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// ParserIngestDriver (Slice 16) — replaces RosDecodeDriver's decode-in-plugin
// (rosx_introspection) with HOST-delegated parsing: one ensureParserBinding
// per topic, one pushMessage per raw CDR record, through the toolbox
// parser-ingest tail slots (SDK >= 0.6.1). The host's parser plugins
// (parser_ros) write the scalars (specialized Imu/Pose handlers included) and
// classify/store object topics (tf, pointclouds, images, grids) with a
// render-time parser registered — 3D-draggable AND renderable.
//
// On hosts without the tail slots every topic reports skip_reason
// "host parser ingest unavailable: ..." — the plugin ships no fallback decoder.
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <pj_base/sdk/data_source_host_views.hpp>
#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/toolbox_plugin_base.hpp>

#include "backend_types.hpp"    // SessionInfo / SessionTopic / SessionSchema
#include "decoded_message.hpp"  // DecodedMessage (protobuf-free)

namespace dexory_cloud {

struct IngestTopic {
  std::uint32_t topic_id = 0;
  std::string topic_name;
  std::string type_name;  // schema name verbatim (e.g. "sensor_msgs/msg/Imu")
  PJ::ParserBindingHandle binding{};
  bool decodable = false;
  bool bound = false;  // binding created (lazily, on the FIRST message)
  // Deferred ParserBindingRequest ingredients: binding happens on the first
  // message (2026-07-12), so a topic that delivers NOTHING never registers in
  // the host data tree — zero-message entries users could drag with no effect.
  std::string schema_encoding;
  std::string schema_data;
  std::string skip_reason;          // set when !decodable
  std::uint64_t rows = 0;           // messages pushed to the host
  std::uint64_t decode_errors = 0;  // pushMessage failures
};

struct IngestBindResult {
  std::size_t decodable = 0;
  std::vector<std::string> errors;  // "<topic> (<type>): <reason>" per skipped topic
};

// Lifetime: the driver retains non-owning host views; the toolbox runtime host
// must outlive the driver (the destructor calls releaseParserIngest through it).
class ParserIngestDriver {
 public:
  ParserIngestDriver() = default;
  ~ParserIngestDriver();  // finalize() if the caller forgot

  ParserIngestDriver(const ParserIngestDriver&) = delete;
  ParserIngestDriver& operator=(const ParserIngestDriver&) = delete;

  // Creates the host parser-ingest context for `ds` and RESOLVES each session
  // topic's schema. Parser bindings are created lazily by decode() on a topic's
  // first message; schema-less topics become !decodable here with a reason.
  IngestBindResult bindSession(
      PJ::ToolboxRuntimeHostView runtime, PJ::sdk::DataSourceHandle ds, const SessionInfo& info);

  // Push one raw message to its topic's binding. Best-effort: failures count
  // into decode_errors and return false.
  bool decode(const DecodedMessage& m);

  // releaseParserIngest (host flushes all parser write hosts). Idempotent.
  // MUST run after the download ends and BEFORE notifyDataChanged so the
  // catalog rebuild sees sealed rows.
  void finalize();

  [[nodiscard]] const std::unordered_map<std::uint32_t, IngestTopic>& decoders() const {
    return topics_;
  }
  [[nodiscard]] bool hasDecodable() const;
  [[nodiscard]] std::unordered_map<std::uint32_t, std::uint64_t> decodedCounts() const;
  [[nodiscard]] std::unordered_map<std::uint32_t, std::uint64_t> errorCounts() const;

 private:
  PJ::ToolboxRuntimeHostView runtime_{};
  PJ::ParserIngestHostView ingest_{};
  std::uint32_t source_id_ = 0;
  bool active_ = false;
  std::unordered_map<std::uint32_t, IngestTopic> topics_;
};

}  // namespace dexory_cloud
