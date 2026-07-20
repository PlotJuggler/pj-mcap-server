// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "parser_ingest_driver.hpp"

#include <memory>

namespace mcap_cloud {

ParserIngestDriver::~ParserIngestDriver() {
  finalize();
}

IngestBindResult ParserIngestDriver::bindSession(
    PJ::ToolboxRuntimeHostView runtime, PJ::sdk::DataSourceHandle ds, const SessionInfo& info) {
  // Single-use by convention (fresh driver per download); a re-bind releases
  // the prior context so its rows are sealed rather than silently leaked.
  if (active_ || !topics_.empty()) {
    finalize();
    topics_.clear();
  }
  IngestBindResult result;
  runtime_ = runtime;
  source_id_ = ds.id;

  std::unordered_map<std::uint32_t, const SessionSchema*> schema_by_id;
  for (const auto& s : info.schemas) {
    schema_by_id.emplace(s.schema_id, &s);
  }

  auto ingest_or = runtime_.createParserIngest(ds.id);
  if (!ingest_or.has_value()) {
    // Older/unconfigured host: every topic carries the reason; no fallback.
    // Error format: "<topic> (<type>): <reason>" — type from schema_by_id when
    // available, "(no schema)" when no schema exists for the topic's schema_id.
    for (const auto& t : info.topics) {
      IngestTopic it;
      it.topic_id = t.topic_id;
      it.topic_name = t.topic_name;
      it.skip_reason = "host parser ingest unavailable: " + ingest_or.error();
      const auto sit = schema_by_id.find(t.schema_id);
      const std::string type_str = (sit != schema_by_id.end()) ? sit->second->name : "(no schema)";
      result.errors.push_back(t.topic_name + " (" + type_str + "): " + it.skip_reason);
      topics_.emplace(t.topic_id, std::move(it));
    }
    return result;
  }
  ingest_ = *ingest_or;
  active_ = true;

  for (const auto& t : info.topics) {
    IngestTopic it;
    it.topic_id = t.topic_id;
    it.topic_name = t.topic_name;

    const auto sit = schema_by_id.find(t.schema_id);
    if (sit == schema_by_id.end()) {
      it.skip_reason = "schema " + std::to_string(t.schema_id) + " missing from session dictionary";
      // type unknown at this point; use "(no schema)" per error-format convention.
      result.errors.push_back(t.topic_name + " (no schema): " + it.skip_reason);
      topics_.emplace(t.topic_id, std::move(it));
      continue;
    }
    const SessionSchema& schema = *sit->second;
    it.type_name = schema.name;

    // Binding is DEFERRED to the first message (decode() below): binding a
    // topic registers it in the host data tree, and a session routinely carries
    // topics that deliver nothing (channels declared in the MCAP summary that
    // never published — 366 of them on real S3-use-case rosbox files). Eager binding
    // filled the tree with zero-message entries users could drag to no effect.
    it.schema_encoding = schema.encoding;
    it.schema_data = schema.data;
    it.decodable = true;
    ++result.decodable;
    topics_.emplace(t.topic_id, std::move(it));
  }
  return result;
}

bool ParserIngestDriver::decode(const DecodedMessage& m) {
  auto it = topics_.find(m.topic_id);
  if (it == topics_.end() || !it->second.decodable || !active_) {
    return false;
  }
  if (!it->second.bound) {
    // First message for this topic: create the host parser binding NOW (the
    // deferred bindSession step). Runs in the same host-write critical section
    // as pushMessage, so the threading discipline is unchanged.
    //
    // Fields forwarded VERBATIM — same fields data_load_mcap forwards; type
    // name normalization happens inside parser_ros.
    //
    // parser_config_json = "{}" (non-empty) is LOAD-BEARING: the host skips
    // parser->loadConfig() for an empty config, and parser_ros selects its
    // SPECIALIZED handlers (tf -> kFrameTransforms, pointclouds, images...)
    // only inside loadConfig; "" silently degrades every topic to generic
    // scalar-only ingest. Empirically pinned by ToolboxParserIngestRealRosTest.
    IngestTopic& topic = it->second;
    auto binding = ingest_.ensureParserBinding(PJ::ParserBindingRequest{
        .topic_name = topic.topic_name,
        .parser_encoding = topic.schema_encoding,
        .type_name = topic.type_name,
        .schema = PJ::Span<const uint8_t>(
            reinterpret_cast<const std::uint8_t*>(topic.schema_data.data()), topic.schema_data.size()),
        .parser_config_json = "{}",
    });
    if (!binding.has_value()) {
      topic.decodable = false;
      topic.skip_reason = binding.error();
      return false;
    }
    topic.binding = *binding;
    topic.bound = true;
  }
  // One owned copy per message; the shared_ptr doubles as the PayloadView
  // anchor, so the host's lazy ObjectStore closures (tf/pointcloud re-reads
  // at render time) stay valid for the dataset's lifetime. pushMessage's
  // contract: the fetcher must be idempotent and thread-safe — a capture-only
  // closure over an immutable shared buffer is both.
  auto owned = std::make_shared<const std::string>(m.payload);
  auto status = ingest_.pushMessage(
      it->second.binding, PJ::Timestamp{m.log_time_ns}, [owned]() -> PJ::sdk::PayloadView {
        return PJ::sdk::PayloadView{
            PJ::Span<const std::uint8_t>(
                reinterpret_cast<const std::uint8_t*>(owned->data()), owned->size()),
            owned};
      });
  if (!status.has_value()) {
    ++it->second.decode_errors;
    return false;
  }
  ++it->second.rows;
  return true;
}

void ParserIngestDriver::finalize() {
  if (!active_) {
    return;
  }
  active_ = false;
  ingest_ = PJ::ParserIngestHostView{};
  (void)runtime_.releaseParserIngest(source_id_);
}

bool ParserIngestDriver::hasDecodable() const {
  for (const auto& [id, t] : topics_) {
    if (t.decodable) {
      return true;
    }
  }
  return false;
}

std::unordered_map<std::uint32_t, std::uint64_t> ParserIngestDriver::decodedCounts() const {
  std::unordered_map<std::uint32_t, std::uint64_t> out;
  for (const auto& [id, t] : topics_) {
    out.emplace(id, t.rows);
  }
  return out;
}

std::unordered_map<std::uint32_t, std::uint64_t> ParserIngestDriver::errorCounts() const {
  std::unordered_map<std::uint32_t, std::uint64_t> out;
  for (const auto& [id, t] : topics_) {
    out.emplace(id, t.decode_errors);
  }
  return out;
}

}  // namespace mcap_cloud
