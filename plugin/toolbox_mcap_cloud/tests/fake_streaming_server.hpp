// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// FakeStreamingServer — an in-process fake pj_cloud server that ACTUALLY
// STREAMS a session (Hello -> HelloResponse; OpenSession -> plan then batches
// + Eos, or a stall, or an empty plan). Hoisted VERBATIM (pure move) from
// fetch_worker_cache_tee_test.cpp (stage-4 PR-1) so the direct-pull /
// promotion / provider suites (PR-3) drive the same wire shape. The
// deterministic session constants and the small tee-test helpers
// (identityFor / readFile / cacheRootHasPartial) ride along for the same
// reason.
#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <system_error>

#include <ixwebsocket/IXWebSocketServer.h>

#include "find_free_port.hpp"
#include "pj_cloud.pb.h"
#include "source_descriptor.hpp"

namespace mcap_cloud_test {

inline constexpr std::uint32_t kFakeTopicId = 1;
inline constexpr std::uint32_t kFakeSchemaId = 5;
inline constexpr int kFakeBatches = 3;
inline constexpr int kFakeMessagesPerBatch = 4;
inline constexpr std::size_t kFakePayloadBytes = 1024;

// A fake pj_cloud server that ACTUALLY STREAMS a session: Hello ->
// HelloResponse; OpenSession -> plan (1 topic / 1 schema) then, per mode,
// either kFakeBatches NONE-encoded batches + Eos{COMPLETE} or two batches
// followed by silence (the stall the cancel tests need).
class FakeStreamingServer {
 public:
  enum class Mode { kComplete, kStallAfterTwoBatches, kEmptyPlan };

  explicit FakeStreamingServer(Mode mode) : mode_(mode), port_(findFreePort()), server_(port_, "127.0.0.1") {
    server_.setOnClientMessageCallback([this](std::shared_ptr<ix::ConnectionState>,
                                              ix::WebSocket& ws,
                                              const ix::WebSocketMessagePtr& msg) {
      if (msg->type != ix::WebSocketMessageType::Message) {
        return;
      }
      pj_cloud::v1::ClientMessage request;
      if (!request.ParseFromString(msg->str)) {
        return;
      }
      if (request.has_hello()) {
        pj_cloud::v1::ServerMessage response;
        response.set_request_id(request.request_id());
        response.mutable_hello_response()->set_server_version("test-fake-1.0");
        send(ws, response);
        return;
      }
      if (!request.has_open_session()) {
        return;  // acks/cancels need no reply here
      }
      open_sessions_.fetch_add(1);
      pj_cloud::v1::ServerMessage response;
      response.set_request_id(request.request_id());
      auto* open = response.mutable_open_session();
      open->set_subscription_id(7);
      open->set_estimated_chunk_bytes(kFakeBatches * kFakeMessagesPerBatch * kFakePayloadBytes);
      open->set_approximate_messages(kFakeBatches * kFakeMessagesPerBatch);
      if (mode_ != Mode::kEmptyPlan) {
        auto* topic = open->add_topic_id_map();
        topic->set_topic_id(kFakeTopicId);
        topic->set_topic_name("/one");
        topic->set_schema_id(kFakeSchemaId);
        topic->set_message_encoding("cdr");
        auto* schema = open->add_schemas();
        schema->set_schema_id(kFakeSchemaId);
        schema->set_name("demo/msg/One");
        schema->set_encoding("ros2msg");
        schema->set_data("int32 value");
      }
      send(ws, response);
      if (mode_ == Mode::kEmptyPlan) {
        return;
      }
      const int batches = (mode_ == Mode::kComplete) ? kFakeBatches : 2;
      std::uint64_t sent = 0;
      for (int b = 0; b < batches; ++b) {
        pj_cloud::v1::ServerMessage frame;
        // Session frames MUST carry the subscription id on the envelope: the
        // client's subscription filter DROPS a zero-id frame once it has
        // learned the nonzero id from the OpenSessionResponse (the real
        // server always stamps it; an unstamped fake hangs the download
        // whenever the response is processed before the first batch).
        frame.set_subscription_id(7);
        auto* batch = frame.mutable_batch();
        batch->set_seq(static_cast<std::uint64_t>(b) + 1);
        batch->set_body_encoding(pj_cloud::v1::BODY_ENCODING_NONE);
        for (int m = 0; m < kFakeMessagesPerBatch; ++m) {
          auto* message = batch->add_messages();
          message->set_topic_id(kFakeTopicId);
          message->set_schema_id(kFakeSchemaId);
          message->set_log_time_ns(1000 + static_cast<std::int64_t>(sent));
          message->set_publish_time_ns(990 + static_cast<std::int64_t>(sent));
          message->set_payload_encoding(pj_cloud::v1::PAYLOAD_ENCODING_RAW);
          message->set_payload(std::string(kFakePayloadBytes, 'x'));
          ++sent;
        }
        send(ws, frame);
      }
      if (mode_ == Mode::kComplete) {
        pj_cloud::v1::ServerMessage eos_frame;
        eos_frame.set_subscription_id(7);  // see the batch-frame comment above
        auto* eos = eos_frame.mutable_eos();
        eos->set_reason(pj_cloud::v1::EOS_REASON_COMPLETE);
        eos->set_total_messages_sent(sent);
        eos->set_total_bytes_sent(sent * kFakePayloadBytes);
        send(ws, eos_frame);
      }
      // kStallAfterTwoBatches: silence — the client's cancel wakes the wait.
    });
    auto res = server_.listen();
    ok_ = res.first;
    if (ok_) {
      server_.start();
    }
  }

  ~FakeStreamingServer() { server_.stop(); }

  void stop() { server_.stop(); }

  [[nodiscard]] bool ok() const { return ok_; }
  [[nodiscard]] std::string uri() const { return "ws://127.0.0.1:" + std::to_string(port_); }
  [[nodiscard]] int openSessions() const { return open_sessions_.load(); }

 private:
  static void send(ix::WebSocket& ws, const pj_cloud::v1::ServerMessage& message) {
    std::string payload;
    message.SerializeToString(&payload);
    ws.sendBinary(payload);
  }

  Mode mode_;
  int port_;
  ix::WebSocketServer server_;
  bool ok_ = false;
  std::atomic<int> open_sessions_{0};
};

// The tuple the worker sends -> the descriptor identity it must tee under.
inline std::string identityFor(const std::string& uri) {
  mcap_cloud::SourceDescriptor d;
  d.version = 1;
  d.kind = "mcap-cloud-session";
  d.server_uri = uri;
  d.s3_keys = {"a.mcap"};
  d.topics = {"/one"};
  d.start_ns = 0;
  d.end_ns = 0;
  d.include_latched = true;
  return mcap_cloud::descriptorIdentity(d);
}

// The matching descriptor object (identity == identityFor(uri)); the
// promotion/provider suites derive descriptor_json/identity from it.
inline mcap_cloud::SourceDescriptor descriptorFor(const std::string& uri,
                                                  const std::string& display_name = "a.mcap") {
  mcap_cloud::SourceDescriptor d;
  d.version = 1;
  d.kind = "mcap-cloud-session";
  d.server_uri = uri;
  d.s3_keys = {"a.mcap"};
  d.topics = {"/one"};
  d.start_ns = 0;
  d.end_ns = 0;
  d.include_latched = true;
  d.display_name = display_name;
  return d;
}

inline std::string readFile(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

inline bool cacheRootHasPartial(const std::filesystem::path& root) {
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(root, ec)) {
    if (entry.path().filename().string().find(".mcap.partial.") != std::string::npos) {
      return true;
    }
  }
  return false;
}

}  // namespace mcap_cloud_test
