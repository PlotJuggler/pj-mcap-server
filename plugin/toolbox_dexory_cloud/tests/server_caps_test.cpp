// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC tests for HelloResponse.capabilities parsing
// (BackendConnection::serverCapabilities()) and the D2 updateTags() gate: a
// server that advertises tag_edit_supported=false (read-only catalog, no
// tag-edit IPC forwarder configured — post-M6 default) must never receive an
// UpdateTags frame; updateTags() must fail FAST with the exact message
// instead.
//
// backend_connection_error_test.cpp's "hermetic fake-server harness" is
// actually just a closed-port / never-connected check — there is no existing
// harness anywhere in this tree that fakes a Hello handshake payload. Faking
// distinct Capabilities values needs a real (if minimal) WS peer, so this file
// stands one up directly with ix::WebSocketServer (already a transitive
// dependency of BackendConnection): it answers Hello with a hand-built
// HelloResponse carrying the capabilities under test, optionally answers
// ListFiles/UpdateTags too (only the tag_edit_supported=true leg needs a
// resolvable file_id to prove updateTags() actually SENDS), and counts
// inbound UpdateTags frames so a false-capability run can assert zero sends.
//
// No env gate, no external process — the fake server is an in-process thread
// bound to a loopback ephemeral port (see findFreePort() below).

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include <ixwebsocket/IXWebSocketServer.h>

#include "backend_connection.hpp"
#include "pj_cloud.pb.h"

namespace {

// Discover a free loopback port by binding a throwaway socket to port 0 (the
// OS assigns one) and reading it back via getsockname, then releasing it
// immediately. ix::WebSocketServer's SocketServer::getPort() only echoes back
// its constructor argument (it never calls getsockname() itself after an
// OS-assigned bind), so this has to happen out-of-band on the client side.
// Small TOCTOU race (another process could grab the same port in the
// microseconds before WebSocketServer binds it) — an accepted trade-off for a
// hermetic unit test; unlike backend_connection_error_test.cpp's reserved
// closed port (127.0.0.1:9), a real listener needs an actually-free port.
int findFreePort() {
  const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ::bind(probe, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  socklen_t len = sizeof(addr);
  ::getsockname(probe, reinterpret_cast<sockaddr*>(&addr), &len);
  const int port = ntohs(addr.sin_port);
  ::close(probe);
  return port;
}

// Minimal fake pj_cloud.v1 server for THIS test file only: answers Hello with
// a fixed HelloResponse carrying the capabilities under test, optionally
// answers ListFiles (one canned FileSummary) + UpdateTags (echoing success),
// ALWAYS answers GetFile (echoing the request's s3_key into the response, so
// listTopicsChecked() works whether or not ListFiles was ever called on this
// connection), and records the last-seen s3_key (+ presence bit) of both the
// GetFile and UpdateTags frames it observes, plus frame counts. Every other
// request type is ignored — these tests exercise the Hello/capabilities parse,
// the updateTags() D2 gate, and the s3_key key-addressing of GetFile/UpdateTags
// only, not a full RPC surface. last-seen fields are written under mu_ inside
// the (server-thread) callback strictly BEFORE the reply is sent, and read
// under the same mu_ on the test thread AFTER the corresponding blocking RPC
// call returns (sendAndWait() cannot return until that reply arrives) — so the
// lock/unlock pair gives the read a real happens-before edge over the write,
// same as the pre-existing update_tags_frames_seen_ atomic below.
class FakeCapsServer {
 public:
  FakeCapsServer(bool resume_supported, bool tag_edit_supported, bool answer_list_and_update_tags)
      : port_(findFreePort()), server_(port_, "127.0.0.1") {
    server_.setOnClientMessageCallback([this, resume_supported, tag_edit_supported, answer_list_and_update_tags](
                                            std::shared_ptr<ix::ConnectionState>, ix::WebSocket& ws,
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
        auto* hello_response = response.mutable_hello_response();
        hello_response->set_server_version("test-fake-1.0");
        auto* caps = hello_response->mutable_capabilities();
        caps->set_resume_supported(resume_supported);
        caps->set_tag_edit_supported(tag_edit_supported);
        std::string payload;
        response.SerializeToString(&payload);
        ws.sendBinary(payload);
      } else if (request.has_list_files() && answer_list_and_update_tags) {
        pj_cloud::v1::ServerMessage response;
        response.set_request_id(request.request_id());
        auto* list_response = response.mutable_list_files();
        auto* file = list_response->add_files();
        file->set_id(1);
        file->set_s3_key(kFakeSequenceName);
        // next_page_token left empty -> listSequences() sees one page.
        std::string payload;
        response.SerializeToString(&payload);
        ws.sendBinary(payload);
      } else if (request.has_update_tags()) {
        ++update_tags_frames_seen_;
        {
          std::lock_guard<std::mutex> lock(mu_);
          last_update_tags_s3_key_ = request.update_tags().s3_key();
          last_update_tags_has_s3_key_ = request.update_tags().has_s3_key();
        }
        if (answer_list_and_update_tags) {
          pj_cloud::v1::ServerMessage response;
          response.set_request_id(request.request_id());
          response.mutable_update_tags();  // empty effective_tags is fine here
          std::string payload;
          response.SerializeToString(&payload);
          ws.sendBinary(payload);
        }
      } else if (request.has_get_file()) {
        ++get_file_frames_seen_;
        {
          std::lock_guard<std::mutex> lock(mu_);
          last_get_file_s3_key_ = request.get_file().s3_key();
          last_get_file_has_s3_key_ = request.get_file().has_s3_key();
        }
        // ALWAYS answered (independent of answer_list_and_update_tags / any
        // prior ListFiles): proves GetFile works purely by key, with no
        // dependency on the client's browse index. Answered by KEY, not
        // file_id — a real key-addressed server would resolve the file this
        // way; this fake just echoes it back with one canned topic.
        pj_cloud::v1::ServerMessage response;
        response.set_request_id(request.request_id());
        auto* get_file_response = response.mutable_get_file();
        auto* summary = get_file_response->mutable_summary();
        summary->set_id(1);
        summary->set_s3_key(request.get_file().s3_key());
        auto* topic = get_file_response->add_topics();
        topic->set_name("/fake_topic");
        topic->set_schema_name("fake/Schema");
        topic->set_schema_encoding("ros2msg");
        topic->set_message_count(42);
        std::string payload;
        response.SerializeToString(&payload);
        ws.sendBinary(payload);
      }
    });
    auto res = server_.listen();
    ok_ = res.first;
    if (ok_) {
      server_.start();
    }
  }

  ~FakeCapsServer() { server_.stop(); }

  [[nodiscard]] bool ok() const { return ok_; }
  [[nodiscard]] std::string uri() const { return "ws://127.0.0.1:" + std::to_string(port_); }
  [[nodiscard]] int updateTagsFramesSeen() const { return update_tags_frames_seen_; }
  [[nodiscard]] int getFileFramesSeen() const { return get_file_frames_seen_; }

  [[nodiscard]] std::string lastUpdateTagsS3Key() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_update_tags_s3_key_;
  }
  [[nodiscard]] bool lastUpdateTagsHasS3Key() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_update_tags_has_s3_key_;
  }
  [[nodiscard]] std::string lastGetFileS3Key() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_get_file_s3_key_;
  }
  [[nodiscard]] bool lastGetFileHasS3Key() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_get_file_has_s3_key_;
  }

  static constexpr const char* kFakeSequenceName = "fake_sequence";

 private:
  int port_;
  ix::WebSocketServer server_;
  bool ok_ = false;
  std::atomic<int> update_tags_frames_seen_{0};
  std::atomic<int> get_file_frames_seen_{0};

  mutable std::mutex mu_;
  std::string last_update_tags_s3_key_;
  bool last_update_tags_has_s3_key_ = false;
  std::string last_get_file_s3_key_;
  bool last_get_file_has_s3_key_ = false;
};

constexpr const char* kExpectedGateError =
    "server does not support tag editing (read-only catalog; no tag-edit IPC configured)";

}  // namespace

// hello advertising tag_edit_supported=false: serverCapabilities() parses it
// correctly, and updateTags() fails FAST with the exact gate message WITHOUT
// ever writing an UpdateTags frame to the socket.
TEST(DexoryCloudServerCaps, TagEditUnsupportedGatesUpdateTagsWithoutSending) {
  FakeCapsServer server(/*resume_supported=*/true, /*tag_edit_supported=*/false,
                        /*answer_list_and_update_tags=*/false);
  ASSERT_TRUE(server.ok());

  dexory_cloud::BackendConnection conn(server.uri(), /*cert_path=*/"", /*api_key=*/"", /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << error;

  const auto caps = conn.serverCapabilities();
  ASSERT_TRUE(caps.has_value());
  EXPECT_TRUE(caps->resume_supported);
  EXPECT_FALSE(caps->tag_edit_supported);

  // No listSequences() call: the gate must fire before any file_id resolution
  // is even attempted.
  std::string update_error;
  const bool ok = conn.updateTags(FakeCapsServer::kFakeSequenceName, {}, {}, nullptr, &update_error);
  EXPECT_FALSE(ok);
  EXPECT_EQ(update_error, kExpectedGateError);

  // Give a (hopefully nonexistent) frame time to arrive if the gate had NOT
  // short-circuited before sendAndWait().
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(server.updateTagsFramesSeen(), 0) << "updateTags() must not send when tag_edit_supported=false";
}

// hello advertising tag_edit_supported=true: updateTags() proceeds PAST the
// gate and actually sends (and, since this fake server answers ListFiles +
// UpdateTags too, completes successfully).
TEST(DexoryCloudServerCaps, TagEditSupportedProceedsToSend) {
  FakeCapsServer server(/*resume_supported=*/false, /*tag_edit_supported=*/true,
                        /*answer_list_and_update_tags=*/true);
  ASSERT_TRUE(server.ok());

  dexory_cloud::BackendConnection conn(server.uri(), /*cert_path=*/"", /*api_key=*/"", /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << error;

  const auto caps = conn.serverCapabilities();
  ASSERT_TRUE(caps.has_value());
  // resume_supported round-trips independently of tag_edit_supported (proves
  // the two flags aren't accidentally tied together in the parse).
  EXPECT_FALSE(caps->resume_supported);
  EXPECT_TRUE(caps->tag_edit_supported);

  // listSequences() is no longer REQUIRED before updateTags() (it addresses
  // the file by s3_key — see UpdateTagsFrameCarriesS3Key /
  // UpdateTagsSucceedsForUnlistedName below), but still call it here to mirror
  // the GUI's normal catalog-browse flow and prove file_id is harmlessly
  // populated alongside s3_key when the index does resolve.
  (void)conn.listSequences();

  std::string update_error;
  const bool ok = conn.updateTags(FakeCapsServer::kFakeSequenceName, {{"k", "v"}}, {}, nullptr, &update_error);
  EXPECT_TRUE(ok) << update_error;
  EXPECT_GE(server.updateTagsFramesSeen(), 1) << "updateTags() must send when tag_edit_supported=true";
}

// -----------------------------------------------------------------------------
// s3_key key-addressing (GetFileRequest.s3_key / UpdateTagsRequest.s3_key):
// catalog file ids RENUMBER across external-builder rebuilds, so the client's
// name->file_id browse index (built by listSequences(), possibly minutes
// stale) must never be a hard dependency for these two RPCs. The wire frame
// must always carry s3_key == the sequence name verbatim, and both RPCs must
// succeed for a name that was NEVER resolved against the index on this
// connection (i.e. no listSequences() call at all).
// -----------------------------------------------------------------------------

// The UpdateTags frame sent over the wire carries s3_key set to the exact
// sequence name passed to updateTags(), regardless of whether that name
// resolves against the (populated, in this test) browse index.
TEST(DexoryCloudServerCaps, UpdateTagsFrameCarriesS3Key) {
  FakeCapsServer server(/*resume_supported=*/false, /*tag_edit_supported=*/true,
                        /*answer_list_and_update_tags=*/true);
  ASSERT_TRUE(server.ok());

  dexory_cloud::BackendConnection conn(server.uri(), /*cert_path=*/"", /*api_key=*/"", /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << error;
  (void)conn.listSequences();

  std::string update_error;
  const bool ok =
      conn.updateTags(FakeCapsServer::kFakeSequenceName, {{"k", "v"}}, {}, nullptr, &update_error);
  ASSERT_TRUE(ok) << update_error;

  EXPECT_TRUE(server.lastUpdateTagsHasS3Key()) << "UpdateTagsRequest.s3_key must be PRESENT on the wire";
  EXPECT_EQ(server.lastUpdateTagsS3Key(), FakeCapsServer::kFakeSequenceName);
}

// updateTags() succeeds for a sequence name that was NEVER listed on this
// connection (no listSequences() call at all, so file_id_by_name_ is empty) —
// proving key-only addressing: the fake server answers UpdateTags purely by
// s3_key, with no file_id resolution required client-side.
TEST(DexoryCloudServerCaps, UpdateTagsSucceedsForUnlistedName) {
  FakeCapsServer server(/*resume_supported=*/false, /*tag_edit_supported=*/true,
                        /*answer_list_and_update_tags=*/true);
  ASSERT_TRUE(server.ok());

  dexory_cloud::BackendConnection conn(server.uri(), /*cert_path=*/"", /*api_key=*/"", /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << error;

  // Deliberately NO listSequences() call: file_id_by_name_ is empty, so a
  // pre-M6 client would have failed this with "unknown sequence".
  constexpr const char* kNeverListedName = "never_listed_sequence.mcap";
  std::string update_error;
  const bool ok = conn.updateTags(kNeverListedName, {{"k", "v"}}, {}, nullptr, &update_error);
  EXPECT_TRUE(ok) << update_error;

  EXPECT_TRUE(server.lastUpdateTagsHasS3Key());
  EXPECT_EQ(server.lastUpdateTagsS3Key(), kNeverListedName);
}

// GetFile (via listTopicsChecked()) carries s3_key on the wire and succeeds
// without a prior listSequences() on this connection — the same key-only
// addressing proof as UpdateTagsSucceedsForUnlistedName above, for the GetFile
// RPC. getTopicMetadata()/listTopics() reuse listTopicsChecked() so this
// covers all three entry points.
TEST(DexoryCloudServerCaps, GetFileCarriesS3KeyWithoutPriorList) {
  FakeCapsServer server(/*resume_supported=*/false, /*tag_edit_supported=*/false,
                        /*answer_list_and_update_tags=*/false);
  ASSERT_TRUE(server.ok());

  dexory_cloud::BackendConnection conn(server.uri(), /*cert_path=*/"", /*api_key=*/"", /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << error;

  // No listSequences() call: file_id_by_name_ is empty. A pre-M6 client would
  // have failed this with "unknown sequence ... refresh the list".
  constexpr const char* kNeverListedName = "never_listed_sequence.mcap";
  const dexory_cloud::TopicsResult result = conn.listTopicsChecked(kNeverListedName);
  EXPECT_TRUE(result.ok) << result.error;
  ASSERT_EQ(result.topics.size(), 1u);
  EXPECT_EQ(result.topics[0].topic_name, "/fake_topic");

  EXPECT_GE(server.getFileFramesSeen(), 1);
  EXPECT_TRUE(server.lastGetFileHasS3Key()) << "GetFileRequest.s3_key must be PRESENT on the wire";
  EXPECT_EQ(server.lastGetFileS3Key(), kNeverListedName);
}
