// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC cancel-responsiveness tests for the SESSION RPC wait
// (docs/canonical-layout-import.md §9 item 2). Before the fix, cancelSession()
// woke only the FRAME wait (waitSessionFrame) — a cancel arriving while
// openSessionFresh was still blocked in sendAndWait on the OpenSession
// response stranded the worker for the full 120 s kOpenSessionTimeout. These
// tests pin the two halves of the fix:
//
//   (i)  a cancel during a pending OpenSession returns promptly (well under
//        2 s) with an error mentioning "cancelled" — never the 120 s timeout;
//   (ii) the cancel wake is SCOPED to the session request path: a stale
//        cancel_requested_ flag (latched by a cancel outside any session) must
//        not wake browse RPCs (ListFiles) spuriously into an empty result.
//
// Same in-process fake-WS-server shape as server_caps_test.cpp (the ONLY
// existing fake-handshake harness): answers Hello so connect() succeeds,
// optionally answers ListFiles, and deliberately NEVER answers OpenSession so
// the RPC wait genuinely blocks. No env gate, no external process. The ctest
// registration adds TIMEOUT 30 so a predicate regression fails fast instead of
// hanging CI for the full OpenSession timeout.

#include <gtest/gtest.h>

#include "find_free_port.hpp"

#include <chrono>
#include <string>
#include <thread>

#include <ixwebsocket/IXWebSocketServer.h>

#include "backend_connection.hpp"
#include "pj_cloud.pb.h"

namespace {

using mcap_cloud_test::findFreePort;

// Minimal fake pj_cloud.v1 server: answers Hello with a fixed HelloResponse
// (and, when constructed with answer_list_files, ListFiles with one canned
// row). Every OpenSession request is swallowed silently — the point of these
// tests is an RPC wait that never completes server-side.
class FakeSilentOpenServer {
 public:
  explicit FakeSilentOpenServer(bool answer_list_files)
      : port_(findFreePort()), server_(port_, "127.0.0.1") {
    server_.setOnClientMessageCallback(
        [answer_list_files](std::shared_ptr<ix::ConnectionState>, ix::WebSocket& ws,
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
            std::string payload;
            response.SerializeToString(&payload);
            ws.sendBinary(payload);
          } else if (request.has_list_files() && answer_list_files) {
            pj_cloud::v1::ServerMessage response;
            response.set_request_id(request.request_id());
            auto* file = response.mutable_list_files()->add_files();
            file->set_id(1);
            file->set_s3_key(kFakeSequenceName);
            std::string payload;
            response.SerializeToString(&payload);
            ws.sendBinary(payload);
          }
          // OpenSession (and anything else): deliberately no reply.
        });
    auto res = server_.listen();
    ok_ = res.first;
    if (ok_) {
      server_.start();
    }
  }

  ~FakeSilentOpenServer() { server_.stop(); }

  [[nodiscard]] bool ok() const { return ok_; }
  [[nodiscard]] std::string uri() const { return "ws://127.0.0.1:" + std::to_string(port_); }

  static constexpr const char* kFakeSequenceName = "fake_sequence";

 private:
  int port_;
  ix::WebSocketServer server_;
  bool ok_ = false;
};

}  // namespace

// (i) cancelSession() during a pending OpenSession must wake the sendAndWait
// predicate and return "cancelled" promptly — never sit out the full 120 s
// kOpenSessionTimeout. Bound: the call must come back within 2 s of the cancel.
TEST(McapCloudBackendCancelWait, CancelUnblocksPendingOpenSession) {
  FakeSilentOpenServer server(/*answer_list_files=*/false);
  ASSERT_TRUE(server.ok());

  mcap_cloud::BackendConnection conn(server.uri(), /*cert_path=*/"", /*api_key=*/"",
                                     /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << error;

  mcap_cloud::OpenSessionParams params;
  params.s3_keys = {"a.mcap"};

  mcap_cloud::SessionInfo info;
  std::string open_error;
  bool open_ok = true;
  std::thread worker([&] { open_ok = conn.openSessionFresh(params, &info, &open_error); });

  // Let the worker reach the RPC wait (the fake server never replies, so the
  // exact settle time only needs to be "past the send", not precise).
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  const auto cancel_at = std::chrono::steady_clock::now();
  conn.cancelSession();
  worker.join();  // ctest TIMEOUT 30 converts a regression's 120 s hang into a fast FAIL
  const auto elapsed = std::chrono::steady_clock::now() - cancel_at;

  EXPECT_FALSE(open_ok) << "a cancelled OpenSession must not report success";
  EXPECT_LT(elapsed, std::chrono::seconds(2))
      << "cancel must wake the OpenSession wait promptly, not after the 120s timeout";
  EXPECT_NE(open_error.find("cancel"), std::string::npos)
      << "the error must mention the cancel, got: " << open_error;
}

// (ii) Scoping: a cancel latched OUTSIDE any session (the flag stays set until
// the next fresh open resets it) must not wake browse RPCs — ListFiles still
// waits for and returns its real response instead of coming back empty.
TEST(McapCloudBackendCancelWait, StaleCancelFlagDoesNotDisturbBrowseRpcs) {
  FakeSilentOpenServer server(/*answer_list_files=*/true);
  ASSERT_TRUE(server.ok());

  mcap_cloud::BackendConnection conn(server.uri(), /*cert_path=*/"", /*api_key=*/"",
                                     /*allow_insecure=*/false);
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << error;

  // No session is active; this latches cancel_requested_ until the next fresh
  // open. A session-scoped wake means the flag is simply ignored here.
  conn.cancelSession();

  const auto sequences = conn.listSequences();
  ASSERT_EQ(sequences.size(), 1u)
      << "a stale cancel flag must not truncate a browse RPC to an empty result";
  EXPECT_EQ(sequences[0].name, FakeSilentOpenServer::kFakeSequenceName);
}
