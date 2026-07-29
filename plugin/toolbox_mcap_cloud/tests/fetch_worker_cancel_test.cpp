// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC pin of the FetchWorker-level cancel WIRING (review-caught, both
// reviewers): BackendConnection's sendAndWait wake predicate is only reachable
// if the worker publishes backend_session_for_cancel_ BEFORE the blocking
// connect()/openSessionFresh() calls. The original registration sat just ahead
// of the download loop, so a GUI cancel during "Opening session…" (exactly the
// phase with no visible progress) found a null pointer, never fired the wire
// cancelSession(), and the pull sat out the full 120 s kOpenSessionTimeout —
// the backend-level fix was dead code on the real path.
// backend_cancel_wait_test.cpp pins the BackendConnection half; this test
// drives the REAL FetchWorker::pullTopicsAsync against the same
// silent-OpenSession fake server and cancels mid-establishment.
// Registered with ctest TIMEOUT 30 so a wiring regression fails fast instead
// of hanging CI for the full OpenSession timeout.

#include <gtest/gtest.h>

#include "find_free_port.hpp"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <ixwebsocket/IXWebSocketServer.h>

#include "fake_toolbox_host.hpp"
#include "fetch_worker.hpp"
#include "pj_cloud.pb.h"

namespace {

using mcap_cloud_test::findFreePort;

// Same shape as backend_cancel_wait_test.cpp's fake: answers Hello so
// connect() succeeds, swallows OpenSession so the session RPC wait genuinely
// blocks server-side.
class FakeSilentOpenServer {
 public:
  FakeSilentOpenServer() : port_(findFreePort()), server_(port_, "127.0.0.1") {
    server_.setOnClientMessageCallback([](std::shared_ptr<ix::ConnectionState>, ix::WebSocket& ws,
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

 private:
  int port_;
  ix::WebSocketServer server_;
  bool ok_ = false;
};

}  // namespace

// requestCancel() while pullTopicsAsync is stuck establishing the session
// (fresh-connection handshake or the never-answered OpenSession wait) must
// unwind the pull promptly — never the 120 s timeout. Wherever the cancel
// lands (pre-publication latch, connect window, OpenSession wait), the pull
// must report the topic failed with a cancel-flavored error.
TEST(McapCloudFetchWorkerCancel, CancelDuringSessionEstablishmentUnblocksPull) {
  FakeSilentOpenServer server;
  ASSERT_TRUE(server.ok());

  mcap_cloud::testsupport::FakeToolboxHost host;
  mcap_cloud::FetchWorker worker;
  worker.setHostProvider([&host]() { return host.view(); });

  bool connected = false;
  worker.connectFinished = [&](bool ok, std::string, std::string, std::string) { connected = ok; };
  worker.connectAsync(server.uri(), /*cert_path=*/"", /*api_key=*/"", /*allow_insecure=*/false);
  ASSERT_TRUE(connected) << "browse connect against the fake server must succeed";

  std::vector<std::pair<bool, std::string>> topic_results;  // read only after join
  worker.pullFinished = [&](std::string, std::string, bool ok, std::string error) {
    topic_results.emplace_back(ok, std::move(error));
  };

  std::thread pull([&] {
    worker.pullTopicsAsync({"a.mcap"}, "a.mcap", {"/topic"}, 0, 0, /*save_directory=*/{});
  });

  // Let the pull reach session establishment: fresh connection + Hello are
  // local round-trips; OpenSession is then swallowed by the fake, so "past the
  // send" is all the settle time needs to cover (same tolerance as the
  // BackendConnection-level test). A cancel landing EARLIER is also a valid —
  // and still asserted — outcome: every stage must honor it promptly.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  const auto cancel_at = std::chrono::steady_clock::now();
  worker.requestCancel();
  pull.join();  // ctest TIMEOUT 30 converts a regression's 120 s hang into a fast FAIL
  const auto elapsed = std::chrono::steady_clock::now() - cancel_at;

  EXPECT_LT(elapsed, std::chrono::seconds(5))
      << "cancel during session establishment must unwind the pull promptly, "
         "not after the 120s OpenSession timeout";
  ASSERT_FALSE(topic_results.empty()) << "the pull must report its topic";
  for (const auto& [ok, error] : topic_results) {
    EXPECT_FALSE(ok) << "a cancelled pull must not report topic success";
    EXPECT_NE(error.find("cancel"), std::string::npos)
        << "the error must mention the cancel, got: " << error;
  }
}
