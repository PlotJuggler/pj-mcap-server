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

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <ixwebsocket/IXWebSocketServer.h>

#include "fake_toolbox_host.hpp"
#include "fetch_worker.hpp"
#include "parser_ingest_test_support.hpp"
#include "pj_cloud.pb.h"

namespace {

using mcap_cloud_test::findFreePort;

// Same shape as backend_cancel_wait_test.cpp's fake: answers Hello so
// connect() succeeds, swallows OpenSession so the session RPC wait genuinely
// blocks server-side. openSessionSeen() lets the test synchronize on the
// worker actually REACHING the pending-OpenSession window before cancelling —
// a blind sleep could otherwise let a descheduled worker take the cooperative
// pre-connect exit and pass without exercising the hook wiring at all
// (review-caught spurious-pass hole).
class FakeSilentOpenServer {
 public:
  FakeSilentOpenServer() : port_(findFreePort()), server_(port_, "127.0.0.1") {
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
        std::string payload;
        response.SerializeToString(&payload);
        ws.sendBinary(payload);
      } else if (request.has_open_session()) {
        open_session_seen_.store(true);
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
  [[nodiscard]] bool openSessionSeen() const { return open_session_seen_.load(); }

 private:
  int port_;
  ix::WebSocketServer server_;
  bool ok_ = false;
  std::atomic<bool> open_session_seen_{false};
};

// Answers Hello AND OpenSession (one topic, one schema) then goes silent —
// the pull binds, announces host progress, and blocks in the FRAME wait with
// no messages flowing. The shape that exposes a host stop nobody polls.
class FakeStallingSessionServer {
 public:
  FakeStallingSessionServer() : port_(findFreePort()), server_(port_, "127.0.0.1") {
    server_.setOnClientMessageCallback([](std::shared_ptr<ix::ConnectionState>, ix::WebSocket& ws,
                                          const ix::WebSocketMessagePtr& msg) {
      if (msg->type != ix::WebSocketMessageType::Message) {
        return;
      }
      pj_cloud::v1::ClientMessage request;
      if (!request.ParseFromString(msg->str)) {
        return;
      }
      pj_cloud::v1::ServerMessage response;
      response.set_request_id(request.request_id());
      if (request.has_hello()) {
        response.mutable_hello_response()->set_server_version("test-fake-1.0");
      } else if (request.has_open_session()) {
        auto* open = response.mutable_open_session();
        open->set_subscription_id(7);
        open->set_approximate_messages(1000);
        auto* topic = open->add_topic_id_map();
        topic->set_topic_id(1);
        topic->set_topic_name("/imu");
        topic->set_schema_id(5);
        topic->set_message_encoding("cdr");
        auto* schema = open->add_schemas();
        schema->set_schema_id(5);
        schema->set_name("sensor_msgs/msg/Imu");
        schema->set_encoding("ros2msg");
        schema->set_data("float64 x");
      } else {
        return;  // anything else (incl. the frame stream): silence
      }
      std::string payload;
      response.SerializeToString(&payload);
      ws.sendBinary(payload);
    });
    auto res = server_.listen();
    ok_ = res.first;
    if (ok_) {
      server_.start();
    }
  }

  ~FakeStallingSessionServer() { server_.stop(); }

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

  // Synchronize on the worker actually REACHING the pending-OpenSession wait:
  // cancel only once the fake has SEEN the OpenSession request, so this test
  // deterministically exercises the published-hook wake path (a blind sleep
  // could let a descheduled worker take the cooperative pre-connect exit and
  // pass without the hook — review-caught spurious-pass hole).
  const auto seen_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!server.openSessionSeen() && std::chrono::steady_clock::now() < seen_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(server.openSessionSeen())
      << "the pull never sent OpenSession — the test precondition did not establish";

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

// A host STOP against a STALLED stream must cancel promptly (review-caught):
// the in-callback isStopRequested() poll only runs when messages flow, so
// without the host-stop watchdog a stop during a silent frame wait sat out
// the 60 s frame timeout (and reconnect backoffs). The fake answers Hello +
// OpenSession then goes silent; the stop is pre-latched, so the watchdog's
// first poll must convert it into requestCancel() and unwind the pull well
// under the frame timeout.
TEST(McapCloudFetchWorkerCancel, HostStopDuringStalledStreamCancelsPromptly) {
  FakeStallingSessionServer server;
  ASSERT_TRUE(server.ok());

  mcap_cloud::testsupport::FakeToolboxHost host;
  pj_ingest_test::FakeIngestHost ingest;
  ingest.stop_requested.store(true);

  mcap_cloud::FetchWorker worker;
  worker.setHostProvider([&host]() { return host.view(); });
  worker.setRuntimeHostProvider(
      [&ingest]() { return PJ::ToolboxRuntimeHostView{ingest.toolboxRuntime()}; });

  bool connected = false;
  worker.connectFinished = [&](bool ok, std::string, std::string, std::string) { connected = ok; };
  worker.connectAsync(server.uri(), /*cert_path=*/"", /*api_key=*/"", /*allow_insecure=*/false);
  ASSERT_TRUE(connected) << "browse connect against the fake server must succeed";

  std::vector<std::pair<bool, std::string>> topic_results;  // read only after join
  worker.pullFinished = [&](std::string, std::string, bool ok, std::string error) {
    topic_results.emplace_back(ok, std::move(error));
  };

  const auto pull_at = std::chrono::steady_clock::now();
  std::thread pull([&] {
    worker.pullTopicsAsync({"a.mcap"}, "a.mcap", {"/imu"}, 0, 0, /*save_directory=*/{});
  });
  pull.join();  // ctest TIMEOUT 30 converts a watchdog regression's 60 s frame wait into FAIL
  const auto elapsed = std::chrono::steady_clock::now() - pull_at;

  EXPECT_LT(elapsed, std::chrono::seconds(10))
      << "a pre-latched host stop must cancel the stalled stream promptly, "
         "never wait out the frame timeout";
  ASSERT_EQ(ingest.progress_starts.size(), 1u)
      << "the pull must have announced host progress before stalling";
  EXPECT_EQ(ingest.progress_finish_events.load(), 0u)
      << "an aborted pull must not paint a completed progress sequence";
  ASSERT_FALSE(topic_results.empty());
  for (const auto& [ok, error] : topic_results) {
    EXPECT_FALSE(ok) << "a host-stopped pull must not report topic success";
    EXPECT_NE(error.find("cancel"), std::string::npos)
        << "the error must mention the cancel, got: " << error;
  }
}
