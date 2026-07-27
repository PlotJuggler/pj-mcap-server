// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC: a hard server Error frame during ListFiles must be SURFACED, not
// swallowed as an empty catalog. Found live on PR #10's degraded-start server:
// `mcap-cloud-cli list` against a server still waiting for its first catalog
// build printed "0 sequence(s)" / exit 0, because listSequences() treated the
// clean ERROR_CATALOG_UNAVAILABLE reply as "partial, no rows" with no error
// channel at all (the same swallow applied to ERROR_INTERNAL before that).
//
// Contract pinned here: listSequences(&complete, ..., &error) on a hard Error
// reply returns empty, sets *complete=false, and sets *error to the formatted
// wire error (code + server message) so both the CLI and the dialog can tell
// the user WHY the catalog is empty-looking.
//
// Same in-process ix::WebSocketServer shape as list_pagination_test.cpp.

#include <gtest/gtest.h>

#include "find_free_port.hpp"

#include <string>

#include <ixwebsocket/IXWebSocketServer.h>

#include "backend_connection.hpp"
#include "pj_cloud.pb.h"

namespace {

using mcap_cloud_test::findFreePort;

// Answers Hello normally, then replies to EVERY ListFiles with a hard Error
// frame carrying the given code+message (the degraded-start server shape).
class FakeErroringServer {
 public:
  FakeErroringServer(pj_cloud::v1::ErrorCode code, std::string message)
      : port_(findFreePort()), server_(port_, "127.0.0.1") {
    server_.setOnClientMessageCallback([this, code, message](std::shared_ptr<ix::ConnectionState>,
                                                             ix::WebSocket& ws,
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
        response.mutable_hello_response()->set_server_version("fake-erroring-1.0");
      } else if (request.has_list_files()) {
        auto* err = response.mutable_error();
        err->set_code(code);
        err->set_message(message);
      } else {
        return;
      }
      std::string payload;
      response.SerializeToString(&payload);
      ws.sendBinary(payload);
    });
    EXPECT_TRUE(server_.listenAndStart());
  }

  ~FakeErroringServer() { server_.stop(); }

  [[nodiscard]] std::string url() const { return "ws://127.0.0.1:" + std::to_string(port_); }

 private:
  int port_;
  ix::WebSocketServer server_;
};

}  // namespace

TEST(McapCloudListErrorSurface, HardErrorSetsErrorOutAndIncomplete) {
  FakeErroringServer server(pj_cloud::v1::ERROR_CATALOG_UNAVAILABLE,
                            "catalog not yet available — first build in progress; retry shortly");
  mcap_cloud::BackendConnection conn(server.url(), "", "", false);
  std::string connect_error;
  ASSERT_TRUE(conn.connect(&connect_error)) << connect_error;

  bool complete = true;
  std::string error;
  const auto sequences = conn.listSequences(&complete, {}, nullptr, nullptr, {}, &error);

  EXPECT_TRUE(sequences.empty());
  EXPECT_FALSE(complete) << "a hard Error reply must not count as a complete listing";
  ASSERT_FALSE(error.empty()) << "the wire error must be surfaced, not swallowed";
  EXPECT_NE(error.find("catalog not yet available"), std::string::npos)
      << "error must carry the server's message, got: " << error;
}

TEST(McapCloudListErrorSurface, InternalErrorAlsoSurfaced) {
  // The swallow predates ERROR_CATALOG_UNAVAILABLE — pin the generic case too.
  FakeErroringServer server(pj_cloud::v1::ERROR_INTERNAL, "catalog query failed");
  mcap_cloud::BackendConnection conn(server.url(), "", "", false);
  std::string connect_error;
  ASSERT_TRUE(conn.connect(&connect_error)) << connect_error;

  bool complete = true;
  std::string error;
  const auto sequences = conn.listSequences(&complete, {}, nullptr, nullptr, {}, &error);

  EXPECT_TRUE(sequences.empty());
  EXPECT_FALSE(complete);
  EXPECT_NE(error.find("catalog query failed"), std::string::npos) << error;
}

TEST(McapCloudListErrorSurface, SuccessLeavesErrorEmpty) {
  // error_out is cleared on entry: a successful (empty-catalog) listing must
  // report complete=true with NO error — genuinely-empty stays distinguishable
  // from failed.
  class EmptyOkServer {
   public:
    EmptyOkServer() : port_(findFreePort()), server_(port_, "127.0.0.1") {
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
          response.mutable_hello_response()->set_server_version("fake-empty-1.0");
        } else if (request.has_list_files()) {
          response.mutable_list_files()->set_catalog_generation("gen-1");
        } else {
          return;
        }
        std::string payload;
        response.SerializeToString(&payload);
        ws.sendBinary(payload);
      });
      EXPECT_TRUE(server_.listenAndStart());
    }
    ~EmptyOkServer() { server_.stop(); }
    [[nodiscard]] std::string url() const { return "ws://127.0.0.1:" + std::to_string(port_); }

   private:
    int port_;
    ix::WebSocketServer server_;
  };

  EmptyOkServer server;
  mcap_cloud::BackendConnection conn(server.url(), "", "", false);
  std::string connect_error;
  ASSERT_TRUE(conn.connect(&connect_error)) << connect_error;

  bool complete = false;
  std::string error = "stale text from a previous attempt";
  const auto sequences = conn.listSequences(&complete, {}, nullptr, nullptr, {}, &error);

  EXPECT_TRUE(sequences.empty());
  EXPECT_TRUE(complete);
  EXPECT_TRUE(error.empty()) << "error_out must be cleared on entry, got: " << error;
}
