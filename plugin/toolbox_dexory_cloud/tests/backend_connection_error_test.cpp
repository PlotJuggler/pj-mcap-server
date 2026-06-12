// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC error-path tests for BackendConnection. Unlike the live test, these
// need NO running server and NO env gate — they pin the failure contract:
//
//   (i)  connect() to a closed port fails (returns false) within the timeout and
//        surfaces a NON-EMPTY error mentioning the target URL.
//   (ii) listSequences()/listTopics() on a never-connected BackendConnection are
//        safe no-ops that return empty rather than dereferencing the null socket.
//
// Registered as DexoryCloudBackendErrorTest; runs in the hermetic CI suite.

#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "backend_connection.hpp"

namespace {

// 127.0.0.1:9 is the "discard" port and is reliably closed on a dev host; the
// TCP connect is refused immediately, so connect() should fail well within its
// internal handshake timeout. We bound the whole call so a hang fails loudly.
constexpr const char* kClosedPortUrl = "ws://127.0.0.1:9";

}  // namespace

TEST(DexoryCloudBackendError, ConnectClosedPortFailsWithUrlInError) {
  dexory_cloud::BackendConnection conn(kClosedPortUrl, /*cert_path=*/"", /*api_key=*/"",
                                       /*allow_insecure=*/false);

  std::string error;
  const auto start = std::chrono::steady_clock::now();
  const bool ok = conn.connect(&error);
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_FALSE(ok) << "connect to a closed port must fail";
  EXPECT_FALSE(error.empty()) << "a failed connect must yield a non-empty error";
  // The error must name the target so the user can see what was dialed. The
  // class appends /api/ws to the host, so match on the host:port substring.
  EXPECT_NE(error.find("127.0.0.1:9"), std::string::npos)
      << "error should mention the URL, got: " << error;
  // Must not hang: the internal request timeout is 10s; allow generous slack.
  EXPECT_LT(elapsed, std::chrono::seconds(30)) << "connect must not hang on a closed port";

  // A failed connect leaves no usable version.
  EXPECT_FALSE(conn.version().has_value());
}

TEST(DexoryCloudBackendError, ListOnNeverConnectedReturnsEmpty) {
  // Construct but never connect(): the socket is null. Every read RPC must
  // short-circuit to an empty result instead of crashing.
  dexory_cloud::BackendConnection conn(kClosedPortUrl, /*cert_path=*/"", /*api_key=*/"",
                                       /*allow_insecure=*/false);

  EXPECT_TRUE(conn.listSequences().empty());
  EXPECT_TRUE(conn.listTopics("anything").empty());
  EXPECT_FALSE(conn.getTopicMetadata("anything", "any-topic").has_value());
  EXPECT_FALSE(conn.version().has_value());
}
