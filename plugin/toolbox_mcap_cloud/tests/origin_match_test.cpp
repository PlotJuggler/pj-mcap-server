// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC tests for the strict ws/wss origin parser + comparator
// (src/origin_match.hpp) backing credential origin binding (spec
// docs/canonical-layout-replay.md §7 guard 2): MCAP_CLOUD_API_KEY may only
// apply when MCAP_CLOUD_URL's parsed origin equals the target's. The parser is
// deliberately STRICT — anything that could smuggle credentials or ambiguity
// through an origin comparison (userinfo, query, fragment, non-ws scheme,
// empty host, junk port) parses to nullopt, and an unparsable URI never
// matches anything, not even itself.

#include <gtest/gtest.h>

#include <string_view>

#include "origin_match.hpp"

namespace {

using mcap_cloud::parseWsOrigin;
using mcap_cloud::sameWsOrigin;

// --- parse: field normalization ---------------------------------------------

// Scheme + host lowercase; the effective port is the scheme default when no
// explicit port is given; a path is allowed and ignored.
TEST(OriginMatch, ParseNormalizesCaseAndDefaultPort) {
  const auto origin = parseWsOrigin("WSS://ExAmPlE.com/some/path");
  ASSERT_TRUE(origin.has_value());
  EXPECT_EQ(origin->scheme, "wss");
  EXPECT_EQ(origin->host, "example.com");
  EXPECT_EQ(origin->port, 443);

  const auto ws = parseWsOrigin("ws://Host");
  ASSERT_TRUE(ws.has_value());
  EXPECT_EQ(ws->scheme, "ws");
  EXPECT_EQ(ws->host, "host");
  EXPECT_EQ(ws->port, 80);
}

// --- match -------------------------------------------------------------------

TEST(OriginMatch, SameOriginMatches) {
  // Host case + a path never break the match.
  EXPECT_TRUE(sameWsOrigin("ws://h:8080", "ws://H:8080/path"));
  // Explicit default port == implicit default port.
  EXPECT_TRUE(sameWsOrigin("wss://h", "wss://h:443"));
}

// --- mismatch ----------------------------------------------------------------

TEST(OriginMatch, DifferentOriginDoesNotMatch) {
  // Scheme differs (same host:port — only the scheme separates them).
  EXPECT_FALSE(sameWsOrigin("ws://h:9", "wss://h:9"));
  // Host differs.
  EXPECT_FALSE(sameWsOrigin("wss://a:443", "wss://b:443"));
  // Port differs (default 443 vs explicit 8443).
  EXPECT_FALSE(sameWsOrigin("wss://h", "wss://h:8443"));
}

// --- reject ------------------------------------------------------------------

// Every rejected shape parses to nullopt AND never matches — not even itself:
// an unparsable URI must fail CLOSED (no token release), never fall back to a
// string-equality "match".
TEST(OriginMatch, RejectedUrisNeverMatchEvenThemselves) {
  constexpr std::string_view kRejected[] = {
      "wss://u:p@h",     // userinfo — credentials in an origin, never
      "wss://h/?q=1",    // query
      "wss://h/#f",      // fragment
      "http://h",        // non-ws/wss scheme
      "wss://:1",        // empty host
      "wss://h:bad",     // unparsable port
  };
  for (const auto uri : kRejected) {
    EXPECT_FALSE(parseWsOrigin(uri).has_value()) << "should reject: " << uri;
    EXPECT_FALSE(sameWsOrigin(uri, uri)) << "must not self-match: " << uri;
  }
}

}  // namespace
