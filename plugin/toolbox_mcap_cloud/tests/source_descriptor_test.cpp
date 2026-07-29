// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Source descriptor (spec docs/canonical-layout-import.md §4): parse/validate
// round trip, canonical-serialization + identity conformance against the
// CROSS-REPO vectors file (MCAP_CLOUD_VECTORS_JSON — the same bytes PJ4-side
// tests consume), display_name identity invariance, and the strict rejection
// matrix (allowlist, limits, URI hygiene, ns-string syntax, range order).
#include "source_descriptor.hpp"

#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>

namespace {

using mcap_cloud::SourceDescriptor;

SourceDescriptor fullDescriptor() {
  SourceDescriptor d;
  d.version = 1;
  d.kind = "mcap-cloud-session";
  d.server_uri = "wss://mcap.example.com";
  d.s3_keys = {"cust=x/site=y/2026/a.mcap", "cust=x/site=y/2026/b.mcap"};
  d.topics = {"/imu", "/tf", "/tf_static"};
  d.start_ns = 1780012345000000000LL;
  d.end_ns = 1780012399000000000LL;
  d.include_latched = true;
  d.display_name = "Run 42";
  return d;
}

// A minimal valid descriptor as mutable JSON — the rejection matrix mutates
// exactly one aspect per case so each error is attributable.
nlohmann::json baseJson() {
  return nlohmann::json{
      {"v", 1},
      {"kind", "mcap-cloud-session"},
      {"server_uri", "ws://localhost:8080"},
      {"s3_keys", nlohmann::json::array({"a.mcap"})},
      {"topics", nlohmann::json::array()},
      {"start_ns", "0"},
      {"end_ns", "0"},
      {"include_latched", true},
      {"display_name", "A"},
  };
}

void expectReject(const std::string& json, const std::string& error_substr) {
  std::string error;
  const auto d = mcap_cloud::parseSourceDescriptor(json, &error);
  EXPECT_FALSE(d.has_value()) << "accepted: " << json.substr(0, 200);
  EXPECT_NE(error.find(error_substr), std::string::npos)
      << "error \"" << error << "\" lacks substring \"" << error_substr << "\"";
}

std::string slurp(const char* path) {
  std::ifstream in(path, std::ios::binary);
  EXPECT_TRUE(in.is_open()) << "cannot open vectors file " << path;
  std::ostringstream buf;
  buf << in.rdbuf();
  return buf.str();
}

}  // namespace

TEST(SourceDescriptor, RoundTrip) {
  const SourceDescriptor d = fullDescriptor();
  std::string error;
  const auto parsed = mcap_cloud::parseSourceDescriptor(mcap_cloud::toSourceDescriptorJson(d), &error);
  ASSERT_TRUE(parsed.has_value()) << error;
  EXPECT_EQ(parsed->version, d.version);
  EXPECT_EQ(parsed->kind, d.kind);
  EXPECT_EQ(parsed->server_uri, d.server_uri);
  EXPECT_EQ(parsed->s3_keys, d.s3_keys);
  EXPECT_EQ(parsed->topics, d.topics);
  EXPECT_EQ(parsed->start_ns, d.start_ns);
  EXPECT_EQ(parsed->end_ns, d.end_ns);
  EXPECT_EQ(parsed->include_latched, d.include_latched);
  EXPECT_EQ(parsed->display_name, d.display_name);
}

// The vectors are the cross-repo canonicalization contract: every case's
// descriptor must parse, and its canonical bytes + identity must match the
// file verbatim. The "display-name-does-not-change-identity" case is doubly
// asserted: a twin differing ONLY in display_name yields the same canonical
// bytes and the same identity (display_name is excluded from the digest).
TEST(SourceDescriptor, VectorConformance) {
  const std::string raw = slurp(MCAP_CLOUD_VECTORS_JSON);
  const auto vectors = nlohmann::json::parse(raw, /*cb=*/nullptr, /*allow_exceptions=*/false);
  ASSERT_FALSE(vectors.is_discarded());
  ASSERT_TRUE(vectors.contains("cases") && vectors["cases"].is_array());
  ASSERT_GE(vectors["cases"].size(), 3u);

  for (const auto& c : vectors["cases"]) {
    const std::string name = c["name"].get<std::string>();
    SCOPED_TRACE(name);
    std::string error;
    const auto d = mcap_cloud::parseSourceDescriptor(c["descriptor"].dump(), &error);
    ASSERT_TRUE(d.has_value()) << error;
    EXPECT_EQ(mcap_cloud::canonicalSourceDescriptorJson(*d), c["canonical"].get<std::string>());
    EXPECT_EQ(mcap_cloud::descriptorIdentity(*d), c["identity"].get<std::string>());

    if (name == "display-name-does-not-change-identity") {
      SourceDescriptor twin = *d;
      twin.display_name = "X";
      EXPECT_EQ(mcap_cloud::canonicalSourceDescriptorJson(twin), mcap_cloud::canonicalSourceDescriptorJson(*d));
      EXPECT_EQ(mcap_cloud::descriptorIdentity(twin), mcap_cloud::descriptorIdentity(*d));
    }
  }
}

TEST(SourceDescriptor, IdentityInvariance) {
  const SourceDescriptor d = fullDescriptor();
  SourceDescriptor renamed = d;
  renamed.display_name = "RENAMED";
  EXPECT_EQ(mcap_cloud::descriptorIdentity(renamed), mcap_cloud::descriptorIdentity(d));

  SourceDescriptor changed = d;
  changed.include_latched = !d.include_latched;
  EXPECT_NE(mcap_cloud::descriptorIdentity(changed), mcap_cloud::descriptorIdentity(d));
}

TEST(SourceDescriptor, RejectionMatrix) {
  {  // Unsupported version.
    auto j = baseJson();
    j["v"] = 2;
    expectReject(j.dump(), "version");
  }
  {  // Missing required field.
    auto j = baseJson();
    j.erase("kind");
    expectReject(j.dump(), "kind");
  }
  {  // Unknown field (strict allowlist — a token must never ride along).
    auto j = baseJson();
    j["token"] = "x";
    expectReject(j.dump(), "unknown field");
  }
  {  // URI userinfo (credential smuggling).
    auto j = baseJson();
    j["server_uri"] = "wss://user:pw@h/";
    expectReject(j.dump(), "userinfo");
  }
  {  // URI query.
    auto j = baseJson();
    j["server_uri"] = "wss://h/?token=1";
    expectReject(j.dump(), "query");
  }
  {  // URI fragment.
    auto j = baseJson();
    j["server_uri"] = "wss://h/#f";
    expectReject(j.dump(), "fragment");
  }
  {  // Non-ws/wss scheme.
    auto j = baseJson();
    j["server_uri"] = "http://h";
    expectReject(j.dump(), "scheme");
  }
  {  // Key-count limit.
    auto j = baseJson();
    auto keys = nlohmann::json::array();
    for (std::size_t i = 0; i < mcap_cloud::kMaxKeys + 1; ++i) {
      keys.push_back("k" + std::to_string(i) + ".mcap");
    }
    j["s3_keys"] = keys;
    expectReject(j.dump(), "s3_keys");
  }
  {  // Non-numeric ns string.
    auto j = baseJson();
    j["start_ns"] = "abc";
    expectReject(j.dump(), "decimal");
  }
  {  // Inverted range (allowed only as the "0"/"0" whole-range sentinel).
    auto j = baseJson();
    j["start_ns"] = "9";
    j["end_ns"] = "5";
    expectReject(j.dump(), "before start_ns");
  }
  {  // Whole-input size limit (checked before parsing).
    const std::string oversized(mcap_cloud::kMaxDescriptorBytes + 1, ' ');
    expectReject(oversized, "byte limit");
  }
}
