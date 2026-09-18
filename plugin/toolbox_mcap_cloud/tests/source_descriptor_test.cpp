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
  {  // Version narrowing (review-caught): 2^32+1 must NOT alias v1 through an
     // unchecked get<int>() cast — exact-value rejection, fail-closed.
    auto j = baseJson();
    j["v"] = 4294967297ull;
    expectReject(j.dump(), "version");
  }
  {  // Negative wraparound sibling of the same bug class.
    auto j = baseJson();
    j["v"] = -4294967295ll;
    expectReject(j.dump(), "version");
  }
  {  // uint64 max — the extreme unsigned magnitude also rejects exactly.
    auto j = baseJson();
    j["v"] = 18446744073709551615ull;
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

// Adversarial F3: the OpenFresh protocol rule (proto/pj_cloud.proto: s3_keys
// ">=1; no empty/duplicate keys") is enforced at DESCRIPTOR parse time — an
// accepted empty-key descriptor previously reached provider code that
// dereferenced s3_keys.front() (UB), and empty/duplicate keys would only fail
// deep in the server. Rejecting at the parse boundary makes query/start fail
// cleanly as a contract error.
TEST(SourceDescriptor, RejectsEmptyKeyArrayEmptyKeysAndDuplicateKeys) {
  {  // Empty s3_keys array (the F3 crash shape).
    auto j = baseJson();
    j["s3_keys"] = nlohmann::json::array();
    expectReject(j.dump(), "at least one");
  }
  {  // Empty-string key.
    auto j = baseJson();
    j["s3_keys"] = nlohmann::json::array({""});
    expectReject(j.dump(), "empty");
  }
  {  // Duplicate keys.
    auto j = baseJson();
    j["s3_keys"] = nlohmann::json::array({"a.mcap", "b.mcap", "a.mcap"});
    expectReject(j.dump(), "duplicate");
  }
  {  // An empty TOPICS array stays legal (empty = all union topics).
    auto j = baseJson();
    j["topics"] = nlohmann::json::array();
    std::string error;
    EXPECT_TRUE(mcap_cloud::parseSourceDescriptor(j.dump(), &error).has_value()) << error;
  }
}

// -----------------------------------------------------------------------------
// v3 (T8b): the SECOND descriptor kind, "mcap-cloud-selection". The server
// froze the membership, the topics and the window when the selection was
// created, so the descriptor carries an OPAQUE id and nothing else — no
// s3_keys, no versions, no credentials. It stays at v:1: the kind lives
// INSIDE the identity, so no existing case's bytes or identity move (the
// cross-repo vectors are append-only).
// -----------------------------------------------------------------------------

namespace {

// The exact bytes the server emits (pj-data-platform crates/platform/src/web.rs
// selection_descriptor()): sorted keys, compact, no display name.
std::string selectionJson(const std::string& selection_id, const std::string& server_uri) {
  return "{\"kind\":\"mcap-cloud-selection\",\"selection_id\":\"" + selection_id +
         "\",\"server_uri\":\"" + server_uri + "\",\"v\":1}";
}

}  // namespace

TEST(McapCloudSourceDescriptor, SelectionKindParsesAndRejectsS3Keys) {
  // The server's own bytes parse, and the canonical round trip is byte-stable.
  const std::string json = selectionJson("sel-7f3a", "ws://127.0.0.1:18080");
  std::string error;
  const auto d = mcap_cloud::parseSourceDescriptor(json, &error);
  ASSERT_TRUE(d.has_value()) << error;
  EXPECT_EQ(d->version, 1);
  EXPECT_EQ(d->kind, "mcap-cloud-selection");
  EXPECT_EQ(d->selection_id, "sel-7f3a");
  EXPECT_EQ(d->server_uri, "ws://127.0.0.1:18080");
  EXPECT_TRUE(d->s3_keys.empty()) << "a selection descriptor never carries object keys";
  EXPECT_TRUE(d->topics.empty());
  EXPECT_EQ(mcap_cloud::canonicalSourceDescriptorJson(*d), json);

  // s3_keys beside a selection is the client-supplied-key shape T8 removed:
  // rejected by the kind's field allowlist, not silently ignored.
  {
    auto j = nlohmann::json::parse(json);
    j["s3_keys"] = nlohmann::json::array({"a.mcap"});
    expectReject(j.dump(), "s3_keys");
  }
  // …and so is every other session-only field.
  for (const char* field : {"topics", "start_ns", "end_ns", "include_latched"}) {
    auto j = nlohmann::json::parse(json);
    j[field] = (std::string(field) == "include_latched") ? nlohmann::json(true)
               : (std::string(field) == "topics")        ? nlohmann::json::array({"/imu"})
                                                         : nlohmann::json("0");
    expectReject(j.dump(), field);
  }
  // A missing or empty id is not a selection.
  {
    auto j = nlohmann::json::parse(json);
    j.erase("selection_id");
    expectReject(j.dump(), "selection_id");
  }
  {
    auto j = nlohmann::json::parse(json);
    j["selection_id"] = "";
    expectReject(j.dump(), "selection_id");
  }
  {
    auto j = nlohmann::json::parse(json);
    j["selection_id"] = 7;
    expectReject(j.dump(), "selection_id");
  }
  // The reverse direction: selection_id has no meaning on a session
  // descriptor, so it is an unknown field there.
  {
    auto j = baseJson();
    j["selection_id"] = "sel-7f3a";
    expectReject(j.dump(), "selection_id");
  }
  // The URI hygiene rules are the kind-independent ones.
  {
    auto j = nlohmann::json::parse(selectionJson("sel-7f3a", "http://127.0.0.1:18080"));
    expectReject(j.dump(), "scheme");
  }
  // display_name stays optional and excluded from the canonical bytes.
  {
    auto j = nlohmann::json::parse(json);
    j["display_name"] = "Run 42 [20s,22s)";
    std::string named_error;
    const auto named = mcap_cloud::parseSourceDescriptor(j.dump(), &named_error);
    ASSERT_TRUE(named.has_value()) << named_error;
    EXPECT_EQ(named->display_name, "Run 42 [20s,22s)");
    EXPECT_EQ(mcap_cloud::canonicalSourceDescriptorJson(*named), json);
    EXPECT_EQ(mcap_cloud::descriptorIdentity(*named), mcap_cloud::descriptorIdentity(*d));
  }
}

// The vectors file is the cross-repo contract; the selection cases are
// APPENDED to it (existing cases keep their bytes and identities, which
// SourceDescriptor.VectorConformance above re-checks over the whole file).
TEST(McapCloudSourceDescriptor, SelectionKindVectorConformance) {
  const std::string raw = slurp(MCAP_CLOUD_VECTORS_JSON);
  const auto vectors = nlohmann::json::parse(raw, /*cb=*/nullptr, /*allow_exceptions=*/false);
  ASSERT_FALSE(vectors.is_discarded());
  ASSERT_TRUE(vectors.contains("cases") && vectors["cases"].is_array());

  unsigned selection_cases = 0;
  for (const auto& c : vectors["cases"]) {
    if (c["descriptor"].value("kind", "") != "mcap-cloud-selection") {
      continue;
    }
    ++selection_cases;
    SCOPED_TRACE(c["name"].get<std::string>());
    std::string error;
    const auto d = mcap_cloud::parseSourceDescriptor(c["descriptor"].dump(), &error);
    ASSERT_TRUE(d.has_value()) << error;
    EXPECT_EQ(mcap_cloud::canonicalSourceDescriptorJson(*d), c["canonical"].get<std::string>());
    EXPECT_EQ(mcap_cloud::descriptorIdentity(*d), c["identity"].get<std::string>());
    // The canonical form of a selection is exactly the four server-emitted
    // fields, in sorted order — no key, no window, no flag.
    EXPECT_EQ(c["canonical"].get<std::string>(),
              selectionJson(d->selection_id, d->server_uri));
  }
  EXPECT_GE(selection_cases, 2u) << "the vectors file carries no selection-kind cases";
}

TEST(McapCloudSourceDescriptor, SelectionIdentityDiffersFromSessionIdentity) {
  std::string error;
  const auto selection =
      mcap_cloud::parseSourceDescriptor(selectionJson("a", "ws://localhost:8080"), &error);
  ASSERT_TRUE(selection.has_value()) << error;

  // The kind is INSIDE the canonical bytes, so a session descriptor over the
  // same server can never collide with a selection — even one whose single
  // object key is spelled like the selection id.
  auto session_json = baseJson();
  session_json["s3_keys"] = nlohmann::json::array({"a"});
  session_json["display_name"] = "";
  const auto session = mcap_cloud::parseSourceDescriptor(session_json.dump(), &error);
  ASSERT_TRUE(session.has_value()) << error;

  EXPECT_NE(mcap_cloud::canonicalSourceDescriptorJson(*selection),
            mcap_cloud::canonicalSourceDescriptorJson(*session));
  EXPECT_NE(mcap_cloud::descriptorIdentity(*selection), mcap_cloud::descriptorIdentity(*session));

  // Two different selections on the same server are two different identities.
  const auto other =
      mcap_cloud::parseSourceDescriptor(selectionJson("b", "ws://localhost:8080"), &error);
  ASSERT_TRUE(other.has_value()) << error;
  EXPECT_NE(mcap_cloud::descriptorIdentity(*other), mcap_cloud::descriptorIdentity(*selection));

  // …and the same selection id on two servers likewise.
  const auto elsewhere =
      mcap_cloud::parseSourceDescriptor(selectionJson("a", "ws://localhost:8081"), &error);
  ASSERT_TRUE(elsewhere.has_value()) << error;
  EXPECT_NE(mcap_cloud::descriptorIdentity(*elsewhere), mcap_cloud::descriptorIdentity(*selection));
}
