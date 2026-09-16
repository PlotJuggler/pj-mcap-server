// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// LIVE v3 frozen-selection open (T8b). Gated on MCAP_CLOUD_LIVE_URL plus
// MCAP_CLOUD_LIVE_SELECTION_ID (the descriptor_import_live_test gating
// pattern), against a real pj-data-platform `pj-platform serve` with a
// selection created through POST /api/v1/selections. MCAP_CLOUD_API_KEY, when
// set, is the bearer token the Hello carries (a normal, non-anonymous build
// requires one); the pinned anonymous integration profile accepts it empty.
//
//   MCAP_CLOUD_LIVE_URL=ws://127.0.0.1:18080 \
//   MCAP_CLOUD_API_KEY=<secret> MCAP_CLOUD_LIVE_SELECTION_ID=<id> \
//   ctest -R McapCloudSelectionOpenLive
//
// The four cases are the whole T8b client contract at the wire:
//   1. NegotiatesAndStreamsTheFrozenWindow — Hello v3 + "open-selection/v3",
//      the server's intersection comes back, OpenSession{selection} opens and
//      the frozen window streams to a COMPLETE Eos.
//   2. RefusesWhenTheFeatureIsNotOffered — a connection that did NOT negotiate
//      the feature refuses the same selection BY NAME and puts NOTHING on the
//      wire: no fallback to client-supplied object keys, ever.
//   3. SendsNoObjectKeyOnTheWire — every ClientMessage of a full successful
//      open+download is captured and parsed: no OpenFresh, no GetFile, no
//      UpdateTags, exactly one OpenSession and it carries only the id.
//   4. ResumeRevalidatesTheSelection — the same session resumed from a SECOND
//      connection: refused by name when that connection did not negotiate the
//      feature, accepted when it did (crates/platform/src/wire.rs: a
//      selection-backed session is re-validated on EVERY resume and the
//      feature must still be negotiated on THAT connection).
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

#include "backend_connection.hpp"
#include "backend_types.hpp"
#include "decoded_message.hpp"
#include "pj_cloud.pb.h"

namespace {

using mcap_cloud::BackendConnection;
using mcap_cloud::OpenSessionParams;
using mcap_cloud::SessionEos;
using mcap_cloud::SessionInfo;
using mcap_cloud::SessionStats;

const char* env(const char* name) {
  const char* value = std::getenv(name);
  return (value != nullptr && *value != '\0') ? value : nullptr;
}

const char* liveUrl() { return env("MCAP_CLOUD_LIVE_URL"); }
const char* selectionId() { return env("MCAP_CLOUD_LIVE_SELECTION_ID"); }
std::string apiKey() {
  const char* key = env("MCAP_CLOUD_API_KEY");
  return key == nullptr ? std::string{} : std::string(key);
}

// Both gates are required; a missing one SKIPS (never silently passes).
#define REQUIRE_LIVE_SELECTION()                                                              \
  do {                                                                                        \
    if (liveUrl() == nullptr) {                                                               \
      GTEST_SKIP() << "MCAP_CLOUD_LIVE_URL not set — live selection-open test skipped";        \
    }                                                                                         \
    if (selectionId() == nullptr) {                                                           \
      GTEST_SKIP() << "MCAP_CLOUD_LIVE_SELECTION_ID not set — live selection-open test skipped"; \
    }                                                                                         \
  } while (false)

// Captures the SERIALIZED bytes of every ClientMessage the connection sends,
// so the test can assert what the plugin did NOT put on the wire. Frames are
// appended on the sending thread and read after the blocking call returns.
class WireLog {
 public:
  void attach(BackendConnection& conn) {
    conn.testSetOutboundFrameObserver([this](const std::string& payload) {
      const std::lock_guard<std::mutex> lock(mu_);
      frames_.push_back(payload);
    });
  }
  [[nodiscard]] std::vector<pj_cloud::v1::ClientMessage> parsed() const {
    std::vector<std::string> copy;
    {
      const std::lock_guard<std::mutex> lock(mu_);
      copy = frames_;
    }
    std::vector<pj_cloud::v1::ClientMessage> out;
    out.reserve(copy.size());
    for (const auto& payload : copy) {
      pj_cloud::v1::ClientMessage message;
      EXPECT_TRUE(message.ParseFromString(payload)) << "an unparsable frame reached the wire";
      out.push_back(std::move(message));
    }
    return out;
  }

 private:
  mutable std::mutex mu_;
  std::vector<std::string> frames_;
};

// connect() with the v3 feature requested, and assert the server negotiated it.
bool connectNegotiated(BackendConnection& conn) {
  conn.requestOpenSelectionFeature();
  std::string error;
  EXPECT_TRUE(conn.connect(&error)) << "connect failed: " << error;
  if (!conn.hasNegotiatedFeature(mcap_cloud::kOpenSelectionFeature)) {
    ADD_FAILURE() << "the server did not negotiate " << mcap_cloud::kOpenSelectionFeature
                  << "; this build of the server cannot serve a frozen selection";
    return false;
  }
  return true;
}

OpenSessionParams selectionParams() {
  OpenSessionParams params;
  params.selection_id = selectionId();
  return params;
}

}  // namespace

// 1. The whole hand-off: negotiate, open by id alone, stream the frozen window.
TEST(McapCloudSelectionOpenLive, NegotiatesAndStreamsTheFrozenWindow) {
  REQUIRE_LIVE_SELECTION();
  BackendConnection conn(liveUrl(), /*cert_path=*/"", apiKey(), /*allow_insecure=*/false);
  ASSERT_TRUE(connectNegotiated(conn));

  SessionInfo info;
  std::string error;
  ASSERT_TRUE(conn.openSessionFresh(selectionParams(), &info, &error))
      << "OpenSession{selection} failed: " << error;
  // The SERVER resolved the membership and the topics: the client sent neither.
  EXPECT_FALSE(info.topics.empty()) << "the frozen selection resolved to no topics";
  EXPECT_LE(info.merged_start_ns, info.merged_end_ns);

  std::uint64_t counted = 0;
  const SessionStats stats = conn.downloadSession(info, [&](const mcap_cloud::DecodedMessage&) -> bool {
    ++counted;
    return true;
  });
  EXPECT_EQ(stats.eos, SessionEos::Complete) << "error: " << stats.error;
  EXPECT_TRUE(stats.error.empty()) << stats.error;
  EXPECT_GT(counted, 0u) << "the frozen window streamed nothing";
  EXPECT_EQ(stats.messages_received, counted);

  // When the harness pins the window's message count, it is checked exactly.
  if (const char* expected = env("MCAP_CLOUD_LIVE_SELECTION_MESSAGES")) {
    EXPECT_EQ(counted, std::strtoull(expected, nullptr, 10))
        << "the frozen window did not deliver its pinned message count";
  }
}

// 2. Without the negotiated feature the open FAILS BY NAME — and never reaches
// the wire, so there is no chance of a server-side fallback either.
TEST(McapCloudSelectionOpenLive, RefusesWhenTheFeatureIsNotOffered) {
  REQUIRE_LIVE_SELECTION();
  BackendConnection conn(liveUrl(), /*cert_path=*/"", apiKey(), /*allow_insecure=*/false);
  WireLog wire;
  wire.attach(conn);
  // NO requestOpenSelectionFeature(): this is a v2 client on the wire, exactly
  // like the pinned plugin, so the server's intersection is empty.
  std::string error;
  ASSERT_TRUE(conn.connect(&error)) << "connect failed: " << error;
  EXPECT_FALSE(conn.hasNegotiatedFeature(mcap_cloud::kOpenSelectionFeature));

  SessionInfo info;
  std::string open_error;
  EXPECT_FALSE(conn.openSessionFresh(selectionParams(), &info, &open_error));
  EXPECT_EQ(open_error, mcap_cloud::kOpenSelectionUnsupportedError);

  for (const auto& message : wire.parsed()) {
    EXPECT_FALSE(message.has_open_session())
        << "a refused selection open must not reach the wire at all";
  }

  // …and the refusal is not a transport casualty: the same connection still
  // works for an ordinary v2 request.
  EXPECT_TRUE(conn.version().has_value());
}

// 3. The point of the whole exercise: the client holds no object identity, so
// none can appear on the wire.
TEST(McapCloudSelectionOpenLive, SendsNoObjectKeyOnTheWire) {
  REQUIRE_LIVE_SELECTION();
  BackendConnection conn(liveUrl(), /*cert_path=*/"", apiKey(), /*allow_insecure=*/false);
  WireLog wire;
  wire.attach(conn);
  ASSERT_TRUE(connectNegotiated(conn));

  SessionInfo info;
  std::string error;
  ASSERT_TRUE(conn.openSessionFresh(selectionParams(), &info, &error)) << error;
  const SessionStats stats =
      conn.downloadSession(info, [](const mcap_cloud::DecodedMessage&) -> bool { return true; });
  ASSERT_EQ(stats.eos, SessionEos::Complete) << stats.error;

  // Every ClientMessage field that can carry an object key: OpenFresh.s3_keys,
  // GetFileRequest.s3_key, UpdateTagsRequest.s3_key. None may appear.
  unsigned selection_opens = 0;
  const auto frames = wire.parsed();
  ASSERT_FALSE(frames.empty()) << "the observer saw no frames at all";
  for (const auto& message : frames) {
    EXPECT_FALSE(message.has_get_file()) << "GetFile addresses an object by key";
    EXPECT_FALSE(message.has_update_tags()) << "UpdateTags addresses an object by key";
    if (!message.has_open_session()) {
      continue;
    }
    EXPECT_FALSE(message.open_session().has_fresh())
        << "OpenFresh is the client-supplied-key shape a selection replaces";
    ASSERT_TRUE(message.open_session().has_selection());
    EXPECT_EQ(message.open_session().selection().selection_id(), selectionId());
    // The oneof carries the id and nothing else: an OpenSelection built from
    // the id alone serializes to the same size as the one actually sent.
    pj_cloud::v1::OpenSelection expected;
    expected.set_selection_id(selectionId());
    EXPECT_EQ(message.open_session().selection().ByteSizeLong(), expected.ByteSizeLong());
    ++selection_opens;
  }
  EXPECT_EQ(selection_opens, 1u) << "exactly one OpenSession{selection} per open";
}

// 4. A selection-backed session is re-validated on EVERY resume, and that
// re-validation needs the feature negotiated on the RESUMING socket. Driven
// through two SEPARATE connections rather than a forced mid-stream drop: a
// bounded frozen window is one batch, so the terminal Eos is already queued
// when the socket goes down and the drop never becomes a resumable gap. Two
// connections make the property deterministic AND assert the half that
// matters — the resume is re-authorized against THIS connection's Hello.
TEST(McapCloudSelectionOpenLive, ResumeRevalidatesTheSelection) {
  REQUIRE_LIVE_SELECTION();
  // Leg 1: open and drain the frozen window, so the session is registered and
  // idle server-side (a busy session refuses every resume for another reason).
  BackendConnection opener(liveUrl(), /*cert_path=*/"", apiKey(), /*allow_insecure=*/false);
  ASSERT_TRUE(connectNegotiated(opener));
  SessionInfo info;
  std::string error;
  ASSERT_TRUE(opener.openSessionFresh(selectionParams(), &info, &error)) << error;
  std::uint64_t counted = 0;
  const SessionStats stats = opener.downloadSession(info, [&](const mcap_cloud::DecodedMessage&) -> bool {
    ++counted;
    return true;
  });
  ASSERT_EQ(stats.eos, SessionEos::Complete) << stats.error;
  ASSERT_GT(counted, 0u);

  // Leg 2: a connection that did NOT negotiate the feature cannot resume it.
  // The session id is not a capability: the selection is re-checked, and the
  // refusal names the reason.
  {
    BackendConnection stranger(liveUrl(), /*cert_path=*/"", apiKey(), /*allow_insecure=*/false);
    std::string connect_error;
    ASSERT_TRUE(stranger.connect(&connect_error)) << connect_error;
    ASSERT_FALSE(stranger.hasNegotiatedFeature(mcap_cloud::kOpenSelectionFeature));
    bool rejected = false;
    std::string resume_error;
    EXPECT_FALSE(stranger.testOpenSessionResume(info.subscription_id, /*resume_after_seq=*/0,
                                                &resume_error, &rejected));
    EXPECT_TRUE(rejected) << "the server must REJECT it, not drop the transport";
    EXPECT_NE(resume_error.find("open-selection/v3"), std::string::npos)
        << "the refusal must name the missing feature; got: " << resume_error;
  }

  // Leg 3: a connection that DID negotiate it resumes the same session, and
  // its handshake is a full v3 Hello carrying the feature.
  BackendConnection resumer(liveUrl(), /*cert_path=*/"", apiKey(), /*allow_insecure=*/false);
  WireLog wire;
  wire.attach(resumer);
  ASSERT_TRUE(connectNegotiated(resumer));
  bool rejected = false;
  std::string resume_error;
  EXPECT_TRUE(resumer.testOpenSessionResume(info.subscription_id, /*resume_after_seq=*/0,
                                            &resume_error, &rejected))
      << "resume refused: " << resume_error;
  EXPECT_FALSE(rejected);

  unsigned hellos = 0;
  for (const auto& message : wire.parsed()) {
    if (message.has_hello()) {
      ++hellos;
      EXPECT_EQ(message.hello().protocol_version(), 3u);
      ASSERT_EQ(message.hello().features_size(), 1);
      EXPECT_EQ(message.hello().features(0), mcap_cloud::kOpenSelectionFeature);
    }
    // The resuming connection addresses the session by its id alone.
    EXPECT_FALSE(message.open_session().has_fresh());
    EXPECT_FALSE(message.has_get_file());
  }
  EXPECT_EQ(hellos, 1u) << "the resuming connection must have handshaken exactly once";
}
