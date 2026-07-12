// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// ParserIngestDriver (Slice 16, HERMETIC): drives bindSession/decode/finalize
// against the FakeIngestHost recorder. Pins the ensureParserBinding fields
// going over the ABI VERBATIM — including the LOAD-BEARING non-empty
// parser_config_json "{}" (an empty config makes the real host skip
// parser->loadConfig(), silently degrading parser_ros to generic scalar-only
// handlers) — plus the per-topic skip paths (refused parser, missing schema),
// the push payload/timestamp fidelity, the counters, the idempotent release
// contract, and the older-host (no tail slots / refused create) reason.
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "parser_ingest_driver.hpp"
#include "parser_ingest_test_support.hpp"

namespace {

using dexory_cloud::DecodedMessage;
using dexory_cloud::ParserIngestDriver;
using dexory_cloud::SessionInfo;
using pj_ingest_test::FakeIngestHost;

SessionInfo makeSession() {
  SessionInfo info;
  info.schemas.push_back(
      {1, "tf2_msgs/msg/TFMessage", "ros2msg", "geometry_msgs/TransformStamped[] transforms\n"});
  info.schemas.push_back({2, "mock/refused", "ros2msg", "bool x\n"});
  info.topics.push_back({10, "/tf", 1, "cdr"});
  info.topics.push_back({20, "/refused", 2, "cdr"});
  info.topics.push_back({30, "/no_schema", 99, "cdr"});
  return info;
}

DecodedMessage makeMessage(std::uint32_t topic_id, std::int64_t ts, std::string payload) {
  DecodedMessage m;
  m.topic_id = topic_id;
  m.log_time_ns = ts;
  m.payload = std::move(payload);
  return m;
}

TEST(ParserIngestDriverTest, BindsVerbatimPushesPayloadsAndReleases) {
  FakeIngestHost fake;
  fake.refuse_type = "mock/refused";

  ParserIngestDriver driver;
  PJ::ToolboxRuntimeHostView runtime{fake.toolboxRuntime()};
  const auto bind = driver.bindSession(runtime, PJ::sdk::DataSourceHandle{42}, makeSession());

  // TWO schema-resolved candidates (/tf + /refused — the latter's parser
  // refusal is only discoverable at its first message now); the ingest context
  // was created for ds id 42.
  EXPECT_EQ(bind.decodable, 2u);
  ASSERT_EQ(fake.created.size(), 1u);
  EXPECT_EQ(fake.created[0], 42u);

  // LAZY BINDING (2026-07-12): bindSession creates NO host bindings — a topic
  // that never delivers a message must never register in the host data tree
  // (zero-message entries users could drag to no effect).
  EXPECT_TRUE(fake.bindings.empty());

  // One skipped topic at bind time (schema missing from the session dictionary),
  // in the "<topic> (<type>): <reason>" format.
  ASSERT_EQ(bind.errors.size(), 1u);
  EXPECT_NE(bind.errors[0].find("/no_schema"), std::string::npos);
  EXPECT_NE(bind.errors[0].find("(no schema)"), std::string::npos);

  const auto& topics = driver.decoders();
  ASSERT_EQ(topics.size(), 3u);
  EXPECT_TRUE(topics.at(10).decodable);
  EXPECT_TRUE(topics.at(10).skip_reason.empty());
  EXPECT_TRUE(topics.at(20).decodable);  // candidate until its first message
  EXPECT_TRUE(topics.at(20).skip_reason.empty());
  EXPECT_FALSE(topics.at(30).decodable);
  EXPECT_FALSE(topics.at(30).skip_reason.empty());
  EXPECT_TRUE(driver.hasDecodable());

  // Decodable topic: the FIRST message creates the binding (all five fields
  // verbatim — including the load-bearing non-empty "{}" parser config), then
  // pushes with exact ts + bytes.
  EXPECT_TRUE(driver.decode(makeMessage(10, 111, std::string("\x01\x02\x03", 3))));
  ASSERT_EQ(fake.bindings.size(), 1u);
  const auto& b = fake.bindings[0];
  EXPECT_EQ(b.topic_name, "/tf");
  EXPECT_EQ(b.parser_encoding, "ros2msg");
  EXPECT_EQ(b.type_name, "tf2_msgs/msg/TFMessage");
  EXPECT_EQ(b.schema, "geometry_msgs/TransformStamped[] transforms\n");
  EXPECT_EQ(b.config, "{}");
  EXPECT_TRUE(driver.decode(makeMessage(10, 222, std::string("\x04", 1))));
  EXPECT_EQ(fake.bindings.size(), 1u);  // bound once, not per message

  // Refused topic: its first message attempts the bind, the host refuses, the
  // topic flips undecodable with the host's reason. Unknown topic: refused.
  EXPECT_FALSE(driver.decode(makeMessage(20, 333, "x")));
  EXPECT_FALSE(topics.at(20).decodable);
  EXPECT_NE(topics.at(20).skip_reason.find("no parser found"), std::string::npos);
  EXPECT_EQ(fake.bindings.size(), 1u);  // the refusal recorded no binding
  EXPECT_FALSE(driver.decode(makeMessage(77, 444, "y")));

  ASSERT_EQ(fake.pushes.size(), 2u);
  EXPECT_EQ(fake.pushes[0].handle, b.handle);
  EXPECT_EQ(fake.pushes[0].ts, 111);
  EXPECT_EQ(fake.pushes[0].bytes, (std::vector<std::uint8_t>{1, 2, 3}));
  EXPECT_EQ(fake.pushes[1].handle, b.handle);
  EXPECT_EQ(fake.pushes[1].ts, 222);
  EXPECT_EQ(fake.pushes[1].bytes, (std::vector<std::uint8_t>{4}));

  const auto counts = driver.decodedCounts();
  EXPECT_EQ(counts.at(10), 2u);
  EXPECT_EQ(counts.at(20), 0u);
  EXPECT_EQ(counts.at(30), 0u);
  const auto errors = driver.errorCounts();
  EXPECT_EQ(errors.at(10), 0u);
  EXPECT_EQ(errors.at(20), 0u);
  EXPECT_EQ(errors.at(30), 0u);

  driver.finalize();
  ASSERT_EQ(fake.released.size(), 1u);
  EXPECT_EQ(fake.released[0], 42u);
  driver.finalize();  // idempotent: no second release
  EXPECT_EQ(fake.released.size(), 1u);

  // C11: decode-after-finalize must be refused; push count unchanged.
  const std::size_t pushes_before = fake.pushes.size();
  EXPECT_FALSE(driver.decode(makeMessage(10, 999, "x")));
  EXPECT_EQ(fake.pushes.size(), pushes_before);
}

TEST(ParserIngestDriverTest, PushFailureCountsDecodeErrors) {
  FakeIngestHost fake;
  fake.refuse_type = "mock/refused";

  ParserIngestDriver driver;
  PJ::ToolboxRuntimeHostView runtime{fake.toolboxRuntime()};
  (void)driver.bindSession(runtime, PJ::sdk::DataSourceHandle{42}, makeSession());

  // With refuse_push set: decode returns false, error counter increments,
  // decoded counter stays zero, nothing recorded in fake.pushes.
  fake.refuse_push = true;
  EXPECT_FALSE(driver.decode(makeMessage(10, 100, std::string("\x01", 1))));
  EXPECT_EQ(driver.errorCounts().at(10), 1u);
  EXPECT_EQ(driver.decodedCounts().at(10), 0u);
  EXPECT_TRUE(fake.pushes.empty());

  // After clearing the refusal: decode returns true, both counters as expected.
  fake.refuse_push = false;
  EXPECT_TRUE(driver.decode(makeMessage(10, 200, std::string("\x02", 1))));
  EXPECT_EQ(driver.decodedCounts().at(10), 1u);
  EXPECT_EQ(driver.errorCounts().at(10), 1u);
  ASSERT_EQ(fake.pushes.size(), 1u);
  EXPECT_EQ(fake.pushes[0].bytes, (std::vector<std::uint8_t>{2}));
}

TEST(ParserIngestDriverTest, OlderHostReportsPerTopicReason) {
  FakeIngestHost fake;
  fake.refuse_create = true;

  ParserIngestDriver driver;
  PJ::ToolboxRuntimeHostView runtime{fake.toolboxRuntime()};
  const auto bind = driver.bindSession(runtime, PJ::sdk::DataSourceHandle{7}, makeSession());

  EXPECT_EQ(bind.decodable, 0u);
  EXPECT_EQ(bind.errors.size(), 3u);
  EXPECT_FALSE(driver.hasDecodable());
  EXPECT_TRUE(fake.created.empty());
  EXPECT_TRUE(fake.bindings.empty());

  ASSERT_EQ(driver.decoders().size(), 3u);
  for (const auto& [id, t] : driver.decoders()) {
    EXPECT_FALSE(t.decodable) << "topic " << id;
    EXPECT_NE(t.skip_reason.find("host parser ingest unavailable"), std::string::npos)
        << "topic " << id << " reason: " << t.skip_reason;
  }

  // Nothing was created, so decode is refused and finalize releases nothing.
  EXPECT_FALSE(driver.decode(makeMessage(10, 1, "z")));
  EXPECT_TRUE(fake.pushes.empty());
  driver.finalize();
  EXPECT_TRUE(fake.released.empty());
}

}  // namespace
