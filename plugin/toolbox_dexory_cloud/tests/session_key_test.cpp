// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// SessionKey unit tests (Plan B Task 8a) — the EXACT four cases from
// 2026-05-28-pj-cloud-client-cpp.md, transcribed verbatim. Hermetic: no env
// gate, no server, std + the header-only computeSessionKey only.

#include <gtest/gtest.h>

#include "session_key.hpp"

using namespace PJ::cloud;

// Unified-plan §6 L1: the key is over the NORMALIZED tuple
// (server_uri, sorted file_ids[], sorted topic_names[], time_range).
TEST(DexoryCloudSessionKeyTest, ReorderedFileIdsAndTopicsProduceSameKey) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {3, 1, 2}, {"/b", "/a"}, {100, 200});
  SessionKey b = computeSessionKey("wss://h/api/ws", {1, 2, 3}, {"/a", "/b"}, {100, 200});
  EXPECT_EQ(a, b);
  EXPECT_EQ(a.hash, b.hash);
}

TEST(DexoryCloudSessionKeyTest, DifferentUriProducesDifferentKey) {
  SessionKey a = computeSessionKey("wss://h1/api/ws", {1}, {"/a"}, {0, 0});
  SessionKey b = computeSessionKey("wss://h2/api/ws", {1}, {"/a"}, {0, 0});
  EXPECT_NE(a, b);
  EXPECT_NE(a.hash, b.hash);
}

TEST(DexoryCloudSessionKeyTest, DifferentTimeRangeProducesDifferentKey) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {1}, {"/a"}, {100, 200});
  SessionKey b = computeSessionKey("wss://h/api/ws", {1}, {"/a"}, {100, 201});
  EXPECT_NE(a, b);
}

TEST(DexoryCloudSessionKeyTest, EmptyTopicsMeansAllTopicsAndIsStable) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {1, 2}, {}, {0, 0});
  SessionKey b = computeSessionKey("wss://h/api/ws", {2, 1}, {}, {0, 0});
  EXPECT_EQ(a, b);
}
