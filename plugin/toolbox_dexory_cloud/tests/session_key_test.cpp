// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// SessionKey unit tests (Plan B Task 8a; RE-KEYED post-M6 — see
// session_key.hpp). The original four cases from
// 2026-05-28-pj-cloud-client-cpp.md are transcribed with sequence_names in
// place of file_ids, plus a regression case documenting the rekey rationale.
// Hermetic: no env gate, no server, std + the header-only computeSessionKey
// only.

#include <gtest/gtest.h>

#include "session_key.hpp"

using namespace PJ::cloud;

// Unified-plan §6 L1: the key is over the NORMALIZED tuple
// (server_uri, sorted sequence_names[], sorted topic_names[], time_range).
TEST(DexoryCloudSessionKeyTest, ReorderedSequenceNamesAndTopicsProduceSameKey) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {"c", "a", "b"}, {"/b", "/a"}, {100, 200});
  SessionKey b = computeSessionKey("wss://h/api/ws", {"a", "b", "c"}, {"/a", "/b"}, {100, 200});
  EXPECT_EQ(a, b);
  EXPECT_EQ(a.hash, b.hash);
}

TEST(DexoryCloudSessionKeyTest, DifferentUriProducesDifferentKey) {
  SessionKey a = computeSessionKey("wss://h1/api/ws", {"a"}, {"/a"}, {0, 0});
  SessionKey b = computeSessionKey("wss://h2/api/ws", {"a"}, {"/a"}, {0, 0});
  EXPECT_NE(a, b);
  EXPECT_NE(a.hash, b.hash);
}

TEST(DexoryCloudSessionKeyTest, DifferentTimeRangeProducesDifferentKey) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {"a"}, {"/a"}, {100, 200});
  SessionKey b = computeSessionKey("wss://h/api/ws", {"a"}, {"/a"}, {100, 201});
  EXPECT_NE(a, b);
}

TEST(DexoryCloudSessionKeyTest, EmptyTopicsMeansAllTopicsAndIsStable) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {"a", "b"}, {}, {0, 0});
  SessionKey b = computeSessionKey("wss://h/api/ws", {"b", "a"}, {}, {0, 0});
  EXPECT_EQ(a, b);
}

// Different sequence NAMES (the identity axis, post-M6) must produce a
// different key — the counterpart to the reordered-equal case above.
TEST(DexoryCloudSessionKeyTest, DifferentSequenceNamesProduceDifferentKey) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {"seq_a"}, {"/a"}, {0, 0});
  SessionKey b = computeSessionKey("wss://h/api/ws", {"seq_b"}, {"/a"}, {0, 0});
  EXPECT_NE(a, b);
  EXPECT_NE(a.hash, b.hash);
}

// Regression documenting the post-M6 re-key rationale: after the auryn Python
// catalog builder replaced the Go in-process indexer, every rebuild RENUMBERS
// file rowids from scratch (fresh SQLite import), so a wire file_id is only a
// generation-scoped handle. The identity axis MUST be the stable s3 object key
// (== SequenceInfo.name) so the same logical selection produces the SAME
// SessionKey no matter what numeric ids a given catalog generation happened to
// assign — a rebuild renumbering ids must never look like "a different
// session" (nor, worse, collide with an unrelated file that inherited the old
// numeric id).
TEST(DexoryCloudSessionKeyTest, NameKeyedIdentitySurvivesCatalogRebuildRenumbering) {
  // "Before a rebuild": some file_id used to resolve to seq_a/seq_b.
  SessionKey before = computeSessionKey("wss://h/api/ws", {"seq_a", "seq_b"}, {"/a"}, {100, 200});
  // "After a rebuild": the SAME names, SAME topics, SAME range — regardless of
  // whatever new numeric ids the builder assigned this generation — must key
  // identically (the whole point: identity is name-based, not id-based).
  SessionKey after = computeSessionKey("wss://h/api/ws", {"seq_b", "seq_a"}, {"/a"}, {100, 200});
  EXPECT_EQ(before, after);
  EXPECT_EQ(before.hash, after.hash);

  // A genuinely different selection (different name) must NOT collide, even
  // though a stale numeric-id-based key could have (if some OTHER file
  // inherited the old id across the rebuild).
  SessionKey different = computeSessionKey("wss://h/api/ws", {"seq_a", "seq_c"}, {"/a"}, {100, 200});
  EXPECT_NE(before, different);
}
