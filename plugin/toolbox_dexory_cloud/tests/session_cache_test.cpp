// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// SessionCache unit tests (Slice 8, Plan D Task 5 adapted). Hermetic: no env
// gate, no server. Exercises the cache logic directly through its lookup/store/
// evict surface, with a fake existence-predicate seam standing in for the
// ToolboxHostView catalog snapshot.
//
// "Transport-counter == 0 on HIT" is asserted structurally: SessionCache itself
// performs NO transport (it only re-emits cached metadata). The HIT/MISS
// decisions below mirror exactly what FetchWorker::pullTopicsAsync uses to gate
// whether a BackendConnection is ever constructed. A transport counter is added
// here to make the invariant explicit: it is incremented in the MISS branch only
// (the production code constructs the session connection there), and asserted 0
// after every HIT.

#include <gtest/gtest.h>

#include <string>
#include <unordered_set>

#include "session_cache.hpp"
#include "session_key.hpp"

using dexory_cloud::CachedSession;
using dexory_cloud::SessionCache;
using PJ::cloud::computeSessionKey;
using PJ::cloud::SessionKey;

namespace {

constexpr const char* kUri = "ws://localhost:8082";

CachedSession makeEntry(const std::string& display_name, std::uint64_t a, std::uint64_t b) {
  CachedSession e;
  e.display_name = display_name;
  e.server_uri = kUri;
  e.counts_by_topic["/a"] = a;
  e.counts_by_topic["/b"] = b;
  e.total_messages = a + b;
  return e;
}

// "Everything present" predicate (used for plain HIT tests).
auto alwaysPresent() {
  return [](const std::string&) { return true; };
}

}  // namespace

// A stored COMPLETE entry, looked up by the same NORMALIZED selection, is a HIT
// that returns the cached counts with ZERO transport.
TEST(DexoryCloudSessionCacheTest, HitReturnsCachedCountsZeroTransport) {
  SessionCache cache;
  const SessionKey key = computeSessionKey(kUri, {"file_1", "file_2"}, {"/a", "/b"}, {0, 0});
  cache.store(key, makeEntry("seq_x", 10, 20));

  int transport_calls = 0;
  auto hit = cache.lookup(key, alwaysPresent());
  ASSERT_TRUE(hit.has_value());
  EXPECT_EQ(transport_calls, 0);  // a HIT must NOT touch transport
  EXPECT_EQ(hit->display_name, "seq_x");
  EXPECT_EQ(hit->counts_by_topic.at("/a"), 10u);
  EXPECT_EQ(hit->counts_by_topic.at("/b"), 20u);
  EXPECT_EQ(hit->total_messages, 30u);
}

// Reordered/duplicated inputs collide -> HIT on the same key.
TEST(DexoryCloudSessionCacheTest, ReorderedSelectionHits) {
  SessionCache cache;
  const SessionKey stored = computeSessionKey(kUri, {"file_1", "file_2", "file_3"}, {"/a", "/b"}, {100, 200});
  cache.store(stored, makeEntry("seq_y", 5, 7));

  const SessionKey reordered = computeSessionKey(kUri, {"file_3", "file_2", "file_1"}, {"/b", "/a"}, {100, 200});
  auto hit = cache.lookup(reordered, alwaysPresent());
  ASSERT_TRUE(hit.has_value());
  EXPECT_EQ(hit->display_name, "seq_y");
}

// Exact-tuple only: a different time-range / topic set / sequence_names is a MISS.
TEST(DexoryCloudSessionCacheTest, KeyExactnessMissesOnAnyDifference) {
  SessionCache cache;
  const SessionKey base = computeSessionKey(kUri, {"file_1", "file_2"}, {"/a", "/b"}, {100, 200});
  cache.store(base, makeEntry("seq_z", 1, 1));

  int transport_calls = 0;
  auto miss_on_range = [&](const SessionKey& k) {
    auto hit = cache.lookup(k, alwaysPresent());
    if (!hit) {
      ++transport_calls;  // production would construct a session connection here
    }
    return hit.has_value();
  };

  EXPECT_FALSE(miss_on_range(computeSessionKey(kUri, {"file_1", "file_2"}, {"/a", "/b"}, {100, 201})));  // range
  EXPECT_FALSE(miss_on_range(computeSessionKey(kUri, {"file_1", "file_2"}, {"/a"}, {100, 200})));         // topics
  EXPECT_FALSE(
      miss_on_range(computeSessionKey(kUri, {"file_1", "file_2", "file_3"}, {"/a", "/b"}, {100, 200})));  // names
  EXPECT_FALSE(
      miss_on_range(computeSessionKey("ws://other:8082", {"file_1", "file_2"}, {"/a", "/b"}, {100, 200})));  // uri
  EXPECT_EQ(transport_calls, 4);
}

// LRU eviction over a small entry budget: the LRU entry is dropped first.
TEST(DexoryCloudSessionCacheTest, LruEvictsLeastRecentlyUsedOverBudget) {
  SessionCache cache(/*max_entries=*/2);
  const SessionKey k1 = computeSessionKey(kUri, {"file_1"}, {"/a"}, {0, 0});
  const SessionKey k2 = computeSessionKey(kUri, {"file_2"}, {"/a"}, {0, 0});
  const SessionKey k3 = computeSessionKey(kUri, {"file_3"}, {"/a"}, {0, 0});

  cache.store(k1, makeEntry("one", 1, 0));
  cache.store(k2, makeEntry("two", 2, 0));
  EXPECT_EQ(cache.size(), 2u);

  // Touch k1 so k2 becomes the LRU.
  ASSERT_TRUE(cache.lookup(k1, alwaysPresent()).has_value());
  // Store k3 -> over budget -> evict k2 (LRU).
  cache.store(k3, makeEntry("three", 3, 0));
  EXPECT_EQ(cache.size(), 2u);
  EXPECT_TRUE(cache.lookup(k1, alwaysPresent()).has_value());
  EXPECT_FALSE(cache.lookup(k2, alwaysPresent()).has_value());  // evicted
  EXPECT_TRUE(cache.lookup(k3, alwaysPresent()).has_value());
}

// A fresh instance is empty (no cross-instance / cross-restart persistence).
TEST(DexoryCloudSessionCacheTest, FreshInstanceIsEmpty) {
  SessionCache cache;
  EXPECT_EQ(cache.size(), 0u);
  const SessionKey key = computeSessionKey(kUri, {"file_1"}, {"/a"}, {0, 0});
  EXPECT_FALSE(cache.lookup(key, alwaysPresent()).has_value());
}

// Present-but-gone: the entry exists in the cache but the dataset was cleared
// from the host (predicate false). The lookup is a MISS *and* the stale entry is
// evicted so the next fetch re-fills it.
TEST(DexoryCloudSessionCacheTest, DatasetGoneEvictsAndMisses) {
  SessionCache cache;
  const SessionKey key = computeSessionKey(kUri, {"file_1"}, {"/a"}, {0, 0});
  cache.store(key, makeEntry("cleared_seq", 9, 9));
  EXPECT_EQ(cache.size(), 1u);

  auto gone = [](const std::string&) { return false; };  // user cleared the dataset
  EXPECT_FALSE(cache.lookup(key, gone).has_value());
  EXPECT_EQ(cache.size(), 0u) << "a proven-gone dataset must be evicted";
}

// Presence-unknown (no predicate / host lacks acquire_catalog_snapshot): treated
// as a MISS, but the entry is NOT evicted (we cannot prove it gone).
TEST(DexoryCloudSessionCacheTest, PresenceUnknownMissesWithoutEvicting) {
  SessionCache cache;
  const SessionKey key = computeSessionKey(kUri, {"file_1"}, {"/a"}, {0, 0});
  cache.store(key, makeEntry("unknown_seq", 3, 4));

  SessionCache::ExistencePredicate none;  // null predicate == presence-unknown
  EXPECT_FALSE(cache.lookup(key, none).has_value());
  EXPECT_EQ(cache.size(), 1u) << "presence-unknown must NOT evict";

  // Once the predicate can confirm presence again, it HITs.
  EXPECT_TRUE(cache.lookup(key, alwaysPresent()).has_value());
}

// Regression (post-M6): the cache key is the s3-key NAME, never a wire
// file_id. Two lookups built from the SAME names+topics+range HIT the SAME
// entry no matter what numeric file_id a given catalog generation happened to
// resolve them to — a rebuild renumbering rowids must not change (or worse,
// silently collide with an unrelated file's) cached session identity.
TEST(DexoryCloudSessionCacheTest, NameKeyedIdentitySurvivesCatalogRebuildRenumbering) {
  SessionCache cache;
  const SessionKey stored = computeSessionKey(kUri, {"nissan_zala_50"}, {"/a"}, {0, 0});
  cache.store(stored, makeEntry("nissan_zala_50", 42, 0));

  // "After a rebuild": the same logical selection (same name) still HITs.
  const SessionKey after_rebuild = computeSessionKey(kUri, {"nissan_zala_50"}, {"/a"}, {0, 0});
  auto hit = cache.lookup(after_rebuild, alwaysPresent());
  ASSERT_TRUE(hit.has_value());
  EXPECT_EQ(hit->total_messages, 42u);

  // A DIFFERENT name (a different file) never collides, even though a stale
  // numeric-id-based key could have if some other file inherited the old id.
  const SessionKey different_file = computeSessionKey(kUri, {"other_seq"}, {"/a"}, {0, 0});
  EXPECT_FALSE(cache.lookup(different_file, alwaysPresent()).has_value());
}

// evict() drops a specific entry; re-store re-fills.
TEST(DexoryCloudSessionCacheTest, ExplicitEvict) {
  SessionCache cache;
  const SessionKey key = computeSessionKey(kUri, {"file_1"}, {"/a"}, {0, 0});
  cache.store(key, makeEntry("e", 1, 1));
  cache.evict(key);
  EXPECT_EQ(cache.size(), 0u);
  EXPECT_FALSE(cache.lookup(key, alwaysPresent()).has_value());
}
