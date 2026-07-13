// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// SessionCache — the in-memory, toolbox-adapted session cache (Plan D Task 5
// semantics, ADAPTED to the toolbox shape). After a COMPLETE Fetch the decoded
// scalars already live in the PJ4 datastore under the group dataset
// (display_name). So a cache HIT does NOT re-materialize bytes — it re-emits the
// per-topic pullFinished ledger from cached counts and skips ALL transport. The
// datastore owns the actual memory; this cache stores only counts metadata.
//
// KEY: SessionKey over the EXACT logical selection requested
// (server_uri, sequence_names[], topics[], time_range) — sequence_names are the
// stable s3 keys (SequenceInfo.name), NOT the wire file_ids sent in
// OpenSessionParams (those renumber across a post-M6 catalog builder rebuild;
// see session_key.hpp for why). Exact-tuple only: a different time-range or
// topic set is a MISS; reordered inputs collide (HIT).
//
// EXISTENCE VERIFICATION (the toolbox adaptation): a HIT additionally requires
// that the cached dataset is STILL present in the host catalog (the user may
// have cleared it). The caller injects an existence predicate
// (std::function<bool(const std::string& display_name)>), normally backed by
// ToolboxHostView::catalogSnapshot().dataSources(). If presence cannot be
// verified (the host lacks acquire_catalog_snapshot) the predicate returns false
// and the entry is treated as a MISS (presence-unknown -> MISS, never a false
// HIT). A present-but-absent entry is evicted so the next fetch re-fills it.
//
// STORE: COMPLETE-only. cancel / error / no-terminal-Eos -> NO entry (no
// half-cached state). LRU over a small ENTRY-COUNT budget (the datastore owns
// the bytes; bounding by entry count, not memory, is the right adaptation).
//
// THREADING: owned by FetchWorker; all access is on the single worker thread, so
// no internal locking is required (matches the worker's single-threaded
// pullTopicsAsync discipline).
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include "session_key.hpp"

namespace dexory_cloud {

// The SessionKey type lives in PJ::cloud (Plan B Task 8a); bring it into scope so
// the cache members read cleanly.
using PJ::cloud::SessionKey;

// The cached metadata for ONE completed session. Only counts + identity — never
// the decoded rows (those live in the datastore under display_name).
struct CachedSession {
  std::string display_name;  // the host dataset/group name (existence is checked against this)
  std::string server_uri;    // the connection this was fetched over (also in the key)
  std::unordered_map<std::string, std::uint64_t> counts_by_topic;  // per-topic appendRecord counts
  std::uint64_t total_messages = 0;
};

// LRU-by-entry-count cache keyed on SessionKey.hash with full-key equality to
// defeat hash collisions. Default budget kMaxEntries=8 (small; the datastore
// owns the real memory).
class SessionCache {
 public:
  // Predicate that answers "is this dataset (display_name) still present in the
  // host catalog?". Presence-unknown MUST return false.
  using ExistencePredicate = std::function<bool(const std::string& display_name)>;

  static constexpr std::size_t kDefaultMaxEntries = 8;

  explicit SessionCache(std::size_t max_entries = kDefaultMaxEntries) : max_entries_(max_entries ? max_entries : 1) {}

  // Look up a HIT for `key`. Returns the cached metadata ONLY when an entry with
  // the same full key exists AND `exists(display_name)` is true. A matching entry
  // whose dataset is gone (exists == false) is EVICTED and nullopt is returned
  // (so the caller falls through to a normal fetch). On a real HIT the entry is
  // moved to the front (most-recently-used). A null predicate means
  // presence-unknown -> treated as MISS (and the stale entry is NOT evicted, since
  // we cannot prove it gone).
  [[nodiscard]] std::optional<CachedSession> lookup(const SessionKey& key, const ExistencePredicate& exists) {
    auto map_it = index_.find(key.hash);
    if (map_it == index_.end()) {
      return std::nullopt;
    }
    // Scan the (tiny) bucket for the full-key match. Each bucket element is an
    // EntryIter into entries_; dereference twice to reach the Entry.
    for (auto bit = map_it->second.begin(); bit != map_it->second.end(); ++bit) {
      EntryIter entry_it = *bit;
      if (entry_it->key != key) {
        continue;
      }
      // Found the entry. Verify dataset existence in the host.
      const bool present = exists ? exists(entry_it->value.display_name) : false;
      if (!present) {
        if (exists) {
          // Predicate ran and said "gone" -> evict the stale entry.
          eraseLocked(entry_it, map_it);
        }
        return std::nullopt;  // presence-unknown OR proven-gone -> MISS
      }
      // Real HIT: promote to MRU and return a copy of the metadata.
      promoteLocked(entry_it);
      return entries_.front().value;
    }
    return std::nullopt;
  }

  // Store (or refresh) the COMPLETE-session entry for `key`. Callers MUST only
  // call this on a COMPLETE download (cancel/error -> no entry). Re-storing the
  // same key updates the value and promotes it to MRU. Over budget -> evict LRU.
  void store(const SessionKey& key, CachedSession value) {
    auto map_it = index_.find(key.hash);
    if (map_it != index_.end()) {
      for (auto bit = map_it->second.begin(); bit != map_it->second.end(); ++bit) {
        EntryIter entry_it = *bit;
        if (entry_it->key == key) {
          entry_it->value = std::move(value);
          promoteLocked(entry_it);
          return;
        }
      }
    }
    entries_.push_front(Entry{key, std::move(value)});
    index_[key.hash].push_back(entries_.begin());
    if (entries_.size() > max_entries_) {
      evictLruLocked();
    }
  }

  // Drop the entry for `key` (if present). No-op otherwise.
  void evict(const SessionKey& key) {
    auto map_it = index_.find(key.hash);
    if (map_it == index_.end()) {
      return;
    }
    for (auto bit = map_it->second.begin(); bit != map_it->second.end(); ++bit) {
      EntryIter entry_it = *bit;
      if (entry_it->key == key) {
        eraseLocked(entry_it, map_it);
        return;
      }
    }
  }

  [[nodiscard]] std::size_t size() const { return entries_.size(); }
  [[nodiscard]] std::size_t maxEntries() const { return max_entries_; }

 private:
  struct Entry {
    SessionKey key;
    CachedSession value;
  };
  using EntryList = std::list<Entry>;
  using EntryIter = EntryList::iterator;

  // Move an entry (by list iterator) to the front of the LRU list, keeping the
  // bucket index iterators valid (splice does not invalidate iterators).
  void promoteLocked(EntryIter list_it) {
    if (list_it != entries_.begin()) {
      entries_.splice(entries_.begin(), entries_, list_it);
    }
  }

  // Erase the entry referenced by `list_it` (whose hash bucket is `map_it`),
  // removing the bucket back-reference too. After erase, map_it may be empty and
  // is dropped.
  void eraseLocked(EntryIter list_it, std::unordered_map<std::uint64_t, std::list<EntryIter>>::iterator map_it) {
    auto& bucket = map_it->second;
    for (auto bit = bucket.begin(); bit != bucket.end(); ++bit) {
      if (*bit == list_it) {
        bucket.erase(bit);
        break;
      }
    }
    if (bucket.empty()) {
      index_.erase(map_it);
    }
    entries_.erase(list_it);
  }

  // Evict the least-recently-used entry (the back of the list).
  void evictLruLocked() {
    if (entries_.empty()) {
      return;
    }
    EntryIter lru = std::prev(entries_.end());
    auto map_it = index_.find(lru->key.hash);
    if (map_it != index_.end()) {
      eraseLocked(lru, map_it);
    } else {
      entries_.erase(lru);  // defensive: index drift cannot happen, but never leak
    }
  }

  std::size_t max_entries_;
  EntryList entries_;  // front = MRU, back = LRU
  std::unordered_map<std::uint64_t, std::list<EntryIter>> index_;  // hash -> entries with that hash
};

}  // namespace dexory_cloud
