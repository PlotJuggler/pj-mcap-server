// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// SessionCache — the in-memory, toolbox-adapted session cache (Plan D Task 5
// semantics, ADAPTED to the toolbox shape). After a COMPLETE Fetch the decoded
// scalars already live in the PJ4 datastore under the group dataset. So a
// cache HIT does NOT re-materialize bytes — it re-emits the per-topic
// pullFinished ledger from cached counts and skips ALL transport. The
// datastore owns the actual memory; this cache stores only counts metadata
// (plus, stage 4: the durable-cache/promotion state the memory-hit rules and
// the descriptor-import provider query — spec docs/canonical-layout-import.md
// §6.1/§13-step-4).
//
// KEY: SessionKey over the EXACT logical selection requested
// (server_uri, sequence_names[], topics[], time_range) — sequence_names are the
// stable s3 keys (SequenceInfo.name), NOT the wire file_ids sent in
// OpenSessionParams (those renumber across a post-M6 catalog builder rebuild;
// see session_key.hpp for why). Exact-tuple only: a different time-range or
// topic set is a MISS; reordered inputs collide (HIT).
//
// EXISTENCE VERIFICATION (the toolbox adaptation, D7-amended): a HIT
// additionally requires that the cached dataset is STILL present in the host
// catalog. The caller injects an existence predicate over the WHOLE
// CachedSession — existence keys on the stable `dataset_id` (display names
// collide and mutate; D7), with the recorded display name as a recycle
// tiebreak. If presence cannot be verified (the host lacks
// acquire_catalog_snapshot) the predicate returns false and the entry is
// treated as a MISS (presence-unknown -> MISS, never a false HIT). A
// present-but-absent entry is evicted so the next fetch re-fills it.
//
// STORE: COMPLETE-only. cancel / error / no-terminal-Eos -> NO entry (no
// half-cached state). LRU over a small ENTRY-COUNT budget (the datastore owns
// the bytes; bounding by entry count, not memory, is the right adaptation).
//
// THREADING (D7-amended): the cache is shared per toolbox instance through
// ImportRuntime — the interactive worker thread and future provider job
// threads all touch it — so every public method locks an internal mutex.
// The internal mutex is a TRUE leaf lock by construction: lookup() runs the
// existence predicate OUTSIDE it (copy entry -> unlock -> run predicate ->
// relock; the predicate may acquire a host catalog snapshot), so predicate
// code can never deadlock against the cache or another lock the host holds.
// The pre-stage-4 single-worker usage still works unchanged.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include "session_key.hpp"

namespace mcap_cloud {

// The SessionKey type lives in PJ::cloud (Plan B Task 8a); bring it into scope so
// the cache members read cleanly.
using PJ::cloud::SessionKey;

// Outcome of the single-encoder cache tee for one session (spec §9.6: a tee
// failure never aborts the ingest — it only suppresses promotion). Recorded
// on the stored entry AND reported through FetchWorker::teeFinished.
enum class TeeOutcome : std::uint8_t {
  kNone = 0,       // no cache tee ran (no ImportRuntime bound / pull aborted before the tee)
  kFinalized,      // cache file validated + atomically published
  kExistingValid,  // memory hit served with an already-valid disk cache file
  kFailed,         // tee open/write/finalize failure — no cache file exists
  kAborted,        // cancel/incomplete download — cache partial deleted (spec §10)
};

// Which spec §6.1 memory-hit rule last applied to this entry.
enum class MemoryHitCase : std::uint8_t {
  kNone = 0,
  kServedValidDisk,    // memory hit + valid disk file (any export satisfied by copy)
  kRefetchedDiskMiss,  // memory hit whose disk file was missing/invalid: evicted + refetched
};

// The cached metadata for ONE completed session. Only counts + identity — never
// the decoded rows (those live in the datastore under display_name).
struct CachedSession {
  std::string display_name;  // the host dataset/group name (recycle tiebreak for existence)
  std::string server_uri;    // the connection this was fetched over (also in the key)
  std::unordered_map<std::string, std::uint64_t> counts_by_topic;  // per-topic appendRecord counts
  std::uint64_t total_messages = 0;
  // ---- stage-4 (D7) durable-cache/promotion state --------------------------
  std::uint32_t dataset_id = 0;   // stable host DataSourceHandle id (0 = unknown/legacy)
  std::string cache_identity;     // descriptor identity of the durable cache file ("" = none)
  TeeOutcome tee_outcome = TeeOutcome::kNone;
  MemoryHitCase last_hit_case = MemoryHitCase::kNone;
};

// LRU-by-entry-count cache keyed on SessionKey.hash with full-key equality to
// defeat hash collisions. Default budget kMaxEntries=8 (small; the datastore
// owns the real memory). Thread-safe (see the THREADING note above).
class SessionCache {
 public:
  // Predicate that answers "is this dataset still present in the host
  // catalog?" over the full cached entry (key on entry.dataset_id, D7).
  // Presence-unknown MUST return false. Runs OUTSIDE the cache's internal
  // lock (on a copy of the entry) — it may call into the host freely.
  using ExistencePredicate = std::function<bool(const CachedSession& entry)>;

  static constexpr std::size_t kDefaultMaxEntries = 8;

  explicit SessionCache(std::size_t max_entries = kDefaultMaxEntries) : max_entries_(max_entries ? max_entries : 1) {}

  // Look up a HIT for `key`. Returns the cached metadata ONLY when an entry with
  // the same full key exists AND `exists(entry)` is true. A matching entry
  // whose dataset is gone (exists == false) is EVICTED and nullopt is returned
  // (so the caller falls through to a normal fetch). On a real HIT the entry is
  // moved to the front (most-recently-used). A null predicate means
  // presence-unknown -> treated as MISS (and the stale entry is NOT evicted, since
  // we cannot prove it gone).
  [[nodiscard]] std::optional<CachedSession> lookup(const SessionKey& key, const ExistencePredicate& exists) {
    // Phase 1 (locked): find the full-key match and COPY it out.
    std::optional<CachedSession> candidate;
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (EntryIter entry_it; findLocked(key, &entry_it)) {
        candidate = entry_it->value;
      }
    }
    if (!candidate.has_value()) {
      return std::nullopt;
    }
    // Phase 2 (UNLOCKED): run the predicate on the copy — it may call into
    // the host (catalog snapshot); the cache mutex stays a leaf lock.
    const bool present = exists ? exists(*candidate) : false;
    // Phase 3 (relocked): act on the verdict against the CURRENT state — a
    // racing store/evict may have changed the entry while we were unlocked.
    std::lock_guard<std::mutex> lock(mu_);
    EntryIter entry_it;
    if (!findLocked(key, &entry_it)) {
      return std::nullopt;  // concurrently evicted either way -> MISS
    }
    if (!present) {
      if (exists && entry_it->value.dataset_id == candidate->dataset_id &&
          entry_it->value.display_name == candidate->display_name) {
        // Predicate ran and said "gone" -> evict, but ONLY the entry we
        // actually validated: a racing re-store may have replaced it with a
        // FRESH dataset this verdict knows nothing about.
        eraseLocked(entry_it, index_.find(key.hash));
      }
      return std::nullopt;  // presence-unknown OR proven-gone -> MISS
    }
    // Real HIT: promote to MRU and return a copy of the CURRENT metadata.
    promoteLocked(entry_it);
    return entries_.front().value;
  }

  // Store (or refresh) the COMPLETE-session entry for `key`. Callers MUST only
  // call this on a COMPLETE download (cancel/error -> no entry). Re-storing the
  // same key updates the value and promotes it to MRU. Over budget -> evict LRU.
  void store(const SessionKey& key, CachedSession value) {
    std::lock_guard<std::mutex> lock(mu_);
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
    std::lock_guard<std::mutex> lock(mu_);
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

  // Record which §6.1 memory-hit rule last applied to `key`'s entry (no LRU
  // effect — the triggering lookup already promoted it). No-op when absent.
  void recordHitCase(const SessionKey& key, MemoryHitCase hit_case) {
    std::lock_guard<std::mutex> lock(mu_);
    auto map_it = index_.find(key.hash);
    if (map_it == index_.end()) {
      return;
    }
    for (EntryIter entry_it : map_it->second) {
      if (entry_it->key == key) {
        entry_it->value.last_hit_case = hit_case;
        return;
      }
    }
  }

  [[nodiscard]] std::size_t size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return entries_.size();
  }
  [[nodiscard]] std::size_t maxEntries() const { return max_entries_; }

 private:
  struct Entry {
    SessionKey key;
    CachedSession value;
  };
  using EntryList = std::list<Entry>;
  using EntryIter = EntryList::iterator;

  // Locate the full-key entry (hash bucket scan + full-key equality). Caller
  // holds mu_. Returns false when absent.
  [[nodiscard]] bool findLocked(const SessionKey& key, EntryIter* out) {
    auto map_it = index_.find(key.hash);
    if (map_it == index_.end()) {
      return false;
    }
    for (EntryIter entry_it : map_it->second) {
      if (entry_it->key == key) {
        *out = entry_it;
        return true;
      }
    }
    return false;
  }

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

  mutable std::mutex mu_;
  std::size_t max_entries_;
  EntryList entries_;  // front = MRU, back = LRU
  std::unordered_map<std::uint64_t, std::list<EntryIter>> index_;  // hash -> entries with that hash
};

}  // namespace mcap_cloud
