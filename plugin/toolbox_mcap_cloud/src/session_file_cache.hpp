// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Request-addressed session file cache core (spec
// docs/canonical-layout-import.md §5): identity->path mapping, cross-process
// exclusive materialization locking, validated atomic finalization, and
// orphan/LRU maintenance. Deliberately consumer-free in stage 1 — the fetch
// tee re-target and the host adoption flow (stage 4) plug into exactly this
// surface.
#pragma once
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

#include "core/file_lock.h"

namespace mcap_cloud {

/// Request-addressed session cache (spec docs/canonical-layout-import.md §5).
/// Files: <root>/<128-bit-hex>.mcap; partials: <name>.mcap.partial.<pid>;
/// sidecars: <name>.mcap.lock (materialize/evict mutual exclusion, and the
/// stage-4 lease point) and <name>.mcap.touch (LRU stamp — atime is
/// unreliable under relatime/noatime, so hits touch explicitly).
/// Root: MCAP_CLOUD_CACHE_DIR || $XDG_CACHE_HOME/mcap_cloud/sessions ||
/// ~/.cache/mcap_cloud/sessions. Directory 0700, files 0600.
class SessionFileCache {
 public:
  explicit SessionFileCache(std::filesystem::path root);   // tests inject
  static SessionFileCache standard(std::string* error);    // env resolution

  struct Config {
    std::uintmax_t max_total_bytes = 20ull * 1024 * 1024 * 1024;  // 20 GiB
    std::uintmax_t min_free_bytes = 2ull * 1024 * 1024 * 1024;    // reserve
    std::chrono::hours orphan_partial_age{24};
  };

  /// Exclusive per-identity materialization guard (RAII; non-blocking).
  /// Holds the identity's sidecar .lock for its lifetime — finalize, orphan
  /// cleanup and eviction all contend on the same lock file.
  class MaterializeLock {
   public:
    MaterializeLock(MaterializeLock&&) noexcept = default;
    MaterializeLock& operator=(MaterializeLock&&) noexcept = default;

   private:
    friend class SessionFileCache;
    MaterializeLock(FileLock lock, std::string hex, std::filesystem::path partial);

    FileLock lock_;
    std::string hex_;  // the identity's 32-char lowercase-hex digest
    std::filesystem::path partial_;
  };

  /// identity = the full "mcap-cloud:v1:sha256/128:<hex>" string (validated).
  /// Returns an empty path for anything malformed — a bad identity can never
  /// name a file outside the root.
  [[nodiscard]] std::filesystem::path pathFor(std::string_view identity) const;
  /// Existing + structurally valid: footer magic + summary readable +
  /// Statistics present + embedded descriptor provenance PRESENT and matching
  /// the identity. The provenance requirement is deliberate (review-caught):
  /// every legitimately finalized cache file carries it, so requiring it here
  /// has zero false negatives — and without it any unrelated valid MCAP
  /// dropped at <digest>.mcap (e.g. under an overridden MCAP_CLOUD_CACHE_DIR)
  /// would classify as a hit for that request. Touches the LRU stamp on hit.
  /// NO network, bounded I/O (footer + summary only). A failed check is a
  /// plain miss — the file is NOT deleted here; re-materialization atomically
  /// renames over it (deletion policy is the provider flow's).
  [[nodiscard]] bool lookup(std::string_view identity, std::filesystem::path* out);

  /// `contended` (optional out, adversarial F9): true iff the failure is the
  /// lock being held by ANOTHER holder (any process — retry-able), false for
  /// OS/identity errors.
  [[nodiscard]] std::optional<MaterializeLock> tryLockForMaterialize(
      std::string_view identity, std::string* error, bool* contended = nullptr);


  /// Adversarial F2: convert a finalized identity's EXCLUSIVE materialize
  /// lock into a SHARED read lease WITHOUT closing the underlying handle —
  /// the exclusive->shared handoff that keeps another process's cleanup from
  /// unlinking a just-published file that live datasets will lazily re-open.
  /// The platform conversion has a microscopic non-atomic window (see
  /// FileLock::downgradeToShared); on failure nullopt is returned with the
  /// lock RELEASED — the caller revalidates and proceeds lease-less. Call
  /// only AFTER finalize() succeeded.
  [[nodiscard]] static std::optional<FileLock> toSharedLease(MaterializeLock&& lock,
                                                             std::string* error);
  /// The partial path this process must write under `lock`.
  [[nodiscard]] std::filesystem::path partialPathFor(const MaterializeLock& lock) const;

  /// Semantic-completeness pin for finalize: the counts the PRODUCER knows the
  /// finished session must contain (the tee's EOS message total and the
  /// session's channel count). Statistics in the file must match exactly —
  /// a cleanly-closed writer over a PREFIX of a stream produces a structurally
  /// valid MCAP that only this check can reject (review-caught).
  struct ExpectedContent {
    std::uint64_t message_count = 0;
    std::uint64_t channel_count = 0;
  };

  /// Validate the finished partial (reader: footer, summary, Statistics,
  /// embedded descriptor == identity, and — when `expected` is provided —
  /// Statistics matching the expected message/channel counts), fsync file,
  /// atomic rename to pathFor(identity), fsync directory (POSIX). On any
  /// failure: remove the partial, return false with the reason.
  /// Pass `expected` whenever the producer knows the counts (the stage-4 tee
  /// always does — its EOS carries them); std::nullopt skips ONLY the
  /// semantic-completeness comparison, never the structural/identity checks.
  [[nodiscard]] bool finalize(const MaterializeLock& lock,
                              const std::optional<ExpectedContent>& expected, std::string* error);

  /// SHARED read lease on `identity`'s lock sidecar (spec §5: every live
  /// cache-backed consumer holds one for the file's whole lifetime — Linux
  /// unlink-while-open does NOT protect lazy re-opens). While any lease is
  /// live, cleanup()/eviction and tryLockForMaterialize (both exclusive on
  /// the same sidecar) fail/skip; leases stack across holders. Pair with
  /// lookup() — this takes no position on whether the cache file exists.
  [[nodiscard]] std::optional<FileLock> acquireReadLease(std::string_view identity,
                                                         std::string* error);

  /// Round-3 F4 test probe: counts how many times lookup()'s validation
  /// reached the SDK's ReadRecord() for the embedded descriptor. The raw
  /// preflight's whole purpose is that a forged record never gets there, and
  /// the SDK's own InvalidRecord rejection is indistinguishable from the
  /// preflight's by return value alone — this counter makes "the parser was
  /// not reached" observable, so removing the preflight turns the forge test
  /// red. Test-only; never read by production code.
  static std::uint64_t readRecordCallsForTest();
  static void resetReadRecordCallsForTest();

  /// Startup/maintenance: remove orphaned partials older than
  /// orphan_partial_age whose lock is free; then LRU-evict unlocked files
  /// (touch-file order) until under max_total_bytes AND min_free_bytes holds.
  /// Skips any file whose identity lock is busy — a live materialization OR a
  /// shared read lease (acquireReadLease) both hold that lock.
  void cleanup(const Config& cfg);

 private:
  std::filesystem::path root_;
};

}  // namespace mcap_cloud
