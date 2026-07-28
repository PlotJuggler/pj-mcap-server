// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Request-addressed session file cache core (spec
// docs/canonical-layout-replay.md §5): identity->path mapping, cross-process
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

/// Request-addressed session cache (spec docs/canonical-layout-replay.md §5).
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
  /// Existing + structurally valid (footer magic + summary readable +
  /// embedded descriptor identity matches when present). Touches the LRU
  /// stamp on hit. NO network, bounded I/O (footer + summary only). A failed
  /// check is a plain miss — the file is NOT deleted here; re-materialization
  /// atomically renames over it (deletion policy is the provider flow's).
  [[nodiscard]] bool lookup(std::string_view identity, std::filesystem::path* out);

  [[nodiscard]] std::optional<MaterializeLock> tryLockForMaterialize(
      std::string_view identity, std::string* error);
  /// The partial path this process must write under `lock`.
  [[nodiscard]] std::filesystem::path partialPathFor(const MaterializeLock& lock) const;
  /// Validate the finished partial (reader: footer, summary, statistics,
  /// embedded descriptor == identity), fsync file, atomic rename to
  /// pathFor(identity), fsync directory (POSIX). On any failure: remove the
  /// partial, return false with the reason.
  [[nodiscard]] bool finalize(const MaterializeLock& lock, std::string* error);

  /// Startup/maintenance: remove orphaned partials older than
  /// orphan_partial_age whose lock is free; then LRU-evict unlocked files
  /// (touch-file order) until under max_total_bytes AND min_free_bytes holds.
  void cleanup(const Config& cfg);

 private:
  std::filesystem::path root_;
};

}  // namespace mcap_cloud
