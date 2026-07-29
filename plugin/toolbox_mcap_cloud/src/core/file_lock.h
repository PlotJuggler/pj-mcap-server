// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Minimal RAII advisory file lock over a dedicated sidecar lock file (POSIX
// flock / Windows LockFileEx), non-blocking try-acquire only. EXCLUSIVE mode
// is the whole surface today; SHARED leases (live cache-backed datasets
// pinning their file against eviction, spec docs/canonical-layout-import.md
// §5) are stage-4 scope and slot in as a second try-acquire entry point
// without changing existing callers.
#pragma once
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace mcap_cloud {

class FileLock {
 public:
  /// Non-blocking try-acquire of an exclusive advisory lock on `path`,
  /// creating the lock file 0600 if absent. Returns nullopt with `*error`
  /// set when the lock is held elsewhere (any process, INCLUDING this one via
  /// a different FileLock) or when the OS call fails. The lock file itself is
  /// never deleted: unlinking a path another process may be about to open
  /// would hand out two "exclusive" locks on different inodes.
  [[nodiscard]] static std::optional<FileLock> tryExclusive(
      const std::filesystem::path& path, std::string* error);

  FileLock(FileLock&& other) noexcept;
  FileLock& operator=(FileLock&& other) noexcept;
  FileLock(const FileLock&) = delete;
  FileLock& operator=(const FileLock&) = delete;
  /// Releases the lock: close(2) drops the flock; UnlockFileEx + CloseHandle
  /// on Windows.
  ~FileLock();

 private:
  explicit FileLock(std::intptr_t handle) : handle_(handle) {}
  void release();

  // POSIX fd / Windows HANDLE. -1 = released/moved-from (INVALID_HANDLE_VALUE
  // is (HANDLE)-1, so one sentinel serves both).
  std::intptr_t handle_ = -1;
};

}  // namespace mcap_cloud
