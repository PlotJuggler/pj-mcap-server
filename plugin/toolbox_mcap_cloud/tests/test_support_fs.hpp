// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Shared filesystem test support (PR-2 quality review): the RAII temp-dir
// helper previously copy-pasted as `struct TempRoot` across five test files.
// Each test file keeps a thin `TempRoot` shim deriving from this base with
// its own per-suite prefix, so directory names (and therefore behavior) stay
// byte-identical to the pre-hoist copies.
#pragma once

#include <filesystem>
#include <string>
#include <system_error>

namespace mcap_cloud_test {

// RAII temp directory under the system temp root: recreated FRESH at
// construction (a stale dir from a crashed prior run is wiped), removed at
// destruction. `dir_name` is the full leaf name — callers derive per-suite
// uniqueness by prefixing (see the TempRoot shims in each test file).
//
// TODO: mkdtemp-style uniqueness would allow concurrent runs of the SAME
// test binary; it is not a trivial drop-in because the deterministic name is
// what makes the construction-time wipe of a crashed run's leftovers work.
struct ScopedTempDir {
  explicit ScopedTempDir(const std::string& dir_name) {
    path = std::filesystem::temp_directory_path() / dir_name;
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    std::filesystem::create_directories(path);
  }
  ~ScopedTempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
  ScopedTempDir(const ScopedTempDir&) = delete;
  ScopedTempDir& operator=(const ScopedTempDir&) = delete;

  std::filesystem::path path;
};

}  // namespace mcap_cloud_test
