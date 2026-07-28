// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace mcap_cloud {

struct McapOutputPaths {
  std::filesystem::path final_path;
  std::filesystem::path partial_path;
};

/// UTC wall-clock stamp suitable for a portable filename: YYYYMMDDTHHMMSSZ.
[[nodiscard]] std::string utcTimestampForFilename();

/// Create `directory` when needed and allocate collision-free final/partial
/// paths for one download. `utc_stamp` is injected so naming is deterministic
/// in tests. Returns nullopt on failure; `*error` is only meaningful then.
[[nodiscard]] std::optional<McapOutputPaths> prepareMcapOutputPaths(
    const std::filesystem::path& directory, const std::vector<std::string>& sequence_names,
    const std::string& utc_stamp, std::string* error);

}  // namespace mcap_cloud
