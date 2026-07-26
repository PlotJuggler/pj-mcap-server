// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace mcap_cloud {

struct McapOutputPaths {
  std::filesystem::path final_path;
  std::filesystem::path partial_path;
};

/// Cross-platform default used by the dialog on first launch.
[[nodiscard]] std::string defaultMcapSaveDirectory();

/// UTC wall-clock stamp suitable for a portable filename: YYYYMMDDTHHMMSSZ.
[[nodiscard]] std::string utcTimestampForFilename();

/// Create `directory` when needed and allocate collision-free final/partial
/// paths for one download. `utc_stamp` is injected so naming is deterministic
/// in tests.
[[nodiscard]] bool prepareMcapOutputPaths(
    const std::filesystem::path& directory, const std::vector<std::string>& sequence_names,
    const std::string& utc_stamp, McapOutputPaths* paths, std::string* error);

}  // namespace mcap_cloud
