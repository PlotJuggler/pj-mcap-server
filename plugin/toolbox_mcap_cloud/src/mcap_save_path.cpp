// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "mcap_save_path.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <system_error>

#include "core/time_format.h"
#include "elide_name.h"

namespace mcap_cloud {
namespace {

std::string portableStem(const std::string& sequence_name) {
  // '/'-split like every other object-key consumer (see elide_name.h — a
  // std::filesystem::path would also split on '\' on Windows), then drop the
  // extension.
  std::string stem = baseName(sequence_name);
  if (const auto dot = stem.rfind('.'); dot != std::string::npos && dot > 0) {
    stem.resize(dot);
  }
  for (char& ch : stem) {
    const unsigned char uch = static_cast<unsigned char>(ch);
    if (!(std::isalnum(uch) != 0 || ch == '-' || ch == '_' || ch == '.')) {
      ch = '_';
    }
  }
  while (!stem.empty() && (stem.front() == '.' || stem.front() == '_')) {
    stem.erase(stem.begin());
  }
  while (!stem.empty() && (stem.back() == '.' || stem.back() == '_')) {
    stem.pop_back();
  }
  if (stem.empty()) {
    stem = "cloud_download";
  }
  constexpr std::size_t kMaxStemLength = 96;
  if (stem.size() > kMaxStemLength) {
    stem.resize(kMaxStemLength);
  }
  return stem;
}

}  // namespace

std::string utcTimestampForFilename() {
  const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  // formatIso8601Utc -> "YYYY-MM-DDTHH:MM:SS"; strip the separators for a
  // portable filename.
  std::string stamp = formatIso8601Utc(now_ns);
  stamp.erase(
      std::remove_if(stamp.begin(), stamp.end(), [](char c) { return c == '-' || c == ':'; }),
      stamp.end());
  return stamp + "Z";
}

std::optional<McapOutputPaths> prepareMcapOutputPaths(
    const std::filesystem::path& directory, const std::vector<std::string>& sequence_names,
    const std::string& utc_stamp, std::string* error) {
  auto fail = [error](std::string message) {
    if (error != nullptr) {
      *error = std::move(message);
    }
    return std::nullopt;
  };
  if (directory.empty()) {
    return fail("save directory is empty");
  }

  std::error_code ec;
  if (std::filesystem::exists(directory, ec) && !std::filesystem::is_directory(directory, ec)) {
    return fail("save path is not a directory: '" + directory.string() + "'");
  }
  std::filesystem::create_directories(directory, ec);  // no-op on an existing directory
  if (ec) {
    return fail("could not create save directory '" + directory.string() + "': " + ec.message());
  }

  std::string stem = portableStem(sequence_names.empty() ? std::string{} : sequence_names.front());
  if (sequence_names.size() > 1) {
    stem += "_plus_" + std::to_string(sequence_names.size() - 1);
  }
  stem += "_download_" + (utc_stamp.empty() ? std::string("unknown_time") : utc_stamp);

  for (std::size_t attempt = 1;; ++attempt) {
    const std::string candidate = stem + (attempt == 1 ? std::string{} : "_" + std::to_string(attempt));
    McapOutputPaths result{
        .final_path = directory / (candidate + ".mcap"),
        .partial_path = directory / (candidate + ".partial.mcap"),
    };
    bool taken = false;
    for (const auto* path : {&result.final_path, &result.partial_path}) {
      ec.clear();
      if (std::filesystem::exists(*path, ec)) {
        taken = true;
      }
      if (ec) {
        return fail("could not inspect output path '" + path->string() + "': " + ec.message());
      }
    }
    if (!taken) {
      return result;
    }
  }
}

}  // namespace mcap_cloud
