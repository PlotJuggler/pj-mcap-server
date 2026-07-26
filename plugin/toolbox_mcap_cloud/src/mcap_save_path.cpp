// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "mcap_save_path.hpp"

#include <chrono>
#include <cctype>
#include <ctime>
#include <system_error>

#include <pj_base/sdk/platform.hpp>

namespace mcap_cloud {
namespace {

std::string portableStem(const std::string& sequence_name) {
  std::string stem = std::filesystem::path(sequence_name).filename().stem().string();
  if (stem.empty()) {
    stem = "cloud_download";
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

std::string defaultMcapSaveDirectory() {
  return (PJ::sdk::userDataDir() / "mcap_cloud" / "downloads").string();
}

std::string utcTimestampForFilename() {
  const std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::tm utc{};
#if defined(_WIN32)
  gmtime_s(&utc, &now);
#else
  gmtime_r(&now, &utc);
#endif
  char text[32] = {};
  (void)std::strftime(text, sizeof(text), "%Y%m%dT%H%M%SZ", &utc);
  return text;
}

bool prepareMcapOutputPaths(
    const std::filesystem::path& directory, const std::vector<std::string>& sequence_names,
    const std::string& utc_stamp, McapOutputPaths* paths, std::string* error) {
  auto fail = [error](std::string message) {
    if (error != nullptr) {
      *error = std::move(message);
    }
    return false;
  };
  if (paths == nullptr) {
    return fail("internal error: output path result is null");
  }
  if (directory.empty()) {
    return fail("save directory is empty");
  }

  std::error_code ec;
  const bool directory_exists = std::filesystem::exists(directory, ec);
  if (ec) {
    return fail("could not inspect save directory '" + directory.string() + "': " + ec.message());
  }
  if (directory_exists && !std::filesystem::is_directory(directory, ec)) {
    return fail("save path is not a directory: '" + directory.string() + "'");
  }
  if (ec) {
    return fail("could not inspect save directory '" + directory.string() + "': " + ec.message());
  }
  std::filesystem::create_directories(directory, ec);
  if (ec) {
    return fail("could not create save directory '" + directory.string() + "': " + ec.message());
  }
  if (!std::filesystem::is_directory(directory, ec) || ec) {
    return fail("save path is not a directory: '" + directory.string() + "'");
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
    ec.clear();
    const bool final_exists = std::filesystem::exists(result.final_path, ec);
    if (ec) {
      return fail("could not inspect output path '" + result.final_path.string() + "': " + ec.message());
    }
    ec.clear();
    const bool partial_exists = std::filesystem::exists(result.partial_path, ec);
    if (ec) {
      return fail("could not inspect output path '" + result.partial_path.string() + "': " + ec.message());
    }
    if (!final_exists && !partial_exists) {
      *paths = std::move(result);
      if (error != nullptr) {
        error->clear();
      }
      return true;
    }
  }
}

}  // namespace mcap_cloud
