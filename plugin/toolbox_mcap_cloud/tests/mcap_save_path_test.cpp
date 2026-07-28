// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "mcap_save_path.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace {

namespace fs = std::filesystem;

class TempDirectory {
 public:
  TempDirectory() {
    path_ = fs::temp_directory_path() / ("mcap-save-path-test-" + std::to_string(++counter_));
    std::error_code ec;
    fs::remove_all(path_, ec);
  }
  ~TempDirectory() {
    std::error_code ec;
    fs::remove_all(path_, ec);
  }
  const fs::path& path() const {
    return path_;
  }

 private:
  inline static unsigned counter_ = 0;
  fs::path path_;
};

}  // namespace

TEST(McapSavePath, CreatesDirectoryAndSingleFileName) {
  TempDirectory temp;
  std::string error;
  const auto paths = mcap_cloud::prepareMcapOutputPaths(
      temp.path() / "nested", {"customer=x/date=2026-01-01/drive.mcap"},
      "20260726T120000Z", &error);
  ASSERT_TRUE(paths.has_value()) << error;

  EXPECT_TRUE(fs::is_directory(temp.path() / "nested"));
  EXPECT_EQ(paths->final_path.filename(), "drive_download_20260726T120000Z.mcap");
  EXPECT_EQ(paths->partial_path.filename(), "drive_download_20260726T120000Z.partial.mcap");
}

TEST(McapSavePath, SanitizesAndNamesStitchedSelection) {
  TempDirectory temp;
  std::string error;
  const auto paths = mcap_cloud::prepareMcapOutputPaths(
      temp.path(), {"dir/a bad:name.mcap", "b.mcap", "c.mcap"},
      "20260726T120000Z", &error);
  ASSERT_TRUE(paths.has_value()) << error;

  EXPECT_EQ(paths->final_path.filename(), "a_bad_name_plus_2_download_20260726T120000Z.mcap");
}

TEST(McapSavePath, AvoidsFinalAndPartialCollisions) {
  TempDirectory temp;
  fs::create_directories(temp.path());
  std::ofstream(temp.path() / "drive_download_20260726T120000Z.mcap").put('x');
  std::ofstream(temp.path() / "drive_download_20260726T120000Z_2.partial.mcap").put('x');

  std::string error;
  const auto paths = mcap_cloud::prepareMcapOutputPaths(
      temp.path(), {"drive.mcap"}, "20260726T120000Z", &error);
  ASSERT_TRUE(paths.has_value()) << error;

  EXPECT_EQ(paths->final_path.filename(), "drive_download_20260726T120000Z_3.mcap");
  EXPECT_EQ(paths->partial_path.filename(), "drive_download_20260726T120000Z_3.partial.mcap");
}

TEST(McapSavePath, RejectsAFileAsDirectory) {
  TempDirectory temp;
  fs::create_directories(temp.path());
  const fs::path not_directory = temp.path() / "plain-file";
  std::ofstream(not_directory).put('x');

  std::string error;
  EXPECT_FALSE(mcap_cloud::prepareMcapOutputPaths(
                   not_directory, {"drive.mcap"}, "20260726T120000Z", &error)
                   .has_value());
  EXPECT_NE(error.find("not a directory"), std::string::npos);
}
