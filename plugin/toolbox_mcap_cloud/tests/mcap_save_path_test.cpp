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
  // `.mcap.partial` order: a truncated file must never carry a final `.mcap`
  // suffix that extension-gated pickers would present as loadable.
  EXPECT_EQ(paths->partial_path.filename(), "drive_download_20260726T120000Z.mcap.partial");
  // The partial is RESERVED (exclusively created) at allocation time.
  EXPECT_TRUE(fs::exists(paths->partial_path));
  EXPECT_FALSE(fs::exists(paths->final_path));
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
  std::ofstream(temp.path() / "drive_download_20260726T120000Z_2.mcap.partial").put('x');

  std::string error;
  const auto paths = mcap_cloud::prepareMcapOutputPaths(
      temp.path(), {"drive.mcap"}, "20260726T120000Z", &error);
  ASSERT_TRUE(paths.has_value()) << error;

  EXPECT_EQ(paths->final_path.filename(), "drive_download_20260726T120000Z_3.mcap");
  EXPECT_EQ(paths->partial_path.filename(), "drive_download_20260726T120000Z_3.mcap.partial");
}

TEST(McapSavePath, ReservationSerializesConcurrentAllocators) {
  // Two prepares with the SAME stamp (two processes racing in one second):
  // the first's exclusive reservation forces the second onto a fresh
  // candidate — no shared name, no silent truncation of the winner's file.
  TempDirectory temp;
  std::string error;
  const auto first = mcap_cloud::prepareMcapOutputPaths(
      temp.path(), {"drive.mcap"}, "20260726T120000Z", &error);
  ASSERT_TRUE(first.has_value()) << error;
  const auto second = mcap_cloud::prepareMcapOutputPaths(
      temp.path(), {"drive.mcap"}, "20260726T120000Z", &error);
  ASSERT_TRUE(second.has_value()) << error;

  EXPECT_NE(first->partial_path, second->partial_path);
  EXPECT_NE(first->final_path, second->final_path);
  EXPECT_TRUE(fs::exists(first->partial_path));
  EXPECT_TRUE(fs::exists(second->partial_path));
  EXPECT_EQ(second->final_path.filename(), "drive_download_20260726T120000Z_2.mcap");
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
