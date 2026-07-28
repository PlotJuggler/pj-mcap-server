// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC pin of the export-result contract on the early-abort path: a pull
// that dies before any transport (worker never connected) must still emit
// EXACTLY ONE McapSaveResult — Skipped, no path — release its reserved
// partial, and fire it BEFORE allFetchesComplete. No network, no host.
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "fetch_worker.hpp"

namespace fs = std::filesystem;

TEST(FetchWorkerExportContract, EarlyAbortEmitsExactlyOneSkippedAndCleansReservation) {
  const fs::path dir = fs::temp_directory_path() / "mcap-export-contract-test";
  std::error_code ec;
  fs::remove_all(dir, ec);

  mcap_cloud::FetchWorker worker;
  std::vector<mcap_cloud::McapSaveResult> results;
  bool all_complete = false;
  bool result_before_complete = false;
  worker.mcapSaveFinished = [&](mcap_cloud::McapSaveResult r) {
    results.push_back(std::move(r));
    result_before_complete = !all_complete;
  };
  worker.allFetchesComplete = [&](std::string) { all_complete = true; };

  // Never connected: the pull aborts before any transport is constructed.
  // pullTopicsAsync runs synchronously on the calling thread.
  worker.pullTopicsAsync({"seq.mcap"}, "seq.mcap", {"/topic"}, 0, 0, dir.string());

  EXPECT_TRUE(all_complete);
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].status, mcap_cloud::McapSaveStatus::Skipped);
  EXPECT_TRUE(results[0].path.empty());
  EXPECT_TRUE(result_before_complete);
  // The reserved `.mcap.partial` was released — only the created directory
  // remains, empty.
  ASSERT_TRUE(fs::is_directory(dir));
  EXPECT_TRUE(fs::is_empty(dir));
  fs::remove_all(dir, ec);
}

TEST(FetchWorkerExportContract, NoExportRequestedEmitsNoResult) {
  mcap_cloud::FetchWorker worker;
  std::vector<mcap_cloud::McapSaveResult> results;
  worker.mcapSaveFinished = [&](mcap_cloud::McapSaveResult r) { results.push_back(std::move(r)); };

  worker.pullTopicsAsync({"seq.mcap"}, "seq.mcap", {"/topic"}, 0, 0, /*save_directory=*/{});

  EXPECT_TRUE(results.empty());
}
