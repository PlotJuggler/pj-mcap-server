// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Trusted-origin ledger (spec docs/canonical-layout-import.md §7 guard 1):
// an origin is trusted for auto-import iff it completed a SUCCESSFUL
// interactive Hello on this machine. Deliberately NOT the credential store —
// credentials may be saved before any successful connect, so the two must be
// separate files with separate lifecycles.
//
// These tests are HERMETIC: every TrustedOrigins is rooted at a unique temp
// directory (NOT the real XDG path), so they never touch the user's real
// ledger and leave no residue (TearDown removes the tree).

#include "trusted_origins.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <atomic>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

// A TrustedOrigins ledger rooted at a private temp dir, removed on teardown.
class TrustedOriginsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::error_code ec;
    root_ = fs::temp_directory_path() /
            fs::path("globex_trust_test_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
                     std::to_string(reinterpret_cast<std::uintptr_t>(this)));
    fs::remove_all(root_, ec);
  }
  void TearDown() override {
    std::error_code ec;
    fs::remove_all(root_, ec);
  }

  mcap_cloud::TrustedOrigins ledger() { return mcap_cloud::TrustedOrigins(root_); }

  // The one file the ledger writes under root_ (empty path when none exists).
  fs::path writtenFile() {
    std::error_code ec;
    if (!fs::exists(root_, ec) || ec) {
      return {};
    }
    for (const auto& entry : fs::recursive_directory_iterator(root_)) {
      if (entry.is_regular_file()) {
        return entry.path();
      }
    }
    return {};
  }

  fs::path root_;
};

}  // namespace

// A recorded origin is trusted; an unrecorded one is not.
TEST_F(TrustedOriginsTest, RecordedIsTrustedUnrecordedIsNot) {
  auto t = ledger();
  ASSERT_TRUE(t.recordSuccessfulHello("ws://host:8080"));
  EXPECT_TRUE(t.isTrusted("ws://host:8080"));
  EXPECT_FALSE(t.isTrusted("ws://other:8080"));
  EXPECT_FALSE(t.isTrusted("ws://host:9090"));  // different port = different origin
}

// The ledger compares ORIGINS, not raw strings: the serialized form carries
// the EFFECTIVE port, so default-port spellings of one origin collide.
TEST_F(TrustedOriginsTest, DefaultPortVariantsAreOneOrigin) {
  auto t = ledger();
  ASSERT_TRUE(t.recordSuccessfulHello("wss://h"));
  EXPECT_TRUE(t.isTrusted("wss://h:443"));
  EXPECT_TRUE(t.isTrusted("wss://h"));
  // Path/case variants of the same origin also match (parseWsOrigin rules).
  EXPECT_TRUE(t.isTrusted("wss://H:443/some/path"));
  // A ws:// spelling of the same host:port is a DIFFERENT origin.
  EXPECT_FALSE(t.isTrusted("ws://h:443"));
}

// A fresh ledger instance over the SAME root sees a previously-recorded
// origin (persistence across "process restart").
TEST_F(TrustedOriginsTest, PersistsAcrossInstances) {
  ASSERT_TRUE(ledger().recordSuccessfulHello("ws://host:8080"));
  EXPECT_TRUE(ledger().isTrusted("ws://host:8080"));
}

// An unparsable uri is a clean no-op on record, and never trusted: rejected
// shapes (bad scheme, userinfo, query, fragment) fail closed even against
// themselves.
TEST_F(TrustedOriginsTest, UnparsableUriIsNoop) {
  auto t = ledger();
  EXPECT_NO_THROW(t.recordSuccessfulHello("http://h"));
  EXPECT_NO_THROW(t.recordSuccessfulHello("wss://user:pw@h"));
  EXPECT_NO_THROW(t.recordSuccessfulHello(""));
  EXPECT_FALSE(t.isTrusted("http://h"));
  EXPECT_FALSE(t.isTrusted("wss://user:pw@h"));
  // Nothing was recorded, so nothing was written to disk either.
  EXPECT_TRUE(writtenFile().empty());
}

// A corrupted ledger file is tolerated: isTrusted() reads it as empty rather
// than throwing, and a subsequent record recovers (same tolerance the
// credential store pins for its file).
TEST_F(TrustedOriginsTest, CorruptedLedgerIsTolerated) {
  ASSERT_TRUE(ledger().recordSuccessfulHello("ws://host:8080"));

  const fs::path written = writtenFile();
  ASSERT_FALSE(written.empty()) << "no ledger file was written under " << root_;
  {
    std::ofstream(written, std::ios::trunc) << "{ this is not valid json ]]]";
  }

  EXPECT_NO_THROW(EXPECT_FALSE(ledger().isTrusted("ws://host:8080")));

  // A fresh record replaces the corrupt content.
  ASSERT_TRUE(ledger().recordSuccessfulHello("ws://host:8080"));
  EXPECT_TRUE(ledger().isTrusted("ws://host:8080"));
}

// Multiple recorded origins coexist; re-recording one is idempotent.
TEST_F(TrustedOriginsTest, MultipleOriginsAndIdempotentRecord) {
  auto t = ledger();
  t.recordSuccessfulHello("ws://a:1");
  t.recordSuccessfulHello("wss://b:2");
  t.recordSuccessfulHello("ws://a:1");  // idempotent
  EXPECT_TRUE(t.isTrusted("ws://a:1"));
  EXPECT_TRUE(t.isTrusted("wss://b:2"));
}

// The ledger file is created with 0600 perms and its directory with 0700 —
// same owner-only discipline as the credential store. (POSIX only.)
TEST_F(TrustedOriginsTest, FilePermissionsAre0600) {
  ASSERT_TRUE(ledger().recordSuccessfulHello("ws://host:8080"));

  const fs::path written = writtenFile();
  ASSERT_FALSE(written.empty()) << "no ledger file was written under " << root_;

#if defined(__unix__) || defined(__APPLE__) || defined(__linux__)
  const fs::perms p = fs::status(written).permissions();
  EXPECT_EQ(p & fs::perms::owner_read, fs::perms::owner_read);
  EXPECT_EQ(p & fs::perms::owner_write, fs::perms::owner_write);
  EXPECT_EQ(p & fs::perms::group_all, fs::perms::none) << "ledger file is group-accessible";
  EXPECT_EQ(p & fs::perms::others_all, fs::perms::none) << "ledger file is world-accessible";

  const fs::perms dp = fs::status(written.parent_path()).permissions();
  EXPECT_EQ(dp & fs::perms::group_all, fs::perms::none) << "ledger dir is group-accessible";
  EXPECT_EQ(dp & fs::perms::others_all, fs::perms::none) << "ledger dir is world-accessible";
#endif
}

// Adversarial F14: concurrent writers over ONE ledger path (each its own
// TrustedOrigins instance — the multiprocess-equivalent for the file lock)
// must never lose an update: the read-modify-write is lock-serialized with a
// re-read under the lock.
TEST_F(TrustedOriginsTest, ConcurrentWritersLoseNoUpdates) {
  constexpr int kThreads = 4;
  constexpr int kPerThread = 8;
  std::vector<std::thread> writers;
  std::atomic<int> failures{0};
  for (int t = 0; t < kThreads; ++t) {
    writers.emplace_back([&, t]() {
      mcap_cloud::TrustedOrigins parallel_ledger(root_);
      for (int i = 0; i < kPerThread; ++i) {
        const std::string uri =
            "ws://host-" + std::to_string(t) + "-" + std::to_string(i) + ":8080";
        if (!parallel_ledger.recordSuccessfulHello(uri)) {
          failures.fetch_add(1);
        }
      }
    });
  }
  for (auto& w : writers) {
    w.join();
  }
  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(ledger().allOrigins().size(), static_cast<std::size_t>(kThreads * kPerThread))
      << "a lost update means the read-modify-write was not serialized (F14)";
}

// Adversarial F14: a write failure returns FALSE (durable-or-false). A
// read-only root cannot inject it (ensureDir0700 deliberately restores the
// perms) — squat a DIRECTORY on the ledger path instead: the atomic rename
// over a directory fails, and there is no direct-overwrite fallback left to
// paper over it.
TEST_F(TrustedOriginsTest, WriteFailureReturnsFalse) {
  auto t = ledger();
  ASSERT_TRUE(t.recordSuccessfulHello("ws://ok:1"));
  const fs::path ledger_file = writtenFile();
  ASSERT_FALSE(ledger_file.empty());
  std::error_code ec;
  fs::remove(ledger_file, ec);
  fs::create_directories(ledger_file / "squat", ec);
  const bool recorded = t.recordSuccessfulHello("ws://fails:2");
  EXPECT_FALSE(recorded) << "a failed durable write must report failure (F14)";
  EXPECT_FALSE(t.isTrusted("ws://fails:2"));
}

// Re-verify R5: durable-or-false includes the DIRECTORY fsync — a failed
// dir sync means the rename may not survive a crash, so the write reports
// failure and (through ImportRuntime) memory is not updated.
TEST_F(TrustedOriginsTest, DirectoryFsyncFailureReportsFalse) {
  auto t = ledger();
  mcap_cloud::testing::setTrustedOriginsDirSyncFailForTest(true);
  const bool recorded = t.recordSuccessfulHello("ws://dirsync:1");
  mcap_cloud::testing::setTrustedOriginsDirSyncFailForTest(false);
  EXPECT_FALSE(recorded) << "a failed directory fsync must report failure (R5)";
  ASSERT_TRUE(t.recordSuccessfulHello("ws://dirsync:1"))
      << "the same record must succeed once the dir fsync works again";
}
