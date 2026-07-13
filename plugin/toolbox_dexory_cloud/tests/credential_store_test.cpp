// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// D6 (Plan D Task 6) — CredentialStore seam + file-backed fallback.
//
// The seam stores the bearer token (the SECRET) per normalized server URL,
// keyed via normalizeServerKey() so "grpc+tls://X:6726", "GRPC+TLS://X:6726",
// and "x:6726" collide on one entry (PJ3 parity, mirrors the SettingsView
// store these tests replace). The default backend is a 0600-perm file under a
// configurable config root (production: $XDG_CONFIG_HOME/dexory_cloud); a
// libsecret backend is a later drop-in behind the same interface (CLAUDE.md
// grounding note 4; Plan D ASSUMPTION A3 authorizes the degraded fallback).
//
// These tests are HERMETIC: every FileCredentialStore is rooted at a unique
// temp directory (NOT the real XDG path), so they never touch the user's real
// credentials and leave no residue (TearDown removes the tree).

#include "credential_store.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

namespace fs = std::filesystem;

namespace {

// A FileCredentialStore rooted at a private temp dir, removed on teardown.
class FileCredentialStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::error_code ec;
    root_ = fs::temp_directory_path() /
            fs::path("dexory_cred_test_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
                     std::to_string(reinterpret_cast<std::uintptr_t>(this)));
    fs::remove_all(root_, ec);
  }
  void TearDown() override {
    std::error_code ec;
    fs::remove_all(root_, ec);
  }

  dexory_cloud::FileCredentialStore store() { return dexory_cloud::FileCredentialStore(root_); }

  fs::path root_;
};

}  // namespace

// Round-trip: set then get returns the stored token verbatim.
TEST_F(FileCredentialStoreTest, SetThenGetRoundTrip) {
  auto s = store();
  s.set("ws://localhost:8081", "secret-bearer-xyz");
  EXPECT_EQ(s.get("ws://localhost:8081"), "secret-bearer-xyz");
}

// A fresh store instance over the SAME root sees a previously-set token
// (persistence across "process restart").
TEST_F(FileCredentialStoreTest, PersistsAcrossInstances) {
  store().set("ws://host:9000", "tok-1");
  EXPECT_EQ(store().get("ws://host:9000"), "tok-1");
}

// get() on an absent server returns an empty optional (no token, not "").
TEST_F(FileCredentialStoreTest, MissingServerReturnsNullopt) {
  auto s = store();
  EXPECT_FALSE(s.get("ws://never-set:1").has_value());
}

// Keys normalize: scheme + case + trailing slash collide on one entry
// (normalizeServerKey parity).
TEST_F(FileCredentialStoreTest, KeysNormalizeAndCollide) {
  auto s = store();
  s.set("grpc+tls://Server.Example:6726", "tok-normalized");
  // Different scheme + different case + trailing slash -> same normalized key.
  EXPECT_EQ(s.get("grpc://server.example:6726/"), "tok-normalized");
  EXPECT_EQ(s.get("server.example:6726"), "tok-normalized");
}

// Distinct servers are isolated.
TEST_F(FileCredentialStoreTest, ServersAreIsolated) {
  auto s = store();
  s.set("ws://a:1", "tok-a");
  s.set("ws://b:2", "tok-b");
  EXPECT_EQ(s.get("ws://a:1"), "tok-a");
  EXPECT_EQ(s.get("ws://b:2"), "tok-b");
}

// set() overwrites a prior token for the same server.
TEST_F(FileCredentialStoreTest, SetOverwrites) {
  auto s = store();
  s.set("ws://h:1", "old");
  s.set("ws://h:1", "new");
  EXPECT_EQ(s.get("ws://h:1"), "new");
}

// erase() removes the token; a subsequent get() is empty.
TEST_F(FileCredentialStoreTest, EraseRemoves) {
  auto s = store();
  s.set("ws://h:1", "tok");
  s.erase("ws://h:1");
  EXPECT_FALSE(s.get("ws://h:1").has_value());
}

// erase() on an absent server is a clean no-op (no throw).
TEST_F(FileCredentialStoreTest, EraseAbsentIsNoop) {
  auto s = store();
  EXPECT_NO_THROW(s.erase("ws://nope:1"));
}

// Setting an empty token is meaningful (dev-anonymous persisted as empty) and
// is distinct from "absent": get() returns an empty STRING, not nullopt.
TEST_F(FileCredentialStoreTest, EmptyTokenIsStoredNotAbsent) {
  auto s = store();
  s.set("ws://h:1", "");
  ASSERT_TRUE(s.get("ws://h:1").has_value());
  EXPECT_EQ(*s.get("ws://h:1"), "");
}

// An empty/whitespace-only server key resolves to nothing usable: set is a
// no-op and get is empty (mirrors normalizeServerKey's empty-key contract).
TEST_F(FileCredentialStoreTest, EmptyServerKeyIsNoop) {
  auto s = store();
  s.set("   ", "ignored");
  EXPECT_FALSE(s.get("   ").has_value());
}

// The backing credentials file is created with 0600 perms (owner rw only) and
// its parent directory with 0700 (owner only) — secrets must not be group/
// world readable. (POSIX only; the bit check is skipped on non-POSIX.)
TEST_F(FileCredentialStoreTest, FilePermissionsAre0600) {
  auto s = store();
  s.set("ws://h:1", "secret");

  // Find the one file written under root_.
  fs::path written;
  for (const auto& entry : fs::recursive_directory_iterator(root_)) {
    if (entry.is_regular_file()) {
      written = entry.path();
      break;
    }
  }
  ASSERT_FALSE(written.empty()) << "no credentials file was written under " << root_;

#if defined(__unix__) || defined(__APPLE__) || defined(__linux__)
  const fs::perms p = fs::status(written).permissions();
  // Exactly owner read+write, nothing else.
  EXPECT_EQ(p & fs::perms::owner_read, fs::perms::owner_read);
  EXPECT_EQ(p & fs::perms::owner_write, fs::perms::owner_write);
  EXPECT_EQ(p & fs::perms::group_all, fs::perms::none) << "credentials file is group-accessible";
  EXPECT_EQ(p & fs::perms::others_all, fs::perms::none) << "credentials file is world-accessible";

  // The parent directory must be 0700.
  const fs::perms dp = fs::status(written.parent_path()).permissions();
  EXPECT_EQ(dp & fs::perms::group_all, fs::perms::none) << "credentials dir is group-accessible";
  EXPECT_EQ(dp & fs::perms::others_all, fs::perms::none) << "credentials dir is world-accessible";
#endif
}

// A corrupted store file is tolerated: get() returns nullopt rather than
// throwing, and a subsequent set() recovers (the corrupt content is replaced).
TEST_F(FileCredentialStoreTest, CorruptedStoreIsTolerated) {
  auto s = store();
  s.set("ws://h:1", "good");

  // Find and clobber the file with non-JSON garbage.
  fs::path written;
  for (const auto& entry : fs::recursive_directory_iterator(root_)) {
    if (entry.is_regular_file()) {
      written = entry.path();
      break;
    }
  }
  ASSERT_FALSE(written.empty());
  {
    std::ofstream(written, std::ios::trunc) << "{ this is not valid json ]]]";
  }

  // get() must not throw; a corrupt store reads as "no credentials".
  EXPECT_NO_THROW({
    auto v = store().get("ws://h:1");
    EXPECT_FALSE(v.has_value());
  });

  // A fresh set() recovers the store.
  store().set("ws://h:1", "recovered");
  EXPECT_EQ(store().get("ws://h:1"), "recovered");
}

// ---------------------------------------------------------------------------
// Token-precedence resolution (the seam's resolution helper). Mirrors
// cli_url_resolve's resolveCliToken precedence but adds the STORED tier:
//   explicit (env/flag) > stored > none.
// An empty stored token is "stored as empty" (dev anonymous), still beating
// "none". Env wins over stored for headless parity (live behavior unchanged).
// ---------------------------------------------------------------------------

TEST(CredentialResolve, EnvBeatsStored) {
  EXPECT_EQ(dexory_cloud::resolveStoredToken(/*env=*/std::string("from-env"),
                                             /*stored=*/std::optional<std::string>("from-store")),
            "from-env");
}

TEST(CredentialResolve, StoredUsedWhenNoEnv) {
  EXPECT_EQ(dexory_cloud::resolveStoredToken(/*env=*/std::nullopt,
                                             /*stored=*/std::optional<std::string>("from-store")),
            "from-store");
}

TEST(CredentialResolve, EmptyStoredBeatsNone) {
  // A stored empty token (deliberate dev-anonymous) is honored over the
  // built-in empty default — it is a real entry, not the absence of one.
  EXPECT_EQ(dexory_cloud::resolveStoredToken(/*env=*/std::nullopt,
                                             /*stored=*/std::optional<std::string>("")),
            "");
}

TEST(CredentialResolve, NoneWhenNeitherPresent) {
  EXPECT_EQ(dexory_cloud::resolveStoredToken(/*env=*/std::nullopt, /*stored=*/std::nullopt), "");
}

TEST(CredentialResolve, EmptyEnvIsTreatedAsAbsent) {
  // An empty env value falls through to stored (getEnv already maps "" ->
  // nullopt in production; this guards the helper's own contract).
  EXPECT_EQ(dexory_cloud::resolveStoredToken(/*env=*/std::nullopt,
                                             /*stored=*/std::optional<std::string>("from-store")),
            "from-store");
}
