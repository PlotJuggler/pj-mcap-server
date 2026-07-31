// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC pins for the shared credential resolver (stage-4 PR-2, D2
// amendment): resolveCredentials() hoisted out of the dialog into
// src/credential_resolve.{hpp,cpp} so PR-3's start_import can resolve on the
// MAIN thread and hand the job an immutable ConnectionSnapshot. These tests
// mirror the dialog's resolution chain EXACTLY:
//   - token precedence: env (MCAP_CLOUD_API_KEY, used IFF MCAP_CLOUD_URL is
//     set AND its parsed ws-origin equals the target's — strict, fail-closed
//     per spec §7 guard 2) > stored (CredentialStore) > empty dev-anonymous;
//   - non-secret prefs (cert_path / allow_insecure) come from the settings
//     view under "mcap_cloud/server_cache/<normalizeServerKey(uri)>/";
//   - saveCredentialsForUri/loadCredentialsForUri round-trip through the
//     same prefix + store.
#include "credential_resolve.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>

#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/settings_store_host.hpp>

#include "credential_store.hpp"
#include "server_history.h"
#include "test_support_fs.hpp"

namespace {

namespace fs = std::filesystem;

// Shared RAII temp-dir base with this suite's unique prefix.
struct TempRoot : mcap_cloud_test::ScopedTempDir {
  explicit TempRoot(const std::string& name) : ScopedTempDir("mcap-cloud-cred-resolve-" + name) {}
};

// Save/restore the two env vars the resolver reads, so tests can set them
// freely without leaking into the rest of the suite (or inheriting state
// from the invoking shell).
struct EnvGuard {
  EnvGuard() : url(capture("MCAP_CLOUD_URL")), key(capture("MCAP_CLOUD_API_KEY")) {
    ::unsetenv("MCAP_CLOUD_URL");
    ::unsetenv("MCAP_CLOUD_API_KEY");
  }
  static std::optional<std::string> capture(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::nullopt : std::optional<std::string>(value);
  }
  ~EnvGuard() {
    restore("MCAP_CLOUD_URL", url);
    restore("MCAP_CLOUD_API_KEY", key);
  }
  static void restore(const char* name, const std::optional<std::string>& value) {
    if (value.has_value()) {
      ::setenv(name, value->c_str(), 1);
    } else {
      ::unsetenv(name);
    }
  }
  std::optional<std::string> url;
  std::optional<std::string> key;
};

struct Fixture {
  Fixture() : store_root("store") {}

  [[nodiscard]] PJ::sdk::SettingsView view() { return PJ::sdk::SettingsView{settings_host.view()}; }

  EnvGuard env;
  TempRoot store_root;
  PJ::sdk::InMemorySettingsBackend backend;
  PJ::sdk::SettingsStoreHost settings_host{backend};
  mcap_cloud::FileCredentialStore store{store_root.path};
};

constexpr const char* kUri = "ws://example.com:8080";

}  // namespace

// The env token applies IFF MCAP_CLOUD_URL is set AND its origin equals the
// target's (strict sameWsOrigin, fail-closed). On a non-match or an unset
// MCAP_CLOUD_URL the chain falls through to the stored per-server token.
TEST(McapCloudCredentialResolve, EnvTokenRequiresMatchingEnvUrlOrigin) {
  Fixture fx;
  fx.store.set(kUri, "stored-token");
  ::setenv("MCAP_CLOUD_API_KEY", "env-token", 1);

  // Same origin (path difference is ignored by the origin parse) -> env wins.
  ::setenv("MCAP_CLOUD_URL", "ws://example.com:8080/some/path", 1);
  EXPECT_EQ(mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri).api_key, "env-token");

  // Different origin -> fail-closed, stored token.
  ::setenv("MCAP_CLOUD_URL", "ws://other.com:8080", 1);
  EXPECT_EQ(mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri).api_key, "stored-token");

  // Different port = different origin.
  ::setenv("MCAP_CLOUD_URL", "ws://example.com:9090", 1);
  EXPECT_EQ(mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri).api_key, "stored-token");

  // MCAP_CLOUD_URL unset -> the env key alone is never released.
  ::unsetenv("MCAP_CLOUD_URL");
  EXPECT_EQ(mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri).api_key, "stored-token");
}

// Chain tail: stored beats none; nothing anywhere resolves to the empty
// dev-anonymous token. An EMPTY env token (getEnv maps "" to unset) falls
// through exactly like an unset one.
TEST(McapCloudCredentialResolve, StoredThenEmptyChainOrder) {
  Fixture fx;

  // Nothing stored, no env -> dev anonymous.
  EXPECT_EQ(mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri).api_key, "");

  // Stored only -> stored.
  fx.store.set(kUri, "stored-token");
  EXPECT_EQ(mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri).api_key, "stored-token");

  // Empty env token with a matching origin -> still the stored one.
  ::setenv("MCAP_CLOUD_URL", kUri, 1);
  ::setenv("MCAP_CLOUD_API_KEY", "", 1);
  EXPECT_EQ(mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri).api_key, "stored-token");
}

// Non-secret prefs come from the settings view under the normalized
// per-server prefix; the token never touches the settings view.
TEST(McapCloudCredentialResolve, NonSecretPrefsUseTheNormalizedSettingsPrefix) {
  Fixture fx;
  const std::string prefix = "mcap_cloud/server_cache/" + normalizeServerKey(kUri) + "/";
  fx.backend.setString(prefix + "cert_path", "/etc/ssl/private-ca.pem");
  fx.backend.setString(prefix + "allow_insecure", "true");

  const mcap_cloud::ServerCredentials creds = mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri);
  EXPECT_EQ(creds.cert_path, "/etc/ssl/private-ca.pem");
  EXPECT_TRUE(creds.allow_insecure);
  EXPECT_EQ(creds.api_key, "");
  EXPECT_FALSE(fx.backend.contains(prefix + "api_key")) << "the secret must never live in settings";
}

// save -> load/resolve round-trip: cert/insecure land under the settings
// prefix, the token lands in the CredentialStore.
TEST(McapCloudCredentialResolve, SaveLoadRoundTrip) {
  Fixture fx;
  mcap_cloud::ServerCredentials creds;
  creds.cert_path = "/tmp/ca.pem";
  creds.allow_insecure = true;
  creds.api_key = "round-trip-token";
  mcap_cloud::saveCredentialsForUri(fx.view(), fx.store, kUri, creds);

  const std::string prefix = "mcap_cloud/server_cache/" + normalizeServerKey(kUri) + "/";
  EXPECT_TRUE(fx.backend.contains(prefix + "cert_path"));
  EXPECT_TRUE(fx.backend.contains(prefix + "allow_insecure"));
  EXPECT_FALSE(fx.backend.contains(prefix + "api_key"));
  EXPECT_EQ(fx.store.get(kUri).value_or(""), "round-trip-token");

  const mcap_cloud::ServerCredentials loaded = mcap_cloud::loadCredentialsForUri(fx.view(), fx.store, kUri);
  EXPECT_EQ(loaded.cert_path, creds.cert_path);
  EXPECT_EQ(loaded.allow_insecure, creds.allow_insecure);
  EXPECT_EQ(loaded.api_key, creds.api_key);

  const mcap_cloud::ServerCredentials resolved = mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri);
  EXPECT_EQ(resolved.api_key, "round-trip-token");
}

// The PR-3 handoff type: an immutable-by-copy snapshot of the resolved
// connection tuple, exactly what the dialog passes to connectAsync.
TEST(McapCloudCredentialResolve, ConnectionSnapshotCarriesTheResolvedTuple) {
  Fixture fx;
  fx.store.set(kUri, "snap-token");
  const std::string prefix = "mcap_cloud/server_cache/" + normalizeServerKey(kUri) + "/";
  fx.backend.setString(prefix + "cert_path", "/snap/ca.pem");

  const mcap_cloud::ConnectionSnapshot snapshot{kUri, mcap_cloud::resolveCredentials(fx.view(), fx.store, kUri)};
  const mcap_cloud::ConnectionSnapshot copy = snapshot;  // job-thread handoff = plain value copy
  EXPECT_EQ(copy.uri, kUri);
  EXPECT_EQ(copy.credentials.api_key, "snap-token");
  EXPECT_EQ(copy.credentials.cert_path, "/snap/ca.pem");
  EXPECT_FALSE(copy.credentials.allow_insecure);
}

// Adversarial F15: token PROVENANCE — a stored EMPTY dev-anonymous token is
// kStored (present!), never kNone; a truly absent credential is kNone.
// Consumers (the §10 auth hint) gate on the source, never on token bytes.
TEST(McapCloudCredentialResolve, ResolvedTokenCarriesItsProvenance) {
  Fixture fx;
  const std::string uri = "ws://prov-host:8080";
  // Absent everywhere -> kNone.
  {
    const auto creds = mcap_cloud::resolveCredentials(fx.view(), fx.store, uri);
    EXPECT_EQ(creds.api_key_source, mcap_cloud::TokenSource::kNone);
    EXPECT_TRUE(creds.api_key.empty());
  }
  // Stored EMPTY dev-anonymous token -> kStored, still empty bytes.
  fx.store.set(uri, "");
  {
    const auto creds = mcap_cloud::resolveCredentials(fx.view(), fx.store, uri);
    EXPECT_EQ(creds.api_key_source, mcap_cloud::TokenSource::kStored);
    EXPECT_TRUE(creds.api_key.empty());
  }
  // Stored real token -> kStored.
  fx.store.set(uri, "stored-token");
  {
    const auto creds = mcap_cloud::resolveCredentials(fx.view(), fx.store, uri);
    EXPECT_EQ(creds.api_key_source, mcap_cloud::TokenSource::kStored);
    EXPECT_EQ(creds.api_key, "stored-token");
  }
  // Origin-bound env token -> kEnvironment.
  ::setenv("MCAP_CLOUD_URL", uri.c_str(), 1);
  ::setenv("MCAP_CLOUD_API_KEY", "env-token", 1);
  {
    const auto creds = mcap_cloud::resolveCredentials(fx.view(), fx.store, uri);
    EXPECT_EQ(creds.api_key_source, mcap_cloud::TokenSource::kEnvironment);
    EXPECT_EQ(creds.api_key, "env-token");
  }
}
