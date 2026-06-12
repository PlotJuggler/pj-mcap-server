// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// D6 (Plan D Task 6) — CredentialStore seam.
//
// Plan D Task 6's intent: the bearer token (the SECRET) persists in an OS
// secret store, not in plaintext SettingsView. CLAUDE.md grounding: qtkeychain
// would pull Qt6 into the (deliberately Qt-free) plugin; libsecret is the
// candidate but is NOT cleanly available on this machine (no dev package
// without sudo; the conancenter recipe is a heavy from-source glib+autotools
// build). Per Plan D ASSUMPTION A3 ("if [keychain] unavailable, AuthProvider
// degrades to ... storage with a warning"), the as-built backend is a
// 0600-perm file under the user's config dir. The seam below makes a libsecret
// backend a later drop-in: implement CredentialStore and swap the factory.
//
// Scope: ONLY the bearer token (the secret) lives here. Non-secret prefs
// (cert_path, allow_insecure, URI history) stay in SettingsView (note 4).
//
// This unit is Qt-free and links only nlohmann_json (already a plugin dep). It
// is compiled ONLY into the plugin .so, never into dexory-cloud-cli, so the
// no-Qt ldd guard on the CLI is unaffected.

#pragma once

#include <filesystem>
#include <optional>
#include <string>

namespace dexory_cloud {

// Per-server bearer-token store. Keys are server URLs; every implementation
// normalizes them through normalizeServerKey() so scheme/case/trailing-slash
// variants of one server collide on a single entry.
class CredentialStore {
 public:
  virtual ~CredentialStore() = default;

  // Retrieve the stored token for `server_url`. Returns std::nullopt when no
  // entry exists (distinct from an entry holding an empty string, which is a
  // deliberate dev-anonymous token). Never throws.
  [[nodiscard]] virtual std::optional<std::string> get(const std::string& server_url) const = 0;

  // Store (or overwrite) `token` for `server_url`. A no-op when the URL
  // normalizes to an empty key. Never throws.
  virtual void set(const std::string& server_url, const std::string& token) = 0;

  // Remove any stored token for `server_url`. A no-op when absent. Never throws.
  virtual void erase(const std::string& server_url) = 0;
};

// Default backend: a single JSON object `{ normalized_key: token }` written to
// `<config_root>/credentials.json` with the directory at 0700 and the file at
// 0600 (owner-only). A corrupt or unreadable file reads as "no credentials"
// and is replaced on the next set() (never throws). The libsecret drop-in
// would replace this class behind the CredentialStore interface.
class FileCredentialStore : public CredentialStore {
 public:
  // `config_root` is the directory under which credentials.json lives. In
  // production this is defaultConfigRoot() ($XDG_CONFIG_HOME/dexory_cloud or
  // $HOME/.config/dexory_cloud); tests inject a private temp dir.
  explicit FileCredentialStore(std::filesystem::path config_root) : config_root_(std::move(config_root)) {}

  [[nodiscard]] std::optional<std::string> get(const std::string& server_url) const override;
  void set(const std::string& server_url, const std::string& token) override;
  void erase(const std::string& server_url) override;

  // The credentials file path (config_root/credentials.json). Exposed for tests.
  [[nodiscard]] std::filesystem::path filePath() const { return config_root_ / "credentials.json"; }

 private:
  std::filesystem::path config_root_;
};

// Resolve the production config root for the file backend:
//   $XDG_CONFIG_HOME/dexory_cloud, else $HOME/.config/dexory_cloud, else
//   <temp>/dexory_cloud (never empty). Credentials are CONFIG (not data), so
//   this intentionally uses XDG_CONFIG_HOME rather than userDataDir().
[[nodiscard]] std::filesystem::path defaultConfigRoot();

// Token precedence resolution for the seam: explicit (env/flag) > stored >
// none. `env` is the env/flag value when present and non-empty (the caller
// passes std::nullopt for an unset/empty env, mirroring SDK getEnv). `stored`
// is the CredentialStore::get() result (std::nullopt = no entry; an empty
// string = a stored dev-anonymous token, which still beats "none"). Returns the
// effective token (empty string = dev anonymous).
[[nodiscard]] inline std::string resolveStoredToken(const std::optional<std::string>& env,
                                                    const std::optional<std::string>& stored) {
  if (env.has_value() && !env->empty()) {
    return *env;  // explicit env/flag wins (live headless behavior unchanged)
  }
  if (stored.has_value()) {
    return *stored;  // stored entry (possibly empty) beats the built-in none
  }
  return std::string{};  // dev anonymous
}

}  // namespace dexory_cloud
