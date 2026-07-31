// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Portable environment-variable mutation for tests — the write-side companion
// to PJ::sdk::getEnv (which already hides the MSVC C4996 read-side quirk).
//
// POSIX setenv/unsetenv do NOT exist in the MSVC CRT: calling them directly is
// a hard C2039/C3861 compile error on the windows-x64 CI leg. Every test that
// pins env state goes through these two helpers instead.
//
// HermeticEnv — pin every environment variable the toolbox/provider stack
// resolves at construction/credential time to private temp roots, restoring
// the prior values on destruction. Hoisted (prefix parameterized) from
// headless_init_test.cpp so the PR-3 provider suites share it: the toolbox
// constructor resolves the SessionFileCache root (MCAP_CLOUD_CACHE_DIR) and
// the trust/credential config root (XDG_CONFIG_HOME) from the environment,
// and credential resolution reads MCAP_CLOUD_URL / MCAP_CLOUD_API_KEY.
// Capture/restore mirrors credential_resolve_test's EnvGuard — a bare unset
// of XDG_CONFIG_HOME would destroy a commonly-set variable for the rest of
// the process.
#pragma once

#include <cstdlib>
#include <optional>
#include <string>

#include "test_support_fs.hpp"

namespace mcap_cloud_test {

// Set `name` to `value`, overwriting any existing value.
//
// WINDOWS CAVEAT: the CRT cannot hold an EMPTY environment variable —
// `_putenv_s(name, "")` REMOVES it instead. That is behaviorally invisible to
// this plugin: PJ::sdk::getEnv maps an empty value to std::nullopt, so an
// empty variable and an absent one already resolve identically.
inline void setEnvVar(const char* name, const char* value) {
#if defined(_WIN32)
  ::_putenv_s(name, value);
#else
  ::setenv(name, value, 1);
#endif
}

// Remove `name` from the environment.
inline void unsetEnvVar(const char* name) {
#if defined(_WIN32)
  ::_putenv_s(name, "");
#else
  ::unsetenv(name);
#endif
}

struct HermeticEnv {
  explicit HermeticEnv(const std::string& prefix)
      : cache_root(prefix + "-cache"),
        config_root(prefix + "-config"),
        saved_cache_dir(capture("MCAP_CLOUD_CACHE_DIR")),
        saved_xdg_config(capture("XDG_CONFIG_HOME")),
        saved_url(capture("MCAP_CLOUD_URL")),
        saved_api_key(capture("MCAP_CLOUD_API_KEY")) {
    setEnvVar("MCAP_CLOUD_CACHE_DIR", cache_root.path.string().c_str());
    setEnvVar("XDG_CONFIG_HOME", config_root.path.string().c_str());
    unsetEnvVar("MCAP_CLOUD_URL");
    unsetEnvVar("MCAP_CLOUD_API_KEY");
  }
  ~HermeticEnv() {
    restore("MCAP_CLOUD_CACHE_DIR", saved_cache_dir);
    restore("XDG_CONFIG_HOME", saved_xdg_config);
    restore("MCAP_CLOUD_URL", saved_url);
    restore("MCAP_CLOUD_API_KEY", saved_api_key);
  }
  HermeticEnv(const HermeticEnv&) = delete;
  HermeticEnv& operator=(const HermeticEnv&) = delete;

  static std::optional<std::string> capture(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::nullopt : std::optional<std::string>(value);
  }
  static void restore(const char* name, const std::optional<std::string>& value) {
    if (value.has_value()) {
      setEnvVar(name, value->c_str());
    } else {
      unsetEnvVar(name);
    }
  }

  ScopedTempDir cache_root;
  ScopedTempDir config_root;
  std::optional<std::string> saved_cache_dir;
  std::optional<std::string> saved_xdg_config;
  std::optional<std::string> saved_url;
  std::optional<std::string> saved_api_key;
};

}  // namespace mcap_cloud_test
