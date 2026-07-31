// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// See credential_resolve.hpp — the bodies are the dialog's former file-local
// helpers, moved verbatim (behavior pinned by credential_resolve_test.cpp).
#include "credential_resolve.hpp"

#include <optional>

#include <pj_base/sdk/platform.hpp>  // PJ::sdk::getEnv

#include "credential_store.hpp"  // CredentialStore + resolveStoredToken
#include "origin_match.hpp"      // sameWsOrigin (strict §7 guard-2 parse)
#include "server_history.h"      // normalizeServerKey (the storage key)
#include "settings_store.hpp"    // SettingsStore over SettingsView

namespace mcap_cloud {

namespace {

std::string credentialsSettingsPrefix(const std::string& uri) {
  return "mcap_cloud/server_cache/" + normalizeServerKey(uri) + "/";
}

}  // namespace

ServerCredentials loadCredentialsForUri(PJ::sdk::SettingsView view, CredentialStore& store, const std::string& uri) {
  SettingsStore settings(view);
  const std::string prefix = credentialsSettingsPrefix(uri);
  ServerCredentials creds;
  creds.cert_path = settings.getString(prefix + "cert_path");
  creds.allow_insecure = settings.getBool(prefix + "allow_insecure", false);
  // The token comes from the secret store; an absent entry leaves api_key empty.
  if (auto tok = store.get(uri)) {
    creds.api_key = *tok;
  }
  return creds;
}

void saveCredentialsForUri(PJ::sdk::SettingsView view, CredentialStore& store, const std::string& uri,
                           const ServerCredentials& creds) {
  SettingsStore settings(view);
  const std::string prefix = credentialsSettingsPrefix(uri);
  settings.setString(prefix + "cert_path", creds.cert_path);
  settings.setBool(prefix + "allow_insecure", creds.allow_insecure);
  // The token is the only secret — store it in the CredentialStore.
  store.set(uri, creds.api_key);
}

ServerCredentials resolveCredentials(PJ::sdk::SettingsView view, CredentialStore& store, const std::string& uri) {
  ServerCredentials creds = loadCredentialsForUri(view, store, uri);
  const std::optional<std::string> env_url = PJ::sdk::getEnv("MCAP_CLOUD_URL");
  const bool env_origin_ok = env_url.has_value() && sameWsOrigin(*env_url, uri);
  creds.api_key = resolveStoredToken(
      env_origin_ok ? PJ::sdk::getEnv("MCAP_CLOUD_API_KEY") : std::optional<std::string>{},
      store.get(uri));
  return creds;
}

}  // namespace mcap_cloud
