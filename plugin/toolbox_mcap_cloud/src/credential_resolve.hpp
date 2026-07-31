// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Shared per-server credential resolution (stage-4 PR-2, D2 amendment).
// Hoisted VERBATIM from mcap_cloud_dialog.cpp so the dialog's connect paths
// and the descriptor-import provider (PR-3) resolve credentials through ONE
// implementation.
//
// ---------------------------------------------------------------------------
// THREADING CONTRACT — MAIN THREAD ONLY.
// Every function here takes a PJ::sdk::SettingsView, and every SettingsView
// call is main-thread-only by the SDK ABI (plugin_data_api.hpp, SettingsView
// class doc: "All calls are main-thread, mirroring QSettings usage" — SDK
// 0.20.0). Callers therefore resolve on the main thread and hand worker/job
// threads an immutable BY-VALUE copy of the result:
//   - the dialog resolves inside its GUI-thread handlers and passes the
//     fields into the queued connectAsync command;
//   - PR-3's start_import() resolves inside the (main-thread) start call and
//     hands the job thread a ConnectionSnapshot — the job NEVER touches the
//     SettingsView or the CredentialStore.
// ---------------------------------------------------------------------------
#pragma once

#include <string>

#include <pj_base/sdk/plugin_data_api.hpp>  // PJ::sdk::SettingsView

namespace mcap_cloud {

class CredentialStore;

// Per-server connection credentials, exactly as the dialog hands them to
// FetchWorker::connectAsync: the non-secret prefs (cert_path/allow_insecure,
// persisted in the settings view) + the resolved bearer token (the SECRET,
// persisted in the CredentialStore — never in plaintext settings).
struct ServerCredentials {
  std::string cert_path;
  std::string api_key;
  bool allow_insecure = false;
};

// The immutable per-job connection snapshot (the PR-3 handoff type): the
// resolved target URI + the resolved credentials, copied BY VALUE onto the
// job before its thread starts. Owning strings only — safe to carry across
// threads for the whole job lifetime with zero main-thread callbacks.
struct ConnectionSnapshot {
  std::string uri;                // the target actually connected to (verbatim)
  ServerCredentials credentials;  // resolveCredentials(view, store, uri) result
};

// D6: the bearer token (the SECRET) lives in the CredentialStore (0600 file,
// libsecret-ready seam), NOT in plaintext SettingsView. The non-secret prefs
// (cert_path, allow_insecure) stay in SettingsView keyed by the normalized-URI
// prefix "mcap_cloud/server_cache/<normalizeServerKey(uri)>/" (Plan D Task 6
// note 4). `view` carries the non-secret prefs; `store` carries the secret
// token. Reads only — an absent entry leaves the field at its default.
[[nodiscard]] ServerCredentials loadCredentialsForUri(PJ::sdk::SettingsView view, CredentialStore& store,
                                                      const std::string& uri);

// Persist `creds` for `uri`: cert_path/allow_insecure into the settings view
// under the normalized prefix, the token into the CredentialStore.
void saveCredentialsForUri(PJ::sdk::SettingsView view, CredentialStore& store, const std::string& uri,
                           const ServerCredentials& creds);

// Load per-server credentials, resolving the token with precedence
// explicit(env) > stored > none: the MCAP_CLOUD_API_KEY env var wins over the
// stored token (headless / live-test parity unchanged), then the stored token,
// then dev-anonymous empty. Mirrors cli_url_resolve's resolveCliToken chain
// (extended with the STORED tier via resolveStoredToken).
//
// ORIGIN BINDING (spec docs/canonical-layout-import.md §7 guard 2): the env
// token is used IFF MCAP_CLOUD_URL is set AND its parsed origin equals the
// target's (sameWsOrigin — strict, fail-closed). Without the binding, a
// hostile layout/import target of a DIFFERENT origin would silently receive
// the env bearer token. On a non-match the chain simply falls through to the
// stored per-server token (unchanged).
[[nodiscard]] ServerCredentials resolveCredentials(PJ::sdk::SettingsView view, CredentialStore& store,
                                                   const std::string& uri);

}  // namespace mcap_cloud
