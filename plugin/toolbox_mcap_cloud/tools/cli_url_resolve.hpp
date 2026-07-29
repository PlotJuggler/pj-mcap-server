// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Pure URL/token resolution for the mcap-cloud-cli, factored out of the CLI
// main translation unit so the precedence rule (explicit --url/--token >
// environment > built-in default) is unit-testable WITHOUT a process env or argv.
// The CLI passes the actual std::getenv() result + the parsed --url; tests pass
// synthetic values. Header-only over std + origin_match (std-only itself).
#pragma once

#include <optional>
#include <string>

#include "origin_match.hpp"

namespace mcap_cloud {

constexpr const char* kDefaultCliUrl = "ws://localhost:8080";

// Resolve the effective WS URL with precedence:
//   1. an explicit --url (cli_url has a value, even if empty? no: an empty --url
//      is rejected at the flag layer, so a present value is always non-empty),
//   2. else the MCAP_CLOUD_URL environment value when set AND non-empty,
//   3. else the built-in default (ws://localhost:8080).
// `cli_url`  = the --url flag value if the flag was given, else nullopt.
// `env_url`  = std::getenv("MCAP_CLOUD_URL") result if set, else nullopt.
inline std::string resolveCliUrl(const std::optional<std::string>& cli_url,
                                 const std::optional<std::string>& env_url) {
  if (cli_url.has_value() && !cli_url->empty()) {
    return *cli_url;
  }
  if (env_url.has_value() && !env_url->empty()) {
    return *env_url;
  }
  return std::string(kDefaultCliUrl);
}

// Resolve the bearer token with the same precedence, but an EMPTY token is
// legitimate (dev anonymous): an explicitly-given --token "" wins as empty, and
// an empty env value falls through to the default (also empty). So the only
// meaningful sources are a present --token (any value, incl. empty) and a present
// non-empty env value.
//
// ORIGIN BINDING (spec docs/canonical-layout-import.md §7 guard 2): the env
// token is bound to MCAP_CLOUD_URL's origin — it applies only when env_url is
// set AND its parsed origin equals the EFFECTIVE URL's (sameWsOrigin, strict
// fail-closed). A --url naming a different origin therefore never receives the
// env bearer token; with --url absent the effective URL IS the env URL, so the
// binding is a self-match and the old behavior is preserved. An env token
// WITHOUT an env URL has nothing to be bound to and is ignored.
inline std::string resolveCliToken(const std::optional<std::string>& cli_token,
                                   const std::optional<std::string>& env_token,
                                   const std::string& effective_url,
                                   const std::optional<std::string>& env_url) {
  if (cli_token.has_value()) {
    return *cli_token;  // explicit --token wins (empty allowed)
  }
  if (env_token.has_value() && !env_token->empty() && env_url.has_value() &&
      sameWsOrigin(*env_url, effective_url)) {
    return *env_token;
  }
  return std::string{};  // dev anonymous
}

// Resolve the CA bundle used to verify a wss:// peer, same precedence as the URL:
//   1. an explicit --cert, 2. else a non-empty MCAP_CLOUD_CACERT, 3. else empty.
// An EMPTY result is meaningful rather than an error: it asks BackendConnection
// to auto-detect the host's system bundle (see detectSystemCaBundle() — the
// ixwebsocket "SYSTEM" keyword is a no-op on Linux), so there is deliberately no
// built-in default path here.
inline std::string resolveCliCert(const std::optional<std::string>& cli_cert,
                                  const std::optional<std::string>& env_cert) {
  if (cli_cert.has_value() && !cli_cert->empty()) {
    return *cli_cert;
  }
  if (env_cert.has_value() && !env_cert->empty()) {
    return *env_cert;
  }
  return std::string{};  // auto-detect
}

}  // namespace mcap_cloud
