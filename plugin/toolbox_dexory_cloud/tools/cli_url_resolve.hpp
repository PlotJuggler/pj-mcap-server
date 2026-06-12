// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Pure URL/token resolution for the dexory-cloud-cli, factored out of the CLI
// main translation unit so the precedence rule (explicit --url/--token >
// environment > built-in default) is unit-testable WITHOUT a process env or argv.
// The CLI passes the actual std::getenv() result + the parsed --url; tests pass
// synthetic values. Header-only, std-only.
#pragma once

#include <optional>
#include <string>

namespace dexory_cloud {

constexpr const char* kDefaultCliUrl = "ws://localhost:8080";

// Resolve the effective WS URL with precedence:
//   1. an explicit --url (cli_url has a value, even if empty? no: an empty --url
//      is rejected at the flag layer, so a present value is always non-empty),
//   2. else the DEXORY_CLOUD_URL environment value when set AND non-empty,
//   3. else the built-in default (ws://localhost:8080).
// `cli_url`  = the --url flag value if the flag was given, else nullopt.
// `env_url`  = std::getenv("DEXORY_CLOUD_URL") result if set, else nullopt.
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
inline std::string resolveCliToken(const std::optional<std::string>& cli_token,
                                   const std::optional<std::string>& env_token) {
  if (cli_token.has_value()) {
    return *cli_token;  // explicit --token wins (empty allowed)
  }
  if (env_token.has_value() && !env_token->empty()) {
    return *env_token;
  }
  return std::string{};  // dev anonymous
}

}  // namespace dexory_cloud
