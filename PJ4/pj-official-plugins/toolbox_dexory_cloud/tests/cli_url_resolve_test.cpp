// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// D12 env-var fallback unit test (HERMETIC): pins the dexory-cloud-cli URL/token
// precedence — explicit --url/--token > environment (DEXORY_CLOUD_URL /
// DEXORY_CLOUD_API_KEY) > built-in default. Pure: tests/cli_url_resolve.hpp takes
// the (flag, env) values directly, so no process env / argv is needed and the
// rule is exercised in isolation. Mirrors how the CLI feeds std::getenv() results
// into the same resolver (tools/dexory_cloud_cli.cpp main()).

#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "cli_url_resolve.hpp"

namespace {

using dexory_cloud::kDefaultCliUrl;
using dexory_cloud::resolveCliToken;
using dexory_cloud::resolveCliUrl;

// --- URL ---------------------------------------------------------------------

// No flag, no env -> the built-in default.
TEST(CliUrlResolve, UrlDefaultWhenNothingSet) {
  EXPECT_EQ(resolveCliUrl(std::nullopt, std::nullopt), std::string(kDefaultCliUrl));
}

// The key requirement: DEXORY_CLOUD_URL is HONORED when --url is absent.
TEST(CliUrlResolve, UrlEnvHonoredWhenFlagAbsent) {
  EXPECT_EQ(resolveCliUrl(std::nullopt, std::string("ws://env-host:9999")), "ws://env-host:9999");
}

// An explicit --url OVERRIDES the environment.
TEST(CliUrlResolve, UrlFlagOverridesEnv) {
  EXPECT_EQ(resolveCliUrl(std::string("ws://flag-host:1234"), std::string("ws://env-host:9999")),
            "ws://flag-host:1234");
}

// An empty env value is ignored (falls through to the default), not adopted as
// an empty URL.
TEST(CliUrlResolve, UrlEmptyEnvFallsThroughToDefault) {
  EXPECT_EQ(resolveCliUrl(std::nullopt, std::string("")), std::string(kDefaultCliUrl));
}

// --- token -------------------------------------------------------------------

// No flag, no env -> empty (dev anonymous).
TEST(CliUrlResolve, TokenEmptyWhenNothingSet) {
  EXPECT_EQ(resolveCliToken(std::nullopt, std::nullopt), "");
}

// DEXORY_CLOUD_API_KEY honored when --token is absent.
TEST(CliUrlResolve, TokenEnvHonoredWhenFlagAbsent) {
  EXPECT_EQ(resolveCliToken(std::nullopt, std::string("secret-bearer")), "secret-bearer");
}

// An explicit --token overrides the env (incl. an explicit empty --token "",
// which selects dev-anonymous even when the env has a value).
TEST(CliUrlResolve, TokenFlagOverridesEnv) {
  EXPECT_EQ(resolveCliToken(std::string("flag-token"), std::string("env-token")), "flag-token");
  EXPECT_EQ(resolveCliToken(std::string(""), std::string("env-token")), "");
}

}  // namespace
