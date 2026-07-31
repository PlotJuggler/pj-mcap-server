// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Portable environment-variable mutation for tests — the write-side companion
// to PJ::sdk::getEnv (which already hides the MSVC C4996 read-side quirk).
//
// POSIX setenv/unsetenv do NOT exist in the MSVC CRT: calling them directly is
// a hard C2039/C3861 compile error on the windows-x64 CI leg. Every test that
// pins env state goes through these two helpers instead.
#pragma once

#include <cstdlib>

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

}  // namespace mcap_cloud_test
