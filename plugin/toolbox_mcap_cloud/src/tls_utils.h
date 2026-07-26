/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#pragma once

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

// Returns true if every byte of `s` is printable ASCII (0x20–0x7E inclusive).
// Header/URI/cert/api-key values handed to gRPC must be pure printable ASCII:
// control bytes (CR, LF, NUL) are exactly what gRPC asserts-and-aborts on, so
// they are rejected at the earliest boundary (PJ3 main_window.cpp:947-963).
[[nodiscard]] inline bool isPrintableAscii(std::string_view s) {
  for (unsigned char c : s) {
    if (c < 0x20 || c > 0x7E) {
      return false;
    }
  }
  return true;
}

// Returns true if the key matches: msco_[32 lowercase alnum]_[8 hex chars]
[[nodiscard]] inline bool isValidApiKey(const std::string& key) {
  static const std::regex re(R"(^msco_[a-z0-9]{32}_[0-9a-f]{8}$)", std::regex::ECMAScript);
  return std::regex_match(key, re);
}

// Returns true if the file exists and is readable.
[[nodiscard]] inline bool isCertReadable(const std::string& path) {
  if (path.empty()) {
    return false;
  }
  const std::filesystem::path p(path);
  if (!std::filesystem::exists(p)) {
    return false;
  }
  return std::ifstream(p).good();
}

// --- system CA bundle discovery (ixwebsocket/mbedTLS workaround) -----------
//
// ixwebsocket's mbedTLS backend implements loadSystemCertificates() for Windows
// ONLY: every other platform takes the `#else return false;` branch, and it never
// assigns errorMsg. So the library default caFile "SYSTEM" makes EVERY wss://
// handshake on Linux fail with the useless message "error: no error". We resolve
// a concrete bundle path ourselves and hand mbedTLS a real file to parse.

// Returns the first readable path in `candidates`, or "" when none is readable.
[[nodiscard]] inline std::string firstReadableCaBundle(const std::vector<std::string>& candidates) {
  for (const std::string& candidate : candidates) {
    if (isCertReadable(candidate)) {
      return candidate;
    }
  }
  return {};
}

// Well-known system CA bundle locations, in descending order of how common the
// layout is. Distro-specific because there is no portable answer.
[[nodiscard]] inline const std::vector<std::string>& systemCaBundleCandidates() {
  static const std::vector<std::string> kCandidates = {
      "/etc/ssl/certs/ca-certificates.crt",                  // Debian, Ubuntu, Alpine
      "/etc/pki/tls/certs/ca-bundle.crt",                    // Fedora, RHEL, CentOS
      "/etc/ssl/ca-bundle.pem",                              // openSUSE
      "/etc/ca-certificates/extracted/tls-ca-bundle.pem",    // Arch
      "/etc/ssl/cert.pem",                                   // LibreSSL layouts, macOS
  };
  return kCandidates;
}

// Resolve the effective system bundle: a readable SSL_CERT_FILE wins, else the
// first readable well-known path, else "". Pure (env value is a parameter) so the
// precedence is unit-testable.
//
// SSL_CERT_FILE matters because the plugin ships in an AppImage and therefore runs
// on distros whose layout we cannot enumerate (NixOS, minimal containers,
// corporate images); those conventionally export it instead of providing one of
// the paths above. An unreadable value falls through rather than failing hard —
// a stale env var must not take down a client that has a perfectly good bundle.
//
// SSL_CERT_DIR is deliberately NOT honoured: it names a hashed-symlink DIRECTORY,
// and ixwebsocket only ever calls mbedtls_x509_crt_parse_file() on caFile (never
// mbedtls_x509_crt_parse_path()), so handing it a directory would fail to parse.
[[nodiscard]] inline std::string resolveSystemCaBundle(const std::string& ssl_cert_file_env,
                                                       const std::vector<std::string>& candidates) {
  if (isCertReadable(ssl_cert_file_env)) {
    return ssl_cert_file_env;
  }
  return firstReadableCaBundle(candidates);
}

// The system bundle for this host, or "" when nothing usable is found.
[[nodiscard]] inline std::string detectSystemCaBundle() {
  const char* env = std::getenv("SSL_CERT_FILE");
  return resolveSystemCaBundle(env != nullptr ? env : "", systemCaBundleCandidates());
}

// The resolved ix::SocketTLSOptions inputs. Kept as a plain struct (no ix types)
// so the decision is unit-testable without linking the transport.
struct TlsCaChoice {
  std::string ca_file;
  // ALWAYS false, deliberately. ixwebsocket gates mbedtls_ssl_set_hostname()
  // (i.e. SNI) on !disable_hostname_validation, so setting this flag silently
  // stops sending SNI and any SNI-dependent TLS front-end (Tailscale, Cloudflare,
  // shared-IP terminators) aborts the handshake with a fatal alert. Peer
  // verification is switched off via ca_file "NONE" instead, which already maps
  // to MBEDTLS_SSL_VERIFY_NONE.
  bool disable_hostname_validation = false;
};

// Resolve the CA configuration: an explicit user cert path wins, else the
// detected system bundle, else ixwebsocket's "SYSTEM" keyword as a last resort
// (correct on macOS/Windows, where loadSystemCertificates() is implemented).
// `allow_insecure` overrides everything with "NONE" (verification disabled).
[[nodiscard]] inline TlsCaChoice chooseTlsCaFile(const std::string& cert_path,
                                                 bool allow_insecure,
                                                 const std::string& system_ca) {
  if (allow_insecure) {
    return TlsCaChoice{"NONE", false};
  }
  if (!cert_path.empty()) {
    return TlsCaChoice{cert_path, false};
  }
  if (!system_ca.empty()) {
    return TlsCaChoice{system_ca, false};
  }
  return TlsCaChoice{"SYSTEM", false};
}
