/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#include "tls_utils.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>

TEST(TlsUtils, ValidApiKey) {
  EXPECT_TRUE(isValidApiKey("msco_s3l8gcdwuadege3pkhou0k0n2t5omfij_f9010b9e"));
}

TEST(TlsUtils, ApiKeyWrongPrefix) {
  EXPECT_FALSE(isValidApiKey("xxxx_s3l8gcdwuadege3pkhou0k0n2t5omfij_f9010b9e"));
}

TEST(TlsUtils, ApiKeyTooShort) {
  EXPECT_FALSE(isValidApiKey("msco_abc_12345678"));
}

TEST(TlsUtils, ApiKeyEmpty) {
  EXPECT_FALSE(isValidApiKey(""));
}

TEST(TlsUtils, ApiKeyBadFingerprint) {
  EXPECT_FALSE(isValidApiKey("msco_s3l8gcdwuadege3pkhou0k0n2t5omfij_zzzzzzzz"));
}

// --- isPrintableAscii (connect-path control-byte gate, gap #1) ---

TEST(TlsUtils, PrintableAsciiAcceptsTypicalUri) {
  EXPECT_TRUE(isPrintableAscii("grpc+tls://demo.mosaico.dev:6726"));
  EXPECT_TRUE(isPrintableAscii("msco_s3l8gcdwuadege3pkhou0k0n2t5omfij_f9010b9e"));
  EXPECT_TRUE(isPrintableAscii("/home/user/cert.pem"));
}

TEST(TlsUtils, PrintableAsciiAcceptsEmpty) {
  // Empty cert path / api key are valid (optional credentials).
  EXPECT_TRUE(isPrintableAscii(""));
}

TEST(TlsUtils, PrintableAsciiRejectsControlBytes) {
  // CR / LF / NUL / TAB are exactly what gRPC asserts-and-aborts on.
  EXPECT_FALSE(isPrintableAscii("host:6726\r\n"));
  EXPECT_FALSE(isPrintableAscii("host:6726\n"));
  EXPECT_FALSE(isPrintableAscii(std::string("host\0port", 9)));
  EXPECT_FALSE(isPrintableAscii("host\t6726"));
}

TEST(TlsUtils, PrintableAsciiRejectsNonAscii) {
  // High-bit / multibyte UTF-8 bytes are not printable ASCII.
  EXPECT_FALSE(isPrintableAscii("hÃ¶st:6726"));  // contains 0xC3-prefixed bytes
  EXPECT_FALSE(isPrintableAscii("\x7f"));        // DEL is excluded (> 0x7E)
}

TEST(TlsUtils, PrintableAsciiBoundaryBytes) {
  // 0x20 (space) and 0x7E (~) are the inclusive printable bounds.
  EXPECT_TRUE(isPrintableAscii(" ~"));
  EXPECT_FALSE(isPrintableAscii(std::string(1, '\x1f')));  // just below 0x20
}

// --- system CA bundle discovery -------------------------------------------
//
// ixwebsocket's mbedTLS backend has NO system-certificate support on Linux:
// SocketMbedTLS::loadSystemCertificates() is `return false` on every non-Windows
// platform, so the default caFile "SYSTEM" makes EVERY wss:// handshake fail with
// an empty error string. We therefore resolve a concrete bundle path ourselves.

namespace {

// Writes `body` to a unique file under the temp dir; returns its path. The
// fixture removes it, so tests never depend on the host distro's real bundle.
std::string writeTempFile(const std::string& stem, const std::string& body) {
  const std::filesystem::path p =
      std::filesystem::temp_directory_path() / ("tls_utils_test_" + stem);
  std::ofstream(p) << body;
  return p.string();
}

}  // namespace

TEST(SystemCaBundle, FirstReadableReturnsFirstExistingCandidate) {
  const std::string real = writeTempFile("bundle_a.crt", "x");
  const std::string missing = "/nonexistent/definitely/not/here.crt";

  EXPECT_EQ(firstReadableCaBundle({missing, real}), real);

  std::filesystem::remove(real);
}

TEST(SystemCaBundle, FirstReadablePrefersEarlierCandidate) {
  const std::string first = writeTempFile("bundle_first.crt", "x");
  const std::string second = writeTempFile("bundle_second.crt", "x");

  EXPECT_EQ(firstReadableCaBundle({first, second}), first);

  std::filesystem::remove(first);
  std::filesystem::remove(second);
}

TEST(SystemCaBundle, FirstReadableReturnsEmptyWhenNoneExist) {
  EXPECT_EQ(firstReadableCaBundle({"/no/such/a.crt", "/no/such/b.crt"}), "");
}

TEST(SystemCaBundle, FirstReadableHandlesEmptyCandidateList) {
  EXPECT_EQ(firstReadableCaBundle({}), "");
}

TEST(SystemCaBundle, CandidateListCoversTheMajorDistroLayouts) {
  const auto& candidates = systemCaBundleCandidates();
  ASSERT_FALSE(candidates.empty());

  const auto has = [&](const std::string& p) {
    return std::find(candidates.begin(), candidates.end(), p) != candidates.end();
  };
  EXPECT_TRUE(has("/etc/ssl/certs/ca-certificates.crt"));           // Debian/Ubuntu
  EXPECT_TRUE(has("/etc/pki/tls/certs/ca-bundle.crt"));             // Fedora/RHEL
  EXPECT_TRUE(has("/etc/ssl/ca-bundle.pem"));                       // openSUSE
  EXPECT_TRUE(has("/etc/ca-certificates/extracted/tls-ca-bundle.pem"));  // Arch
}

// --- chooseTlsCaFile: the exact ix::SocketTLSOptions decision --------------
//
// Encodes the second bug too: ixwebsocket gates mbedtls_ssl_set_hostname() (SNI)
// on `!disable_hostname_validation`, so setting that flag silently stops sending
// SNI. Any SNI-dependent front-end (Tailscale, Cloudflare) then kills the
// handshake with a fatal alert. Insecure mode must therefore rely on caFile
// "NONE" (which already forces MBEDTLS_SSL_VERIFY_NONE) and leave the flag off.

TEST(ChooseTlsCaFile, ExplicitCertPathWins) {
  const auto choice = chooseTlsCaFile("/my/ca.pem", /*allow_insecure=*/false, "/etc/detected.crt");
  EXPECT_EQ(choice.ca_file, "/my/ca.pem");
  EXPECT_FALSE(choice.disable_hostname_validation);
}

TEST(ChooseTlsCaFile, EmptyCertPathUsesDetectedSystemBundle) {
  const auto choice = chooseTlsCaFile("", /*allow_insecure=*/false, "/etc/detected.crt");
  EXPECT_EQ(choice.ca_file, "/etc/detected.crt");
  EXPECT_FALSE(choice.disable_hostname_validation);
}

TEST(ChooseTlsCaFile, FallsBackToSystemKeywordWhenNothingDetected) {
  // macOS/Windows DO implement loadSystemCertificates(), so "SYSTEM" remains the
  // correct last resort rather than a hard failure.
  const auto choice = chooseTlsCaFile("", /*allow_insecure=*/false, "");
  EXPECT_EQ(choice.ca_file, "SYSTEM");
  EXPECT_FALSE(choice.disable_hostname_validation);
}

TEST(ChooseTlsCaFile, InsecureDisablesVerificationButKeepsSni) {
  const auto choice = chooseTlsCaFile("", /*allow_insecure=*/true, "/etc/detected.crt");
  EXPECT_EQ(choice.ca_file, "NONE");
  // The whole point: leaving this false keeps mbedtls_ssl_set_hostname() alive.
  EXPECT_FALSE(choice.disable_hostname_validation);
}

TEST(ChooseTlsCaFile, InsecureOverridesAnExplicitCertPath) {
  const auto choice = chooseTlsCaFile("/my/ca.pem", /*allow_insecure=*/true, "/etc/detected.crt");
  EXPECT_EQ(choice.ca_file, "NONE");
  EXPECT_FALSE(choice.disable_hostname_validation);
}

// --- SSL_CERT_FILE override -----------------------------------------------
//
// The plugin ships inside an AppImage, so it runs on distros whose layout we
// cannot enumerate (NixOS, minimal containers, corporate images). Those set the
// de-facto standard SSL_CERT_FILE instead of shipping a well-known path, so it
// takes precedence over the built-in candidate list.
//
// SSL_CERT_DIR is deliberately NOT honoured: it names a hashed-symlink DIRECTORY,
// and ixwebsocket only ever calls mbedtls_x509_crt_parse_file() on caFile — it
// never calls mbedtls_x509_crt_parse_path(), so a directory would fail to parse.

TEST(SystemCaBundle, SslCertFileEnvWinsOverCandidates) {
  const std::string env_bundle = writeTempFile("env_bundle.crt", "x");
  const std::string candidate = writeTempFile("candidate_bundle.crt", "x");

  EXPECT_EQ(resolveSystemCaBundle(env_bundle, {candidate}), env_bundle);

  std::filesystem::remove(env_bundle);
  std::filesystem::remove(candidate);
}

TEST(SystemCaBundle, UnsetSslCertFileFallsBackToCandidates) {
  const std::string candidate = writeTempFile("fallback_bundle.crt", "x");

  EXPECT_EQ(resolveSystemCaBundle("", {candidate}), candidate);

  std::filesystem::remove(candidate);
}

TEST(SystemCaBundle, UnreadableSslCertFileFallsBackRatherThanBreaking) {
  // A stale/incorrect SSL_CERT_FILE must not take the whole client down when a
  // perfectly good system bundle is present.
  const std::string candidate = writeTempFile("still_good.crt", "x");

  EXPECT_EQ(resolveSystemCaBundle("/no/such/env.crt", {candidate}), candidate);

  std::filesystem::remove(candidate);
}

TEST(SystemCaBundle, ReturnsEmptyWhenEnvAndCandidatesAllMissing) {
  EXPECT_EQ(resolveSystemCaBundle("/no/such/env.crt", {"/no/such/cand.crt"}), "");
}
