// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "origin_match.hpp"

#include <cctype>

namespace mcap_cloud {

namespace {

std::string toLower(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return out;
}

// Strict decimal port: non-empty, all digits, <= 65535. Anything else (empty
// after ':', letters, overflow) is unparsable -> the whole URI is rejected.
std::optional<std::uint16_t> parsePort(std::string_view s) {
  if (s.empty() || s.size() > 5) {
    return std::nullopt;
  }
  std::uint32_t value = 0;
  for (const char c : s) {
    if (c < '0' || c > '9') {
      return std::nullopt;
    }
    value = value * 10 + static_cast<std::uint32_t>(c - '0');
  }
  if (value > 65535) {
    return std::nullopt;
  }
  return static_cast<std::uint16_t>(value);
}

}  // namespace

std::optional<Origin> parseWsOrigin(std::string_view uri) {
  // Query/fragment are rejected ANYWHERE in the URI, not just in the
  // authority: an origin has no use for either, and a token-bearing query
  // string must never ride into a credential-release comparison.
  if (uri.find('?') != std::string_view::npos || uri.find('#') != std::string_view::npos) {
    return std::nullopt;
  }
  const auto scheme_end = uri.find("://");
  if (scheme_end == std::string_view::npos) {
    return std::nullopt;
  }
  Origin origin;
  origin.scheme = toLower(uri.substr(0, scheme_end));
  if (origin.scheme != "ws" && origin.scheme != "wss") {
    return std::nullopt;
  }
  const std::string_view rest = uri.substr(scheme_end + 3);
  // authority = everything up to the path (a path is allowed and ignored).
  const std::string_view authority = rest.substr(0, rest.find('/'));
  if (authority.empty() || authority.find('@') != std::string_view::npos) {
    return std::nullopt;  // empty host / userinfo (credentials in a URI)
  }
  // Split host[:port]. Bracketed IPv6 gets a minimal split on the ']' so the
  // address's own colons can't be misread as a port separator; an unbracketed
  // authority with more than one ':' is ambiguous and rejected.
  std::string_view host;
  std::optional<std::string_view> port_str;  // nullopt = no ':' -> default port
  if (authority.front() == '[') {
    const auto close = authority.find(']');
    if (close == std::string_view::npos) {
      return std::nullopt;
    }
    host = authority.substr(0, close + 1);  // keep the brackets: they are part of the identity
    const std::string_view after = authority.substr(close + 1);
    if (!after.empty()) {
      if (after.front() != ':') {
        return std::nullopt;
      }
      port_str = after.substr(1);
    }
  } else {
    const auto colon = authority.find(':');
    if (colon == std::string_view::npos) {
      host = authority;
    } else {
      host = authority.substr(0, colon);
      port_str = authority.substr(colon + 1);
      if (port_str->find(':') != std::string_view::npos) {
        return std::nullopt;
      }
    }
  }
  if (host.empty()) {
    return std::nullopt;
  }
  origin.host = toLower(host);
  if (port_str.has_value()) {
    const auto port = parsePort(*port_str);
    if (!port.has_value()) {
      return std::nullopt;
    }
    origin.port = *port;
  } else {
    origin.port = (origin.scheme == "wss") ? std::uint16_t{443} : std::uint16_t{80};
  }
  return origin;
}

bool sameWsOrigin(std::string_view a, std::string_view b) {
  const auto oa = parseWsOrigin(a);
  const auto ob = parseWsOrigin(b);
  // Fail CLOSED: an unparsable side never matches — not even the identical
  // string (string equality is NOT origin equality).
  return oa.has_value() && ob.has_value() && oa->scheme == ob->scheme && oa->host == ob->host &&
         oa->port == ob->port;
}

}  // namespace mcap_cloud
