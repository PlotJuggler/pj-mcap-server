// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "source_descriptor.hpp"

#include <limits>
#include <nlohmann/json.hpp>

#include "core/sha256.h"

namespace mcap_cloud {

namespace {

constexpr const char* kKind = "mcap-cloud-session";

// The complete field allowlist — a descriptor carrying ANYTHING else (a
// token, a cert path, a future field) is rejected outright rather than
// silently ignored: unknown fields are the credential-smuggling and
// forward-compat hazard the spec's §7 layer 1 closes.
constexpr const char* kAllowedFields[] = {"v",        "kind",     "server_uri",
                                          "s3_keys",  "topics",   "start_ns",
                                          "end_ns",   "include_latched",
                                          "display_name"};

bool fail(std::string* error, std::string message) {
  if (error != nullptr) {
    *error = std::move(message);
  }
  return false;
}

// Strict decimal-digit parse of an epoch-nanosecond string ("0" = unset).
// No sign, no whitespace, no exponent — the wire format is decimal strings
// precisely so 64-bit values survive JSON without double rounding.
bool parseDecimalNs(const std::string& text, std::int64_t* out) {
  if (text.empty() || text.size() > 20) {
    return false;
  }
  std::int64_t value = 0;
  for (const char c : text) {
    if (c < '0' || c > '9') {
      return false;
    }
    const int digit = c - '0';
    if (value > (std::numeric_limits<std::int64_t>::max() - digit) / 10) {
      return false;  // would overflow int64
    }
    value = value * 10 + digit;
  }
  *out = value;
  return true;
}

// Minimal URI hygiene for the descriptor (spec §4: reject userinfo, query,
// fragment outright; ws/wss only). Full origin comparison is a separate
// module — this only enforces what makes a descriptor unsafe to embed.
bool validateServerUri(const std::string& uri, std::string* error) {
  std::size_t authority_start = 0;
  if (uri.rfind("ws://", 0) == 0) {
    authority_start = 5;
  } else if (uri.rfind("wss://", 0) == 0) {
    authority_start = 6;
  } else {
    return fail(error, "server_uri scheme must be ws:// or wss://");
  }
  if (uri.find('?') != std::string::npos) {
    return fail(error, "server_uri must not contain a query string");
  }
  if (uri.find('#') != std::string::npos) {
    return fail(error, "server_uri must not contain a fragment");
  }
  const std::size_t authority_end = uri.find('/', authority_start);
  const std::size_t authority_len =
      (authority_end == std::string::npos ? uri.size() : authority_end) - authority_start;
  if (uri.substr(authority_start, authority_len).find('@') != std::string::npos) {
    return fail(error, "server_uri must not contain userinfo");
  }
  return true;
}

// Require `field` to be a string no longer than kMaxStringBytes; copy it out.
bool takeString(const nlohmann::json& obj, const char* field, std::string* out,
                std::string* error) {
  const auto it = obj.find(field);
  if (it == obj.end()) {
    return fail(error, std::string("missing field \"") + field + "\"");
  }
  if (!it->is_string()) {
    return fail(error, std::string("field \"") + field + "\" must be a string");
  }
  *out = it->get<std::string>();
  if (out->size() > kMaxStringBytes) {
    return fail(error, std::string("field \"") + field + "\" exceeds the " +
                           std::to_string(kMaxStringBytes) + "-byte string limit");
  }
  return true;
}

// Require `field` to be an array of <= max_entries strings, each within the
// string limit; copy it out.
bool takeStringArray(const nlohmann::json& obj, const char* field, std::size_t max_entries,
                     std::vector<std::string>* out, std::string* error) {
  const auto it = obj.find(field);
  if (it == obj.end()) {
    return fail(error, std::string("missing field \"") + field + "\"");
  }
  if (!it->is_array()) {
    return fail(error, std::string("field \"") + field + "\" must be an array of strings");
  }
  if (it->size() > max_entries) {
    return fail(error, std::string(field) + " exceeds the " + std::to_string(max_entries) +
                           "-entry limit");
  }
  out->clear();
  out->reserve(it->size());
  for (const auto& entry : *it) {
    if (!entry.is_string()) {
      return fail(error, std::string("field \"") + field + "\" must be an array of strings");
    }
    std::string value = entry.get<std::string>();
    if (value.size() > kMaxStringBytes) {
      return fail(error, std::string("field \"") + field + "\" entry exceeds the " +
                             std::to_string(kMaxStringBytes) + "-byte string limit");
    }
    out->push_back(std::move(value));
  }
  return true;
}

bool takeNs(const nlohmann::json& obj, const char* field, std::int64_t* out,
            std::string* error) {
  std::string text;
  if (!takeString(obj, field, &text, error)) {
    return false;
  }
  if (!parseDecimalNs(text, out)) {
    return fail(error, std::string(field) + " is not a decimal nanosecond string");
  }
  return true;
}

}  // namespace

std::optional<SourceDescriptor> parseSourceDescriptor(std::string_view json,
                                                       std::string* error) {
  if (json.size() > kMaxDescriptorBytes) {
    fail(error, "descriptor exceeds the " + std::to_string(kMaxDescriptorBytes) +
                    "-byte limit");
    return std::nullopt;
  }
  const auto obj = nlohmann::json::parse(json, /*cb=*/nullptr, /*allow_exceptions=*/false);
  if (obj.is_discarded()) {
    fail(error, "descriptor is not valid JSON");
    return std::nullopt;
  }
  if (!obj.is_object()) {
    fail(error, "descriptor is not a JSON object");
    return std::nullopt;
  }

  // Allowlist BEFORE field extraction: an unknown field is rejected even when
  // everything required is present and valid.
  for (const auto& [key, value] : obj.items()) {
    (void)value;
    bool allowed = false;
    for (const char* candidate : kAllowedFields) {
      if (key == candidate) {
        allowed = true;
        break;
      }
    }
    if (!allowed) {
      fail(error, "unknown field \"" + key + "\"");
      return std::nullopt;
    }
  }

  SourceDescriptor d;

  const auto v_it = obj.find("v");
  if (v_it == obj.end()) {
    fail(error, "missing field \"v\"");
    return std::nullopt;
  }
  if (!v_it->is_number_integer()) {
    fail(error, "field \"v\" must be an integer");
    return std::nullopt;
  }
  d.version = v_it->get<int>();
  if (d.version != 1) {
    fail(error, "unsupported descriptor version " + std::to_string(d.version) +
                    " (expected 1)");
    return std::nullopt;
  }

  if (!takeString(obj, "kind", &d.kind, error)) {
    return std::nullopt;
  }
  if (d.kind != kKind) {
    fail(error, "unsupported kind \"" + d.kind + "\" (expected \"" + kKind + "\")");
    return std::nullopt;
  }

  if (!takeString(obj, "server_uri", &d.server_uri, error) ||
      !validateServerUri(d.server_uri, error)) {
    return std::nullopt;
  }
  if (!takeStringArray(obj, "s3_keys", kMaxKeys, &d.s3_keys, error) ||
      !takeStringArray(obj, "topics", kMaxTopics, &d.topics, error)) {
    return std::nullopt;
  }
  if (!takeNs(obj, "start_ns", &d.start_ns, error) ||
      !takeNs(obj, "end_ns", &d.end_ns, error)) {
    return std::nullopt;
  }

  const auto latched_it = obj.find("include_latched");
  if (latched_it == obj.end()) {
    fail(error, "missing field \"include_latched\"");
    return std::nullopt;
  }
  if (!latched_it->is_boolean()) {
    fail(error, "field \"include_latched\" must be a boolean");
    return std::nullopt;
  }
  d.include_latched = latched_it->get<bool>();

  // display_name is optional: the canonical form omits it, and a canonical
  // string must itself parse (dedup/cache comparison feeds it back in).
  if (obj.contains("display_name")) {
    if (!takeString(obj, "display_name", &d.display_name, error)) {
      return std::nullopt;
    }
  }

  // "0"/"0" is the whole-range sentinel; any other end < start is nonsense.
  if (d.end_ns < d.start_ns) {
    fail(error, "end_ns " + std::to_string(d.end_ns) + " is before start_ns " +
                    std::to_string(d.start_ns));
    return std::nullopt;
  }
  return d;
}

std::string canonicalSourceDescriptorJson(const SourceDescriptor& d) {
  // ordered_json populated in ALPHABETICAL key order EXPLICITLY — the insert
  // order below IS the cross-repo contract (vectors file), never an artifact
  // of map ordering. display_name is deliberately absent (identity excludes
  // it: a rename is not a collision).
  nlohmann::ordered_json j;
  j["end_ns"] = std::to_string(d.end_ns);
  j["include_latched"] = d.include_latched;
  j["kind"] = d.kind;
  j["s3_keys"] = d.s3_keys;
  j["server_uri"] = d.server_uri;
  j["start_ns"] = std::to_string(d.start_ns);
  j["topics"] = d.topics;
  j["v"] = d.version;
  return j.dump();
}

std::string descriptorIdentity(const SourceDescriptor& d) {
  // sha256/128 = the first 128 bits (16 bytes -> 32 lowercase hex chars).
  return "mcap-cloud:v1:sha256/128:" + sha256HexPrefix(canonicalSourceDescriptorJson(d), 16);
}

std::string toSourceDescriptorJson(const SourceDescriptor& d) {
  // Canonical fields + display_name, still in alphabetical insert order
  // ("display_name" sorts first).
  nlohmann::ordered_json j;
  j["display_name"] = d.display_name;
  j["end_ns"] = std::to_string(d.end_ns);
  j["include_latched"] = d.include_latched;
  j["kind"] = d.kind;
  j["s3_keys"] = d.s3_keys;
  j["server_uri"] = d.server_uri;
  j["start_ns"] = std::to_string(d.start_ns);
  j["topics"] = d.topics;
  j["v"] = d.version;
  return j.dump();
}

}  // namespace mcap_cloud
