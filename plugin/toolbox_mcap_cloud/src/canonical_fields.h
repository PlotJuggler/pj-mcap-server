// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// The 4 canonical S3-key filter fields (customer/customer_site/robot/source)
// and the vocabulary built from them. These are the ONLY filter dimensions:
// the Basic-tab dropdowns, the browse gate and the Advanced (Lua) query all
// reason about this set — never MCAP-content stats.
//
// buildCanonicalSchema exists so the vocabulary is computed in ONE pass per
// catalog change (epoch-cached by the caller) instead of re-parsing every S3
// key per dropdown per widget_data() call: that pattern cost ~1.3 ms per 1000
// files per call, at a 20 Hz tick plus every forwarded widget event.
// Pure + std-only -> hermetically unit-testable (like s3_key_fields.h).

#pragma once

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "s3_key_fields.h"

namespace mcap_cloud {

// The canonical S3-key field keys, with display labels, in display order. This
// stays the full 4-field set even though customer/site are no longer LOCAL
// dropdowns (they moved to the mandatory browse gate): it is ALSO the
// Lua/Advanced-tab vocabulary, which still reasons about customer/site.
inline constexpr std::array<std::pair<const char*, const char*>, 4> kBasicFilterKeys = {{
    {"customer", "Customer"},
    {"customer_site", "Site"},
    {"robot", "Robot"},
    {"source", "Source"},
}};

// Parse an object key and keep ONLY the canonical fields (date/filename and any
// other Hive segment are dropped — they are display data, never a filter).
inline Metadata canonicalFilterFields(std::string_view s3_key) {
  const Metadata all = parseS3KeyFields(s3_key);
  Metadata out;
  for (const auto& kv : kBasicFilterKeys) {
    if (auto it = all.find(kv.first); it != all.end()) {
      out.emplace(std::string(kv.first), it->second);
    }
  }
  return out;
}

// The Advanced-tab query-assist vocabulary: exactly the canonical keys, a
// constant independent of the server.
inline std::vector<std::string> canonicalVocabularyKeys() {
  std::vector<std::string> keys;
  keys.reserve(kBasicFilterKeys.size());
  for (const auto& kv : kBasicFilterKeys) {
    keys.emplace_back(kv.first);
  }
  return keys;
}

// One-pass canonical vocabulary across the records (anything with a `.name`
// object key): canonical field -> SORTED UNIQUE values, honoring the Schema
// type contract ("key -> sorted unique values"). Keys with no values anywhere
// are absent from the result.
template <typename Records>
Schema buildCanonicalSchema(const Records& records) {
  Schema schema;
  for (const auto& rec : records) {
    for (const auto& kv : canonicalFilterFields(rec.name)) {
      schema[kv.first].push_back(kv.second);
    }
  }
  for (auto& [key, values] : schema) {
    std::sort(values.begin(), values.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
  }
  return schema;
}

}  // namespace mcap_cloud
