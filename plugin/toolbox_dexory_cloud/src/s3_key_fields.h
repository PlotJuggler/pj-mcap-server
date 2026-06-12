// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Parse the Dexory-relevant fields out of an S3 object key for the
// "non-advanced" metadata filter. The keys are Hive-partitioned:
//   customer=dexory/customer_site=nashville/robot=arri-182/source=ros-bags/date=2026-05-19/rosbox_….mcap
// which yields exactly the fields the user cares about: each `k=v` path segment
// becomes a (k -> v) entry, and the leaf filename becomes `filename`. Segments
// without an '=' are ignored. This is intentionally generic (any Hive scheme),
// so it degrades sanely on a flat key (no '=' segments -> just `filename`).
//
// Returned type matches the query engine's Metadata (std::map<…,std::less<>>),
// so it slots directly into the schema build + FilterSequence with no
// conversion. Pure + std-only -> hermetically unit-testable.

#pragma once

#include <map>
#include <string>
#include <string_view>

#include "core/types.h"  // Metadata = std::map<std::string, std::string, std::less<>>

namespace dexory_cloud {

// Parse the Hive `k=v` segments + leaf filename of an s3 key into a Metadata
// map. Example above -> {customer, customer_site, date, filename, robot, source}.
inline Metadata parseS3KeyFields(std::string_view key) {
  Metadata out;
  std::string_view leaf;
  std::size_t pos = 0;
  while (pos <= key.size()) {
    const std::size_t slash = key.find('/', pos);
    const std::string_view seg = slash == std::string_view::npos ? key.substr(pos) : key.substr(pos, slash - pos);
    const bool is_leaf = slash == std::string_view::npos;
    if (is_leaf) {
      leaf = seg;
      break;
    }
    // Directory segment: split on the FIRST '=' into k=v.
    const std::size_t eq = seg.find('=');
    if (eq != std::string_view::npos && eq > 0) {
      out.emplace(std::string(seg.substr(0, eq)), std::string(seg.substr(eq + 1)));
    }
    pos = slash + 1;
  }
  if (!leaf.empty()) {
    out.emplace("filename", std::string(leaf));
  }
  return out;
}

}  // namespace dexory_cloud
