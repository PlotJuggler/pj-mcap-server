// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Sequence display-name shortening for the seqTable Name column.
//
// Real Dexory sequence identifiers are full S3 object keys, Hive-partitioned:
//   customer=dexory/customer_site=nashville/robot=arri-182/source=ros-bags/date=2026-05-19/rosbox_2026-05-19_16-43-46.mcap
// which is long and noisy. The displayed name strips it down to just the
// values, slash-separated:
//   dexory/nashville/arri-182/ros-bags/rosbox_2026-05-19_16-43-46.mcap
// Two transforms: every Hive `k=v` directory segment shows only `v` (the `k=`
// prefix is dropped), and the `date=<...>/` segment is dropped entirely because
// the seqTable already has a dedicated "Date" column (sourced from the
// sequence's max timestamp). The leaf filename is always kept verbatim.
//
// CRITICAL: this is DISPLAY-ONLY. The full S3 key remains the sequence identity
// for every backend call (listTopics / resolveFileIds / download / tags) AND for
// the PanelEngine selection harvest (which returns column-0 text). The dialog
// keeps a display->key map and translates the harvested display name back to the
// real key the instant a selection re-enters the plugin. Because two distinct
// keys could in principle strip to the same display, the dialog detects
// collisions and falls back to the full key for the colliding rows (see
// rebuildSeqDisplayLocked) -- so this pure helper only has to do the strip.
//
// Header-only, std-only, so it is hermetically unit-testable.

#pragma once

#include <string>
#include <string_view>

namespace dexory_cloud {

// Build the seqTable display name from a full S3 key: drop the redundant
// `date=<...>/` Hive segment, and strip the `k=` prefix from every other
// `k=v` directory segment so only the values show. A directory segment without
// '=' is kept verbatim; the leaf filename is ALWAYS kept verbatim (never
// prefix-stripped, never dropped). A flat key with no Hive segments therefore
// degrades to itself. The key/value split is on the FIRST '=' (matching
// parseS3KeyFields), so a value may itself contain '='.
inline std::string shortenSequenceName(std::string_view key) {
  static constexpr std::string_view kDateMarker = "date=";
  std::string out;
  out.reserve(key.size());
  std::size_t pos = 0;
  while (pos < key.size()) {
    std::size_t slash = key.find('/', pos);
    bool has_slash = slash != std::string_view::npos;
    std::size_t seg_end = has_slash ? slash : key.size();
    std::string_view segment = key.substr(pos, seg_end - pos);
    if (!has_slash) {
      // Leaf filename: keep verbatim (no strip, no drop).
      out.append(segment.data(), segment.size());
      pos = seg_end;
      break;
    }
    // Directory segment. Drop a `date=...` component outright; otherwise show
    // only the value of a `k=v` component (keep non-`k=v` segments as-is).
    if (segment.starts_with(kDateMarker)) {
      pos = slash + 1;  // skip the segment and its trailing '/'
      continue;
    }
    if (std::size_t eq = segment.find('='); eq != std::string_view::npos && eq > 0) {
      segment = segment.substr(eq + 1);  // value only
    }
    out.append(segment.data(), segment.size());
    out.push_back('/');
    pos = slash + 1;
  }
  return out;
}

}  // namespace dexory_cloud
