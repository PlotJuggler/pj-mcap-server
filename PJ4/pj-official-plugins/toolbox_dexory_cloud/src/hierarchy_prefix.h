// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// D8 (Plan D Task 8, CLIENT half) — client-side '/'-prefix hierarchy.
//
// Plan D Task 8 envisioned an additive QTreeWidget browsing GCS prefixes. The
// as-built host renderer (PJ::PanelEngine + widget_binding.cpp) has NO
// QTreeWidget binding (the qobject_cast whitelist supports QComboBox /
// QTableWidget / QListWidget / … but not QTreeWidget — verified in
// /home/gn/ws/PJ4/pj_dialog_host/src/widget_binding.cpp). A QTreeWidget would
// instantiate visually but could neither be populated nor harvested over the
// WidgetData vtable. ADAPTATION (recorded deviation): a prefix-filter combo
// over the existing seqTable, populated from the distinct top-level '/'-prefixes
// of the sequence (object-key) names. Selecting a prefix narrows the visible
// rows; "All" shows everything.
//
// This unit is the PURE derivation logic (no Qt, header-only) so it is
// hermetically unit-testable. The dialog wires it into the existing
// setItems/setVisibleRows substrate only when caps.supports_file_hierarchy.
//
// IMPORTANT: the hierarchy derives from the SEQUENCE (object-key) NAMES, never
// from topic names. Topic names like "/nissan/gps/duro/imu" contain '/' but are
// out of scope (deriving from them would wrongly flip the flat Dexory corpus
// into hierarchy mode). The server's supports_file_hierarchy flag (derived from
// the s3_key '/' test) is the gate; this is the client rendering of it.

#pragma once

#include <set>
#include <string>
#include <vector>

namespace dexory_cloud {

// The combo's sentinel first item: selecting it disables the prefix narrowing
// (all rows visible, subject to the other filters). Kept ASCII.
inline constexpr const char* kAllPrefixesLabel = "All";

// Derive the sorted, de-duplicated set of top-level '/'-prefixes from a list of
// object-key (sequence) names. A name "a/b/c.mcap" contributes the prefix "a/";
// a name with no '/' contributes nothing (it is a flat/root-level entry). The
// prefix retains its trailing '/' so it is unambiguous and so a row test is a
// simple starts-with. Example:
//   {"runA/x.mcap", "runA/y.mcap", "runB/z.mcap", "loose.mcap"}
//     -> {"runA/", "runB/"}
[[nodiscard]] inline std::vector<std::string> deriveTopLevelPrefixes(const std::vector<std::string>& names) {
  std::set<std::string> prefixes;
  for (const auto& name : names) {
    const auto slash = name.find('/');
    if (slash != std::string::npos) {
      prefixes.insert(name.substr(0, slash + 1));  // include the trailing '/'
    }
  }
  return std::vector<std::string>(prefixes.begin(), prefixes.end());
}

// Build the combo item list: the "All" sentinel followed by the derived
// prefixes (sorted). Empty derived set still yields {"All"} so the combo is
// never empty when shown (though the dialog hides it when hierarchy is off).
[[nodiscard]] inline std::vector<std::string> buildPrefixComboItems(const std::vector<std::string>& names) {
  std::vector<std::string> items;
  items.reserve(1);
  items.emplace_back(kAllPrefixesLabel);
  for (auto& p : deriveTopLevelPrefixes(names)) {
    items.push_back(std::move(p));
  }
  return items;
}

// True when `name` belongs under the selected `prefix`. An empty prefix or the
// "All" sentinel matches everything (no narrowing). Otherwise a simple
// starts-with on the object-key name.
[[nodiscard]] inline bool nameUnderPrefix(const std::string& name, const std::string& prefix) {
  if (prefix.empty() || prefix == kAllPrefixesLabel) {
    return true;
  }
  return name.size() >= prefix.size() && name.compare(0, prefix.size(), prefix) == 0;
}

// Intersect a prefix selection with an already-computed visible-row set: from
// `base_visible` (row indices the name/date/Lua filters left visible) keep only
// the rows whose name is under `prefix`. `names` is indexed by row. An empty /
// "All" prefix returns `base_visible` unchanged. This composes the hierarchy
// narrowing ON TOP OF the existing filters rather than replacing them.
[[nodiscard]] inline std::vector<int> applyPrefixToVisible(const std::vector<int>& base_visible,
                                                           const std::vector<std::string>& names,
                                                           const std::string& prefix) {
  if (prefix.empty() || prefix == kAllPrefixesLabel) {
    return base_visible;
  }
  std::vector<int> out;
  out.reserve(base_visible.size());
  for (int row : base_visible) {
    if (row >= 0 && static_cast<std::size_t>(row) < names.size() && nameUnderPrefix(names[static_cast<std::size_t>(row)], prefix)) {
      out.push_back(row);
    }
  }
  return out;
}

}  // namespace dexory_cloud
