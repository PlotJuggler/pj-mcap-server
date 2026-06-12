// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// stitch_select — the file_ids[]-vs-sequence_name selection-model adapter for
// the multi-file (stitched) selection of Slice 7 (unified plan §3.3 "Correction
// A"). A multi-recording selection is presented as ONE synthetic stitched
// SequenceRecord (time range = union, topics = union elsewhere) and the pull
// maps to a SINGLE OpenFresh{file_ids[], topic_names[], time_range}.
//
// Pure free functions (no Qt / no transport link), header-only — same idiom as
// fetch_summary.h / name_filter.h / date_filter.h — so the merge / ordering /
// reorder-identical-request / overlap-validation behavior is unit-testable.
#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace dexory_cloud {

// One selected sequence's catalog facts, the merge's input. Mirrors the fields
// of a SequenceRecord the dialog already holds (name + min/max ts + size +
// message count from the "message_count" metadata key).
struct SelInput {
  std::string name;
  std::int64_t min_ts_ns = 0;
  std::int64_t max_ts_ns = 0;
  std::int64_t size_bytes = 0;
  std::int64_t message_count = 0;
};

// The synthetic stitched record derived from a multi-file selection, plus the
// deterministically-ordered name list the OpenFresh request resolves to. The
// ordering is STABLE across selection order so a reordered seqTable selection
// ([B, A] vs [A, B]) yields the SAME ordered_names — and thus the SAME resolved
// file_ids order and the SAME OpenFresh request (Plan D Task 3 parity).
struct StitchedSelection {
  std::vector<std::string> ordered_names;  // sorted by (min_ts_ns, then name)
  std::int64_t union_min_ts_ns = 0;
  std::int64_t union_max_ts_ns = 0;
  std::int64_t total_size_bytes = 0;
  std::int64_t total_message_count = 0;
  std::string display_name;  // "first (+N-1 more)" for N>1; the single name for N==1
};

// Build the synthetic stitched record + ordered name list from the selected
// sequences. Empty input yields an empty result. The order is (min_ts_ns, name)
// so it is independent of the order the rows were selected/clicked in.
inline StitchedSelection buildStitchedSelection(const std::vector<SelInput>& selected) {
  StitchedSelection out;
  if (selected.empty()) {
    return out;
  }

  // Order by (min_ts_ns, name) — stable across selection order.
  std::vector<SelInput> ordered = selected;
  std::sort(ordered.begin(), ordered.end(), [](const SelInput& a, const SelInput& b) {
    if (a.min_ts_ns != b.min_ts_ns) {
      return a.min_ts_ns < b.min_ts_ns;
    }
    return a.name < b.name;
  });

  out.ordered_names.reserve(ordered.size());
  bool have_min = false;
  for (const SelInput& s : ordered) {
    out.ordered_names.push_back(s.name);
    if (s.min_ts_ns > 0 && (!have_min || s.min_ts_ns < out.union_min_ts_ns)) {
      out.union_min_ts_ns = s.min_ts_ns;
      have_min = true;
    }
    if (s.max_ts_ns > out.union_max_ts_ns) {
      out.union_max_ts_ns = s.max_ts_ns;
    }
    out.total_size_bytes += s.size_bytes;
    out.total_message_count += s.message_count;
  }

  if (ordered.size() == 1) {
    out.display_name = ordered.front().name;
  } else {
    out.display_name = ordered.front().name + " (+" + std::to_string(ordered.size() - 1) + " more)";
  }
  return out;
}

// Pairwise non-overlap pre-validation on half-open [min, max) ranges, mirroring
// the server (BuildPlan sorts by StartTimeNs then requires
// ordered[i].StartTimeNs >= ordered[i-1].EndTimeNs, plan.go:103-113). Returns
// an empty string when the selection is orderable + non-overlapping; otherwise
// a per-pair message: "files 'A' and 'B' overlap in time". The server check
// stays authoritative (BuildPlan errOverlap -> Error{INVALID_REQUEST}); this is
// the fast client-side UX guard (design spec §6.3).
inline std::string validateNonOverlapping(const std::vector<SelInput>& selected) {
  if (selected.size() < 2) {
    return {};
  }
  std::vector<SelInput> ordered = selected;
  std::sort(ordered.begin(), ordered.end(), [](const SelInput& a, const SelInput& b) {
    if (a.min_ts_ns != b.min_ts_ns) {
      return a.min_ts_ns < b.min_ts_ns;
    }
    return a.name < b.name;
  });
  for (std::size_t i = 1; i < ordered.size(); ++i) {
    // Half-open ranges: [min, max). ordered[i] must start at or after the
    // previous file's end, else the two ranges overlap.
    if (ordered[i].min_ts_ns < ordered[i - 1].max_ts_ns) {
      return "files '" + ordered[i - 1].name + "' and '" + ordered[i].name + "' overlap in time";
    }
  }
  return {};
}

}  // namespace dexory_cloud
