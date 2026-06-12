// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

// Pure mapping from the RangeSlider's proportional handle positions
// [0, slider_steps] onto an absolute nanosecond window over the selected
// stitched union [union_min_ns, union_max_ns].
//
// OVERFLOW SAFETY (the whole reason this is a separate, tested helper): the
// naive `union_min + span * lower / slider_steps` evaluates `span * lower` in
// int64 BEFORE the divide. For a multi-hour aggregate union (span ~2.6e13 ns
// over ~7.4h) times a slider position up to slider_steps=1e6, the product
// (~2.6e19) overflows INT64_MAX (9.2e18) and wraps NEGATIVE — the window then
// lands BEFORE all data and the server plan intersects zero chunks ("[Nx] no
// messages in the selected time range"). A single ~13-min file never overflows,
// which is exactly why single-file fetches worked and large aggregates did not.
// The proportional offset is computed in 128-bit here so it is exact for any
// realistic span; only the small offset (< span) is widened — union_min stays
// int64 (it is ~1.78e18 and would lose nanosecond precision as a double).

namespace dexory_cloud {

struct SliderWindow {
  std::int64_t start_ns = 0;
  std::int64_t end_ns = 0;
  // True when a real sub-window was produced. When false (degenerate union),
  // start/end are (0,0) — the caller's "whole stitched range" sentinel.
  bool has_window = false;
};

// lower/upper are slider units in [0, slider_steps]. The retrieval window is
// half-open [start, end): when the upper handle is pinned to slider_steps the
// end is extended one tick past union_max so the final frame is not dropped.
// A degenerate union (union_max <= union_min) yields (0,0)/has_window=false.
inline SliderWindow sliderToWindow(std::int64_t union_min_ns, std::int64_t union_max_ns, int lower, int upper,
                                   int slider_steps) {
  SliderWindow out;
  if (union_max_ns <= union_min_ns || slider_steps <= 0) {
    return out;  // (0,0): whole-range sentinel
  }
  const std::int64_t span = union_max_ns - union_min_ns;
  // 128-bit intermediate: span (<=~int64) * lower (<=1e6) cannot overflow.
  const auto offset = [&](int pos) -> std::int64_t {
    return static_cast<std::int64_t>(static_cast<__int128>(span) * pos / slider_steps);
  };
  out.start_ns = union_min_ns + offset(lower);
  out.end_ns = (upper >= slider_steps) ? union_max_ns + 1 : union_min_ns + offset(upper);
  out.has_window = true;
  return out;
}

}  // namespace dexory_cloud
