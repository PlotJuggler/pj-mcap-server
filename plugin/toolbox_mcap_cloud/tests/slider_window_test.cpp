// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Hermetic unit tests for the slider->nanoseconds window mapping. The bug this
// pins: the naive `union_min + span*pos/steps` overflowed int64 for multi-hour
// aggregate spans (span*pos before the divide), wrapping the window NEGATIVE so
// the server matched zero chunks ("[Nx] no messages in the selected time
// range") — single-file fetches never overflowed, which masked it.

#include <gtest/gtest.h>

#include <cstdint>

#include "slider_window.h"

namespace {

using mcap_cloud::sliderToWindow;
using mcap_cloud::SliderWindow;

constexpr int kSteps = 1'000'000;

// Real S3-use-case staging numbers: 34 contiguous files, ~7.35h union. union_min is
// the first file's start; span ~2.65e13 ns. The persisted middle band that
// triggered the report.
constexpr std::int64_t kAggMin = 1779209029609533992LL;
constexpr std::int64_t kAggMax = 1779235502659755586LL;  // ~7.35h later

}  // namespace

// THE REGRESSION: a multi-hour aggregate span must produce a window INSIDE the
// union, not a negative-wrapped one before it. With the old int64 multiply,
// span*624146 (~1.65e19) overflowed INT64_MAX and start landed below union_min.
TEST(SliderWindow, MultiHourAggregateDoesNotOverflow) {
  const SliderWindow w = sliderToWindow(kAggMin, kAggMax, 502278, 624146, kSteps);
  ASSERT_TRUE(w.has_window);
  // Both bounds strictly inside the union and correctly ordered.
  EXPECT_GE(w.start_ns, kAggMin) << "start wrapped below union_min (the overflow bug)";
  EXPECT_LE(w.end_ns, kAggMax + 1);
  EXPECT_LT(w.start_ns, w.end_ns);
  // Exact proportional positions (computed in 128-bit, then verified here in
  // 128-bit so the expectation itself can't overflow).
  const auto expect_offset = [](int pos) -> std::int64_t {
    return static_cast<std::int64_t>(static_cast<__int128>(kAggMax - kAggMin) * pos / kSteps);
  };
  EXPECT_EQ(w.start_ns, kAggMin + expect_offset(502278));
  EXPECT_EQ(w.end_ns, kAggMin + expect_offset(624146));
}

// Single ~13-min file: the old code worked here (no overflow); the new code
// must match it exactly.
TEST(SliderWindow, SingleFileUnchanged) {
  constexpr std::int64_t fmin = 1779209029609533992LL;
  constexpr std::int64_t fmax = 1779209780845898011LL;  // ~751s span
  const SliderWindow w = sliderToWindow(fmin, fmax, 502278, 624146, kSteps);
  ASSERT_TRUE(w.has_window);
  const std::int64_t span = fmax - fmin;
  EXPECT_EQ(w.start_ns, fmin + span * 502278 / kSteps);  // no overflow at this span
  EXPECT_EQ(w.end_ns, fmin + span * 624146 / kSteps);
  EXPECT_GT(w.start_ns, fmin);
  EXPECT_LT(w.end_ns, fmax);
}

// Full-range upper handle extends one tick past union_max (half-open [start,end)
// must include the final frame).
TEST(SliderWindow, FullUpperHandleExtendsPastMax) {
  const SliderWindow w = sliderToWindow(kAggMin, kAggMax, 0, kSteps, kSteps);
  ASSERT_TRUE(w.has_window);
  EXPECT_EQ(w.start_ns, kAggMin);
  EXPECT_EQ(w.end_ns, kAggMax + 1);
}

// Degenerate union -> the (0,0) whole-range sentinel.
TEST(SliderWindow, DegenerateUnionIsWholeRangeSentinel) {
  EXPECT_FALSE(sliderToWindow(100, 100, 0, kSteps, kSteps).has_window);
  EXPECT_FALSE(sliderToWindow(200, 100, 0, kSteps, kSteps).has_window);
  const SliderWindow w = sliderToWindow(100, 100, 0, kSteps, kSteps);
  EXPECT_EQ(w.start_ns, 0);
  EXPECT_EQ(w.end_ns, 0);
}

// Worst case: maximum plausible span (a full day) at the maximum sub-range
// handle still stays ordered and in-union — proves the 128-bit path holds well
// past the int64 overflow threshold (~2.56h).
TEST(SliderWindow, FullDaySpanStaysInUnion) {
  constexpr std::int64_t dmin = 1779000000000000000LL;
  constexpr std::int64_t dmax = dmin + 86'400LL * 1'000'000'000LL;  // +24h
  const SliderWindow w = sliderToWindow(dmin, dmax, 999998, 999999, kSteps);
  ASSERT_TRUE(w.has_window);
  EXPECT_GE(w.start_ns, dmin);
  EXPECT_LE(w.end_ns, dmax);
  EXPECT_LT(w.start_ns, w.end_ns);
}
