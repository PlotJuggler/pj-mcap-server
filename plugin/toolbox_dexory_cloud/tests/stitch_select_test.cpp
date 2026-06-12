// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Hermetic unit tests for the stitched-selection merge (Slice 7, unified plan
// §3.3 "Correction A"; Plan D Task 3). Pure free functions over SelInput — NO
// transport object is touched, so "no transport call" is guaranteed structurally
// (the dialog-level guard that calls these before postCommand is documented;
// exercising it would need the panel harness, out of scope here).

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "stitch_select.h"

namespace {

using dexory_cloud::buildStitchedSelection;
using dexory_cloud::SelInput;
using dexory_cloud::StitchedSelection;
using dexory_cloud::validateNonOverlapping;

// Two disjoint, time-ordered nissan files (zeg_2 precedes zeg_3). Counts mirror
// the live ground truth pinned in smoke.sh / the live tests.
SelInput zeg2() { return {"nissan_zala_50_zeg_2_0.mcap", 1000, 2000, 4321, 43301}; }
SelInput zeg3() { return {"nissan_zala_50_zeg_3_0.mcap", 2000, 3000, 1234, 21731}; }

}  // namespace

// Plan D Task 3: a REORDERED selection yields the SAME request inputs. The
// ordered_names + union bounds are identical whether the rows were selected
// [zeg_2, zeg_3] or [zeg_3, zeg_2] — so the resolved file_ids order, and thus
// the OpenFresh request, are byte-identical regardless of selection order.
TEST(StitchSelect, ReorderYieldsIdenticalRequest) {
  const StitchedSelection a = buildStitchedSelection({zeg2(), zeg3()});
  const StitchedSelection b = buildStitchedSelection({zeg3(), zeg2()});

  // ordered_names is sorted by (min_ts, name) — order-insensitive.
  ASSERT_EQ(a.ordered_names.size(), 2u);
  EXPECT_EQ(a.ordered_names, b.ordered_names);
  EXPECT_EQ(a.ordered_names[0], "nissan_zala_50_zeg_2_0.mcap");
  EXPECT_EQ(a.ordered_names[1], "nissan_zala_50_zeg_3_0.mcap");

  // Union bounds + sums identical across selection order.
  EXPECT_EQ(a.union_min_ts_ns, b.union_min_ts_ns);
  EXPECT_EQ(a.union_max_ts_ns, b.union_max_ts_ns);
  EXPECT_EQ(a.union_min_ts_ns, 1000);
  EXPECT_EQ(a.union_max_ts_ns, 3000);
  EXPECT_EQ(a.total_size_bytes, 4321 + 1234);
  EXPECT_EQ(a.total_message_count, 43301 + 21731);  // 65032

  // display_name: "first (+N-1 more)" for N>1, identical across order.
  EXPECT_EQ(a.display_name, b.display_name);
  EXPECT_EQ(a.display_name, "nissan_zala_50_zeg_2_0.mcap (+1 more)");
}

// A single selection is byte-identical to the pre-Slice-7 single-sequence path:
// display_name == the single name; ordered_names == {name}.
TEST(StitchSelect, SingleSelectionDisplayName) {
  const StitchedSelection s = buildStitchedSelection({zeg2()});
  ASSERT_EQ(s.ordered_names.size(), 1u);
  EXPECT_EQ(s.ordered_names[0], "nissan_zala_50_zeg_2_0.mcap");
  EXPECT_EQ(s.display_name, "nissan_zala_50_zeg_2_0.mcap");
  EXPECT_EQ(s.union_min_ts_ns, 1000);
  EXPECT_EQ(s.union_max_ts_ns, 2000);
  EXPECT_EQ(s.total_message_count, 43301);
}

TEST(StitchSelect, EmptySelectionIsEmpty) {
  const StitchedSelection s = buildStitchedSelection({});
  EXPECT_TRUE(s.ordered_names.empty());
  EXPECT_TRUE(s.display_name.empty());
  EXPECT_EQ(s.union_min_ts_ns, 0);
  EXPECT_EQ(s.union_max_ts_ns, 0);
}

// More than two files: ordered by start time, "(+N-1 more)" reflects N.
TEST(StitchSelect, ThreeFilesOrderedByStart) {
  SelInput a{"c.mcap", 3000, 4000, 1, 1};
  SelInput b{"a.mcap", 1000, 2000, 1, 1};
  SelInput c{"b.mcap", 2000, 3000, 1, 1};
  const StitchedSelection s = buildStitchedSelection({a, b, c});
  ASSERT_EQ(s.ordered_names.size(), 3u);
  EXPECT_EQ(s.ordered_names[0], "a.mcap");
  EXPECT_EQ(s.ordered_names[1], "b.mcap");
  EXPECT_EQ(s.ordered_names[2], "c.mcap");
  EXPECT_EQ(s.display_name, "a.mcap (+2 more)");
}

// validateNonOverlapping: disjoint half-open ranges are OK (empty string). The
// nissan zeg_2 [1000,2000) / zeg_3 [2000,3000) abut exactly — non-overlapping.
TEST(StitchSelect, DisjointRangesValidate) {
  EXPECT_EQ(validateNonOverlapping({zeg2(), zeg3()}), "");
  EXPECT_EQ(validateNonOverlapping({zeg3(), zeg2()}), "");  // order-insensitive
}

// A single (or empty) selection can never overlap.
TEST(StitchSelect, SingleNeverOverlaps) {
  EXPECT_EQ(validateNonOverlapping({zeg2()}), "");
  EXPECT_EQ(validateNonOverlapping({}), "");
}

// Overlapping ranges yield a per-pair message naming both files. [1000,2500)
// overlaps [2000,3000) because 2000 < 2500.
TEST(StitchSelect, OverlapDetected) {
  SelInput a{"early.mcap", 1000, 2500, 1, 1};
  SelInput b{"late.mcap", 2000, 3000, 1, 1};
  const std::string msg = validateNonOverlapping({a, b});
  EXPECT_FALSE(msg.empty());
  EXPECT_NE(msg.find("early.mcap"), std::string::npos);
  EXPECT_NE(msg.find("late.mcap"), std::string::npos);
  EXPECT_NE(msg.find("overlap"), std::string::npos);
  // Order-insensitive: same verdict regardless of input order.
  EXPECT_FALSE(validateNonOverlapping({b, a}).empty());
}

// Exact-abut [min,max) is the boundary: end==next.start is NOT an overlap
// (half-open), while end==next.start+1 IS.
TEST(StitchSelect, HalfOpenBoundary) {
  EXPECT_EQ(validateNonOverlapping({{"a", 0, 100, 0, 0}, {"b", 100, 200, 0, 0}}), "");
  EXPECT_FALSE(validateNonOverlapping({{"a", 0, 101, 0, 0}, {"b", 100, 200, 0, 0}}).empty());
}
