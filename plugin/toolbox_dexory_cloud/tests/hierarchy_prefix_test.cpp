// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// D8 (Plan D Task 8, CLIENT half) — hermetic tests for the client-side
// '/'-prefix hierarchy derivation that drives the additive prefix-filter combo
// (the as-built adaptation of Plan D's unrenderable QTreeWidget). Pure logic,
// std only — no Qt, no host.

#include "hierarchy_prefix.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

using dexory_cloud::applyPrefixToVisible;
using dexory_cloud::buildPrefixComboItems;
using dexory_cloud::deriveTopLevelPrefixes;
using dexory_cloud::nameUnderPrefix;

}  // namespace

// The flat Dexory corpus (no '/' in any name) yields NO prefixes: the combo is
// empty (just the "All" sentinel) and the dialog keeps it hidden.
TEST(HierarchyPrefix, FlatCorpusYieldsNoPrefixes) {
  const std::vector<std::string> flat = {
      "nissan_zala_50_zeg_1_0.mcap", "nissan_zala_50_zeg_2_0.mcap", "nissan_zala_50_zeg_3_0.mcap",
  };
  EXPECT_TRUE(deriveTopLevelPrefixes(flat).empty());
  // The combo is the lone "All" sentinel — nothing to browse.
  EXPECT_EQ(buildPrefixComboItems(flat), std::vector<std::string>{"All"});
}

// A hierarchical corpus yields the sorted, de-duplicated top-level prefixes
// (each with a trailing '/'), and loose (no-slash) names contribute nothing.
TEST(HierarchyPrefix, DerivesSortedDistinctPrefixes) {
  const std::vector<std::string> names = {
      "runB/c.mcap", "runA/x.mcap", "runA/y.mcap", "loose.mcap", "runB/d.mcap", "runC/deep/z.mcap",
  };
  const std::vector<std::string> expected = {"runA/", "runB/", "runC/"};
  EXPECT_EQ(deriveTopLevelPrefixes(names), expected);
}

// The combo items are the "All" sentinel first, then the sorted prefixes.
TEST(HierarchyPrefix, ComboItemsHaveAllSentinelFirst) {
  const std::vector<std::string> names = {"b/1.mcap", "a/1.mcap"};
  const std::vector<std::string> expected = {"All", "a/", "b/"};
  EXPECT_EQ(buildPrefixComboItems(names), expected);
}

// nameUnderPrefix: starts-with on the object-key name; "All"/empty matches all.
TEST(HierarchyPrefix, NameUnderPrefixSemantics) {
  EXPECT_TRUE(nameUnderPrefix("runA/x.mcap", "runA/"));
  EXPECT_FALSE(nameUnderPrefix("runB/x.mcap", "runA/"));
  EXPECT_FALSE(nameUnderPrefix("runAx.mcap", "runA/"));  // missing the slash boundary
  EXPECT_TRUE(nameUnderPrefix("anything", "All"));        // sentinel matches all
  EXPECT_TRUE(nameUnderPrefix("anything", ""));           // empty matches all
}

// applyPrefixToVisible narrows an existing visible-row set; it never widens it
// (composes ON TOP OF the name/date/Lua filters, never replacing them).
TEST(HierarchyPrefix, ApplyPrefixNarrowsVisibleRows) {
  const std::vector<std::string> names = {
      "runA/x.mcap",  // 0
      "runB/y.mcap",  // 1
      "runA/z.mcap",  // 2
      "loose.mcap",   // 3
  };
  // The name/date filter already hid row 0; visible = {1, 2, 3}.
  const std::vector<int> base_visible = {1, 2, 3};

  // Selecting "runA/" keeps only row 2 (row 0 was already hidden upstream).
  EXPECT_EQ(applyPrefixToVisible(base_visible, names, "runA/"), std::vector<int>{2});
  // Selecting "runB/" keeps only row 1.
  EXPECT_EQ(applyPrefixToVisible(base_visible, names, "runB/"), std::vector<int>{1});
}

// "All" (or empty) returns the base visible set unchanged — hierarchy off path.
TEST(HierarchyPrefix, ApplyPrefixAllIsIdentity) {
  const std::vector<std::string> names = {"a/1.mcap", "b/1.mcap"};
  const std::vector<int> base_visible = {0, 1};
  EXPECT_EQ(applyPrefixToVisible(base_visible, names, "All"), base_visible);
  EXPECT_EQ(applyPrefixToVisible(base_visible, names, ""), base_visible);
}

// Out-of-range / negative rows in the base set are skipped defensively (never
// index out of bounds).
TEST(HierarchyPrefix, ApplyPrefixSkipsOutOfRangeRows) {
  const std::vector<std::string> names = {"a/1.mcap"};
  const std::vector<int> base_visible = {-1, 0, 5};
  EXPECT_EQ(applyPrefixToVisible(base_visible, names, "a/"), std::vector<int>{0});
}
