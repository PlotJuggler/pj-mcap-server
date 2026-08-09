// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// The progressive listing sweep appends pages incrementally instead of
// rebuilding all N rows per render window (which made a 43k-row sweep
// O(rows x windows) and froze the GUI thread). This pins the invariant that
// makes that safe:
//
//   appending in chunks must leave EXACTLY the state one full populate leaves.
//
// Nothing else catches a break here: the end-of-sweep onSequencesReady always
// repopulates authoritatively, so mid-sweep corruption is invisible to every
// other test while being very visible to a user watching rows arrive.
#include <gtest/gtest.h>

#include <set>
#include <string>
#include <vector>

#include "mcap_cloud_dialog.hpp"
#include "test_support_env.hpp"

namespace mcap_cloud {

// Grants the test access to the private sweep path (declared a friend in the
// dialog header).
struct McapCloudDialogSweepAccess {
  static bool append(McapCloudDialog& d, const std::vector<SequenceInfo>& s, std::size_t from) {
    return d.appendSequencesLocked(s, from);
  }
  static void populate(McapCloudDialog& d, std::vector<SequenceInfo>& s) {
    d.populateSequencesLocked(s, /*seed_dates=*/false);
  }
  static const std::vector<std::string>& names(McapCloudDialog& d) { return d.state_.sequence_names; }
  static const std::vector<std::string>& displays(McapCloudDialog& d) { return d.state_.seq_display_names; }
  static std::size_t stablePrefix(McapCloudDialog& d) { return d.state_.seq_stable_prefix; }
  static std::size_t count(McapCloudDialog& d) { return d.state_.sequences.size(); }
  static const std::vector<std::string>& selected(McapCloudDialog& d) { return d.state_.selected_sequences; }
  // Renders once so the seqTable view cache (and its row_to_keys map, in the
  // modes that build one) reflects current state — the dialog resolves a click
  // against that cache.
  static void render(McapCloudDialog& d) { (void)d.widget_data(); }
  static void sort(McapCloudDialog& d) { d.sortSequencesLocked(); }
  static void beginGate(McapCloudDialog& d) { (void)d.beginGateRequestLocked(GatePhase::kNeedsSelection); }
  static void seedRowMaps(McapCloudDialog& d) {
    d.state_.seq_view_cache.row_to_keys["stale-label"] = {"stale-key.mcap"};
    d.state_.seq_view_cache.prev_row_to_keys["older-label"] = {"older-key.mcap"};
  }
  static bool rowMapsEmpty(McapCloudDialog& d) {
    return d.state_.seq_view_cache.row_to_keys.empty() && d.state_.seq_view_cache.prev_row_to_keys.empty();
  }
};

namespace {

using Access = McapCloudDialogSweepAccess;

std::vector<SequenceInfo> corpus(int n, const char* robot = "arri-182") {
  std::vector<SequenceInfo> v;
  v.reserve(static_cast<std::size_t>(n));
  for (int i = 0; i < n; ++i) {
    SequenceInfo s;
    s.name = std::string("customer=dexory/customer_site=nashvillee/robot=") + robot +
             "/source=rosbox/date=2026-07-13/rosbox_light_" + std::to_string(i) + ".mcap";
    s.min_ts_ns = 1752000000000000000LL + i * 1000;
    s.max_ts_ns = s.min_ts_ns + 500;
    s.total_size_bytes = 2200000;
    v.push_back(std::move(s));
  }
  return v;
}

TEST(SweepIncremental, AppendInChunksMatchesOneFullPopulate) {
  mcap_cloud_test::HermeticEnv env("mcap-cloud-sweep-append");
  const auto all = corpus(500);

  // Reference: one full populate of the complete corpus.
  McapCloudDialog reference;
  {
    auto copy = all;
    Access::populate(reference, copy);
  }

  // Subject: the same corpus delivered as a sweep — first page via populate
  // (what onGatePageReady does on `reset`), the rest appended.
  McapCloudDialog subject;
  {
    std::vector<SequenceInfo> first(all.begin(), all.begin() + 100);
    Access::populate(subject, first);
    for (std::size_t upto : {200u, 350u, 500u}) {
      const std::vector<SequenceInfo> grown(all.begin(), all.begin() + static_cast<long>(upto));
      ASSERT_TRUE(Access::append(subject, grown, Access::count(subject)))
          << "append rejected a clean, collision-free growth at " << upto;
    }
  }

  ASSERT_EQ(Access::count(subject), Access::count(reference));
  EXPECT_EQ(Access::names(subject), Access::names(reference))
      << "appended sequence_names diverged from a full populate";
  EXPECT_EQ(Access::displays(subject), Access::displays(reference))
      << "appended display names diverged — the seqTable would show wrong labels "
         "mid-sweep, and column-0 text is the selection identity";
}

// The append path must REFUSE anything it cannot do correctly, so the caller
// falls back to a full populate rather than producing a wrong table.
TEST(SweepIncremental, AppendRefusesWhenItCannotBeCorrect) {
  mcap_cloud_test::HermeticEnv env("mcap-cloud-sweep-refuse");
  const auto base = corpus(50);

  McapCloudDialog d;
  {
    auto copy = base;
    Access::populate(d, copy);
  }

  // `from` not matching what we hold is not a clean append.
  EXPECT_FALSE(Access::append(d, base, 10u)) << "append must refuse a non-prefix growth";

  // A duplicate name collides on its display candidate, which would force the
  // EARLIER row to fall back to its full key — a global re-derive append cannot do.
  std::vector<SequenceInfo> collide = base;
  collide.push_back(base.front());
  EXPECT_FALSE(Access::append(d, collide, base.size()))
      << "append must refuse a display-name collision instead of producing duplicate labels";
}

// REVIEW DEFECT (both reviewers, blocking): the collision preflight checked each
// new candidate against the ALREADY-HELD labels but not against the other
// candidates in the SAME batch.
//
// shortenSequenceName deliberately drops the `date=` segment, so two files that
// differ only by date collapse to one display label — and a single server page
// routinely spans a date boundary. Both rows would then carry identical column-0
// text, which IS the PanelEngine selection identity, so clicking the second
// resolves to the FIRST: wrong topics, and a Download that silently fetches the
// wrong recording.
TEST(SweepIncremental, AppendRefusesAWithinBatchDisplayCollision) {
  mcap_cloud_test::HermeticEnv env("mcap-cloud-sweep-tail-collision");
  const auto base = corpus(40);

  McapCloudDialog d;
  {
    auto copy = base;
    Access::populate(d, copy);
  }

  // Two NEW rows differing ONLY by date= — neither collides with anything
  // already held, but they collide with each other.
  std::vector<SequenceInfo> grown = base;
  for (const char* date : {"2026-07-13", "2026-07-14"}) {
    SequenceInfo s;
    s.name = std::string("customer=dexory/customer_site=nashvillee/robot=arri-182/source=rosbox/date=") +
             date + "/rosbox_light_twin.mcap";
    s.min_ts_ns = 1752999000000000000LL;
    s.max_ts_ns = s.min_ts_ns + 500;
    s.total_size_bytes = 2200000;
    grown.push_back(std::move(s));
  }

  EXPECT_FALSE(Access::append(d, grown, base.size()))
      << "append accepted two rows whose display labels collide WITHIN the appended batch. "
         "Column-0 text is the selection identity, so the table would carry two identical "
         "labels and a click on the second would resolve to the first file.";

  // And the fallback must actually produce unique labels.
  {
    auto copy = grown;
    Access::populate(d, copy);
  }
  const auto& disp = Access::displays(d);
  std::set<std::string> unique(disp.begin(), disp.end());
  EXPECT_EQ(unique.size(), disp.size())
      << "the full-populate fallback must yield unique display labels (it falls back to the "
         "full key on collision)";
}

// The stable prefix is what authorizes the view cache to extend rather than
// rebuild. If it ever overstates what is unchanged, stale rows persist.
TEST(SweepIncremental, StablePrefixMarksExactlyTheUnchangedRows) {
  mcap_cloud_test::HermeticEnv env("mcap-cloud-sweep-prefix");
  const auto all = corpus(300);

  McapCloudDialog d;
  std::vector<SequenceInfo> first(all.begin(), all.begin() + 100);
  Access::populate(d, first);
  EXPECT_EQ(Access::stablePrefix(d), 0u) << "a full populate must claim NO stable prefix";

  ASSERT_TRUE(Access::append(d, all, 100u));
  EXPECT_EQ(Access::stablePrefix(d), 100u)
      << "after appending onto 100 rows, exactly those 100 are unchanged";
  EXPECT_EQ(Access::count(d), 300u);
}

// Clicking a row must resolve to the right s3_key.
//
// The seqTable row->keys hash map (43k string-keyed inserts, each allocating a
// key copy, a vector and a string) was MEASURED at 48% of the view-cache rebuild
// and is no longer built in file mode: `display` is seq_display_names[i] and the
// sole key is sequences[i].name, a 1:1 correspondence sortSequencesLocked
// preserves. onSelectionChanged resolves through those parallel vectors instead.
//
// That path had ZERO coverage while being rewritten. If it is wrong, a user
// clicks a recording and the Topics panel silently stays empty — no crash, no
// failing test, just a dead UI.
TEST(SweepIncremental, ClickResolvesDisplayLabelToTheRealS3Key) {
  mcap_cloud_test::HermeticEnv env("mcap-cloud-sweep-click");
  const auto all = corpus(300);

  McapCloudDialog d;
  {
    auto copy = all;
    Access::populate(d, copy);
  }
  Access::render(d);

  // The host harvests column-0 TEXT, so a click arrives as the DISPLAY label.
  const auto& displays = Access::displays(d);
  const auto& names = Access::names(d);
  ASSERT_EQ(displays.size(), names.size());
  ASSERT_GE(displays.size(), 3u);

  for (std::size_t idx : {std::size_t{0}, displays.size() / 2, displays.size() - 1}) {
    ASSERT_TRUE(d.onSelectionChanged("seqTable", {displays[idx]}))
        << "the dialog must claim the seqTable selection event";
    const auto& sel = Access::selected(d);
    ASSERT_EQ(sel.size(), 1u) << "display label " << displays[idx] << " resolved to "
                              << sel.size() << " keys, expected exactly 1";
    EXPECT_EQ(sel[0], names[idx])
        << "display label " << displays[idx] << " resolved to the WRONG file: got " << sel[0]
        << ", expected " << names[idx];
  }

  // Multi-select must resolve every row, not just the first.
  ASSERT_TRUE(d.onSelectionChanged("seqTable", {displays[1], displays[5], displays[9]}));
  EXPECT_EQ(Access::selected(d).size(), 3u) << "a 3-row selection must resolve to 3 keys";

  // An unknown label must resolve to nothing rather than to an arbitrary row.
  ASSERT_TRUE(d.onSelectionChanged("seqTable", {"no-such-row.mcap"}));
  EXPECT_TRUE(Access::selected(d).empty()) << "an unknown label must not resolve to some other file";
}

// Selection must survive a sort: sortSequencesLocked reorders `sequences`, then
// rebuilds sequence_names and re-derives seq_display_names. The lookup depends
// on those three staying parallel — if a sort ever broke that, clicks would
// silently resolve to the wrong recording.
TEST(SweepIncremental, ClickResolvesCorrectlyAfterASort) {
  mcap_cloud_test::HermeticEnv env("mcap-cloud-sweep-click-sorted");
  const auto all = corpus(120);

  McapCloudDialog d;
  {
    auto copy = all;
    Access::populate(d, copy);
  }
  Access::sort(d);
  Access::render(d);

  const auto& displays = Access::displays(d);
  const auto& names = Access::names(d);
  ASSERT_EQ(displays.size(), names.size());
  ASSERT_GE(displays.size(), 2u);

  for (std::size_t idx : {std::size_t{0}, displays.size() - 1}) {
    ASSERT_TRUE(d.onSelectionChanged("seqTable", {displays[idx]}));
    const auto& sel = Access::selected(d);
    ASSERT_EQ(sel.size(), 1u);
    EXPECT_EQ(sel[0], names[idx])
        << "after sorting, display/name vectors are no longer parallel — clicks resolve to the wrong file";
  }
}

// REVIEW DEFECT: a gate transition must drop BOTH row->keys generations.
//
// A label still on screen from a previous gate — or a previous SERVER — must
// never resolve to that gate's keys. Before the fix the maps survived
// beginGateRequestLocked, leaving a window where a click could return keys
// belonging to a site the user had already navigated away from.
TEST(SweepIncremental, GateTransitionInvalidatesBothRowKeyMaps) {
  mcap_cloud_test::HermeticEnv env("mcap-cloud-gate-invalidate");
  const auto all = corpus(40);

  McapCloudDialog d;
  {
    auto copy = all;
    Access::populate(d, copy);
  }
  Access::render(d);           // builds the view cache
  Access::seedRowMaps(d);      // simulate an AGGREGATE generation having been built
  ASSERT_FALSE(Access::rowMapsEmpty(d)) << "precondition: the maps must be non-empty";

  Access::beginGate(d);
  EXPECT_TRUE(Access::rowMapsEmpty(d))
      << "a gate transition left a row->keys map behind: a label from the previous gate/server "
         "could still resolve to that gate's file keys";
}

}  // namespace
}  // namespace mcap_cloud
