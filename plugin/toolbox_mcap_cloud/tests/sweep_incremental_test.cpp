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

}  // namespace
}  // namespace mcap_cloud
