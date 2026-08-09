// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "vocab_select.hpp"

#include <gtest/gtest.h>

namespace {
mcap_cloud::VocabularyInfo vocab() {
  mcap_cloud::VocabularyInfo v;
  v.generation = "gen-7";
  mcap_cloud::VocabCustomer c;
  c.id = 11; c.name = "dexory"; c.file_count = 100;
  mcap_cloud::VocabSite nashville{21, "nashville", 60, {}};
  nashville.robots.push_back({31, "arri-182", 35});
  nashville.robots.push_back({32, "arri-183", 25});
  mcap_cloud::VocabSite wallingford{22, "wallingford", 40, {}};
  wallingford.robots.push_back({33, "arri-900", 40});  // sole robot: auto-selectable
  c.sites.push_back(std::move(nashville));
  c.sites.push_back(std::move(wallingford));
  v.customers.push_back(c);
  return v;
}
}  // namespace

TEST(ResolveGate, ResolvesNamesToIdsAndGeneration) {
  const auto f = mcap_cloud::resolveGateFilter(vocab(), "dexory", "nashville", "arri-182");
  ASSERT_TRUE(f.has_value());
  EXPECT_EQ(f->customer_id, std::optional<std::uint64_t>(11));
  EXPECT_EQ(f->site_id, std::optional<std::uint64_t>(21));
  // The robot id must reach the wire: it is what keeps the server from listing
  // the whole site (auryn_read.go ANDs `f.robot_id = ?`).
  EXPECT_EQ(f->robot_id, std::optional<std::uint64_t>(31));
  EXPECT_EQ(f->generation, "gen-7");
}

TEST(ResolveGate, UnknownSiteFailsEvenWhenCustomerResolves) {
  // A rebuild can remove a site (or fix a typo like nashvillee) while the
  // client still holds the old name.
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "dexory", "gone", "arri-182").has_value());
}

TEST(ResolveGate, UnknownCustomerFails) {
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "acme", "nashville", "arri-182").has_value());
}

TEST(ResolveGate, UnknownRobotFailsEvenWhenCustomerAndSiteResolve) {
  // A retired robot must NOT degrade into a whole-site listing — that is exactly
  // the download this gate exists to prevent.
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "dexory", "nashville", "gone").has_value());
}

TEST(ResolveGate, MissingRobotIsNotAWildcard) {
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "dexory", "nashville", "").has_value());
}

TEST(ResolveGate, EmptyNamesFail) {
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "", "", "").has_value());
}

TEST(RobotNames, ListsRobotsForTheSelectedSiteOnly) {
  const auto names = mcap_cloud::robotNamesFor(vocab(), "dexory", "nashville");
  ASSERT_EQ(names.size(), 2u);
  EXPECT_EQ(names[0], "arri-182");
  EXPECT_EQ(names[1], "arri-183");
  EXPECT_EQ(mcap_cloud::robotNamesFor(vocab(), "dexory", "wallingford").size(), 1u);
  EXPECT_EQ(mcap_cloud::robotNamesFor(vocab(), "dexory", "gone").size(), 0u);
  EXPECT_EQ(mcap_cloud::robotNamesFor(vocab(), "acme", "nashville").size(), 0u);
}

TEST(AutoSelect, SingleRobotIsAutoSelectedButSeveralAreNot) {
  EXPECT_EQ(mcap_cloud::autoSelectRobot(vocab(), "dexory", "wallingford"), "arri-900");
  EXPECT_EQ(mcap_cloud::autoSelectRobot(vocab(), "dexory", "nashville"), "");
}

TEST(AutoSelect, SingleCustomerIsAutoSelected) {
  EXPECT_EQ(mcap_cloud::autoSelectCustomer(vocab()), "dexory");
}

TEST(AutoSelect, MultipleCustomersRequireAnExplicitPick) {
  auto v = vocab();
  mcap_cloud::VocabCustomer c2;
  c2.id = 12; c2.name = "acme";
  v.customers.push_back(c2);
  EXPECT_EQ(mcap_cloud::autoSelectCustomer(v), "");
}

TEST(SiteNames, ListsSitesForTheSelectedCustomerOnly) {
  const auto names = mcap_cloud::siteNamesFor(vocab(), "dexory");
  ASSERT_EQ(names.size(), 2u);
  EXPECT_EQ(names[0], "nashville");
  EXPECT_EQ(mcap_cloud::siteNamesFor(vocab(), "acme").size(), 0u);
}

// --- GatePhase pill text (explicit phases, not booleans) --------------------

TEST(GateHint, EveryPhaseHasTheRightText) {
  using mcap_cloud::GatePhase;
  using mcap_cloud::gateHintText;
  EXPECT_EQ(gateHintText(GatePhase::kDisconnected, 0, 0),
            "Connect to a server to browse recordings");
  EXPECT_EQ(gateHintText(GatePhase::kVocabularyLoading, 0, 0), "");
  EXPECT_EQ(gateHintText(GatePhase::kVocabularyError, 0, 0),
            "Could not load the catalog - use Refresh to retry");
  EXPECT_EQ(gateHintText(GatePhase::kEmptyCatalog, 0, 0),
            "The catalog is empty - no recordings have been indexed yet");
  EXPECT_EQ(gateHintText(GatePhase::kNeedsSelection, 25550, 6),
            "Select customer, site and robot to load recordings - 25550 recordings across 6 sites");
  EXPECT_EQ(gateHintText(GatePhase::kListLoading, 25550, 6), "");
  EXPECT_EQ(gateHintText(GatePhase::kListError, 25550, 6),
            "Recording list failed or is incomplete - use Refresh to retry");
  EXPECT_EQ(gateHintText(GatePhase::kListEmpty, 25550, 6),
            "No recordings match the current selection");
  EXPECT_EQ(gateHintText(GatePhase::kRows, 25550, 6), "");
}
