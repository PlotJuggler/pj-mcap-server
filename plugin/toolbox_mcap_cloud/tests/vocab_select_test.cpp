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
  c.sites.push_back({21, "nashville", 60});
  c.sites.push_back({22, "wallingford", 40});
  v.customers.push_back(c);
  return v;
}
}  // namespace

TEST(ResolveGate, ResolvesNamesToIdsAndGeneration) {
  const auto f = mcap_cloud::resolveGateFilter(vocab(), "dexory", "nashville");
  ASSERT_TRUE(f.has_value());
  EXPECT_EQ(f->customer_id, std::optional<std::uint64_t>(11));
  EXPECT_EQ(f->site_id, std::optional<std::uint64_t>(21));
  EXPECT_EQ(f->generation, "gen-7");
}

TEST(ResolveGate, UnknownSiteFailsEvenWhenCustomerResolves) {
  // A rebuild can remove a site (or fix a typo like nashvillee) while the
  // client still holds the old name.
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "dexory", "gone").has_value());
}

TEST(ResolveGate, UnknownCustomerFails) {
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "acme", "nashville").has_value());
}

TEST(ResolveGate, EmptyNamesFail) {
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "", "").has_value());
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
            "Select customer and site to load recordings - 25550 recordings across 6 sites");
  EXPECT_EQ(gateHintText(GatePhase::kListLoading, 25550, 6), "");
  EXPECT_EQ(gateHintText(GatePhase::kListError, 25550, 6),
            "Recording list failed or is incomplete - use Refresh to retry");
  EXPECT_EQ(gateHintText(GatePhase::kListEmpty, 25550, 6),
            "No recordings match the current selection");
  EXPECT_EQ(gateHintText(GatePhase::kRows, 25550, 6), "");
}
