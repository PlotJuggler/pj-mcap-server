// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Hermetic tests for canonical_fields.h — the canonical-field extraction and
// the one-pass sorted-unique vocabulary (buildCanonicalSchema) that replaced
// the per-dropdown-per-call distinctFieldValues() re-parse. The Basic-tab
// combos are INDEX-addressed against these vectors (onIndexChanged resolves
// index -> value), so the sorted-unique ordering pinned here is a wire-level
// contract between widget_data() and the event handlers.

#include "canonical_fields.h"

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <vector>

namespace {

using mcap_cloud::buildCanonicalSchema;
using mcap_cloud::canonicalFilterFields;
using mcap_cloud::parseS3KeyFields;

struct Rec {
  std::string name;
};

std::string hiveKey(const std::string& customer, const std::string& site, const std::string& robot,
                    const std::string& source, const std::string& file) {
  return "customer=" + customer + "/customer_site=" + site + "/robot=" + robot + "/source=" + source +
         "/date=2026-05-19/" + file;
}

TEST(CanonicalFields, FilterFieldsKeepOnlyCanonicalKeys) {
  const Metadata f = canonicalFilterFields(hiveKey("acme", "nashville", "arri-182", "ros-bags", "a.mcap"));
  EXPECT_EQ(f.size(), 4u);
  EXPECT_EQ(f.at("customer"), "acme");
  EXPECT_EQ(f.at("customer_site"), "nashville");
  EXPECT_EQ(f.at("robot"), "arri-182");
  EXPECT_EQ(f.at("source"), "ros-bags");
  EXPECT_EQ(f.count("date"), 0u);      // display data, not a filter dimension
  EXPECT_EQ(f.count("filename"), 0u);  // ditto
}

TEST(CanonicalFields, FilterFieldsDegradeOnFlatKey) {
  EXPECT_TRUE(canonicalFilterFields("just_a_file.mcap").empty());
}

TEST(CanonicalFields, SchemaIsSortedUniquePerKey) {
  // Duplicates, out-of-order values, and a partial (flat) key.
  const std::vector<Rec> recs = {
      {hiveKey("acme", "s1", "zeta", "ros-bags", "a.mcap")},
      {hiveKey("acme", "s1", "alpha", "ros-bags", "b.mcap")},
      {hiveKey("acme", "s2", "alpha", "ros-bags", "c.mcap")},
      {hiveKey("acme", "s1", "zeta", "flight-logs", "d.mcap")},
      {"flat_key.mcap"},
  };
  const Schema schema = buildCanonicalSchema(recs);

  ASSERT_EQ(schema.count("robot"), 1u);
  EXPECT_EQ(schema.at("robot"), (std::vector<std::string>{"alpha", "zeta"}));
  ASSERT_EQ(schema.count("source"), 1u);
  EXPECT_EQ(schema.at("source"), (std::vector<std::string>{"flight-logs", "ros-bags"}));
  ASSERT_EQ(schema.count("customer"), 1u);
  EXPECT_EQ(schema.at("customer"), (std::vector<std::string>{"acme"}));
  ASSERT_EQ(schema.count("customer_site"), 1u);
  EXPECT_EQ(schema.at("customer_site"), (std::vector<std::string>{"s1", "s2"}));
  // Non-canonical fields never appear, and neither do keys with no values.
  EXPECT_EQ(schema.count("date"), 0u);
  EXPECT_EQ(schema.count("filename"), 0u);
}

TEST(CanonicalFields, SchemaMatchesNaiveDistinctPerKey) {
  // Equivalence with the retired per-key scan (std::set over parseS3KeyFields):
  // the dropdowns must offer the same values in the same order as before.
  std::vector<Rec> recs;
  for (int i = 0; i < 200; ++i) {
    recs.push_back({hiveKey("c" + std::to_string(i % 3), "s" + std::to_string(i % 5), "r" + std::to_string(i % 17),
                            i % 2 == 0 ? "ros-bags" : "flight-logs", "f" + std::to_string(i) + ".mcap")});
  }
  const Schema schema = buildCanonicalSchema(recs);
  for (const char* key : {"customer", "customer_site", "robot", "source"}) {
    std::set<std::string, std::less<>> naive;
    for (const auto& rec : recs) {
      const Metadata fields = parseS3KeyFields(rec.name);
      if (auto it = fields.find(key); it != fields.end()) {
        naive.insert(it->second);
      }
    }
    ASSERT_EQ(schema.count(key), 1u) << key;
    EXPECT_EQ(schema.at(key), (std::vector<std::string>{naive.begin(), naive.end()})) << key;
  }
}

TEST(CanonicalFields, EmptyRecordsYieldEmptySchema) {
  EXPECT_TRUE(buildCanonicalSchema(std::vector<Rec>{}).empty());
}

}  // namespace
