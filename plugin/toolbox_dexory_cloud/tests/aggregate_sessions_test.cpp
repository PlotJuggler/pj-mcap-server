// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT

#include "aggregate_sessions.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using dexory_cloud::AggInput;
using dexory_cloud::aggregateSessions;
using dexory_cloud::partitionKey;

namespace {

constexpr std::int64_t kSec = 1'000'000'000LL;

TEST(PartitionKey, DropsFilenameAndDateSegment) {
  EXPECT_EQ(partitionKey("customer=dexory/customer_site=nashville/robot=arri-182/source=ros-bags/"
                         "date=2026-05-19/rosbox_2026-05-19_16-43-46.mcap"),
            "customer=dexory/customer_site=nashville/robot=arri-182/source=ros-bags");
}

TEST(PartitionKey, FlatKeyHasEmptyPartition) {
  EXPECT_EQ(partitionKey("nissan_zala_50_zeg_1_0.mcap"), "");
  EXPECT_EQ(partitionKey("recordings/nissan.mcap"), "recordings");
}

// 34 contiguous chunks (ms-scale seams) across midnight -> ONE session under a
// 5-min threshold. Mirrors the real staging run.
TEST(Aggregate, ContiguousRunIsOneSession) {
  std::vector<AggInput> files;
  std::int64_t base = 1'700'000'000LL * kSec;
  for (int i = 0; i < 34; ++i) {
    std::int64_t s = base + static_cast<std::int64_t>(i) * 780 * kSec + i * 1'000'000LL;  // +1ms seam each
    files.push_back({"robot=arri/source=ros-bags/date=2026-05-19/chunk_" + std::to_string(i) + ".mcap", s,
                     s + 779 * kSec});
  }
  auto sessions = aggregateSessions(files, 300 * kSec);  // 5 min
  ASSERT_EQ(sessions.size(), 1u);
  EXPECT_EQ(sessions[0].keys.size(), 34u);
  EXPECT_EQ(sessions[0].min_ts_ns, files.front().min_ts_ns);
  EXPECT_EQ(sessions[0].max_ts_ns, files.back().max_ts_ns);
}

TEST(Aggregate, LargeGapSplitsIntoTwoSessions) {
  std::int64_t base = 1'700'000'000LL * kSec;
  std::vector<AggInput> files = {
      {"r=a/s=x/f0.mcap", base, base + 60 * kSec},
      {"r=a/s=x/f1.mcap", base + 61 * kSec, base + 120 * kSec},         // 1s gap -> same
      {"r=a/s=x/f2.mcap", base + 1200 * kSec, base + 1260 * kSec},      // 18min gap -> split
      {"r=a/s=x/f3.mcap", base + 1261 * kSec, base + 1320 * kSec},      // 1s gap -> same as f2
  };
  auto sessions = aggregateSessions(files, 300 * kSec);  // 5 min
  ASSERT_EQ(sessions.size(), 2u);
  EXPECT_EQ(sessions[0].keys.size(), 2u);  // f0,f1
  EXPECT_EQ(sessions[1].keys.size(), 2u);  // f2,f3
}

TEST(Aggregate, DistinctPartitionsNeverMergeEvenIfTimeOverlaps) {
  std::int64_t base = 1'700'000'000LL * kSec;
  std::vector<AggInput> files = {
      {"robot=a/source=x/f.mcap", base, base + 60 * kSec},
      {"robot=b/source=x/f.mcap", base, base + 60 * kSec},  // same time, different robot
  };
  auto sessions = aggregateSessions(files, 300 * kSec);
  ASSERT_EQ(sessions.size(), 2u);
  EXPECT_EQ(sessions[0].keys.size(), 1u);
  EXPECT_EQ(sessions[1].keys.size(), 1u);
}

TEST(Aggregate, CrossMidnightDateDiffersButStaysOneSession) {
  std::int64_t base = 1'700'000'000LL * kSec;
  std::vector<AggInput> files = {
      // Same robot/source, different date= folders, contiguous in time.
      {"robot=a/source=x/date=2026-05-19/late.mcap", base, base + 60 * kSec},
      {"robot=a/source=x/date=2026-05-20/early.mcap", base + 61 * kSec, base + 120 * kSec},
  };
  auto sessions = aggregateSessions(files, 300 * kSec);
  ASSERT_EQ(sessions.size(), 1u);  // date ignored for membership
  EXPECT_EQ(sessions[0].keys.size(), 2u);
}

TEST(Aggregate, KeysOrderedByTimeWithinSession) {
  std::int64_t base = 1'700'000'000LL * kSec;
  std::vector<AggInput> files = {
      {"r=a/s=x/second.mcap", base + 10 * kSec, base + 20 * kSec},
      {"r=a/s=x/first.mcap", base, base + 9 * kSec},
  };
  auto sessions = aggregateSessions(files, 300 * kSec);
  ASSERT_EQ(sessions.size(), 1u);
  ASSERT_EQ(sessions[0].keys.size(), 2u);
  EXPECT_EQ(sessions[0].keys[0], "r=a/s=x/first.mcap");
  EXPECT_EQ(sessions[0].keys[1], "r=a/s=x/second.mcap");
}

TEST(Aggregate, EmptyInputYieldsNoSessions) {
  EXPECT_TRUE(aggregateSessions({}, 300 * kSec).empty());
}

}  // namespace
