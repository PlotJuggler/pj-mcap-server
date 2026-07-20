// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT

#include "seq_display.h"

#include <gtest/gtest.h>

#include <string>

using mcap_cloud::shortenSequenceName;

TEST(SeqDisplay, StripsHivePrefixesAndDateFromRealKey) {
  const std::string key =
      "customer=globex/customer_site=nashville/robot=arri-182/source=ros-bags/date=2026-05-19/"
      "rosbox_2026-05-19_16-43-46.mcap";
  EXPECT_EQ(shortenSequenceName(key), "globex/nashville/arri-182/ros-bags/rosbox_2026-05-19_16-43-46.mcap");
}

TEST(SeqDisplay, FlatKeyWithoutHiveSegmentsIsUnchanged) {
  // No '=' directory segments: every segment kept verbatim.
  EXPECT_EQ(shortenSequenceName("recordings/nissan_zala_50_zeg_1_0.mcap"), "recordings/nissan_zala_50_zeg_1_0.mcap");
  EXPECT_EQ(shortenSequenceName("plain.mcap"), "plain.mcap");
  EXPECT_EQ(shortenSequenceName(""), "");
}

TEST(SeqDisplay, StripsKeyPrefixKeepingValue) {
  EXPECT_EQ(shortenSequenceName("customer=globex/file.mcap"), "globex/file.mcap");
  EXPECT_EQ(shortenSequenceName("robot=arri-182/x.mcap"), "arri-182/x.mcap");
  EXPECT_EQ(shortenSequenceName("a=1/b=2/c=3/leaf.mcap"), "1/2/3/leaf.mcap");
}

TEST(SeqDisplay, DropsDateSegmentEntirely) {
  EXPECT_EQ(shortenSequenceName("date=2026-05-19/file.mcap"), "file.mcap");
  EXPECT_EQ(shortenSequenceName("robot=arri/date=2026-05-19/x.mcap"), "arri/x.mcap");
  // An empty date value is still a date segment.
  EXPECT_EQ(shortenSequenceName("a=1/date=/b=2/c.mcap"), "1/2/c.mcap");
  EXPECT_EQ(shortenSequenceName("a=1/date=2026/b=2/date=2027/c.mcap"), "1/2/c.mcap");
}

TEST(SeqDisplay, LeafIsKeptVerbatim) {
  // The leaf filename is never prefix-stripped or date-dropped, even if it looks
  // like a `k=v` or a `date=` segment.
  EXPECT_EQ(shortenSequenceName("robot=arri/date=2026-05-19.mcap"), "arri/date=2026-05-19.mcap");
  EXPECT_EQ(shortenSequenceName("robot=arri/customer=x.mcap"), "arri/customer=x.mcap");
}

TEST(SeqDisplay, SplitsOnFirstEquals) {
  // Matches parseS3KeyFields: split on the FIRST '=', so a value may contain '='.
  EXPECT_EQ(shortenSequenceName("robot=update=x/file.mcap"), "update=x/file.mcap");
}

TEST(SeqDisplay, NonHiveDirSegmentKeptVerbatim) {
  // A directory segment without '=' keeps its text; only `k=v` segments are stripped.
  EXPECT_EQ(shortenSequenceName("bucketdir/robot=arri/file.mcap"), "bucketdir/arri/file.mcap");
}
