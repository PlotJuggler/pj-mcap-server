// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT

#include "s3_key_fields.h"

#include <gtest/gtest.h>

using dexory_cloud::parseS3KeyFields;

TEST(S3KeyFields, ParsesTheSixDexoryFields) {
  auto m = parseS3KeyFields(
      "customer=dexory/customer_site=nashville/robot=arri-182/source=ros-bags/date=2026-05-19/"
      "rosbox_2026-05-19_16-43-46.mcap");
  EXPECT_EQ(m.size(), 6u);
  EXPECT_EQ(m["customer"], "dexory");
  EXPECT_EQ(m["customer_site"], "nashville");
  EXPECT_EQ(m["robot"], "arri-182");
  EXPECT_EQ(m["source"], "ros-bags");
  EXPECT_EQ(m["date"], "2026-05-19");
  EXPECT_EQ(m["filename"], "rosbox_2026-05-19_16-43-46.mcap");
}

TEST(S3KeyFields, FlatKeyYieldsOnlyFilename) {
  auto m = parseS3KeyFields("nissan_zala_50_zeg_1_0.mcap");
  EXPECT_EQ(m.size(), 1u);
  EXPECT_EQ(m["filename"], "nissan_zala_50_zeg_1_0.mcap");
}

TEST(S3KeyFields, NonHiveDirSegmentsIgnored) {
  // A directory segment without '=' contributes no field; the filename still does.
  auto m = parseS3KeyFields("recordings/2026/file.mcap");
  EXPECT_EQ(m.size(), 1u);
  EXPECT_EQ(m["filename"], "file.mcap");
  EXPECT_EQ(m.count("recordings"), 0u);
}

TEST(S3KeyFields, ValueMayContainEquals) {
  // Split on the FIRST '=' only.
  auto m = parseS3KeyFields("k=a=b/f.mcap");
  EXPECT_EQ(m["k"], "a=b");
}

TEST(S3KeyFields, EmptyKey) {
  EXPECT_TRUE(parseS3KeyFields("").empty());
}
