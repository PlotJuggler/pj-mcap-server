// SPDX-License-Identifier: MIT
// Copyright 2026 Davide Faconti
//
// Hermetic unit tests for the display-elision helpers (elide_name.h).
#include "elide_name.h"

#include <gtest/gtest.h>

#include <string>

namespace mcap_cloud {
namespace {

constexpr char kHiveKey[] =
    "customer=globex/customer_site=nashville/robot=arri-182/source=low-fat-bags/"
    "date=2026-05-19/2026-05-19_15-30-14_BLOCK-A_0.mcap";

TEST(ElideName, ShortStringUnchanged) {
  EXPECT_EQ(elideMiddle("short.mcap", 56), "short.mcap");
  EXPECT_EQ(elideMiddle("", 56), "");
  // Exactly at the limit is not elided.
  const std::string exact(56, 'x');
  EXPECT_EQ(elideMiddle(exact, 56), exact);
}

TEST(ElideName, LongStringIsBoundedAndKeepsHeadAndTail) {
  const std::string key = kHiveKey;
  ASSERT_GT(key.size(), 56u);
  const std::string out = elideMiddle(key, 56);
  EXPECT_EQ(out.size(), 56u);
  EXPECT_NE(out.find("..."), std::string::npos);
  // Head context (customer) and the distinguishing tail (filename) both survive.
  EXPECT_TRUE(out.rfind("customer=", 0) == 0);
  EXPECT_TRUE(out.find("BLOCK-A_0.mcap") != std::string::npos);
}

TEST(ElideName, AsciiOnlyEllipsis) {
  const std::string out = elideMiddle(kHiveKey, 40);
  for (unsigned char c : out) {
    EXPECT_LT(c, 0x80u) << "elision must stay ASCII for the NoWrap panel";
  }
}

TEST(ElideName, FloorClampNeverUnderflows) {
  // max_len below the floor must not underflow the head/tail arithmetic.
  const std::string out = elideMiddle(kHiveKey, 1);
  EXPECT_GE(out.size(), 5u);   // floor (8) - "..." => head+tail present
  EXPECT_LE(out.size(), 8u);
  EXPECT_NE(out.find("..."), std::string::npos);
}

TEST(ElideName, BaseName) {
  EXPECT_EQ(baseName(kHiveKey), "2026-05-19_15-30-14_BLOCK-A_0.mcap");
  EXPECT_EQ(baseName("flat_nissan.mcap"), "flat_nissan.mcap");
  EXPECT_EQ(baseName("a/b/c"), "c");
  EXPECT_EQ(baseName("trailing/"), "");
}

}  // namespace
}  // namespace mcap_cloud
