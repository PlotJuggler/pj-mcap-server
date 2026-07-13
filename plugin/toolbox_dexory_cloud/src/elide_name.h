// SPDX-License-Identifier: MIT
// Copyright 2026 Davide Faconti
//
// Display elision for long object-key sequence names. Real buckets use deep
// Hive-partitioned keys (customer=.../customer_site=.../robot=.../date=.../<file>.mcap)
// that are ~120+ chars; printed verbatim in the NoWrap Info panel they blow the
// panel width out. These helpers shorten such keys for DISPLAY ONLY — selection,
// requests, and the catalog always use the full key.
#pragma once

#include <cstddef>
#include <string>

namespace dexory_cloud {

// Middle-elide s to at most max_len characters, splicing an ASCII "..." in the
// middle so the head (the customer/site context) and the tail (the distinguishing
// filename) both stay visible. ASCII-only on purpose: the value lands in a
// QPlainTextEdit and must not introduce non-ASCII bytes. max_len is clamped to a
// sane floor so the head+ellipsis+tail arithmetic never underflows.
[[nodiscard]] inline std::string elideMiddle(const std::string& s, std::size_t max_len) {
  static constexpr std::size_t kFloor = 8;
  if (max_len < kFloor) {
    max_len = kFloor;
  }
  if (s.size() <= max_len) {
    return s;
  }
  const std::string dots = "...";
  const std::size_t keep = max_len - dots.size();
  // Bias the tail: the filename at the end is the most distinguishing part of a
  // Hive key (the leading customer=.../ segment is shared across siblings).
  const std::size_t head = keep / 2;
  const std::size_t tail = keep - head;
  return s.substr(0, head) + dots + s.substr(s.size() - tail);
}

// The last '/'-segment of a key ("a/b/c.mcap" -> "c.mcap"); the whole string when
// there is no '/'. Useful when only the filename matters.
[[nodiscard]] inline std::string baseName(const std::string& key) {
  const auto pos = key.find_last_of('/');
  return pos == std::string::npos ? key : key.substr(pos + 1);
}

}  // namespace dexory_cloud
