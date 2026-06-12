// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// SessionKey — the NORMALIZED logical identity of a reconstructed session
// (Plan B Task 8a, transcribed into the plugin tree). A session is identified
// by its normalized selection — independent of input order and of transport
// details: (server_uri, sorted+deduped file_ids, sorted+deduped topics,
// time_range). An FNV-1a hash over a length-prefixed canonical encoding gives a
// fast 64-bit bucket key; full-key operator== defeats hash collisions.
//
// HEADER-ONLY (slice mandate): computeSessionKey is `inline` so no separate .cpp
// is needed (functionally identical to the plan's .h+.cpp split). The hash never
// crosses the wire — it keys only the in-process SessionCache — so hashing the
// raw uint64_t/int64_t object bytes (host-endianness-dependent) is acceptable.
//
// The plan's namespace (PJ::cloud) is preserved so the exact Plan B test cases
// (`using namespace PJ::cloud;`) compile verbatim.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace PJ::cloud {

// SessionKey identifies a reconstructed session by its NORMALIZED logical
// selection. file_ids/topics are ascending + deduped; empty topics == "all
// topics". start_ns/end_ns are the absolute wire time window (0,0 = whole).
struct SessionKey {
  std::string server_uri;           // normalized as-passed (caller normalizes the URI)
  std::vector<std::uint64_t> file_ids;  // ascending, deduped
  std::vector<std::string> topics;  // ascending, deduped; empty == "all topics"
  std::int64_t start_ns{0};
  std::int64_t end_ns{0};
  std::uint64_t hash{0};            // FNV-1a over the canonical encoding

  bool operator==(const SessionKey& o) const {
    return server_uri == o.server_uri && file_ids == o.file_ids && topics == o.topics &&
           start_ns == o.start_ns && end_ns == o.end_ns;
  }
  bool operator!=(const SessionKey& o) const { return !(*this == o); }
};

struct TimeRangeNs {
  std::int64_t start_ns{0};
  std::int64_t end_ns{0};
};

namespace detail {

constexpr std::uint64_t kFnvOffset = 1469598103934665603ull;
constexpr std::uint64_t kFnvPrime = 1099511628211ull;

inline void fnvBytes(std::uint64_t& h, const void* p, std::size_t n) {
  const auto* b = static_cast<const unsigned char*>(p);
  for (std::size_t i = 0; i < n; ++i) {
    h ^= b[i];
    h *= kFnvPrime;
  }
}

inline void fnvU64(std::uint64_t& h, std::uint64_t v) { fnvBytes(h, &v, sizeof(v)); }
inline void fnvI64(std::uint64_t& h, std::int64_t v) { fnvBytes(h, &v, sizeof(v)); }

inline void fnvStr(std::uint64_t& h, const std::string& s) {
  fnvU64(h, s.size());  // length-prefix -> unambiguous boundaries
  fnvBytes(h, s.data(), s.size());
}

}  // namespace detail

// Builds the normalized key: sorts + dedupes file_ids and topics, copies the
// time range, and computes a stable 64-bit FNV-1a hash over the canonical byte
// form. Reordered/duplicated inputs collide; an empty topics vector means "all".
inline SessionKey computeSessionKey(const std::string& server_uri, std::vector<std::uint64_t> file_ids,
                                    std::vector<std::string> topics, TimeRangeNs time_range) {
  std::sort(file_ids.begin(), file_ids.end());
  file_ids.erase(std::unique(file_ids.begin(), file_ids.end()), file_ids.end());
  std::sort(topics.begin(), topics.end());
  topics.erase(std::unique(topics.begin(), topics.end()), topics.end());

  std::uint64_t h = detail::kFnvOffset;
  detail::fnvStr(h, server_uri);
  detail::fnvU64(h, file_ids.size());
  for (std::uint64_t id : file_ids) {
    detail::fnvU64(h, id);
  }
  detail::fnvU64(h, topics.size());
  for (const auto& t : topics) {
    detail::fnvStr(h, t);
  }
  detail::fnvI64(h, time_range.start_ns);
  detail::fnvI64(h, time_range.end_ns);

  SessionKey key;
  key.server_uri = server_uri;
  key.file_ids = std::move(file_ids);
  key.topics = std::move(topics);
  key.start_ns = time_range.start_ns;
  key.end_ns = time_range.end_ns;
  key.hash = h;
  return key;
}

}  // namespace PJ::cloud
