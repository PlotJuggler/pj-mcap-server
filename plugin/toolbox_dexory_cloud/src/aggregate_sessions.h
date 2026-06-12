// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Time-based session aggregation (client-side, display layer).
//
// Real Dexory recordings are sliced into ~13-min / ~22MB chunk files by a
// rolling recorder. Users want to browse the continuous *run* as one entity,
// not 34 chunks. This groups files into sessions purely from data we already
// have in the catalog: the Hive partition path + per-file [min_ts, max_ts].
//
// Rule (deliberately NOT exact end==start — real recorder seams are µs..ms,
// not zero, so exact matching would shatter a run into singletons):
//   * partition by (customer/site/robot/source) — the s3_key prefix with the
//     `date=` segment and the filename removed (date is ignored so a run that
//     crosses midnight stays one session);
//   * within a partition, sort by (min_ts, key) and start a NEW session
//     wherever the gap to the previous file's max_ts exceeds gap_threshold_ns.
//
// Pure + std-only so it is hermetically unit-testable. Label/columns are the
// dialog's concern; this returns the grouping + spans only.

#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace dexory_cloud {

// One file's identity + time span, as the dialog already holds per sequence.
struct AggInput {
  std::string key;  // full s3_key (the backend identity)
  std::int64_t min_ts_ns = 0;
  std::int64_t max_ts_ns = 0;
};

// One aggregated session: the constituent file keys (in stitched order) plus
// the union span. The dialog formats a label/columns from these.
struct AggSession {
  std::vector<std::string> keys;  // constituent file keys, ordered by (min_ts, key)
  std::int64_t min_ts_ns = 0;     // earliest start across the session
  std::int64_t max_ts_ns = 0;     // latest end across the session
  std::string partition;          // the partition key the session belongs to
};

// The partition key for an s3 object key: the path with the trailing filename
// segment and any `date=` segment removed. Flat keys (no '/') -> empty string,
// so a flat corpus groups purely by time.
inline std::string partitionKey(std::string_view key) {
  // Split into '/'-segments, drop the last (filename) and any date= segment.
  std::vector<std::string_view> kept;
  std::size_t pos = 0;
  std::vector<std::string_view> segs;
  while (pos <= key.size()) {
    std::size_t slash = key.find('/', pos);
    if (slash == std::string_view::npos) {
      segs.push_back(key.substr(pos));
      break;
    }
    segs.push_back(key.substr(pos, slash - pos));
    pos = slash + 1;
  }
  if (segs.empty()) {
    return {};
  }
  segs.pop_back();  // drop the filename
  std::string out;
  for (std::string_view s : segs) {
    if (s.substr(0, 5) == "date=") {
      continue;
    }
    if (!out.empty()) {
      out.push_back('/');
    }
    out.append(s.data(), s.size());
  }
  return out;
}

// Group files into time-contiguous sessions. Deterministic: sessions are
// returned ordered by (min_ts_ns, partition); within a session, keys are
// ordered by (min_ts_ns, key). gap_threshold_ns <= 0 still splits only on a
// strictly-positive gap (so identical-timestamp files never split).
inline std::vector<AggSession> aggregateSessions(
    const std::vector<AggInput>& files, std::int64_t gap_threshold_ns) {
  // Bucket by partition.
  std::map<std::string, std::vector<AggInput>> by_partition;
  for (const auto& f : files) {
    by_partition[partitionKey(f.key)].push_back(f);
  }

  std::vector<AggSession> sessions;
  for (auto& [partition, items] : by_partition) {
    std::sort(items.begin(), items.end(), [](const AggInput& a, const AggInput& b) {
      if (a.min_ts_ns != b.min_ts_ns) {
        return a.min_ts_ns < b.min_ts_ns;
      }
      return a.key < b.key;
    });
    AggSession cur;
    cur.partition = partition;
    bool open = false;
    std::int64_t prev_max = 0;
    for (const auto& it : items) {
      const bool split = open && (it.min_ts_ns - prev_max) > gap_threshold_ns;
      if (!open || split) {
        if (open) {
          sessions.push_back(std::move(cur));
        }
        cur = AggSession{};
        cur.partition = partition;
        cur.min_ts_ns = it.min_ts_ns;
        cur.max_ts_ns = it.max_ts_ns;
        open = true;
      }
      cur.keys.push_back(it.key);
      cur.min_ts_ns = std::min(cur.min_ts_ns, it.min_ts_ns);
      cur.max_ts_ns = std::max(cur.max_ts_ns, it.max_ts_ns);
      prev_max = std::max(prev_max, it.max_ts_ns);
    }
    if (open) {
      sessions.push_back(std::move(cur));
    }
  }

  std::sort(sessions.begin(), sessions.end(), [](const AggSession& a, const AggSession& b) {
    if (a.min_ts_ns != b.min_ts_ns) {
      return a.min_ts_ns < b.min_ts_ns;
    }
    return a.partition < b.partition;
  });
  return sessions;
}

}  // namespace dexory_cloud
