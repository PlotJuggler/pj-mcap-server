// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// PURE helpers for the customer/site browse gate (no ix/proto/Qt includes).
// Names are the durable identity (persisted, survive rebuilds); ids are session
// handles bound to a generation.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "backend_types.hpp"

namespace mcap_cloud {

// Resolve a persisted/typed (customer, site) NAME pair against `vocab` into a
// ListFilter carrying their current ids + the vocabulary's generation. Names
// are matched exactly (case-sensitive, no trimming) — the caller normalizes if
// it wants that. Returns nullopt when either name is absent from `vocab`: a
// rebuild can rename/remove a site (or the caller is replaying a stale
// persisted selection typed before this vocabulary was fetched), and a
// half-resolved filter (customer id but no site) is not a valid ListFilter.
// The returned ids are session handles: they are only meaningful together with
// the embedded generation, and go stale the moment a new vocabulary arrives.
// `robot` is REQUIRED: the file list is by far the most expensive thing the
// browse path transfers, and robot is the cheapest dimension that cuts it down,
// so nothing is listed until all three are chosen. The server ANDs the ids it is
// given, so sending robot_id narrows the query server-side rather than
// downloading a whole site and filtering locally.
[[nodiscard]] inline std::optional<ListFilter> resolveGateFilter(const VocabularyInfo& vocab,
                                                                 const std::string& customer,
                                                                 const std::string& site,
                                                                 const std::string& robot) {
  for (const auto& c : vocab.customers) {
    if (c.name != customer) {
      continue;
    }
    for (const auto& s : c.sites) {
      if (s.name == site) {
        for (const auto& r : s.robots) {
          if (r.name != robot) {
            continue;
          }
          ListFilter f;
          f.customer_id = c.id;
          f.site_id = s.id;
          f.robot_id = r.id;
          f.generation = vocab.generation;
          return f;
        }
        // Site matched but the robot did not: a rebuild can retire a robot while
        // the client still holds the persisted name. Site names are unique
        // within a customer, so no later site could match either.
        return std::nullopt;
      }
    }
    // Customer matched but no site under it matched: stop here rather than
    // keep scanning. Customer names are unique within one VocabularyInfo (the
    // catalog builds this list via a GROUP BY on customer), so no later
    // customer entry could match `customer` either.
    return std::nullopt;
  }
  return std::nullopt;
}

// Returns the sole customer name when `vocab` has EXACTLY one customer, else
// "". The empty result is deliberately ambiguous between "zero customers"
// (empty catalog) and "more than one" (needs an explicit pick) — those are
// different GatePhases, so a caller that must tell them apart checks
// vocab.customers.size() itself rather than relying on this return value.
[[nodiscard]] inline std::string autoSelectCustomer(const VocabularyInfo& vocab) {
  return vocab.customers.size() == 1 ? vocab.customers[0].name : std::string{};
}

// Lists the site names under `customer`, in vocabulary (catalog) order — no
// sorting is applied here, so the caller sees whatever order the server built
// the vocabulary in. Returns {} when `customer` is not found in `vocab`.
[[nodiscard]] inline std::vector<std::string> siteNamesFor(const VocabularyInfo& vocab,
                                                           const std::string& customer) {
  for (const auto& c : vocab.customers) {
    if (c.name == customer) {
      std::vector<std::string> names;
      names.reserve(c.sites.size());
      for (const auto& s : c.sites) {
        names.push_back(s.name);
      }
      return names;
    }
  }
  return {};
}

// Robot names under (customer, site), in vocabulary order. {} when either name
// is absent. These come straight from GetVocabulary — no file list is needed to
// populate the robot combo, which is the whole point of gating on it.
[[nodiscard]] inline std::vector<std::string> robotNamesFor(const VocabularyInfo& vocab,
                                                            const std::string& customer,
                                                            const std::string& site) {
  for (const auto& c : vocab.customers) {
    if (c.name != customer) {
      continue;
    }
    for (const auto& s : c.sites) {
      if (s.name == site) {
        std::vector<std::string> names;
        names.reserve(s.robots.size());
        for (const auto& r : s.robots) {
          names.push_back(r.name);
        }
        return names;
      }
    }
    return {};
  }
  return {};
}

// The sole robot name when (customer, site) has EXACTLY one, else "". Mirrors
// autoSelectCustomer: requiring a pick from a one-item list is pure friction.
[[nodiscard]] inline std::string autoSelectRobot(const VocabularyInfo& vocab,
                                                 const std::string& customer,
                                                 const std::string& site) {
  const auto names = robotNamesFor(vocab, customer, site);
  return names.size() == 1 ? names[0] : std::string{};
}

// The browse gate's lifecycle. Exactly one phase is active; the pill text is a
// pure function of it (booleans could not represent VocabularyError /
// EmptyCatalog / disconnected-with-cached-rows).
enum class GatePhase {
  kDisconnected,       // no connection (cached rows may still be on screen)
  kVocabularyLoading,  // connect done, GetVocabulary in flight (sub-second)
  kVocabularyError,    // GetVocabulary failed on a live connection
  kEmptyCatalog,       // vocabulary arrived with zero customers (fresh bucket)
  kNeedsSelection,     // vocabulary ready, customer/site not both chosen
  kListLoading,        // filtered list in flight (first page lands ~150 ms)
  kListError,          // list failed / incomplete / rebuild storm
  kListEmpty,          // list COMPLETE with zero rows for the selection
  kRows,               // rows on screen
};

// "" = hide the pill. ASCII only (the .ui/source embed pipeline pin).
[[nodiscard]] inline std::string gateHintText(GatePhase phase, std::uint64_t total_files,
                                              std::size_t site_count) {
  switch (phase) {
    case GatePhase::kDisconnected:
      return "Connect to a server to browse recordings";
    case GatePhase::kVocabularyError:
      return "Could not load the catalog - use Refresh to retry";
    case GatePhase::kEmptyCatalog:
      return "The catalog is empty - no recordings have been indexed yet";
    case GatePhase::kNeedsSelection:
      return "Select customer, site and robot to load recordings - " + std::to_string(total_files) +
             " recordings across " + std::to_string(site_count) + " sites";
    case GatePhase::kListError:
      return "Recording list failed or is incomplete - use Refresh to retry";
    case GatePhase::kListEmpty:
      return "No recordings match the current selection";
    case GatePhase::kVocabularyLoading:
    case GatePhase::kListLoading:
    case GatePhase::kRows:
      return {};
  }
  return {};
}

}  // namespace mcap_cloud
