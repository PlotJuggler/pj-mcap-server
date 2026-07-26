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

[[nodiscard]] inline std::optional<ListFilter> resolveGateFilter(const VocabularyInfo& vocab,
                                                                 const std::string& customer,
                                                                 const std::string& site) {
  for (const auto& c : vocab.customers) {
    if (c.name != customer) {
      continue;
    }
    for (const auto& s : c.sites) {
      if (s.name == site) {
        ListFilter f;
        f.customer_id = c.id;
        f.site_id = s.id;
        f.generation = vocab.generation;
        return f;
      }
    }
    return std::nullopt;  // customer matched, site didn't
  }
  return std::nullopt;
}

[[nodiscard]] inline std::string autoSelectCustomer(const VocabularyInfo& vocab) {
  return vocab.customers.size() == 1 ? vocab.customers[0].name : std::string{};
}

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
      return "Select customer and site to load recordings - " + std::to_string(total_files) +
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
