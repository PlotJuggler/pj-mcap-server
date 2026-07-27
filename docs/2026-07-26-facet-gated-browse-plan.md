# Facet-Gated Browse (customer/site gate + GetVocabulary UI) Implementation Plan — v2 (post-Codex-review)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop fetching the whole 25,550-file catalog on connect — fetch the tiny `GetVocabulary` tree instead, require a customer+site selection (an unconditional gate), then stream only that site's server-filtered list, with a pill empty-state making the gate visually explicit.

**Architecture:** Client-only; zero wire changes. The server side is complete: `GetVocabulary` (`server/internal/ws/handlers_catalog.go:273`) returns a customer→site tree with per-node `file_count` plus `catalog_generation`; `FileFilter.customer_id/site_id` + `ListFilesRequest.expected_catalog_generation` filter server-side; the server **rejects a first page carrying dimension ids without a generation echo as `ERROR_INVALID_REQUEST`** (`handlers_catalog.go:150`, pinned by `generation_list_test.go:180`), validates later pages via the generation embedded in `page_token` (`handlers_catalog.go:157`), and answers a dead generation with `ERROR_STALE_CATALOG` before querying — a rebuild between vocabulary-fetch and page one **cannot** silently serve a renumbered wrong site (`handlers_catalog.go:125,175`).

Client additions: (1) vocabulary types + `getVocabulary()`; (2) `ListFilter` on `listSequences()` with **abort-on-stale** semantics (filtered stale is never blind-retried — the ids belong to a dead generation) and an **abort predicate** for supersession; (3) pure name→id resolution (names are the durable identity; ids are session handles); (4) `FetchWorker` gate commands carrying a **monotonic gate-request id** with a latest-wins cancellation guard; (5) the dialog gate: a **permanent gate row** (outside the Basic/Advanced pane swap), an explicit **GatePhase** state machine driving the pill, a single `beginGateRequestLocked` transition that clears all site-scoped state, and per-server persistence under a **canonical server key**.

**Tech Stack:** C++20, ixwebsocket, protobuf (checked-in bindings; proto UNCHANGED), gtest/ctest, PanelEngine-rendered `.ui` (ASCII; `wd.setItems`/`wd.setCurrentIndex`/`wd.setLabel`/`wd.setVisible`; combo events via `onIndexChanged`).

**Decisions locked with the user (do not re-litigate):** date default = All; gate unconditional; merged all-sites view removed; combo items = plain names; customer combo permanent (more customers coming).

**Codex-review integration (2026-07-26, session 019f9e80-0011-7150-96bb-2e4fe38c552f).** All 14 findings accepted: exact proto names confirmed (F1 — `get_vocabulary` = field 17 client / 19 server); fake server enforces the real generation contract (F2); recovery no longer double-triggers a sweep (F3 — `vocabularyReady` carries a `recovery` flag; the dialog auto-starts only on non-recovery); gate-request ids + abort predicate + latest-wins guard (F4); terminal `GateListResult{id, complete, error}` instead of overloading `sequencesReady` (F5); gate combos moved OUT of `basicPane` into a permanent row (F6); real WidgetData/Settings APIs (F7 — `setCurrentIndex(-1)` = unselected, `onIndexChanged`, `SettingsStore` wrapper); canonical server key + connected-URI capture (F8); `beginGateRequestLocked` clears rows/selection/topics/date and bumps `seq_epoch` (F9); `kBasicFilterKeys` split three ways + one-shot settings migration (F10); pill = same-grid-cell overlay + `setLabel` (F11); GatePhase enum replaces the boolean pill model (F12); no `serve_vocabulary` flag, legacy discovery callbacks stay inert (F13); `.ui` ASCII/embed pipeline confirmed safe (F14).

**Key existing code (read first):** `src/backend_connection.{hpp,cpp}` (`listSequences` + `kMaxStaleRetries` at `.cpp:427`, `kListFilesPageLimit=500`); `tests/list_pagination_test.cpp` (`FakePagingServer`); `src/fetch_worker.{hpp,cpp}` (FIFO command queue, worker-thread callbacks, `mcap_cloud_dialog.cpp:654/670` queue drain); `src/mcap_cloud_dialog.cpp` (callback wiring 464, `postEvent` 678, combo feed 1140–1151, basic-filter keys `kBasicFilterKeys` at 252 — ALSO the Lua canonical fields at 297, settings restore 608, clear 2626, refresh 1587, connect-finished ~2390, `clearSelectionAndTopicsLocked`-style helper at 2186, date seeding 2699, `onConnectionLost` 3128); `src/settings_store.hpp:18` (`SettingsStore` over `PJ::sdk::SettingsView`); `src/server_history.cpp:14` (`normalizeServerKey` — currently only knows `grpc://`); SDK `widget_data.hpp` (`setItems:60`, `setLabel:253`, `setVisible:415`; NO `setCurrentText`) and `dialog_plugin_typed.hpp:133` (`onIndexChanged`).

**File structure (under `plugin/toolbox_mcap_cloud/`):**

| File | Change | Responsibility |
|---|---|---|
| `src/backend_types.hpp` | Modify | `VocabSite/VocabCustomer/VocabularyInfo`, `ListFilter` |
| `src/backend_connection.hpp/.cpp` | Modify | `getVocabulary()`; `listSequences(..., filter, stale_vocabulary, abort)` |
| `src/vocab_select.hpp` | Create | PURE: resolver, auto-select, site names, `GatePhase`, `gateHintText` |
| `src/fetch_worker.hpp/.cpp` | Modify | Gate commands with request ids, latest-wins guard, stale recovery, `GateListResult` |
| `src/mcap_cloud_dialog.hpp/.cpp` | Modify | GatePhase machine, gate row combos, `beginGateRequestLocked`, persistence + migration, pill |
| `src/server_history.cpp/.h` | Modify | `normalizeServerKey` learns `ws://`/`wss://` |
| `ui/mcap_cloud_panel.ui` | Modify | Permanent gate row; pill overlaid in the seqTable grid cell (ASCII) |
| `tests/list_pagination_test.cpp` | Modify | Vocabulary + contract-enforcing filter + stale + supersession tests |
| `tests/vocab_select_test.cpp` | Create | Resolver / auto-select / GatePhase / pill-text tests |
| `tests/server_history_test.cpp` | Modify | ws/wss canonicalization tests |
| `CMakeLists.txt` | Modify | Register `vocab_select_test` |
| READMEs + `CLAUDE.md` | Modify | Browse-flow docs |

---

### Task 1: Vocabulary types + `BackendConnection::getVocabulary()`

**Files:** Modify `src/backend_types.hpp`, `src/backend_connection.{hpp,cpp}`; Test `tests/list_pagination_test.cpp`.

Proto names are CONFIRMED (F1): `ClientMessage` oneof field `get_vocabulary` (= 17) → `request.mutable_get_vocabulary()`; `ServerMessage` oneof field `get_vocabulary` (= 19) → `response.has_get_vocabulary()/get_vocabulary()`; `GetVocabularyResponse{customers, sources, tags, catalog_generation}`; `DimCustomer{id, name, file_count, sites}`; `DimSite{id, name, file_count, robots}`.

- [ ] **Step 1: Value types** (`src/backend_types.hpp`, after `struct ServerCaps`):

```cpp
// GetVocabularyResponse mapped for the client (catalog-vocabulary-rpc.md).
// ids are SESSION HANDLES bound to `generation` — they renumber across builder
// rebuilds and MUST only be echoed together with the generation they came from.
struct VocabSite {
  std::uint64_t id = 0;
  std::string name;
  std::uint64_t file_count = 0;
};
struct VocabCustomer {
  std::uint64_t id = 0;
  std::string name;
  std::uint64_t file_count = 0;
  std::vector<VocabSite> sites;
};
struct VocabularyInfo {
  std::vector<VocabCustomer> customers;
  std::string generation;  // opaque bytes; echo with any dimension-id filter
  [[nodiscard]] std::uint64_t totalFiles() const {
    std::uint64_t n = 0;
    for (const auto& c : customers) { n += c.file_count; }
    return n;
  }
  [[nodiscard]] std::size_t totalSites() const {
    std::size_t n = 0;
    for (const auto& c : customers) { n += c.sites.size(); }
    return n;
  }
};
```

- [ ] **Step 2: Failing tests.** Extend `FakePagingServer`'s message callback — vocabulary is ALWAYS answered, no constructor flag (F13); insert before the `has_list_files()` branch:

```cpp
      } else if (request.has_get_vocabulary()) {
        auto* vocab = response.mutable_get_vocabulary();
        auto* customer = vocab->add_customers();
        customer->set_id(11);
        customer->set_name("dexory");
        customer->set_file_count(static_cast<std::uint64_t>(total_rows_));
        auto* site_a = customer->add_sites();
        site_a->set_id(21);
        site_a->set_name("nashville");
        site_a->set_file_count(static_cast<std::uint64_t>(total_rows_ / 2));
        auto* site_b = customer->add_sites();
        site_b->set_id(22);
        site_b->set_name("wallingford");
        site_b->set_file_count(static_cast<std::uint64_t>(total_rows_ - total_rows_ / 2));
        vocab->set_catalog_generation("gen-1");
```

Tests:

```cpp
TEST(Vocabulary, MapsTheCustomerSiteTree) {
  FakePagingServer server(/*total_rows=*/1000, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  const auto vocab = conn.getVocabulary();
  ASSERT_TRUE(vocab.has_value());
  EXPECT_EQ(vocab->generation, "gen-1");
  ASSERT_EQ(vocab->customers.size(), 1u);
  EXPECT_EQ(vocab->customers[0].name, "dexory");
  EXPECT_EQ(vocab->customers[0].id, 11u);
  ASSERT_EQ(vocab->customers[0].sites.size(), 2u);
  EXPECT_EQ(vocab->customers[0].sites[0].name, "nashville");
  EXPECT_EQ(vocab->customers[0].sites[0].id, 21u);
  EXPECT_EQ(vocab->customers[0].sites[0].file_count, 500u);
  EXPECT_EQ(vocab->totalFiles(), 1000u);
  EXPECT_EQ(vocab->totalSites(), 2u);
}

TEST(Vocabulary, FailsCleanlyWhenNotConnected) {
  mcap_cloud::BackendConnection conn("ws://127.0.0.1:1", "", "", false);
  EXPECT_FALSE(conn.getVocabulary().has_value());
}
```

- [ ] **Step 3: Build the test target, verify RED** (`'getVocabulary' is not a member`).

- [ ] **Step 4: Implement.** `src/backend_connection.hpp`:

```cpp
  // GetVocabulary RPC: the customer->site filter tree + the generation its ids
  // are bound to. nullopt on timeout / dead socket / server Error. ids are ONLY
  // valid together with result.generation — echo both, never cache ids alone.
  [[nodiscard]] std::optional<VocabularyInfo> getVocabulary();
```

`src/backend_connection.cpp`:

```cpp
std::optional<VocabularyInfo> BackendConnection::getVocabulary() {
  if (!socket_) {
    return std::nullopt;
  }
  pj_cloud::v1::ClientMessage request;
  request.mutable_get_vocabulary();  // empty request message
  pj_cloud::v1::ServerMessage response;
  if (!sendAndWait(request, &response) || !response.has_get_vocabulary()) {
    return std::nullopt;
  }
  const auto& wire = response.get_vocabulary();
  VocabularyInfo out;
  out.generation = wire.catalog_generation();
  out.customers.reserve(static_cast<std::size_t>(wire.customers_size()));
  for (const auto& c : wire.customers()) {
    VocabCustomer customer;
    customer.id = c.id();
    customer.name = c.name();
    customer.file_count = c.file_count();
    customer.sites.reserve(static_cast<std::size_t>(c.sites_size()));
    for (const auto& s : c.sites()) {
      customer.sites.push_back(VocabSite{s.id(), s.name(), s.file_count()});
    }
    out.customers.push_back(std::move(customer));
  }
  return out;
}
```

- [ ] **Step 5: Build + PASS + full `ctest -E Live`.**
- [ ] **Step 6: Commit** `feat(plugin): map GetVocabulary into client vocabulary types`.

---

### Task 2: `ListFilter` + filtered `listSequences()` — contract-faithful fake + abort predicate

**Files:** Modify `src/backend_types.hpp`, `src/backend_connection.{hpp,cpp}`; Test `tests/list_pagination_test.cpp`.

Semantics (confirmed against the real server, F2): filtered + `ERROR_STALE_CATALOG` → abort, report `stale_vocabulary`, NO internal retry (ids belong to a dead generation; a blind retry could select a renumbered WRONG site). Unfiltered keeps the bounded restart. Generation echo goes on **page one only**; later pages carry it inside `page_token`. The abort predicate (F4) lets a superseded sweep stop between pages.

- [ ] **Step 1: `ListFilter`** in `src/backend_types.hpp`:

```cpp
// Server-side ListFiles dimension filter. ids come from a VocabularyInfo and
// are ONLY meaningful with that vocabulary's generation (echoed on page one).
struct ListFilter {
  std::optional<std::uint64_t> customer_id;
  std::optional<std::uint64_t> site_id;
  std::string generation;  // REQUIRED when any id is set (server: INVALID_REQUEST otherwise)
  [[nodiscard]] bool empty() const { return !customer_id && !site_id; }
};
```

- [ ] **Step 2: Contract-faithful fake.** In `FakePagingServer`'s `has_list_files()` branch (F2): record per-page `(limit, filter site_id, generation echo)`; **reject dimension-ids-without-generation on page one with `ERROR_INVALID_REQUEST`** (the real contract, `handlers_catalog.go:150`); reject a wrong generation with `ERROR_STALE_CATALOG`; serve `site_rows_ = 300` rows when `filter.site_id` set; support `stale_at_page_` also for filtered requests (a rebuild mid-sweep). Add members `int site_rows_ = 300;`, `std::vector<std::string> generations_seen_;`, `std::vector<std::optional<std::uint64_t>> filter_sites_seen_;` (+ locked accessors), and an optional `std::atomic<int> page_delay_ms_{0}` (used by Task 4's supersession test):

```cpp
        const auto& req = request.list_files();
        const bool filtered = req.filter().has_site_id() || req.filter().has_customer_id();
        const int page_index = req.page_token().empty() ? 0 : std::stoi(req.page_token());
        {
          std::lock_guard<std::mutex> lock(mu_);
          limits_seen_.push_back(req.limit());
          generations_seen_.push_back(req.expected_catalog_generation());
          filter_sites_seen_.push_back(req.filter().has_site_id()
                                           ? std::optional<std::uint64_t>(req.filter().site_id())
                                           : std::nullopt);
        }
        if (page_delay_ms_.load() > 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(page_delay_ms_.load()));
        }
        // Real contract (handlers_catalog.go:150): first page + dimension ids
        // REQUIRES the generation echo -> INVALID_REQUEST, not stale.
        if (filtered && page_index == 0 && req.expected_catalog_generation().empty()) {
          auto* err = response.mutable_error();
          err->set_code(pj_cloud::v1::ERROR_INVALID_REQUEST);
          err->set_message("dimension filter without expected_catalog_generation");
        } else if (!req.expected_catalog_generation().empty() &&
                   req.expected_catalog_generation() != "gen-1") {
          auto* err = response.mutable_error();
          err->set_code(pj_cloud::v1::ERROR_STALE_CATALOG);
          err->set_message("stale generation");
        } else if (page_index == stale_at_page_ && !stale_fired_.exchange(true)) {
          auto* err = response.mutable_error();
          err->set_code(pj_cloud::v1::ERROR_STALE_CATALOG);
          err->set_message("catalog rebuilt mid-pagination");
        } else {
          const int rows_for_request = filtered ? site_rows_ : total_rows_;
          // ... existing page-slicing code, using rows_for_request ...
        }
```

(Restructure the existing branch to this if/else chain; the final `else` keeps the current slicing + `next_page_token` logic verbatim.)

- [ ] **Step 3: Failing tests:**

```cpp
TEST(FilteredList, SendsSiteIdWithGenerationOnPageOneOnly) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  mcap_cloud::ListFilter filter;
  filter.customer_id = 11;
  filter.site_id = 21;
  filter.generation = "gen-1";
  bool complete = false;
  bool stale = false;
  const auto rows = conn.listSequences(&complete, {}, &filter, &stale);
  EXPECT_TRUE(complete);
  EXPECT_FALSE(stale);
  EXPECT_EQ(rows.size(), 300u);  // the fake's site_rows_
  const auto gens = server.generationsSeen();
  ASSERT_GE(gens.size(), 1u);
  EXPECT_EQ(gens[0], "gen-1");            // page one: explicit echo
  for (std::size_t i = 1; i < gens.size(); ++i) {
    EXPECT_TRUE(gens[i].empty()) << "later pages ride page_token, no echo";
  }
  for (const auto& s : server.filterSitesSeen()) {
    EXPECT_EQ(s, std::optional<std::uint64_t>(21)) << "filter on EVERY page";
  }
}

TEST(FilteredList, StaleOnPageTwoAbortsWithoutRetryAndReportsStaleVocabulary) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;
  mcap_cloud::ListFilter filter;
  filter.site_id = 21;
  filter.generation = "gen-1";
  bool complete = false;
  bool stale = false;
  const auto rows = conn.listSequences(&complete, {}, &filter, &stale);
  EXPECT_FALSE(complete);
  EXPECT_TRUE(stale) << "caller must re-resolve names->ids, never blind-retry";
  EXPECT_TRUE(rows.empty());
  EXPECT_EQ(server.limitsSeen().size(), 2u) << "page 0 ok, page 1 stale, NO retry";
}

TEST(FilteredList, DeadGenerationOnPageOneAbortsStale) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;
  mcap_cloud::ListFilter filter;
  filter.site_id = 21;
  filter.generation = "gen-0-DEAD";
  bool complete = false;
  bool stale = false;
  const auto rows = conn.listSequences(&complete, {}, &filter, &stale);
  EXPECT_FALSE(complete);
  EXPECT_TRUE(stale);
  EXPECT_TRUE(rows.empty());
  EXPECT_EQ(server.limitsSeen().size(), 1u);
}

TEST(FilteredList, UnfilteredStaleRetryBehaviourIsUnchanged) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;
  bool complete = false;
  bool stale = false;
  const auto rows = conn.listSequences(&complete, {}, nullptr, &stale);
  EXPECT_TRUE(complete);
  EXPECT_FALSE(stale);
  EXPECT_EQ(rows.size(), 1200u);
}

TEST(FilteredList, AbortPredicateStopsTheSweepBetweenPages) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/-1);
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;
  int pages_seen = 0;
  bool complete = true;
  const auto rows = conn.listSequences(
      &complete,
      [&](const std::vector<mcap_cloud::SequenceInfo>&, bool) { ++pages_seen; },
      nullptr, nullptr,
      /*abort=*/[&] { return pages_seen >= 1; });  // supersession after page one
  EXPECT_FALSE(complete);
  EXPECT_EQ(pages_seen, 1);
  EXPECT_LT(rows.size(), 1200u);
}
```

- [ ] **Step 4: Verify RED** (no such overload), **then implement.** Signature (defaults keep every caller compiling):

```cpp
  [[nodiscard]] std::vector<SequenceInfo> listSequences(
      bool* complete = nullptr, const PageCallback& on_page = {},
      const ListFilter* filter = nullptr, bool* stale_vocabulary = nullptr,
      const std::function<bool()>& abort = {});
```

In the page loop of `backend_connection.cpp`:
1. Initialize `if (stale_vocabulary) { *stale_vocabulary = false; }` at entry.
2. Top of each page iteration: `if (abort && abort()) { return sequences; }  // superseded — PARTIAL (exhausted stays false)`.
3. After `list->set_limit(kListFilesPageLimit);`:

```cpp
      if (filter != nullptr && !filter->empty()) {
        auto* wire_filter = list->mutable_filter();
        if (filter->customer_id) { wire_filter->set_customer_id(*filter->customer_id); }
        if (filter->site_id)     { wire_filter->set_site_id(*filter->site_id); }
        // Page-one-only echo: later pages carry the generation inside
        // page_token (server contract, handlers_catalog.go:157).
        if (page_token.empty()) {
          list->set_expected_catalog_generation(filter->generation);
        }
      }
```

4. In the `ERROR_STALE_CATALOG` branch:

```cpp
        if (filter != nullptr && !filter->empty()) {
          // Filtered ids belong to a dead generation: retrying is at best
          // futile, at worst selects a renumbered WRONG dimension. Abort;
          // the caller re-resolves names -> ids from a fresh vocabulary.
          if (stale_vocabulary != nullptr) { *stale_vocabulary = true; }
          return {};
        }
        stale = true;  // unfiltered: bounded restart (existing behaviour)
        break;
```

- [ ] **Step 5: Build + PASS + full `ctest -E Live`** (pre-existing pagination tests prove caller compatibility).
- [ ] **Step 6: Commit** `feat(plugin): server-filtered listSequences with stale-abort + supersession predicate`.

---

### Task 3: Pure gate helpers (`vocab_select.hpp`) — resolver + GatePhase + pill text

**Files:** Create `src/vocab_select.hpp`, `tests/vocab_select_test.cpp`; Modify `CMakeLists.txt` (copy the `tls_utils_test` target shape at ~line 422; name `toolbox_mcap_cloud_vocab_select_test`, ctest `McapCloudVocabSelectTest`).

The boolean pill model was rejected in review (F12): an explicit phase enum makes the states mutually exclusive by construction and representable for vocabulary failure, empty catalog, and disconnected-with-cached-rows.

- [ ] **Step 1: Failing tests** (`tests/vocab_select_test.cpp`):

```cpp
// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "vocab_select.hpp"

#include <gtest/gtest.h>

namespace {
mcap_cloud::VocabularyInfo vocab() {
  mcap_cloud::VocabularyInfo v;
  v.generation = "gen-7";
  mcap_cloud::VocabCustomer c;
  c.id = 11; c.name = "dexory"; c.file_count = 100;
  c.sites.push_back({21, "nashville", 60});
  c.sites.push_back({22, "wallingford", 40});
  v.customers.push_back(c);
  return v;
}
}  // namespace

TEST(ResolveGate, ResolvesNamesToIdsAndGeneration) {
  const auto f = mcap_cloud::resolveGateFilter(vocab(), "dexory", "nashville");
  ASSERT_TRUE(f.has_value());
  EXPECT_EQ(f->customer_id, std::optional<std::uint64_t>(11));
  EXPECT_EQ(f->site_id, std::optional<std::uint64_t>(21));
  EXPECT_EQ(f->generation, "gen-7");
}

TEST(ResolveGate, UnknownSiteFailsEvenWhenCustomerResolves) {
  // A rebuild can remove a site (or fix a typo like nashvillee) while the
  // client still holds the old name.
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "dexory", "gone").has_value());
}

TEST(ResolveGate, UnknownCustomerFails) {
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "acme", "nashville").has_value());
}

TEST(ResolveGate, EmptyNamesFail) {
  EXPECT_FALSE(mcap_cloud::resolveGateFilter(vocab(), "", "").has_value());
}

TEST(AutoSelect, SingleCustomerIsAutoSelected) {
  EXPECT_EQ(mcap_cloud::autoSelectCustomer(vocab()), "dexory");
}

TEST(AutoSelect, MultipleCustomersRequireAnExplicitPick) {
  auto v = vocab();
  mcap_cloud::VocabCustomer c2;
  c2.id = 12; c2.name = "acme";
  v.customers.push_back(c2);
  EXPECT_EQ(mcap_cloud::autoSelectCustomer(v), "");
}

TEST(SiteNames, ListsSitesForTheSelectedCustomerOnly) {
  const auto names = mcap_cloud::siteNamesFor(vocab(), "dexory");
  ASSERT_EQ(names.size(), 2u);
  EXPECT_EQ(names[0], "nashville");
  EXPECT_EQ(mcap_cloud::siteNamesFor(vocab(), "acme").size(), 0u);
}

// --- GatePhase pill text (F12: explicit phases, not booleans) --------------

TEST(GateHint, EveryPhaseHasTheRightText) {
  using mcap_cloud::GatePhase;
  using mcap_cloud::gateHintText;
  EXPECT_EQ(gateHintText(GatePhase::kDisconnected, 0, 0),
            "Connect to a server to browse recordings");
  EXPECT_EQ(gateHintText(GatePhase::kVocabularyLoading, 0, 0), "");
  EXPECT_EQ(gateHintText(GatePhase::kVocabularyError, 0, 0),
            "Could not load the catalog - use Refresh to retry");
  EXPECT_EQ(gateHintText(GatePhase::kEmptyCatalog, 0, 0),
            "The catalog is empty - no recordings have been indexed yet");
  EXPECT_EQ(gateHintText(GatePhase::kNeedsSelection, 25550, 6),
            "Select customer and site to load recordings - 25550 recordings across 6 sites");
  EXPECT_EQ(gateHintText(GatePhase::kListLoading, 25550, 6), "");
  EXPECT_EQ(gateHintText(GatePhase::kListError, 25550, 6),
            "Recording list failed or is incomplete - use Refresh to retry");
  EXPECT_EQ(gateHintText(GatePhase::kListEmpty, 25550, 6),
            "No recordings match the current selection");
  EXPECT_EQ(gateHintText(GatePhase::kRows, 25550, 6), "");
}
```

- [ ] **Step 2: Standalone-compile RED** (`g++ -std=c++20 -Wall -Wextra tests/vocab_select_test.cpp -Isrc ... -lgtest_main -lgtest -lpthread` — same loop as tls_utils).

- [ ] **Step 3: Implement** `src/vocab_select.hpp`:

```cpp
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
// pure function of it (F12 — booleans could not represent VocabularyError /
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
```

- [ ] **Step 4: PASS standalone; add the CMake target; `ctest -E Live` green.**
- [ ] **Step 5: Commit** `feat(plugin): pure gate resolver + GatePhase pill model`.

---

### Task 4: `FetchWorker` — gate request ids, latest-wins, stale recovery, terminal result

**Files:** Modify `src/fetch_worker.hpp`, `src/fetch_worker.cpp`.

Design (F3/F4/F5): every gate command carries a dialog-issued monotonic `request_id`. The worker keeps `std::atomic<std::uint64_t> latest_gate_request_`; a queued command no-ops if superseded before starting, and an in-flight sweep aborts between pages via the Task 2 predicate. The terminal signal is a dedicated `GateListResult` — `sequencesReady` is NOT reused for gated lists (it conflates completion and failure, F5). The recovery path's vocabulary refresh is marked `recovery=true` so the dialog does NOT auto-start a second sweep (F3). Legacy `sequenceListStarted`/`sequenceInfoReady` stay declared but inert (F13).

- [ ] **Step 1: API** (`src/fetch_worker.hpp`):

```cpp
  /// Terminal result of ONE gated (filtered) list request. Exactly one is
  /// emitted per listSequencesFilteredAsync call that is not superseded before
  /// starting. `complete=false` + error explains why (the dialog must NOT
  /// render "no recordings" for anything but a complete empty result).
  struct GateListResult {
    std::uint64_t request_id = 0;
    bool complete = false;
    enum class Error { kNone, kPartial, kConnectionLost, kSelectionGone, kRebuildStorm, kSuperseded };
    Error error = Error::kNone;
    std::vector<SequenceInfo> sequences;
  };

  /// Vocabulary result. recovery=true when this refresh was triggered from
  /// INSIDE a filtered-list stale recovery — the dialog must then only refresh
  /// combos, never auto-start another sweep (the recovery owns the retry).
  std::function<void(VocabularyInfo vocab, bool recovery)> vocabularyReady;
  /// GetVocabulary failed on a live connection (F12's kVocabularyError phase).
  std::function<void(std::uint64_t request_id)> vocabularyFailed;
  /// One page of a gated sweep; reset semantics as in BackendConnection.
  std::function<void(std::uint64_t request_id, std::vector<SequenceInfo> page, bool reset)>
      gatePageReady;
  std::function<void(GateListResult result)> gateListFinished;

  /// Fetch the vocabulary (the gate's data source). request_id is echoed to
  /// vocabularyReady/vocabularyFailed so the dialog can ignore stale answers.
  void fetchVocabularyAsync(std::uint64_t request_id);
  /// List ONE site server-filtered, resolving NAMES against the latest
  /// vocabulary; supersedable by a later gate request id.
  void listSequencesFilteredAsync(std::uint64_t request_id, std::string customer, std::string site);
```

Private members: `std::optional<VocabularyInfo> vocab_;` (worker-thread only), `std::atomic<std::uint64_t> latest_gate_request_{0};`, `std::string last_gate_customer_, last_gate_site_;` (worker-thread only, for the tag-edit re-list). Public: `void supersedeGateRequests(std::uint64_t latest) { latest_gate_request_.store(latest); }` — called from the GUI thread when a new request is issued so an in-flight sweep aborts promptly (atomic: safe cross-thread).

- [ ] **Step 2: Implement** (`src/fetch_worker.cpp`):

```cpp
void FetchWorker::fetchVocabularyAsync(std::uint64_t request_id) {
  if (!backend_) {
    if (vocabularyFailed) { vocabularyFailed(request_id); }
    return;
  }
  auto vocab = backend_->getVocabulary();
  if (backend_->isClosed()) {
    notifyConnectionLostOnce();
    return;
  }
  if (!vocab) {
    if (vocabularyFailed) { vocabularyFailed(request_id); }
    return;
  }
  vocab_ = std::move(*vocab);
  if (vocabularyReady) { vocabularyReady(*vocab_, /*recovery=*/false); }
}

void FetchWorker::listSequencesFilteredAsync(std::uint64_t request_id, std::string customer,
                                             std::string site) {
  auto finish = [this, request_id](bool complete, GateListResult::Error error,
                                   std::vector<SequenceInfo> sequences) {
    if (gateListFinished) {
      gateListFinished(GateListResult{request_id, complete, error, std::move(sequences)});
    }
  };
  if (latest_gate_request_.load() != request_id) {
    finish(false, GateListResult::Error::kSuperseded, {});
    return;  // a newer gate request is already queued behind us — don't sweep
  }
  if (!backend_) {
    finish(false, GateListResult::Error::kConnectionLost, {});
    return;
  }
  last_gate_customer_ = customer;
  last_gate_site_ = site;
  // Two resolution attempts: the cached vocabulary, then ONE refresh when the
  // generation died mid-request (builder rebuild).
  for (int attempt = 0; attempt < 2; ++attempt) {
    if (!vocab_) {
      auto fresh = backend_->getVocabulary();
      if (!fresh) {
        break;  // connection-level failure — the error below explains it
      }
      vocab_ = std::move(*fresh);
      // recovery=true: combos refresh, but the dialog must NOT start a second
      // sweep — THIS loop owns the retry (F3, the duplicate-sweep finding).
      if (vocabularyReady) { vocabularyReady(*vocab_, /*recovery=*/true); }
    }
    const auto filter = resolveGateFilter(*vocab_, customer, site);
    if (!filter) {
      finish(false, GateListResult::Error::kSelectionGone, {});
      return;
    }
    bool complete = false;
    bool stale = false;
    auto abort = [this, request_id] { return latest_gate_request_.load() != request_id; };
    BackendConnection::PageCallback on_page;
    if (gatePageReady) {
      on_page = [this, request_id](const std::vector<SequenceInfo>& page, bool reset) {
        gatePageReady(request_id, page, reset);
      };
    }
    auto sequences = backend_->listSequences(&complete, on_page, &*filter, &stale, abort);
    if (backend_->isClosed()) {
      notifyConnectionLostOnce();
      finish(false, GateListResult::Error::kConnectionLost, std::move(sequences));
      return;
    }
    if (latest_gate_request_.load() != request_id) {
      finish(false, GateListResult::Error::kSuperseded, {});
      return;
    }
    if (stale) {
      vocab_.reset();  // ids died with the old generation — refresh + retry once
      continue;
    }
    finish(complete, complete ? GateListResult::Error::kNone : GateListResult::Error::kPartial,
           std::move(sequences));
    return;
  }
  finish(false, GateListResult::Error::kRebuildStorm, {});
}
```

Add `#include "vocab_select.hpp"`. Reset `vocab_`, `last_gate_customer_/site_` where `backend_` is (re)assigned in `connectAsync` (`fetch_worker.cpp:74`). Route the worker-internal tag-edit re-list (~line 258): if `last_gate_customer_/site_` non-empty, call `listSequencesFilteredAsync(latest_gate_request_.load(), last_gate_customer_, last_gate_site_)`; else leave the existing unfiltered call (defensive — a tag edit implies a listed file, so the pair is set in practice). The legacy `listSequencesAsync` stays for that fallback only; `sequencePageReady`/`sequencesReady` keep their current signatures for it.

- [ ] **Step 3: Build whole plugin + full `ctest -E Live`** (no-regression).
- [ ] **Step 4: Commit** `feat(plugin): gate-scoped worker requests with supersession + stale recovery`.

---

### Task 5: Dialog — permanent gate row, GatePhase machine, `beginGateRequestLocked`, persistence

**Files:** Modify `src/mcap_cloud_dialog.{hpp,cpp}`, `ui/mcap_cloud_panel.ui`, `src/server_history.{h,cpp}`; Test `tests/server_history_test.cpp`.

- [ ] **Step 1: Canonical server key first (F8), TDD.** Extend `normalizeServerKey` (`server_history.cpp:14`) to canonicalize `ws://`/`wss://`: lowercase scheme+host, trim whitespace, drop trailing slashes, keep an explicit port. Failing tests in `tests/server_history_test.cpp`:

```cpp
TEST(ServerKey, CanonicalizesWsAndWss) {
  EXPECT_EQ(normalizeServerKey("wss://Plotjuggler.Example.TS.net/"),
            normalizeServerKey("wss://plotjuggler.example.ts.net"));
  EXPECT_EQ(normalizeServerKey("  ws://host:8080/  "), normalizeServerKey("ws://host:8080"));
  EXPECT_NE(normalizeServerKey("ws://host:8080"), normalizeServerKey("ws://host:8081"));
  EXPECT_NE(normalizeServerKey("ws://host"), normalizeServerKey("wss://host"));
}
```

Implement, PASS, commit `feat(plugin): canonical ws/wss server keys`.

- [ ] **Step 2: `.ui` — permanent gate row (F6) + pill overlay (F11).** In `ui/mcap_cloud_panel.ui`:
  - MOVE the `labelCustomer`/`filter_customer` and `labelSite`/`filter_customer_site` rows OUT of `basicPane`'s grid into a NEW always-visible `QHBoxLayout` (`gateRow`) placed directly above the Basic/Advanced switcher: `Customer [combo] Site [combo]` (labels + expanding combos; same widget NAMES so the event plumbing keys stay). `basicPane` keeps only robot/source (grid `rowstretch` shrinks to `"1,1"`).
  - Wrap `seqTable` in a `QGridLayout` where BOTH the table and the pill occupy cell (0,0) — a same-cell overlay, the standard Qt stacking idiom (the label is added after the table so it paints on top):

```xml
<item>
 <layout class="QGridLayout" name="seqTableStack">
  <item row="0" column="0">
   <widget class="QTableWidget" name="seqTable"> <!-- existing widget, moved verbatim --> </widget>
  </item>
  <item row="0" column="0" alignment="Qt::AlignHCenter|Qt::AlignVCenter">
   <widget class="QLabel" name="gateHintLabel">
    <property name="visible"><bool>false</bool></property>
    <property name="styleSheet"><string notr="true">QLabel { background-color: rgba(60,60,60,220); color: white; border-radius: 4px; padding: 6px 14px; }</string></property>
    <property name="text"><string>Select customer and site to load recordings</string></property>
   </widget>
  </item>
 </layout>
</item>
```

  Build + run `McapCloudUiFilesWellFormed` (ASCII + XML gate). If PanelEngine's loader rejects the same-cell overlay at runtime (verify in Task 7's live smoke), the documented fallback is the table-hide swap — keep the label in the cell and add `wd.setVisible("seqTable", ...)` back; note it in the task journal.

- [ ] **Step 3: DialogState + request ids.** In `struct DialogState` (near the discovery block):

```cpp
  // Browse gate: the catalog is NEVER fetched unfiltered. One monotonic id per
  // gate transition; every worker callback echoes it and stale echoes are
  // dropped (F4). Phase drives the pill (F12).
  std::optional<VocabularyInfo> vocabulary;
  std::string gate_customer;  // selected NAMES (durable identity, persisted)
  std::string gate_site;
  std::uint64_t gate_request_seq = 0;   // last issued id
  GatePhase gate_phase = GatePhase::kDisconnected;
  std::string active_server_key;        // canonical key of the CONNECTED server (F8)
```

Include `vocab_select.hpp` in the dialog header.

- [ ] **Step 4: The single transition helper** (dialog cpp; F9 verbatim):

```cpp
// EVERY gate change funnels through here: it clears all site-scoped state so
// rows/selection/topics/dates from the previous site can never leak into (or
// hide — the date span would mask the new site's progressive pages) the next
// one, then issues the new request id and supersedes in-flight sweeps.
std::uint64_t McapCloudDialog::beginGateRequestLocked(GatePhase next_phase) {
  state_.sequences.clear();
  state_.sequence_names.clear();
  progressive_seqs_.clear();
  ++state_.seq_epoch;
  clearSelectionAndTopicsLocked();          // existing helper, ~line 2186
  resetDateFilterToAllLocked();             // extract from the date-seeding code at ~2699
  state_.gate_phase = next_phase;
  const std::uint64_t id = ++state_.gate_request_seq;
  worker_->supersedeGateRequests(id);       // atomic — safe from the GUI thread
  return id;
}
```

(`resetDateFilterToAllLocked` is a small extraction of the existing "seed dates from the full span" reset — locate the date-picker state written at `mcap_cloud_dialog.cpp:2699` and write the "no constraint" values.)

- [ ] **Step 5: Flow wiring.**
  - **Connect finished** (~2390): capture the canonical key + issue the vocabulary request:

```cpp
    std::uint64_t id = 0;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      state_.active_server_key = normalizeServerKey(connected_uri);  // the uri CAPTURED at connect (see below)
      id = beginGateRequestLocked(GatePhase::kVocabularyLoading);
    }
    postCommand([w = worker_.get(), id] { w->fetchVocabularyAsync(id); });
```

  `connected_uri`: thread the URI captured at connect time (`mcap_cloud_dialog.cpp:1527`) through the `connectFinished` callback payload instead of re-reading the editable `state_.uri` (F8 — check the callback signature at line 448 and extend it with the uri string).
  - **Callbacks** (wiring block ~464): `vocabularyReady`/`vocabularyFailed`/`gatePageReady`/`gateListFinished` each wrapped in `postEvent`, handlers below. EVERY handler first checks `request_id == state_.gate_request_seq` (or for vocabularyReady with `recovery=true`: skip the auto-start) and drops stale echoes.
  - **`onVocabularyReady(vocab, recovery)`**:

```cpp
  std::string customer, site;
  std::uint64_t id = 0;
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.vocabulary = std::move(vocab);
    if (recovery) {
      return;  // combos refresh on the next tick; the worker owns the retry (F3)
    }
    if (state_.vocabulary->customers.empty()) {
      state_.gate_phase = GatePhase::kEmptyCatalog;
      return;
    }
    SettingsStore settings(settings_);
    if (state_.gate_customer.empty()) {
      state_.gate_customer =
          settings.getString("mcap_cloud/gate/" + state_.active_server_key + "/customer");
    }
    if (state_.gate_site.empty()) {
      state_.gate_site = settings.getString("mcap_cloud/gate/" + state_.active_server_key + "/site");
    }
    if (state_.gate_customer.empty()) {
      state_.gate_customer = autoSelectCustomer(*state_.vocabulary);
    }
    // One-shot migration (F10): seed from the legacy GLOBAL basic-filter keys,
    // then clear them so a downgrade cannot resurrect stale values.
    if (state_.gate_customer.empty() || state_.gate_site.empty()) {
      const std::string old_c = settings.getString("mcap_cloud/basic_filter/customer");
      const std::string old_s = settings.getString("mcap_cloud/basic_filter/customer_site");
      if (state_.gate_customer.empty() && !old_c.empty()) { state_.gate_customer = old_c; }
      if (state_.gate_site.empty() && !old_s.empty()) { state_.gate_site = old_s; }
      settings.setString("mcap_cloud/basic_filter/customer", "");
      settings.setString("mcap_cloud/basic_filter/customer_site", "");
    }
    // A persisted selection that no longer resolves reverts to the gate.
    if (!state_.gate_customer.empty() && !state_.gate_site.empty() &&
        !resolveGateFilter(*state_.vocabulary, state_.gate_customer, state_.gate_site)) {
      state_.gate_site.clear();
    }
    if (state_.gate_customer.empty() || state_.gate_site.empty()) {
      state_.gate_phase = GatePhase::kNeedsSelection;
      return;
    }
    customer = state_.gate_customer;
    site = state_.gate_site;
    id = beginGateRequestLocked(GatePhase::kListLoading);
  }
  postCommand([w = worker_.get(), id, customer, site] {
    w->listSequencesFilteredAsync(id, customer, site);
  });
```

  - **`onVocabularyFailed(id)`**: drop if stale id; else `state_.gate_phase = GatePhase::kVocabularyError`.
  - **`onGatePageReady(id, page, reset)`**: drop if `id != state_.gate_request_seq`; else exactly the current `onSequencePageReady` body (reset→clear accumulator, append, populate without date seeding, sort).
  - **`onGateListFinished(result)`**: drop if stale id; on `complete` → existing `onSequencesReady` semantics (authoritative populate, date seeding, reselect, index publish) + `gate_phase = result.sequences.empty() ? kListEmpty : kRows`; on `kSuperseded` → ignore (a newer request owns the UI); on any other error → `gate_phase = kListError` + `notify(kWarning, ...)` with a per-error message (kSelectionGone additionally clears `gate_site` and sets `kNeedsSelection`).
  - **`onConnectionLost` (3128)**: also `state_.gate_phase = GatePhase::kDisconnected` (rows stay on screen; the pill hides while `kDisconnected` has rows? NO — F12: kDisconnected text shows regardless, BUT only when there are no rows: keep the pill visible only when `state_.sequences.empty()`, i.e. compute visibility as `hint.empty() || !state_.sequences.empty() ? hide : show`. Concretely: `show_pill = !hint.empty() && state_.sequences.empty()` — cached rows suppress the pill, the existing disconnect notification covers messaging).

- [ ] **Step 6: Combos (F7).** Per-tick block (~1140–1151): split `kBasicFilterKeys` (252) into `kLuaCanonicalFields` (all four — line 297's Lua schema keeps working), `kLocalBasicFilterKeys` (`robot`, `source` — combo feed + `matchesBasicFilter` + persistence at 608/2626), and the gate pair (fed from the vocabulary):

```cpp
    // Gate combos: vocabulary-fed, index-addressed, no "(any)" (the gate is
    // unconditional). -1 = nothing selected yet (the SDK's unselected state).
    {
      std::vector<std::string> customers, sites;
      int customer_idx = -1, site_idx = -1;
      if (state_.vocabulary) {
        for (const auto& c : state_.vocabulary->customers) {
          if (c.name == state_.gate_customer) { customer_idx = static_cast<int>(customers.size()); }
          customers.push_back(c.name);
        }
        sites = siteNamesFor(*state_.vocabulary, state_.gate_customer);
        for (std::size_t i = 0; i < sites.size(); ++i) {
          if (sites[i] == state_.gate_site) { site_idx = static_cast<int>(i); }
        }
      }
      wd.setItems("filter_customer", customers);
      wd.setCurrentIndex("filter_customer", customer_idx);
      wd.setItems("filter_customer_site", sites);
      wd.setCurrentIndex("filter_customer_site", site_idx);
    }
```

  Change handling goes in **`onIndexChanged`** (existing handler, `mcap_cloud_dialog.cpp:2107`), not the click chain:

```cpp
  if (widget_name == "filter_customer") {
    std::string customer;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      if (!state_.vocabulary || index < 0 ||
          index >= static_cast<int>(state_.vocabulary->customers.size())) {
        return true;
      }
      customer = state_.vocabulary->customers[static_cast<std::size_t>(index)].name;
      if (customer == state_.gate_customer) { return true; }
      state_.gate_customer = customer;
      state_.gate_site.clear();  // sites cascade from the customer
      SettingsStore settings(settings_);
      settings.setString("mcap_cloud/gate/" + state_.active_server_key + "/customer", customer);
      settings.setString("mcap_cloud/gate/" + state_.active_server_key + "/site", "");
      (void)beginGateRequestLocked(GatePhase::kNeedsSelection);  // clears old site's rows NOW (F9)
    }
    return true;
  }
  if (widget_name == "filter_customer_site") {
    std::string customer, site;
    std::uint64_t id = 0;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      const auto sites = state_.vocabulary
                             ? siteNamesFor(*state_.vocabulary, state_.gate_customer)
                             : std::vector<std::string>{};
      if (index < 0 || index >= static_cast<int>(sites.size())) { return true; }
      site = sites[static_cast<std::size_t>(index)];
      if (site == state_.gate_site) { return true; }
      state_.gate_site = site;
      customer = state_.gate_customer;
      SettingsStore settings(settings_);
      settings.setString("mcap_cloud/gate/" + state_.active_server_key + "/site", site);
      id = beginGateRequestLocked(GatePhase::kListLoading);
    }
    notify(PJ::ToolboxMessageLevel::kInfo, "Loading " + customer + "/" + site + "...");
    postCommand([w = worker_.get(), id, customer, site] {
      w->listSequencesFilteredAsync(id, customer, site);
    });
    return true;
  }
```

  (Match the real `onIndexChanged` signature/name-dispatch at 2107; `SettingsStore` per `settings_store.hpp:18`. Verify `setCurrentIndex` argument order against `widget_data.hpp:60`.)

- [ ] **Step 7: Refresh + tag-edit preserve the gate.** Refresh (1587): if `gate_customer/site` set → `beginGateRequestLocked(kListLoading)` + `listSequencesFilteredAsync(id, ...)`; else → `beginGateRequestLocked(kVocabularyLoading)` + `fetchVocabularyAsync(id)`. The worker-internal tag-edit re-list is Task 4's `last_gate_*` route.

- [ ] **Step 8: Pill drive** in `getWidgetData()` (next to `fetchStatusLabel`, ~1280; `setLabel` NOT `setText` — F11):

```cpp
    {
      const std::string hint = gateHintText(
          state_.gate_phase, state_.vocabulary ? state_.vocabulary->totalFiles() : 0,
          state_.vocabulary ? state_.vocabulary->totalSites() : 0);
      const bool show_pill = !hint.empty() && state_.sequences.empty();
      wd.setVisible("gateHintLabel", show_pill);
      if (show_pill) {
        wd.setLabel("gateHintLabel", hint);
      }
    }
```

- [ ] **Step 9: Build + full `ctest -E Live` + commit** `feat(plugin): customer/site browse gate (permanent gate row, GatePhase, request-scoped sweeps)`.

---

### Task 6: Supersession + gate-flow hermetic tests

**Files:** Modify `tests/list_pagination_test.cpp`.

The dialog itself has no test harness; the request-scoped machinery lives in worker+backend and IS testable via `FakePagingServer` + a real `BackendConnection` and direct callback wiring. (FetchWorker needs `connectAsync`; test at the `BackendConnection` level plus one worker-level test if `FetchWorker` proves constructible against the fake server — attempt it, fall back to backend-level coverage if the worker's host dependencies block it.)

- [ ] **Step 1: A→B rapid-switch test at the backend level** (the abort predicate is Task 2-tested; this pins the WORKER semantics if constructible, else document):

```cpp
TEST(GateSupersession, AbortedSweepDeliversNoFurtherPagesAfterSupersession) {
  FakePagingServer server(/*total_rows=*/1200, /*stale_at_page=*/-1);
  server.setPageDelayMs(30);  // slow pages so supersession lands mid-sweep
  ASSERT_TRUE(server.ok());
  mcap_cloud::BackendConnection conn(server.uri(), "", "", false);
  std::string err;
  ASSERT_TRUE(conn.connect(&err)) << err;

  std::atomic<std::uint64_t> latest{1};
  int pages_after_supersede = 0;
  int pages_seen = 0;
  bool complete = true;
  (void)conn.listSequences(
      &complete,
      [&](const std::vector<mcap_cloud::SequenceInfo>&, bool) {
        ++pages_seen;
        if (latest.load() != 1) { ++pages_after_supersede; }
        if (pages_seen == 1) { latest.store(2); }  // the user picked another site
      },
      nullptr, nullptr, [&] { return latest.load() != 1; });
  EXPECT_FALSE(complete);
  EXPECT_LE(pages_after_supersede, 0) << "no page may arrive after supersession";
  EXPECT_EQ(pages_seen, 1);
}
```

(`setPageDelayMs` writes the Task 2 `page_delay_ms_` atomic.)

- [ ] **Step 2: Build, RED→GREEN as needed, full `ctest -E Live`, commit** `test(plugin): pin gate supersession semantics`.

---

### Task 7: Docs + full verification + live smoke

**Files:** Modify `plugin/toolbox_mcap_cloud/README.md`, root `README.md`, `CLAUDE.md`. No code.

- [ ] **Step 1: Docs.** Plugin README: the gated flow (connect → vocabulary → pick customer+site → server-filtered progressive list; robot/source/date/name/Lua unchanged client-side; merged all-sites view removed by design; CLI `list` remains unfiltered). Root README: one paragraph in the remote-server section. `CLAUDE.md`: flip the "C++ facet UI" follow-up bullet to landed.
- [ ] **Step 2: Full hermetic suite** green.
- [ ] **Step 3: Live smoke** against `wss://plotjuggler.velociraptor-tuna.ts.net` in PJ4 (`~/ws_plotjuggler/PJ4 && ./run.sh` — the MAIN checkout, per the machine's layout note):
  - pill: "Select customer and site ... 25550 recordings across 6 sites", customer auto-selected `dexory`;
  - **verify the same-cell pill overlay renders** (F11 fallback: table-hide swap — apply and note if needed);
  - `nashville` → first rows < 1 s, complete ~3–4 s, 14,480 rows; `vnv-u9` → < 1 s, 473 rows;
  - rapid `nashville` → `wallingford` switch mid-sweep: old pages stop, no mixed rows, wallingford loads;
  - Advanced/Lua mode: gate row still visible (F6);
  - Refresh keeps the site; PJ4 restart → reconnect lands in the persisted site;
  - tag-edit a file → re-list stays filtered;
  - disconnect the server mid-browse → no "No recordings" lie (pill only on empty+complete).
- [ ] **Step 4: Commit** `docs: facet-gated browse flow`; report nashville first-render/complete timings + persistence check to the user before any merge/PR.

---

## Self-review notes (v2)

- Spec coverage: gate (T5), vocabulary (T1), filtered list + contract-faithful stale (T2), resolver+phases (T3), worker ids/supersession/recovery (T4), supersession tests (T6), pill (T3/T5), persistence+migration (T5), canonical keys (T5.1), docs+live (T7).
- All 14 Codex findings addressed; the four BLOCKERs map to: F4→T2 abort predicate + T4 ids + T6 tests; F6→T5.2 gate row; F7→T5.6 real APIs; F9→T5.4 `beginGateRequestLocked`.
- Type consistency: `GateListResult`/`GatePhase`/`ListFilter`/`VocabularyInfo` defined once, consumed by name everywhere; `gateHintText(phase, total_files, site_count)` matches T3 tests and T5.8 usage.
- Remaining verify-on-site points are RENDER-time only (PanelEngine same-cell overlay — explicit fallback documented in T5.2/T7.3) and exact handler signatures (`onIndexChanged` at 2107, `connectFinished` payload at 448) — look-ups, not designs.
