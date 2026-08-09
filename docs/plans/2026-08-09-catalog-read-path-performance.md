# Catalog Read-Path Performance Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop a large, actively-ingesting catalog from making the browse path unusable — first by making the WebSocket handshake independent of the catalog entirely, then by making the vocabulary query cheap, then by bounding it.

**Architecture:** Make the expensive work **cheap and bounded before caching it**. (1) `Hello` stops touching SQLite at all: capabilities are served from an atomically-published value refreshed off the request path. (2) `GetVocabulary` gains opt-in scoping so the GUI stops paying for three whole-table `GROUP BY` scans and a tag-facet scan it never reads. (3) Catalog RPCs get server-owned deadlines and stop blocking the connection read loop. Only then do we re-measure and decide whether a cache is still worth its complexity.

**Tech Stack:** Go 1.23, `modernc.org/sqlite` v1.34.1 (pure-Go, `CGO_ENABLED=0`), `nhooyr.io/websocket`, Prometheus client.

---

## Why v2 supersedes v1

v1 led with a revision-keyed cache (`Revision = generation + PRAGMA data_version`) in front of both RPCs. A Codex adversarial review plus independent verification found it unsound **as sequenced**:

| v1 defect | Verified how | Consequence |
|---|---|---|
| "DB-free Hello" still called `Acquire()` + `PRAGMA data_version` per handshake | reading the v1 plan against `readonly.go:194` | the pragma needs the **single pooled connection**; a 40 s query still starves the handshake. The task did not achieve its own goal. |
| Cache would be built per request | `server.go:587` — `catalogHandler()` returns `&CatalogHandler{Store: ...}` **per RPC** | a cache with a 100% miss rate that reviews as correct. |
| Metrics used package globals | `metrics.go:17` — `Metrics` is an instance struct with its own registry | would not compile. |
| Deadlines deferred as "modernc may not interrupt mid-query" | `modernc.org/sqlite@v1.34.1/sqlite.go:758-786`, `:1315` — installs a `ctx.Done()` watcher calling `sqlite3_interrupt` | the deferral rationale was factually wrong; deadlines are viable today. |
| compute closure re-acquired its own snapshot | v1 plan, Tasks 3-4 | fixed a lease-lifetime bug by creating a **coherence** bug: rows from generation 2 returned under generation 1's token. |
| `store()` ordered revisions by `data_version` | SQLite pragma semantics | `data_version` is connection-local and not a cross-connection sequence; the guard was meaningless. |

**Retained from v1 as still-true findings:**
- `PRAGMA data_version` behaves as assumed *on a stable connection* — measured 2026-08-09 with modernc v1.34.1, the production read-only DSN, `MaxOpenConns(1)`, `journal_mode=wal`: unchanged across our own reads (2→2), moved on another connection's commit (2→3), moved on a **separate process** commit (3→4), and the reader saw all external rows. Keep this evidence if caching is revisited in Task 5.
- Dimension rows are append-only within a published generation today (the in-place writer inserts dimensions and deletes files; full rebuilds republish atomically and bump the generation) — so same-generation id aliasing is not a live hazard, but it is an **unenforced** invariant. Any future dimension GC/renumber must bump the generation.

---

## Measured problem (2026-08-09, production: 8.78M files, 74 customers / 162 sites / 275 robots)

| Fact | Evidence |
|---|---|
| `GetVocabulary` steady state | **2953 ms ± 40 ms** (5 samples, `mcap-cloud-cli vocab`) |
| `GetVocabulary` under builder load | **40,727 ms** (while `files_scanned` moved 8,781,595 → 8,790,227) |
| Response size | **10,001 B → 6,534 B** — a compute problem, not bandwidth |
| Client budget | 10 s shared `kRequestTimeout` → hard "Could not load the catalog" |
| `Hello` does DB work | `catalog.BackendCaps` → `SELECT DISTINCT key FROM tags_effective` + a `files` probe (`server.go:554`, `caps.go:101`) |
| Consequence | while a catalog RPC holds the sole connection, **new clients fail their handshake** |

Synthetic benchmark of the exact query set at 8.78M rows (modernc, `MaxOpenConns(1)`, warm): `groupCount` customer 414 ms / site 406 ms / robot 405 ms / source 400 ms; 4 EXISTS dimension scans 7 ms; `tagFacets` 804 ms; **total 2.44 s**.

**What the shipped C++ client reads** (verified in `wire_mapping.cpp:99-120` after the 2026-08-09 robot-gate change): customers (id, name, file_count), sites (id, name), robots (id, name). It does **not** read site/robot `file_count`, `sources`, or `tags`. Robots became load-bearing when the browse gate started requiring customer+site+robot and sending `ListFilter.robot_id` — any plan predating that change wrongly lists robots as waste.

**Non-goals:** demand-paged `ListFiles` (breaks the client's all-rows-resident assumption used by the Lua filters and aggregate views); builder-maintained `dimension_counts` (schema v3→v4, separate cross-repo plan).

---

## File Structure

| File | Responsibility |
|---|---|
| `server/internal/catalog/caps.go` *(modify)* | Add `CapsSnapshot`: an `atomic.Pointer`-published capability value + a `Refresh` that does the DB work off the request path. |
| `server/internal/ws/server.go` *(modify)* | Own the `CapsSnapshot`, run its refresh loop, and read it lock-free in `handleHello`. |
| `proto/pj_cloud.proto` *(modify)* | Additive scoping fields on `GetVocabularyRequest`. |
| `server/internal/catalog/vocabulary.go` *(modify)* | `VocabOptions` + skip the unrequested aggregations. |
| `server/internal/ws/handlers_catalog.go` *(modify)* | Pass options through; add per-RPC deadlines. |
| `plugin/toolbox_mcap_cloud/src/backend_connection.{hpp,cpp}` *(modify)* | `getVocabulary(VocabScope)`; dialog asks lean, CLI asks full. |
| `server/internal/metrics/metrics.go` *(modify)* | Collectors as `Metrics` **instance fields** (not globals). |

---

## Task 1: `Hello` must never touch SQLite

**This is the availability fix.** A handshake that waits on the catalog fails as `no response to handshake`, and the fields it waits for are `BackendCapabilities` — which the shipped dialog ignores (`backend_connection.cpp:441`, `mcap_cloud_dialog.cpp:2603`). The CLI *does* print them, so they must stay populated and reasonably fresh; they must simply not be computed on the request path.

**Files:**
- Modify: `server/internal/catalog/caps.go`
- Modify: `server/internal/ws/server.go`
- Test: `server/internal/catalog/caps_snapshot_test.go` *(new)*

- [ ] **Step 1: Write the failing test**

```go
package catalog

import (
	"context"
	"testing"
)

// The contract: reading capabilities must be a pure memory read. Any DB work
// happens in Refresh, which the server runs OFF the handshake path.
func TestCapsSnapshotServesWithoutTouchingTheDatabase(t *testing.T) {
	st := openCapsFixtureStore(t, []string{"flat_a.mcap"}, nil) // caps_test.go:81

	snap := NewCapsSnapshot()

	// Before any refresh: a safe, non-empty floor, and NO store access.
	vocab, hierarchy := snap.Get()
	if len(vocab) == 0 {
		t.Fatal("pre-refresh Get must return the derived-key floor, not empty")
	}
	if hierarchy {
		t.Fatal("pre-refresh hierarchy must default false")
	}

	// After a refresh, Get reflects the catalog.
	if err := snap.Refresh(context.Background(), st); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	vocab2, _ := snap.Get()
	if len(vocab2) == 0 {
		t.Fatal("post-refresh vocabulary must be non-empty")
	}

	// Get must be safe to call concurrently and must not block on the store.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			_, _ = snap.Get()
		}
	}()
	<-done
}

// A refresh failure must never blank out a previously-good value: the handshake
// keeps working with slightly stale capabilities rather than degrading.
func TestCapsSnapshotRefreshFailureKeepsLastGood(t *testing.T) {
	st := openCapsFixtureStore(t, []string{"flat_a.mcap"}, nil)
	snap := NewCapsSnapshot()
	if err := snap.Refresh(context.Background(), st); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	before, hierBefore := snap.Get()

	// A cancelled context makes the underlying queries fail.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := snap.Refresh(ctx, st); err == nil {
		t.Fatal("expected Refresh to fail on a cancelled context")
	}

	after, hierAfter := snap.Get()
	if len(after) != len(before) || hierAfter != hierBefore {
		t.Fatalf("a failed refresh clobbered the last-good value: %v/%v -> %v/%v",
			before, hierBefore, after, hierAfter)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && go test ./internal/catalog/ -run TestCapsSnapshot -v`
Expected: FAIL — `undefined: NewCapsSnapshot`.

- [ ] **Step 3: Write minimal implementation**

Append to `server/internal/catalog/caps.go`:

```go
// CapsSnapshot publishes the Hello BackendCapabilities as a value that can be
// read WITHOUT touching SQLite.
//
// Before this, every WebSocket handshake ran `SELECT DISTINCT key FROM
// tags_effective` plus a files probe on the single shared catalog connection
// (readonly.go pins exactly one). While a slow catalog RPC held that
// connection, a brand-new client could not complete its handshake at all: the
// upgrade succeeds, then HelloResponse starves and the client reports "no
// response to handshake" (measured 2026-08-09 against an 8.78M-file catalog).
//
// Note this is NOT a cache with a lookup on the request path — a revision check
// would itself need that one connection, which is exactly the coupling being
// removed. Get() is an atomic pointer load and nothing else.
type CapsSnapshot struct {
	v atomic.Pointer[capsValue]
}

type capsValue struct {
	vocab     []string
	hierarchy bool
}

func NewCapsSnapshot() *CapsSnapshot {
	s := &CapsSnapshot{}
	// Floor: the constant derived keys, no hierarchy. Serving this before the
	// first refresh keeps connect working during a degraded start.
	s.v.Store(&capsValue{vocab: DerivedMetadataKeys(), hierarchy: false})
	return s
}

// Get returns the published capabilities. Pure memory read — safe on the
// handshake path, safe concurrently.
func (s *CapsSnapshot) Get() (vocab []string, hierarchy bool) {
	v := s.v.Load()
	return v.vocab, v.hierarchy
}

// Refresh recomputes from the catalog and republishes. Call OFF the request
// path. A failure leaves the previously published value in place — stale
// capabilities beat a blank handshake.
func (s *CapsSnapshot) Refresh(ctx context.Context, st *Store) error {
	vocab, hierarchy, err := BackendCaps(ctx, st)
	if err != nil {
		return err
	}
	s.v.Store(&capsValue{vocab: vocab, hierarchy: hierarchy})
	return nil
}
```

Add `"sync/atomic"` to that file's imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && go test ./internal/catalog/ -run TestCapsSnapshot -race -v`
Expected: PASS (2 tests), no race warnings.

- [ ] **Step 5: Wire it into the server**

In `server/internal/ws/server.go`, add a `caps *catalog.CapsSnapshot` field to the long-lived `Handler` (**not** `connState` — one per process, not per connection), initialize it with `catalog.NewCapsSnapshot()` where the handler is constructed, and replace the derive at `:554`:

```go
	// Hello does NO catalog I/O: capabilities are published by the refresh loop
	// below. A handshake must not be able to queue behind a slow catalog RPC on
	// the single read connection.
	vocab, hierarchy := c.h.caps.Get()
```

Delete the now-dead `ctx := context.Background()` and the error/fallback branch at `:555-559` (the floor lives in `NewCapsSnapshot`).

Start a refresh loop wherever the server's other background loops start (alongside the chunk-index warmer), refreshing once at startup and then on a ticker:

```go
	// Capability refresh: OFF the handshake path by construction. 60s is far
	// tighter than these values move (a metadata-key vocabulary changes only when
	// the builder ingests a brand-new tag key) and costs one cheap query per
	// minute against the shared connection.
	go func() {
		if err := h.caps.Refresh(ctx, h.store); err != nil {
			h.log.Warn("ws: initial capability refresh failed; serving derived floor", "err", err)
		}
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := h.caps.Refresh(ctx, h.store); err != nil {
					h.log.Warn("ws: capability refresh failed; keeping last-good", "err", err)
				}
			}
		}
	}()
```

- [ ] **Step 6: Verify the handshake no longer touches the catalog**

Run: `cd server && go test ./internal/ws/ ./internal/catalog/ -race`
Expected: PASS, including the existing Hello/auth tests.

Then confirm by inspection that `handleHello` contains **no** call into `catalog.*` other than `c.h.caps.Get()`:

Run: `cd server && sed -n '/func (c \*connState) handleHello/,/^}/p' internal/ws/server.go | grep -n "catalog\." `
Expected: exactly one hit, `c.h.caps.Get()`.

- [ ] **Step 7: Commit**

```bash
git add server/internal/catalog/caps.go server/internal/catalog/caps_snapshot_test.go server/internal/ws/server.go
git commit -m "fix(ws): serve Hello capabilities from a published snapshot so handshakes never touch SQLite"
```

---

## Task 2: Opt-in lean `GetVocabulary`

Cuts the cold cost from 2.44 s to roughly 0.42 s by skipping three whole-table `GROUP BY` scans and the tag-facet scan the GUI never reads. Additive proto3 fields: absent = the full legacy response, so old clients and the CLI are unaffected. **No `protocol_version` bump** — unknown request fields are ignored by older peers and the defaults reproduce today's behaviour exactly.

**Files:**
- Modify: `proto/pj_cloud.proto`, regenerate `server/internal/wire/pj_cloud/`
- Modify: `server/internal/catalog/vocabulary.go`, `server/internal/ws/handlers_catalog.go`
- Modify: `plugin/toolbox_mcap_cloud/src/backend_connection.{hpp,cpp}`, `tools/mcap_cloud_cli.cpp`

- [ ] **Step 1: Extend the request message**

```protobuf
message GetVocabularyRequest {
  // Scoping (2026-08-09). The full response needs four whole-table GROUP BY
  // aggregations plus a tag-facet scan — 2.44 s at 8.78M files, recomputed per
  // call. A client that only renders the customer->site->robot picker reads
  // none of the following. Absent (proto3 default false) = the FULL legacy
  // response, so old clients are unaffected.
  bool omit_sources            = 1;
  bool omit_tag_facets         = 2;
  // Skip the per-site and per-robot COUNT(*) GROUP BYs (two of the four scans).
  // Customer counts are ALWAYS computed — they drive the picker's summary hint.
  bool omit_site_robot_counts  = 3;
}
```

- [ ] **Step 2: Regenerate and check the binding diff**

```bash
cd /home/davide/ws_plotjuggler/mcap_server
protoc --version    # MUST print libprotoc 3.21.12 (CLAUDE.md pin)
make proto
git diff --stat server/internal/wire/pj_cloud/
```
Expected: additions for the three fields only, no unrelated churn.

- [ ] **Step 3: Thread options through the query**

Add to `server/internal/catalog/vocabulary.go`:

```go
// VocabOptions selects which parts of the vocabulary to compute. The zero value
// is the FULL response (backwards-compatible with every existing caller).
type VocabOptions struct {
	OmitSources         bool
	OmitTagFacets       bool
	OmitSiteRobotCounts bool
}
```

Give `GetVocabularyDB` a `VocabOptions` parameter and, in `vocabularyFromSnapshot`, skip `groupCount(site_id)`, `groupCount(robot_id)`, `groupCount(source_id)`, the sources query, and `tagFacets` accordingly. Keep the customer `groupCount` and all four EXISTS-gated dimension scans (7 ms total) unconditional — the tree itself is what the picker needs.

Update existing callers to pass `VocabOptions{}`.

- [ ] **Step 4: Test both shapes**

Add to `server/internal/catalog/vocabulary_test.go`:

```go
func TestGetVocabulary_LeanOmitsUnreadSections(t *testing.T) {
	st := openCapsFixtureStore(t, []string{"flat_a.mcap"}, nil)
	ctx := context.Background()

	full, err := GetVocabulary(ctx, st, VocabOptions{})
	if err != nil {
		t.Fatalf("full: %v", err)
	}
	lean, err := GetVocabulary(ctx, st, VocabOptions{
		OmitSources: true, OmitTagFacets: true, OmitSiteRobotCounts: true,
	})
	if err != nil {
		t.Fatalf("lean: %v", err)
	}

	// The TREE must be identical — that is what the picker renders.
	if len(lean.Customers) != len(full.Customers) {
		t.Fatalf("lean dropped customers: %d vs %d", len(lean.Customers), len(full.Customers))
	}
	// Customer counts survive (they drive the summary hint).
	for i := range full.Customers {
		if lean.Customers[i].FileCount != full.Customers[i].FileCount {
			t.Fatalf("lean must keep customer counts: %d vs %d",
				lean.Customers[i].FileCount, full.Customers[i].FileCount)
		}
	}
	// The omitted sections must be absent.
	if len(lean.Sources) != 0 {
		t.Fatalf("lean must omit sources, got %d", len(lean.Sources))
	}
	if len(lean.Tags) != 0 {
		t.Fatalf("lean must omit tag facets, got %d", len(lean.Tags))
	}
}
```

Run: `cd server && go test ./internal/catalog/ -run TestGetVocabulary -v`
Expected: PASS.

- [ ] **Step 5: Client — dialog asks lean, CLI asks full**

The CLI prints per-site `file_count` (`tools/mcap_cloud_cli.cpp` `runVocab`), so it must NOT request `omit_site_robot_counts` or every site would read `0 files`. Give the C++ API an explicit scope instead of hard-coding one:

In `plugin/toolbox_mcap_cloud/src/backend_connection.hpp`:

```cpp
  // Which parts of the vocabulary to ask the server to COMPUTE. The browse gate
  // needs only the customer->site->robot tree (wire_mapping.cpp maps nothing
  // else); the CLI's `vocab` probe displays site counts, so it asks for the full
  // response. Defaults to kFull so no caller silently loses data.
  enum class VocabScope { kFull, kPickerOnly };
  [[nodiscard]] std::optional<VocabularyInfo> getVocabulary(
      std::chrono::seconds timeout = kVocabularyTimeout, VocabScope scope = VocabScope::kFull);
```

In `backend_connection.cpp`, set the request fields when `scope == kPickerOnly`. In `fetch_worker.cpp`'s `fetchVocabularyAsync`, pass `VocabScope::kPickerOnly`. Leave `runVocab` on the default.

- [ ] **Step 6: Verify end to end**

```bash
cd server && go test ./... -race
cd ../plugin/toolbox_mcap_cloud && cmake --build build -j"$(nproc)" && (cd build && ctest -E live)
cd /home/davide/ws_plotjuggler/mcap_server && make smoke
```
Expected: all green, `SMOKE PASS`.

- [ ] **Step 7: Commit**

```bash
git add proto/pj_cloud.proto server/internal/wire/pj_cloud/ server/internal/catalog/vocabulary.go server/internal/ws/handlers_catalog.go plugin/toolbox_mcap_cloud/src plugin/toolbox_mcap_cloud/tools
git commit -m "perf(catalog): opt-in lean GetVocabulary; GUI asks picker-only, CLI keeps full"
```

---

## Task 3: Server-owned deadlines, and catalog RPCs off the read loop

`modernc.org/sqlite` v1.34.1 installs a `ctx.Done()` watcher that calls `sqlite3_interrupt` (`sqlite.go:758-786`, `:1315`), so a deadline genuinely interrupts a running aggregation rather than merely freeing the caller.

- [ ] **Step 1: Prove mid-query interruption before relying on it**

Write a throwaway test that starts a deliberately long aggregation against a large temp DB with a 100 ms context and asserts it returns `context.DeadlineExceeded` *quickly* (well under the query's uninterrupted runtime). If it does NOT, stop: the rest of this task is invalid and the connection stays pinned regardless of deadlines. Record the outcome in this plan before continuing.

- [ ] **Step 2: Add a per-RPC deadline**

In `server/internal/ws/handlers_catalog.go`, wrap each catalog handler's context:

```go
// A catalog RPC must not be able to hold the sole read connection indefinitely.
const catalogRPCTimeout = 30 * time.Second

ctx, cancel := context.WithTimeout(ctx, catalogRPCTimeout)
defer cancel()
```

- [ ] **Step 3: Stop blocking the read loop**

Catalog RPCs currently run inline on the connection's read loop (`server.go:406`, `:452`); only OpenSession is offloaded (`:430`, whose comment already describes this failure mode — blocked RPCs starve pong processing until the keepalive reaps the connection: 30 s interval, 10 s pong timeout, 2 failures). Offload `GetVocabulary` and `ListFiles` the same way OpenSession is offloaded. Response routing is already request-id based, so ordering is not a correctness concern — but add a concurrency test proving two in-flight catalog RPCs on one connection both return correctly.

- [ ] **Step 4: Verify + commit**

```bash
cd server && go test ./... -race
git commit -am "fix(ws): per-RPC catalog deadlines + run catalog RPCs off the connection read loop"
```

---

## Task 4: Re-measure before adding any cache

- [ ] Deploy Tasks 1-3 and re-run against the production catalog:
  - `mcap-cloud-cli vocab --json` (expect ~0.4-0.8 s picker-only vs the 2.95 s baseline)
  - a `hello` from a second terminal **while** a `vocab` is in flight — must return promptly
- [ ] Record the numbers here. **Only if** the lean cold path is still too slow, plan a cache — and if so it must satisfy, at minimum:
  - one cache owned by the long-lived `Handler`, injected into the per-RPC `CatalogHandler` (`server.go:587` builds one per call)
  - the computation **pinned to a single snapshot** for its whole duration, storing under *that* snapshot's revision (never a revision sampled from a different lease)
  - an internal monotonic epoch for ordering — never raw `data_version`, which is connection-local
  - `singleflight.DoChan` so a caller's cancellation cannot fail every other waiter
  - a bounded, cancellable background refresh owned by the server lifecycle
  - a cache key including `VocabOptions`, or full/lean responses will cross-contaminate

---

## Task 5: Metrics (instance fields, not globals)

`Metrics` is a struct of collectors registered on its own registry (`metrics.go:17`). Add fields there, pass the instance in, and label outcomes explicitly (`hit`/`miss`/`stale`/`refresh_error`) plus a per-RPC latency histogram. A metric that cannot distinguish a hit from a stale serve cannot prove the change worked.

---

## Documentation audit (required before the PR)

- [ ] `docs/catalog-vocabulary-rpc.md` — document the scoping fields; its "≤ ~99 dimensions" assumption (§36) is stated as fact and is false in production (74 customers / 162 sites / 275 robots / 8.78M files). Correct it.
- [ ] Root `CLAUDE.md` "Decisions & pins" — record that Hello is catalog-free by construction, and why (a revision check on the request path would reintroduce the coupling).
- [ ] `docs/CATALOG_CONTRACT.md` **and** `mcap_catalog/CATALOG_CONTRACT.md` — if the append-only dimension invariant is written down (it should be), both copies must stay byte-identical (`cmp`).
- [ ] Deploy runbooks — any new metrics.

## Verification gate

- [ ] `cd server && go test ./... -race` green
- [ ] `make smoke` prints `SMOKE PASS`
- [ ] `handleHello` contains no `catalog.*` call except `caps.Get()`
- [ ] With a `vocab` in flight against the production catalog, a concurrent `hello` returns promptly
