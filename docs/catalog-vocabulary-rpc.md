# Catalog Filter Vocabulary RPC — Design

**Status:** Decisions LOCKED (V1–V7). **SERVER SIDE IMPLEMENTED — M3 (2026-06-23):**
proto (`GetVocabulary` RPC + `FileFilter` dimension fields), Go bindings, the
`catalog.GetVocabulary` builder (tree + flat source + tag facets with the cap), the
dimension filter predicates in `aurynFilterFiles`, and the WS handler/dispatch are
all landed + tested (hermetic + cross-language). **DEFERRED — the C++ client facet
UI** (§9: replace the `comboPrefix` hack with the cascading comboboxes): a
client-plugin effort needing the C++/SDK toolchain; the server RPC unblocks it.
Reviewed by Claude + Codex at the M3 boundary.
**Scope:** the wire design for **filtering the catalog** by a strict
customer→site→robot hierarchy plus flat tags, and the RPC that ships the
filter vocabulary to the client. Depends on the auryn dimension schema (see
[`auryn-catalog-migration-plan.md`](auryn-catalog-migration-plan.md);
this resolves that plan's open decision **D3**).
**Canonical wire schema:** `proto/pj_cloud.proto` — every message below is drop-in.

---

## 1. Motivation & use case

The client (PlotJuggler toolbox) needs to let a user narrow a potentially large
catalog down to the handful of recordings they want. The **primary filter** is:

> **`date` + `customer` + `site` + `robot`** — a strongly-restricting selection.

`customer → site → robot` is a **strict hierarchy**: a site belongs to exactly
one customer, a robot to exactly one site. The UI renders them as **cascading
comboboxes** — pick a customer, the site combo shows only that customer's sites;
pick a site, the robot combo shows only that site's robots. *There is no reason
to ever show a robot that isn't in the selected site.*

Two scale facts drive the design:

- **The vocabulary is small and bounded** — ≤ ~99 customers / sites / robots
  each. So the *entire* customer→site→robot tree can be shipped **once, upfront**,
  and the client drives the cascade locally with **zero per-selection round-trips**.
- **A dimension selection narrows results sharply** — a `(customer, site, robot,
  date)` selection typically yields a single-digit number of files. So **file
  rows are filtered server-side** and only the small result set is returned,
  regardless of total lake size.

Beyond the hierarchy, recordings also carry **flat tags** (`key=value`, e.g.
`mission=inventory`, `quality=good`). These are *independent facets* — no tag key
constrains another — so they are filtered, not cascaded.

Recordings also have a **source** (the data origin, e.g. `ros-bags`, `synthetic`)
— a single *flat normalized* dimension (V7), modeled as one standalone combo,
independent of the customer/site/robot hierarchy.

### Why the current design is insufficient
Today the server has **no dimension concept** (the legacy Go schema is flat
`s3_key`). It advertises only:
- `BackendCapabilities.metadata_key_vocabulary` — a list of metadata **key
  names** (not values) for the Lua query-assist dropdown.
- `BackendCapabilities.supports_file_hierarchy` — a single boolean ("does any
  `s3_key` contain `/`").

The client's only structural filter is a **single-level, client-derived
prefix combobox** (`hierarchy_prefix.h` / `comboPrefix`) built from the top-level
`/`-prefixes of the filenames it already pulled — an adaptation made because the
host can't render a `QTreeWidget`. It is one level deep, client-derived, and
unaware of customer/site/robot structure. **This RPC replaces that hack** with a
server-provided, structured, strictly-hierarchical vocabulary.

---

## 2. Design principles

1. **Make illegal states unrepresentable.** Dimensions are delivered as a
   **nested tree** (`Customer{ sites:[ Site{ robots:[…] } ] }`), not a flat list
   with parent ids. A robot lives *inside* its site node, so "a robot outside its
   site" cannot be expressed on the wire — the cascade guarantee is structural,
   not client-discipline.
2. **The wire shape mirrors the storage shape.** Three shapes: the *hierarchical
   normalized* dimensions (`customers`/`sites(customer_id)`/`robots(site_id)`) → a
   **tree**, filtered by **id**; the *flat normalized* dimension (`sources`) → a
   **standalone combo**, filtered by **id**; the *EAV* tags
   (`tags`/`tags_effective`) → **flat facets**, filtered by **string value**. A
   tree where children depend on parents; independent combos/facets where they don't.
3. **Vocabulary upfront, results on demand.** Ship the small, bounded vocabulary
   once; filter the (potentially large) file set server-side on the selection.
4. **Reuse what exists.** Tag filtering already works via
   `FileFilter.tag_all`/`tag_any`. Date filtering already works via
   `FileFilter.recorded_between`. This design adds **vocabulary** + **dimension
   predicates**, and reuses the rest.

---

## 3. Proto schema (drop-in for `proto/pj_cloud.proto`)

### 3.1 Envelope additions

```proto
message ClientMessage {
  uint64 request_id = 1;
  oneof payload {
    Hello                  hello          = 10;
    ListFilesRequest       list_files     = 11;
    GetFileRequest         get_file       = 12;
    UpdateTagsRequest      update_tags    = 13;
    OpenSessionRequest     open_session   = 14;
    CancelSession          cancel         = 15;
    SessionAck             ack            = 16;
    GetVocabularyRequest   get_vocabulary = 17;   // NEW
  }
}

message ServerMessage {
  uint64 request_id      = 1;
  uint64 subscription_id = 2;
  oneof payload {
    HelloResponse          hello_response = 10;
    ListFilesResponse      list_files     = 11;
    GetFileResponse        get_file       = 12;
    UpdateTagsResponse     update_tags    = 13;
    OpenSessionResponse    open_session   = 14;
    MessageBatch           batch          = 15;
    Progress               progress       = 16;
    Eos                    eos            = 17;
    Error                  error          = 18;
    GetVocabularyResponse  get_vocabulary = 19;   // NEW
  }
}
```

### 3.2 The vocabulary RPC

```proto
// GetVocabulary returns the FILTER VOCABULARY: the strict customer→site→robot
// hierarchy (for cascading comboboxes) plus the flat tag facets (independent
// filter widgets). Small and bounded — sent in one response, cached by the
// client, re-fetched only when the client wants a fresh view of the catalog.
// Built from the catalog dimension tables + the tag facet query; NO `files`
// table scan for the tree (only the dimension tables) — see §6.
message GetVocabularyRequest {}                    // no args today; reserved for future scoping

message GetVocabularyResponse {
  repeated DimCustomer customers = 1;              // hierarchical dimensions (tree)
  repeated DimSource   sources   = 2;              // flat normalized dimension (V7)
  repeated TagFacet    tags      = 3;              // flat EAV facets
  bytes catalog_generation       = 4;              // (LANDED 2026-07-17) the generation
                                                   // these dimension ids belong to — echo it
                                                   // in ListFilesRequest.expected_catalog_generation
                                                   // (see "Generation-scoped ids" below).
}

// --- Hierarchical dimensions (strict tree: customer → site → robot) ----------

message DimCustomer {
  uint64 id               = 1;                     // opaque handle, valid for THIS session
  string name             = 2;                     // display value
  uint64 file_count       = 3;                     // files under this customer (UX; 0 if unset)
  repeated DimSite sites  = 4;
}

message DimSite {
  uint64 id                 = 1;
  string name               = 2;
  uint64 file_count         = 3;
  repeated DimRobot robots  = 4;
}

message DimRobot {
  uint64 id          = 1;
  string name        = 2;
  uint64 file_count  = 3;
  // (date bounds min/max DEFERRED — V6; additive later, see §7)
}

// --- Flat NORMALIZED dimension: source (independent single combo, V7) --------

message DimSource {
  uint64 id         = 1;
  string name       = 2;                           // e.g. "ros-bags", "synthetic"
  uint64 file_count = 3;
}

// --- Flat tag facets (independent key → {values}) ----------------------------

message TagFacet {
  string key                    = 1;               // e.g. "mission", "quality"
  repeated TagFacetValue values = 2;               // bounded — see cardinality cap, §8
}

message TagFacetValue {
  string value      = 1;                           // a STRING (tags have no ids)
  uint64 file_count = 2;                           // UX; 0 if unset
}
```

### 3.3 FileFilter extension (the selection echoed back)

```proto
message FileFilter {
  TimeRange recorded_between     = 1;              // (existing) the DATE range
  repeated string topics_any_of  = 2;              // (existing)
  repeated TagPredicate tag_all  = 3;              // (existing) — tag facets filter HERE
  repeated TagPredicate tag_any  = 4;              // (existing)

  // NEW — dimension selection (V1: by id; V5: proto3 `optional` = explicit
  // presence). Send the ids the user picked from the most recent
  // GetVocabularyResponse, PAIRED with that response's catalog_generation
  // (echo it in ListFilesRequest.expected_catalog_generation). The ids are
  // GENERATION-SCOPED rowids that RENUMBER on a builder rebuild; a stale echo
  // returns ERROR_STALE_CATALOG (re-fetch the vocabulary). Strict hierarchy ⇒
  // a set robot_id already implies its site+customer; the server ANDs whatever
  // is present, so the deepest is sufficient. `source_id` (V7) is an
  // INDEPENDENT flat dimension, ANDed in alongside the hierarchy.
  optional uint64 customer_id = 5;
  optional uint64 site_id     = 6;
  optional uint64 robot_id    = 7;
  optional uint64 source_id   = 8;
}
```

**No new fields are needed for tag filtering** — each selected tag facet value
becomes one `tag_all` predicate `{key, value}`, ANDed server-side. This is the
deliberate asymmetry from principle 2: dimensions filter by id, tags by string.

---

## 4. RPC semantics

- **When called:** once after `Hello`, before the first `ListFiles`, to populate
  the filter widgets. Re-called on an explicit "refresh" (the catalog grew/changed)
  — it is cheap, so a manual refresh button or a periodic re-fetch is fine.
- **Caching:** the client caches the whole response and drives the cascading
  combos + facet widgets locally. No round-trip per combobox interaction.
- **Generation-scoped ids (LANDED 2026-07-17 — the generation token):** dimension
  `id`s are rowids that RENUMBER across a full builder rebuild, so they are valid
  only within the catalog GENERATION that issued them. `GetVocabularyResponse`
  carries an opaque `catalog_generation` (server epoch + swap ordinal;
  equality-only). A client that filters by a dimension id MUST echo that token in
  `ListFilesRequest.expected_catalog_generation`; a builder rebuild between the
  vocabulary fetch and the filter returns **`ERROR_STALE_CATALOG`**, and the
  client transparently re-fetches the vocabulary + restarts the listing. (This
  replaced the earlier "session-scoped ids" framing, which held only because a
  rebuild used to force a reconnect — the token makes the binding explicit and
  survives across the live `ReopenIfSwapped`.)
- **Errors:** standard `Error` envelope. An empty catalog returns empty
  `customers`/`tags` (not an error).

---

## 5. Filtering semantics

- **Partial selection filters at the chosen depth.** Customer only ⇒ all files
  under that customer. Customer+site ⇒ that site. Down to a robot ⇒ that robot's
  files. The server ANDs whichever of `customer_id`/`site_id`/`robot_id` is
  PRESENT (V5 proto3 `optional`); because the hierarchy is strict, the deepest one
  is sufficient and the others are redundant-but-harmless.
- **`source` is an independent flat filter.** `source_id` ANDs in alongside the
  hierarchy selection (a file has both a customer/site/robot AND a source); it is
  a standalone combo, not part of the cascade.
- **Date** ⇒ `recorded_between` (unchanged). Daily granularity over long spans is
  the wrong shape for a combobox, so date stays a range, not a facet.
- **Tag facets** ⇒ each selected `(key, value)` is one `tag_all` predicate.
  Multiple facet selections AND together.
- **Composability:** dimension + date + tag predicates all AND in one
  `FileFilter` on a single `ListFiles` call. The result set (small) then feeds the
  existing **client-side** name/Lua refinement over the returned metadata map.

---

## 6. Server implementation (Go reader over the auryn schema — `vocabulary.go`)

- **Tree:** read the dimension tables and assemble nested messages — but **each
  dimension SELECT is `EXISTS`-gated against `files`** (`… FROM customers c WHERE
  EXISTS(SELECT 1 FROM files WHERE customer_id=c.id)`, likewise sites/robots/
  sources). This **prunes ORPHAN lookup rows**: the auryn builder leaves
  dimension rows behind on delete/rename by design (no GC), so an ungated read
  would surface stale `file_count=0` ghost nodes. A dimension is shown iff a file
  references it; the gates stay mutually consistent because `files` carries all
  four FKs denormalized.
- **Counts:** `SELECT customer_id,count(*) FROM files GROUP BY customer_id` (and
  per site/robot/source). **Indexing caveat:** only `customer_id` is index-backed
  today (it leads the composite `UNIQUE`); `site_id`/`robot_id`/`source_id`-only
  GROUP BYs and filters **scan `files`**. Fine at the v1 corpus; covering indexes
  (`files(robot_id,id)`, …) are a deferred auryn-schema add for lake scale (needs a
  SchemaVersion bump). `file_count` **counts all files incl. `has_error=1`**
  (consistent with the filter path).
- **Tag facets:** `SELECT key, value, count(*) FROM tags_effective GROUP BY
  key, value`, grouped into `TagFacet`s, **dropping keys with > 50 distinct
  values** (the cap, §8: `len(values) > TagFacetCap` → exactly 50 kept, 51
  dropped). GROUNDING: embedded tags come from the **MCAP footer `pj.user_tags`
  only** — no S3/GCS object-metadata / `Head` path; the value-space is *footer
  tags ∪ override keys* (`tags_effective`).
- **Sources (flat):** `SELECT id,name FROM sources` (EXISTS-gated) + the
  `source_id` count — one standalone combo.
- **Dimension filter:** `WHERE files.customer_id=? [AND files.site_id=? AND
  files.robot_id=?] [AND files.source_id=?]` — the present (proto3 `optional`) ids
  are ANDed on the denormalized FK columns (`customer_id` index-backed; the rest
  scan today, see the caveat above).

---

## 7. Date bounds (DEFERRED — V6)

To let the date picker self-bound, `DimRobot.min_date_ns`/`max_date_ns` could
carry that robot's data span (`SELECT min(start_time_ns),max(end_time_ns) FROM
files GROUP BY robot_id`). **Deferred for v1** (V6) — purely additive proto
fields, so they can be added later with no breaking change if the date-picker UX
needs self-bounding.

---

## 8. Tag cardinality cap (the one real tag decision)

Dimensions are bounded by nature; **tags are not**. A key like `mission` has a
few values (great combobox); a key like `operator_note` or `run_uuid` may have
thousands (useless as a combobox, and bloats the frame). So the server emits a
tag key as a `TagFacet` **only if its distinct-value count ≤ a cap** (proposed
**50**, mirroring the existing `maxVocabKeys = 256` guard on
`DistinctMetadataKeys`). High-cardinality keys remain filterable via the existing
free-text / Lua path — they're simply not rendered as comboboxes.

---

## 9. Relationship to existing capabilities

- **Replaces** the `hierarchy_prefix.h` / `comboPrefix` single-level prefix hack
  (and arguably retires the `supports_file_hierarchy` boolean once dimensions are
  the structural source).
- **Complements** `metadata_key_vocabulary`: that stays the key-name source for
  the free-text Lua assist; `TagFacet` adds key+values for the bounded facet combos.

---

## 10. Decisions (LOCKED 2026-06-22, with Davide)

| # | Decision | Resolution |
|---|---|---|
| V1 | Dimension filter by `id` vs `name` | **by `id`** — session handle; `robot_id` pins the path; direct indexed filter |
| V2 | Per-node `file_count` | **include** at every dimension node + tag value |
| V3 | Delivery | **dedicated `GetVocabulary` RPC** (refreshable without reconnect) |
| V4 | Tag cardinality cap | **50** distinct values/key; above ⇒ free-text/Lua |
| V5 | Unset dimension repr | **proto3 `optional`** (explicit presence) — *overrides the 0=unset draft* |
| V6 | `DimRobot` date bounds now | **defer** (additive later) |
| V7 | `source` flat dimension | **INCLUDE now** — top-level `DimSource` + `optional uint64 source_id` |

## 11. Out of scope (this document)
- The auryn schema itself and the Python-writer / Go-reader split (covered by the
  migration plan).
- Server-side **pagination** behavior under dimension filters (existing
  keyset pagination still applies to the filtered result).
- Numeric "signal > X" filtering (`file_metrics`).
