package catalog

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"sync/atomic"
)

// caps.go answers the two questions the Hello handler derives BackendCapabilities
// from (Plan D Task 8): the catalog-wide metadata-key vocabulary and whether any
// indexed object key forms a '/'-prefix hierarchy. Both are live reads — the
// values are no longer hardcoded in the WS handler (Slice 14).

// derivedMetadataKeys is the constant set of per-file DERIVED metadata keys the
// client-ingest flat map always carries (ws.flatMetadata builds exactly these,
// overlaid by tags_effective). It is the floor of the metadata vocabulary so the
// dropdown is meaningful and STABLE even on a tag-free corpus (the real S3
// nissan corpus carries no embedded MCAP tags, so a pure tags vocabulary would be
// empty and time-varying during tag-edit flows). Keep in lockstep with
// ws.flatMetadata's derived keys.
var derivedMetadataKeys = []string{
	"chunk_count",
	"duration_ns",
	"end_ns",
	"message_count",
	"s3_key",
	"size_bytes",
	"start_ns",
	"topic_count",
}

// DerivedMetadataKeys returns a sorted copy of the constant derived metadata-key
// set. Callers must not mutate the returned slice's backing array.
func DerivedMetadataKeys() []string {
	out := make([]string, len(derivedMetadataKeys))
	copy(out, derivedMetadataKeys)
	sort.Strings(out)
	return out
}

// DistinctMetadataKeys returns the catalog-wide metadata-key vocabulary: the
// constant DERIVED keys UNION the distinct effective-tag keys, sorted and
// de-duplicated. This is what HelloResponse.backend.metadata_key_vocabulary
// advertises (Plan D Task 8 — the Lua query-assist dropdown source). The cap on
// size keeps a pathological tag set from bloating the Hello frame.
func DistinctMetadataKeys(ctx context.Context, s *Store) ([]string, error) {
	return distinctMetadataKeysDB(ctx, s.DB())
}

func distinctMetadataKeysDB(ctx context.Context, db *sql.DB) ([]string, error) {
	const maxVocabKeys = 256

	seen := make(map[string]struct{}, len(derivedMetadataKeys)+16)
	for _, k := range derivedMetadataKeys {
		seen[k] = struct{}{}
	}

	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT key FROM tags_effective ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		seen[k] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	if len(out) > maxVocabKeys {
		out = out[:maxVocabKeys]
	}
	return out, nil
}

// HasHierarchicalKey reports whether ANY indexed object key contains a '/',
// i.e. whether the bucket's keys form a prefix hierarchy. This is the live
// derivation behind HelloResponse.backend.supports_file_hierarchy (Plan D
// Task 8). Every auryn object key is Hive-partitioned (rebuilt from
// customer/site/robot/source/date + filename), so in practice this is simply
// "does the catalog have any files at all" — see aurynHasHierarchicalKey.
func HasHierarchicalKey(ctx context.Context, s *Store) (bool, error) {
	return aurynHasHierarchicalKey(ctx, s.DB())
}

// BackendCaps derives the Hello BackendCapabilities pair — metadata-key
// vocabulary + hierarchy flag — against ONE pinned db handle (B1), so a catalog
// swap landing between the two queries can never advertise a mixed-generation
// view (e.g. one generation's vocabulary with another's hierarchy flag).
func BackendCaps(ctx context.Context, s *Store) (vocab []string, hierarchy bool, err error) {
	lease := s.Acquire() // one snapshot for both queries (and drain-then-close)
	defer lease.Release()
	db := lease.DB()
	if db == nil {
		// Degraded start: no catalog yet. The Hello handler already treats a
		// BackendCaps error as non-fatal and falls back to the derived-key floor.
		return nil, false, errors.New("catalog not yet available (first build in progress)")
	}
	vocab, err = distinctMetadataKeysDB(ctx, db)
	if err != nil {
		return nil, false, err
	}
	hierarchy, err = aurynHasHierarchicalKey(ctx, db)
	if err != nil {
		return nil, false, err
	}
	return vocab, hierarchy, nil
}

// CapsSnapshot publishes the Hello BackendCapabilities as a value that can be
// read WITHOUT touching SQLite.
//
// Before this, every WebSocket handshake called BackendCaps, running
// `SELECT DISTINCT key FROM tags_effective` plus a files probe against the ONE
// pinned catalog connection (readonly.go's C1 pin). While a slow catalog RPC
// held that connection, a brand-new client could not complete its handshake:
// the WebSocket upgrade succeeds, Hello is received and authenticated, and then
// the HelloResponse starves waiting for the connection — the client gives up
// and reports "no response to handshake". Measured 2026-08-09 against an
// 8.78M-file catalog where GetVocabulary alone took 40.7 s.
//
// Deliberately NOT a cache with a freshness check on the request path: any
// revision probe (e.g. PRAGMA data_version) would itself need that one
// connection, reintroducing exactly the coupling this removes. Get() is an
// atomic pointer load and nothing else; all DB work happens in Refresh, which
// the server runs on a background ticker.
type CapsSnapshot struct {
	v atomic.Pointer[capsValue]
}

type capsValue struct {
	vocab     []string
	hierarchy bool
}

// NewCapsSnapshot publishes the derived-key floor so connect works before the
// first refresh lands (and during a degraded start with no catalog at all).
func NewCapsSnapshot() *CapsSnapshot {
	s := &CapsSnapshot{}
	s.v.Store(&capsValue{vocab: DerivedMetadataKeys(), hierarchy: false})
	return s
}

// Get returns the published capabilities. Pure memory read — safe on the
// handshake path and safe to call concurrently. The returned slice is shared
// and MUST NOT be mutated by callers.
func (s *CapsSnapshot) Get() (vocab []string, hierarchy bool) {
	v := s.v.Load()
	return v.vocab, v.hierarchy
}

// Refresh recomputes from the catalog and republishes. Call OFF the request
// path. On failure the previously published value stays in place — stale
// capabilities beat a blank or blocked handshake.
func (s *CapsSnapshot) Refresh(ctx context.Context, st *Store) error {
	vocab, hierarchy, err := BackendCaps(ctx, st)
	if err != nil {
		return err
	}
	s.v.Store(&capsValue{vocab: vocab, hierarchy: hierarchy})
	return nil
}
