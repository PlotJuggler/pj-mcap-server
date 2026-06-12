package catalog

import (
	"context"
	"database/sql"
	"errors"
	"sort"
)

// caps.go answers the two questions the Hello handler derives BackendCapabilities
// from (Plan D Task 8): the catalog-wide metadata-key vocabulary and whether any
// indexed object key forms a '/'-prefix hierarchy. Both are live reads — the
// values are no longer hardcoded in the WS handler (Slice 14).

// derivedMetadataKeys is the constant set of per-file DERIVED metadata keys the
// client-ingest flat map always carries (ws.flatMetadata builds exactly these,
// overlaid by tags_effective). It is the floor of the metadata vocabulary so the
// dropdown is meaningful and STABLE even on a tag-free corpus (the real Dexory
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
	const maxVocabKeys = 256

	seen := make(map[string]struct{}, len(derivedMetadataKeys)+16)
	for _, k := range derivedMetadataKeys {
		seen[k] = struct{}{}
	}

	rows, err := s.DB().QueryContext(ctx,
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
// Task 8): true for Asensus/GCS prefix layouts, false for the flat Dexory/S3
// nissan corpus. Derived from s3_key (the OBJECT key), NOT topic names — topics
// like /nissan/gps/imu contain '/' but are out of scope.
func HasHierarchicalKey(ctx context.Context, s *Store) (bool, error) {
	var one int
	row := s.DB().QueryRowContext(ctx,
		`SELECT 1 FROM files WHERE s3_key LIKE '%/%' LIMIT 1`)
	err := row.Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		// No '/'-bearing key => no hierarchy.
		return false, nil
	default:
		return false, err
	}
}
