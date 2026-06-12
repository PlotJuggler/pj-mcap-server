package catalog

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
)

// seedFileWithKey inserts a minimal files row with the given s3_key and returns
// its rowid. Only the columns the caps queries touch (s3_key) carry meaning; the
// rest are arbitrary-but-valid.
func seedFileWithKey(t *testing.T, s *Store, key string) uint64 {
	t.Helper()
	ctx := context.Background()
	var id int64
	err := s.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(
			`INSERT INTO files (s3_key, s3_etag, s3_last_modified, size_bytes,
			    indexed_at, start_time_ns, end_time_ns, chunk_count, message_count, has_message_index)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			key, "etag", 1, 1, 1, 0, 1, 1, 1, 1,
		)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		return err
	})
	if err != nil {
		t.Fatalf("seedFileWithKey(%q): %v", key, err)
	}
	return uint64(id)
}

// TestDistinctMetadataKeys_DerivedUnionTags proves the catalog returns the
// constant DERIVED metadata keys UNION the distinct effective-tag keys, sorted
// and de-duplicated. On a tag-free corpus the result is exactly the derived set
// (the property the Hello vocabulary depends on for a stable, non-empty value).
func TestDistinctMetadataKeys_DerivedUnionTags(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Tag-free corpus: vocabulary == the 8 constant derived keys, sorted.
	id := seedFileWithKey(t, s, "flat_a.mcap")
	got, err := DistinctMetadataKeys(ctx, s)
	if err != nil {
		t.Fatalf("DistinctMetadataKeys: %v", err)
	}
	want := DerivedMetadataKeys() // already sorted by the function
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tag-free vocabulary: got %v want %v", got, want)
	}

	// Add two override tags; vocabulary == derived ∪ {robot_id, site}, sorted,
	// no duplicate even though "s3_key" is both a derived key and (here) a tag key.
	if err := SetOverride(ctx, s, id, "robot_id", "zala-7"); err != nil {
		t.Fatal(err)
	}
	if err := SetOverride(ctx, s, id, "site", "warehouse-3"); err != nil {
		t.Fatal(err)
	}
	if err := SetOverride(ctx, s, id, "s3_key", "shadows-a-derived-key"); err != nil {
		t.Fatal(err)
	}
	got, err = DistinctMetadataKeys(ctx, s)
	if err != nil {
		t.Fatalf("DistinctMetadataKeys: %v", err)
	}
	// Expected: derived ∪ {robot_id, site}; "s3_key" already present, deduped.
	wantSet := map[string]bool{"robot_id": true, "site": true}
	for _, k := range DerivedMetadataKeys() {
		wantSet[k] = true
	}
	if len(got) != len(wantSet) {
		t.Fatalf("vocabulary size: got %d (%v) want %d", len(got), got, len(wantSet))
	}
	for _, k := range got {
		if !wantSet[k] {
			t.Errorf("unexpected key %q in vocabulary", k)
		}
	}
	// Sorted + unique.
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Errorf("vocabulary not strictly sorted: %q then %q", got[i-1], got[i])
		}
	}
}

// TestHasHierarchicalKey proves the catalog reports whether ANY indexed object
// key contains a '/' — the SupportsFileHierarchy derivation. Flat keys (the
// Dexory nissan corpus) => false; a single '/'-bearing key => true.
func TestHasHierarchicalKey(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Empty catalog: no hierarchy.
	got, err := HasHierarchicalKey(ctx, s)
	if err != nil {
		t.Fatalf("HasHierarchicalKey: %v", err)
	}
	if got {
		t.Fatal("empty catalog must report no hierarchy")
	}

	// Flat keys only (Dexory shape) => false.
	seedFileWithKey(t, s, "nissan_zala_50_zeg_1_0.mcap")
	seedFileWithKey(t, s, "nissan_zala_50_zeg_2_0.mcap")
	got, err = HasHierarchicalKey(ctx, s)
	if err != nil {
		t.Fatalf("HasHierarchicalKey: %v", err)
	}
	if got {
		t.Fatal("flat-key corpus must report no hierarchy")
	}

	// One '/'-bearing object key (Asensus/GCS prefix shape) => true.
	seedFileWithKey(t, s, "robot-7/2026-06-05/run.mcap")
	got, err = HasHierarchicalKey(ctx, s)
	if err != nil {
		t.Fatalf("HasHierarchicalKey: %v", err)
	}
	if !got {
		t.Fatal("a '/'-bearing object key must report hierarchy=true")
	}
}
