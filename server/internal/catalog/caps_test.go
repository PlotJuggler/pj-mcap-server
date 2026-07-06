package catalog

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

// buildCapsFixtureDB writes a fresh auryn-schema (v3) DB at path with the given
// files (sharing one dimension tuple) and, optionally, override tags on the
// first file — enough to drive DistinctMetadataKeys / HasHierarchicalKey
// without the (now-deleted) Go catalog writer.
func buildCapsFixtureDB(t *testing.T, path string, filenames []string, overrides []TagKV) {
	t.Helper()
	db := openAurynTestDB(t, path)
	defer db.Close()
	ddl := []string{
		`INSERT INTO customers(id,name) VALUES (1,'t')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'t')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'t')`,
		`INSERT INTO sources(id,name) VALUES (1,'t')`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/a')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
	}
	for _, s := range ddl {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ddl %q: %v", s, err)
		}
	}
	for i, name := range filenames {
		id := i + 1
		if _, err := db.Exec(`INSERT INTO files
			(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (?,?,?,1,1,1,1,1,'2026-01-01',1,2,1,1,X'01')`, id, name, "e"+name); err != nil {
			t.Fatalf("insert file %q: %v", name, err)
		}
	}
	for _, tg := range overrides {
		if _, err := db.Exec(`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (1,?,?,1)`,
			tg.Key, tg.Value); err != nil {
			t.Fatalf("insert override %q: %v", tg.Key, err)
		}
	}
}

func openCapsFixtureStore(t *testing.T, filenames []string, overrides []TagKV) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "caps.db")
	buildCapsFixtureDB(t, path, filenames, overrides)
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestDistinctMetadataKeys_DerivedUnionTags proves the catalog returns the
// constant DERIVED metadata keys UNION the distinct effective-tag keys, sorted
// and de-duplicated. On a tag-free corpus the result is exactly the derived set
// (the property the Hello vocabulary depends on for a stable, non-empty value).
func TestDistinctMetadataKeys_DerivedUnionTags(t *testing.T) {
	// Tag-free corpus: vocabulary == the 8 constant derived keys, sorted.
	st := openCapsFixtureStore(t, []string{"flat_a.mcap"}, nil)
	got, err := DistinctMetadataKeys(context.Background(), st)
	if err != nil {
		t.Fatalf("DistinctMetadataKeys: %v", err)
	}
	want := DerivedMetadataKeys() // already sorted by the function
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tag-free vocabulary: got %v want %v", got, want)
	}

	// Add two override tags (one deliberately colliding with a derived key
	// name); vocabulary == derived ∪ {robot_id, site}, sorted, no duplicate
	// even though "s3_key" is both a derived key and (here) a tag key.
	st2 := openCapsFixtureStore(t, []string{"flat_a.mcap"}, []TagKV{
		{Key: "robot_id", Value: "zala-7"},
		{Key: "site", Value: "warehouse-3"},
		{Key: "s3_key", Value: "shadows-a-derived-key"},
	})
	got, err = DistinctMetadataKeys(context.Background(), st2)
	if err != nil {
		t.Fatalf("DistinctMetadataKeys: %v", err)
	}
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
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Errorf("vocabulary not strictly sorted: %q then %q", got[i-1], got[i])
		}
	}
}

// TestHasHierarchicalKey proves the catalog reports whether the catalog has
// any files at all — the only meaningful hierarchy signal left once the
// catalog is auryn-only: every auryn object key is Hive-partitioned (always
// contains '/'), so "hierarchical" and "non-empty" coincide exactly
// (aurynHasHierarchicalKey).
func TestHasHierarchicalKey(t *testing.T) {
	// Empty catalog: no hierarchy.
	st := openCapsFixtureStore(t, nil, nil)
	got, err := HasHierarchicalKey(context.Background(), st)
	if err != nil {
		t.Fatalf("HasHierarchicalKey: %v", err)
	}
	if got {
		t.Fatal("empty catalog must report no hierarchy")
	}

	// Any cataloged file => true (its rebuilt key always carries a '/').
	st2 := openCapsFixtureStore(t, []string{"nissan_zala_50_zeg_1_0.mcap"}, nil)
	got, err = HasHierarchicalKey(context.Background(), st2)
	if err != nil {
		t.Fatalf("HasHierarchicalKey: %v", err)
	}
	if !got {
		t.Fatal("a non-empty auryn catalog must report hierarchy=true")
	}
}
