package ws

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"pj-cloud/server/internal/catalog"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// seedGenListDB writes a minimal auryn catalog. Variant "A" is two customers
// (alpha=1, beta=2) with one file each; variant "B" simulates a full rebuild
// that RENUMBERS everything: the dimension ids are swapped (beta=1, alpha=2)
// and an extra file shifts the file rowids — the exact hazard the generation
// token exists to detect.
func seedGenListDB(t *testing.T, path, variant string) {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE schema_version (id INTEGER PRIMARY KEY CHECK (id=1), version INTEGER NOT NULL);
			INSERT INTO schema_version(id,version) VALUES (1,%d)`, catalog.SchemaVersion),
		`CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE sites (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE robots (id INTEGER PRIMARY KEY, site_id INTEGER NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE sources (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE topic_names (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE schemas (id INTEGER PRIMARY KEY, name TEXT NOT NULL, encoding TEXT NOT NULL)`,
		`CREATE TABLE topic_sets (id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE topic_set_members (set_id INTEGER NOT NULL, topic_id INTEGER NOT NULL, schema_id INTEGER NOT NULL, PRIMARY KEY(set_id,topic_id)) WITHOUT ROWID`,
		`CREATE TABLE files (id INTEGER PRIMARY KEY, filename TEXT NOT NULL, etag TEXT NOT NULL, size_bytes INTEGER NOT NULL,
			last_modified_ns INTEGER NOT NULL DEFAULT 0, cataloged_at_ns INTEGER NOT NULL DEFAULT 0,
			customer_id INTEGER NOT NULL, site_id INTEGER NOT NULL, robot_id INTEGER NOT NULL, source_id INTEGER NOT NULL,
			date TEXT NOT NULL, start_time_ns INTEGER NOT NULL, end_time_ns INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL DEFAULT 0, topic_set_id INTEGER NOT NULL, topic_counts BLOB NOT NULL,
			has_error INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE tags_embedded (file_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID`,
		`CREATE TABLE tags_override (file_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT, updated_at INTEGER NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID`,
		`CREATE VIEW tags_effective AS
			SELECT file_id,key,value,1 AS is_override FROM tags_override WHERE value IS NOT NULL
			UNION ALL
			SELECT e.file_id,e.key,e.value,0 FROM tags_embedded e
			LEFT JOIN tags_override o ON (o.file_id=e.file_id AND o.key=e.key) WHERE o.file_id IS NULL`,
		`INSERT INTO sources(id,name) VALUES (1,'ros-bags')`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/a')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
	}
	switch variant {
	case "A":
		stmts = append(stmts,
			`INSERT INTO customers(id,name) VALUES (1,'alpha'),(2,'beta')`,
			`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'s1'),(2,2,'s2')`,
			`INSERT INTO robots(id,site_id,name) VALUES (1,1,'r1'),(2,2,'r2')`,
			`INSERT INTO files (id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
				VALUES (1,'a1.mcap','e',1,1,1,1,1,'2026-06-01',1,2,1,1,X'01'),
				       (2,'b1.mcap','e',1,2,2,2,1,'2026-06-01',1,2,1,1,X'01')`,
		)
	case "B": // renumbered rebuild: ids swapped + rowids shifted
		stmts = append(stmts,
			`INSERT INTO customers(id,name) VALUES (1,'beta'),(2,'alpha')`,
			`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'s2'),(2,2,'s1')`,
			`INSERT INTO robots(id,site_id,name) VALUES (1,1,'r2'),(2,2,'r1')`,
			`INSERT INTO files (id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
				VALUES (1,'zz_new.mcap','e',1,1,1,1,1,'2026-06-01',1,2,1,1,X'01'),
				       (2,'a1.mcap','e',1,2,2,2,1,'2026-06-01',1,2,1,1,X'01'),
				       (3,'b1.mcap','e',1,1,1,1,1,'2026-06-01',1,2,1,1,X'01')`,
		)
	default:
		t.Fatalf("unknown variant %q", variant)
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}
}

// openGenListStore opens variant A at a stable path and returns (store, path)
// so tests can atomically publish variant B over it.
func openGenListStore(t *testing.T) (*catalog.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "served.db")
	seedGenListDB(t, path, "A")
	st, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, path
}

// rebuildGenListStore publishes variant B over the served path (os.Rename =
// the builder's atomic publish) and forces the reader swap.
func rebuildGenListStore(t *testing.T, st *catalog.Store, path string) {
	t.Helper()
	next := filepath.Join(t.TempDir(), "next.db")
	seedGenListDB(t, next, "B")
	if err := os.Rename(next, path); err != nil {
		t.Fatalf("publish variant B: %v", err)
	}
	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil || !swapped {
		t.Fatalf("ReopenIfSwapped: swapped=%v err=%v", swapped, err)
	}
}

func u64(v uint64) *uint64 { return &v }

// A pagination cursor taken from generation A must be REJECTED with
// ERROR_STALE_CATALOG after a rebuild — never silently continued in the
// renumbered id-space (skipped/repeated files).
func TestListFiles_CursorStaleAcrossRebuild(t *testing.T) {
	st, path := openGenListStore(t)
	h := &CatalogHandler{Store: st}
	ctx := context.Background()

	page1, err := h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 1})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1.GetFiles()) != 1 || page1.GetNextPageToken() == "" {
		t.Fatalf("page1 = %d files, token %q — want 1 file + a token", len(page1.GetFiles()), page1.GetNextPageToken())
	}
	if len(page1.GetCatalogGeneration()) == 0 {
		t.Fatal("page1 must carry catalog_generation")
	}

	rebuildGenListStore(t, st, path)

	_, err = h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 1, PageToken: page1.GetNextPageToken()})
	var stale errStaleCatalog
	if !errors.As(err, &stale) {
		t.Fatalf("page2 with a pre-rebuild cursor: err = %v, want errStaleCatalog", err)
	}

	// A fresh listing (no cursor) works and carries the NEW generation.
	fresh, err := h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 10})
	if err != nil {
		t.Fatalf("fresh listing after rebuild: %v", err)
	}
	if bytes.Equal(fresh.GetCatalogGeneration(), page1.GetCatalogGeneration()) {
		t.Fatal("generation did not change across the rebuild")
	}
}

// expected_catalog_generation mismatching the current generation is stale;
// matching passes.
func TestListFiles_ExpectedGenerationChecked(t *testing.T) {
	st, _ := openGenListStore(t)
	h := &CatalogHandler{Store: st}
	ctx := context.Background()

	cur, err := h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 1})
	if err != nil {
		t.Fatalf("probe listing: %v", err)
	}
	gen := cur.GetCatalogGeneration()

	if _, err := h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 1, ExpectedCatalogGeneration: gen}); err != nil {
		t.Fatalf("matching expected generation must pass: %v", err)
	}
	_, err = h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 1, ExpectedCatalogGeneration: []byte("bogus-generation")})
	var stale errStaleCatalog
	if !errors.As(err, &stale) {
		t.Fatalf("bogus expected generation: err = %v, want errStaleCatalog", err)
	}
}

// Dimension ids are generation-scoped handles: a first-page filter carrying one
// WITHOUT the expected generation is a protocol violation (the server could not
// detect a stale id), and a filter whose generation went stale after a rebuild
// must fail stale — never silently select the RENUMBERED dimension.
func TestListFiles_DimensionIdsRequireGeneration(t *testing.T) {
	st, path := openGenListStore(t)
	h := &CatalogHandler{Store: st}
	ctx := context.Background()

	probe, err := h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 10})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	gen := probe.GetCatalogGeneration()

	// customer_id=1 is 'alpha' in generation A.
	req := &pb.ListFilesRequest{Limit: 10, Filter: &pb.FileFilter{CustomerId: u64(1)}}
	_, err = h.ListFiles(ctx, req)
	var need errGenerationRequired
	if !errors.As(err, &need) {
		t.Fatalf("dimension id without expected generation: err = %v, want errGenerationRequired", err)
	}

	req.ExpectedCatalogGeneration = gen
	byDim, err := h.ListFiles(ctx, req)
	if err != nil {
		t.Fatalf("dimension filter with generation: %v", err)
	}
	if len(byDim.GetFiles()) != 1 || byDim.GetFiles()[0].GetS3Key() == "" {
		t.Fatalf("dimension filter: got %d files", len(byDim.GetFiles()))
	}

	// After the rebuild customer_id=1 means 'beta' — the OLD generation echo
	// must be rejected stale instead of silently serving beta's files.
	rebuildGenListStore(t, st, path)
	_, err = h.ListFiles(ctx, req)
	var stale errStaleCatalog
	if !errors.As(err, &stale) {
		t.Fatalf("stale dimension-id filter: err = %v, want errStaleCatalog", err)
	}
}

// Malformed cursors and cursors reused with a DIFFERENT filter are invalid
// requests (not stale — the client is misusing the API).
func TestListFiles_CursorValidation(t *testing.T) {
	st, _ := openGenListStore(t)
	h := &CatalogHandler{Store: st}
	ctx := context.Background()

	_, err := h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 1, PageToken: "!!!not-a-cursor!!!"})
	var bad errBadPageToken
	if !errors.As(err, &bad) {
		t.Fatalf("malformed cursor: err = %v, want errBadPageToken", err)
	}

	page1, err := h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 1})
	if err != nil || page1.GetNextPageToken() == "" {
		t.Fatalf("page1: %v (token %q)", err, page1.GetNextPageToken())
	}
	// Reuse the unfiltered cursor with a topics filter — a different query.
	_, err = h.ListFiles(ctx, &pb.ListFilesRequest{
		Limit:     1,
		PageToken: page1.GetNextPageToken(),
		Filter:    &pb.FileFilter{TopicsAnyOf: []string{"/a"}},
	})
	if !errors.As(err, &bad) {
		t.Fatalf("cursor reused with a different filter: err = %v, want errBadPageToken", err)
	}

	// Cursor + explicit expectation that DISAGREE (cursor is gen-current,
	// expectation bogus): invalid request, not stale.
	_, err = h.ListFiles(ctx, &pb.ListFilesRequest{
		Limit:                     1,
		PageToken:                 page1.GetNextPageToken(),
		ExpectedCatalogGeneration: []byte("bogus-generation"),
	})
	if !errors.As(err, &bad) {
		t.Fatalf("cursor/expectation disagreement: err = %v, want errBadPageToken", err)
	}
}

// GetVocabulary stamps the SAME generation ListFiles serves, so the client can
// echo it with dimension-id filters.
func TestGetVocabulary_CarriesGeneration(t *testing.T) {
	st, path := openGenListStore(t)
	h := &CatalogHandler{Store: st}
	ctx := context.Background()

	vocab, err := h.GetVocabulary(ctx, &pb.GetVocabularyRequest{})
	if err != nil {
		t.Fatalf("GetVocabulary: %v", err)
	}
	if len(vocab.GetCatalogGeneration()) == 0 {
		t.Fatal("vocabulary must carry catalog_generation")
	}
	listing, err := h.ListFiles(ctx, &pb.ListFilesRequest{Limit: 1})
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if !bytes.Equal(vocab.GetCatalogGeneration(), listing.GetCatalogGeneration()) {
		t.Fatal("vocabulary and listing disagree on the current generation")
	}

	rebuildGenListStore(t, st, path)
	vocab2, err := h.GetVocabulary(ctx, &pb.GetVocabularyRequest{})
	if err != nil {
		t.Fatalf("GetVocabulary after rebuild: %v", err)
	}
	if bytes.Equal(vocab2.GetCatalogGeneration(), vocab.GetCatalogGeneration()) {
		t.Fatal("vocabulary generation did not change across the rebuild")
	}
}
