package ws

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"pj-cloud/server/internal/catalog"
)

// newWSTestServer stands up a dev-anonymous WS handler over a caller-supplied,
// already-opened *catalog.Store and returns the ws:// URL. Shared by every
// test file that needs a specific store shape (freshly seeded, auryn
// read-only, …) fronted by the real handler/mux/httptest scaffolding, so that
// scaffolding is written exactly once.
func newWSTestServer(t *testing.T, store *catalog.Store) string {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(store, "", log)
	mux := http.NewServeMux()
	mux.Handle("/api/ws", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws"
}

// buildCapsFixtureDB writes a fresh auryn-schema (v3) DB at path with two
// files under a shared dimension tuple, the second carrying an override tag
// "site"="wh-3" — enough to exercise the Hello BackendCapabilities derivation
// (DistinctMetadataKeys / HasHierarchicalKey) without the (deleted) Go
// catalog writer.
func buildCapsFixtureDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
	ddl := []string{
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
		`INSERT INTO customers(id,name) VALUES (1,'robot-7')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'t')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'t')`,
		`INSERT INTO sources(id,name) VALUES (1,'t')`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/a')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
		`INSERT INTO files (id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (1,'run-2026-06-05.mcap','e1',1,1,1,1,1,'2026-06-05',1,2,1,1,X'01'),
			       (2,'run-2026-06-06.mcap','e2',1,1,1,1,1,'2026-06-06',1,2,1,1,X'01')`,
		`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (2,'site','wh-3',1)`,
	}
	for _, s := range ddl {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ddl %q: %v", s, err)
		}
	}
}

// TestHello_DerivesCapsFromCatalog_HierarchyAndTags proves the Hello handler
// derives BackendCapabilities LIVE from the catalog: every auryn object key is
// Hive-partitioned, so a non-empty catalog always advertises
// supports_file_hierarchy=true (catalog.HasHierarchicalKey), and a distinct
// override tag key joins the metadata_key_vocabulary (derived ∪ tags, sorted,
// deduped, still containing the stable derived floor).
func TestHello_DerivesCapsFromCatalog_HierarchyAndTags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "caps.db")
	buildCapsFixtureDB(t, path)
	cat, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	resp := helloRoundTrip(t, newWSTestServer(t, cat), "")
	hr := resp.GetHelloResponse()
	if hr == nil {
		t.Fatalf("expected HelloResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	b := hr.GetBackend()
	if b == nil {
		t.Fatal("HelloResponse.backend missing")
	}
	if !b.GetSupportsFileHierarchy() {
		t.Error("a non-empty auryn catalog must advertise supports_file_hierarchy=true")
	}
	vocab := b.GetMetadataKeyVocabulary()
	hasSite := false
	for _, k := range vocab {
		if k == "site" {
			hasSite = true
		}
	}
	if !hasSite {
		t.Errorf("vocabulary %v must include the distinct override tag key \"site\"", vocab)
	}
	// Must still contain the derived floor.
	for _, dk := range catalog.DerivedMetadataKeys() {
		found := false
		for _, k := range vocab {
			if k == dk {
				found = true
			}
		}
		if !found {
			t.Errorf("vocabulary %v dropped derived key %q", vocab, dk)
		}
	}
}
