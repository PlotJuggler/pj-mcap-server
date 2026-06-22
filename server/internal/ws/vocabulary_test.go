package ws

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"pj-cloud/server/internal/catalog"
	pb "pj-cloud/server/internal/wire/pj_cloud"

	_ "modernc.org/sqlite"
)

// openAurynReadStore builds a minimal auryn-schema DB (2 customers, a nested
// site/robot, 2 sources, a tag) and returns it opened read-only — enough to
// exercise CatalogHandler.GetVocabulary's proto tree mapping end-to-end.
func openAurynReadStore(t *testing.T) *catalog.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "auryn.db")
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
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
		`INSERT INTO customers(id,name) VALUES (1,'alpha'),(2,'beta')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'s1'),(2,2,'s2')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'r1'),(2,2,'r2')`,
		`INSERT INTO sources(id,name) VALUES (1,'ros-bags'),(2,'synthetic')`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/a')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
		`INSERT INTO files (id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (1,'f1.mcap','e',1,1,1,1,1,'2026-06-01',1,2,1,1,X'01'),
			       (2,'f2.mcap','e',1,2,2,2,2,'2026-06-01',1,2,1,1,X'01')`,
		`INSERT INTO tags_embedded(file_id,key,value) VALUES (1,'mission','inv')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}
	db.Close()
	store, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestCatalogHandler_GetVocabulary(t *testing.T) {
	h := &CatalogHandler{Store: openAurynReadStore(t)}
	resp, err := h.GetVocabulary(context.Background(), &pb.GetVocabularyRequest{})
	if err != nil {
		t.Fatalf("GetVocabulary: %v", err)
	}
	// Tree mapping: 2 customers, each with a nested site -> robot.
	if len(resp.GetCustomers()) != 2 {
		t.Fatalf("customers = %d, want 2", len(resp.GetCustomers()))
	}
	c0 := resp.GetCustomers()[0]
	if c0.GetName() != "alpha" || len(c0.GetSites()) != 1 || c0.GetSites()[0].GetName() != "s1" {
		t.Fatalf("customer[0] = %+v, want alpha/s1", c0)
	}
	if len(c0.GetSites()[0].GetRobots()) != 1 || c0.GetSites()[0].GetRobots()[0].GetName() != "r1" {
		t.Fatalf("alpha/s1 robots = %+v, want [r1]", c0.GetSites()[0].GetRobots())
	}
	if c0.GetFileCount() != 1 {
		t.Fatalf("alpha file_count = %d, want 1", c0.GetFileCount())
	}
	// Flat sources.
	if len(resp.GetSources()) != 2 {
		t.Fatalf("sources = %d, want 2", len(resp.GetSources()))
	}
	// Tag facet.
	var hasMission bool
	for _, f := range resp.GetTags() {
		if f.GetKey() == "mission" && len(f.GetValues()) == 1 && f.GetValues()[0].GetValue() == "inv" {
			hasMission = true
		}
	}
	if !hasMission {
		t.Fatalf("tags missing mission=inv facet: %+v", resp.GetTags())
	}
}

func TestCatalogHandler_GetVocabulary_LegacyEmpty(t *testing.T) {
	// On a legacy (read-write Go-schema) store, the vocabulary is empty, not an error.
	h := &CatalogHandler{Store: openCatalogStore(t)}
	resp, err := h.GetVocabulary(context.Background(), &pb.GetVocabularyRequest{})
	if err != nil {
		t.Fatalf("GetVocabulary (legacy): %v", err)
	}
	if len(resp.GetCustomers()) != 0 || len(resp.GetSources()) != 0 || len(resp.GetTags()) != 0 {
		t.Fatalf("legacy vocabulary should be empty, got %+v", resp)
	}
}
