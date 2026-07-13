package ws

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"pj-cloud/server/internal/catalog"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// chFixtureTopic is one topic for buildOneFileAurynStore.
type chFixtureTopic struct {
	Name, SchemaName, SchemaEnc string
	Count                       uint64
}

// buildOneFileAurynStore writes a fresh auryn-schema (v3) DB containing
// exactly one file (id 1, under a shared dimension tuple) with the given
// shape, opens it read-only, and returns the store. It is the CatalogHandler
// test fixture builder — the migration of these tests off the (deleted) Go
// catalog writer's UpsertFile/ReplaceTopicsForFile/SetOverride.
func buildOneFileAurynStore(t *testing.T, filename string, sizeBytes, startNs, endNs int64, chunkCount uint32,
	topics []chFixtureTopic, overrides map[string]string) *catalog.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.db")
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
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
		`INSERT INTO customers(id,name) VALUES (1,'t')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'t')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'t')`,
		`INSERT INTO sources(id,name) VALUES (1,'t')`,
	}
	for _, s := range ddl {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ddl %q: %v", s, err)
		}
	}

	var topicIDs []int64
	counts := []byte{} // NOT NULL column; empty (not nil) when there are no topics
	for i, top := range topics {
		tid := int64(i + 1)
		sid := int64(i + 1)
		if _, err := db.Exec(`INSERT INTO topic_names(id,name) VALUES (?,?)`, tid, top.Name); err != nil {
			t.Fatalf("insert topic_name %q: %v", top.Name, err)
		}
		if _, err := db.Exec(`INSERT INTO schemas(id,name,encoding) VALUES (?,?,?)`, sid, top.SchemaName, top.SchemaEnc); err != nil {
			t.Fatalf("insert schema: %v", err)
		}
		topicIDs = append(topicIDs, tid)
		counts = append(counts, encodeVarintByte(top.Count)...)
	}
	if len(topics) > 0 {
		if _, err := db.Exec(`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp')`); err != nil {
			t.Fatalf("insert topic_set: %v", err)
		}
		for i, tid := range topicIDs {
			if _, err := db.Exec(`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,?,?)`, tid, i+1); err != nil {
				t.Fatalf("insert topic_set_member: %v", err)
			}
		}
	} else {
		// A file with no topics still needs a (fingerprint-unique, empty) topic_set row.
		if _, err := db.Exec(`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'empty')`); err != nil {
			t.Fatalf("insert empty topic_set: %v", err)
		}
	}

	if _, err := db.Exec(`INSERT INTO files
		(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
		VALUES (1,?,?,?,1,1,1,1,'2026-01-01',?,?,?,1,?)`,
		filename, "e-"+filename, sizeBytes, startNs, endNs, chunkCount, counts); err != nil {
		t.Fatalf("insert file %q: %v", filename, err)
	}
	for k, v := range overrides {
		if _, err := db.Exec(`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (1,?,?,1)`, k, v); err != nil {
			t.Fatalf("insert override %q: %v", k, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close writer handle: %v", err)
	}

	st, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestCatalogHandler_ListFiles(t *testing.T) {
	store := buildOneFileAurynStore(t, "a.mcap", 100, 1, 2, 0,
		[]chFixtureTopic{{Name: "/x", SchemaName: "X", SchemaEnc: "ros2msg", Count: 10}}, nil)

	h := &CatalogHandler{Store: store}
	resp, err := h.ListFiles(context.Background(), &pb.ListFilesRequest{Limit: 100})
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(resp.Files) != 1 {
		t.Fatalf("files: got %d want 1", len(resp.Files))
	}
	if !hasSuffixKeyWS(resp.Files[0].S3Key, "a.mcap") {
		t.Errorf("s3_key: got %q", resp.Files[0].S3Key)
	}
	if resp.Files[0].TopicCount != 1 {
		t.Errorf("topic_count: got %d", resp.Files[0].TopicCount)
	}
}

func TestCatalogHandler_GetFileWithTopics(t *testing.T) {
	store := buildOneFileAurynStore(t, "b.mcap", 100, 1, 2, 0, []chFixtureTopic{
		{Name: "/gps/fix", SchemaName: "G", SchemaEnc: "ros2msg", Count: 5},
		{Name: "/imu/data", SchemaName: "I", SchemaEnc: "ros2msg", Count: 5},
	}, nil)

	h := &CatalogHandler{Store: store}
	resp, err := h.GetFile(context.Background(), &pb.GetFileRequest{FileId: 1})
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if len(resp.Topics) != 2 {
		t.Errorf("topics: got %d want 2", len(resp.Topics))
	}
}

func TestCatalogHandler_GetFileNotFound(t *testing.T) {
	store := buildOneFileAurynStore(t, "a.mcap", 1, 1, 2, 0, nil, nil)
	h := &CatalogHandler{Store: store}
	_, err := h.GetFile(context.Background(), &pb.GetFileRequest{FileId: 9999})
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	var nf errFileNotFound
	if !asErrFileNotFound(err, &nf) {
		t.Errorf("expected errFileNotFound, got %T: %v", err, err)
	}
}

// TestFlatMetadata_EffectiveTagsOverlayDerived proves the slice contract: the
// flat metadata map = derived entries OVERLAID by tags_effective, with effective
// tags WINNING on key collision (a user override named "size_bytes" shadows the
// derived value), while derived keys with no tag collision remain.
func TestFlatMetadata_EffectiveTagsOverlayDerived(t *testing.T) {
	store := buildOneFileAurynStore(t, "e.mcap", 100, 10, 30, 3,
		[]chFixtureTopic{{Name: "/x", SchemaName: "X", SchemaEnc: "ros2msg", Count: 42}},
		map[string]string{"size_bytes": "OVERRIDDEN", "robot_id": "r7"})

	h := &CatalogHandler{Store: store}
	resp, err := h.ListFiles(context.Background(), &pb.ListFilesRequest{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	flat := resp.GetMetadata()["1"].GetEntries() // first/only file id is 1
	if flat["size_bytes"] != "OVERRIDDEN" {
		t.Errorf("effective tag should win on collision: size_bytes=%q", flat["size_bytes"])
	}
	if flat["robot_id"] != "r7" {
		t.Errorf("override tag missing from flat map: %+v", flat)
	}
	// Derived keys without a tag collision survive (Lua/live-test contract).
	if flat["message_count"] != "42" || flat["chunk_count"] != "3" ||
		flat["duration_ns"] != "20" || !hasSuffixKeyWS(flat["s3_key"], "e.mcap") {
		t.Errorf("derived keys clobbered: %+v", flat)
	}
}

// asErrFileNotFound is a tiny errors.As wrapper (kept here so the test file has
// no extra imports beyond the ones above).
func asErrFileNotFound(err error, target *errFileNotFound) bool {
	for err != nil {
		if e, ok := err.(errFileNotFound); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// hasSuffixKeyWS reports whether s3Key (the rebuilt Hive key) ends with the
// given bare filename — these tests only care about WHICH file matched, not
// the shared dimension prefix every fixture in this file uses.
func hasSuffixKeyWS(s3Key, filename string) bool {
	n := len(s3Key) - len(filename)
	return n >= 0 && s3Key[n:] == filename
}
