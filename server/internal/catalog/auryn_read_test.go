package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

// encodeVarint mirrors the Python writer's unsigned-LEB128 encoder, so the test
// can stamp a topic_counts blob the Go reader must decode identically.
func encodeVarint(n uint64) []byte {
	var out []byte
	for {
		b := byte(n & 0x7f)
		n >>= 7
		if n != 0 {
			out = append(out, b|0x80)
		} else {
			out = append(out, b)
			return out
		}
	}
}

func encodeCounts(counts ...uint64) []byte {
	var out []byte
	for _, c := range counts {
		out = append(out, encodeVarint(c)...)
	}
	return out
}

func TestDecodeCountsBlob(t *testing.T) {
	cases := [][]uint64{
		{},
		{0},
		{5},
		{128},          // 2-byte varint
		{3, 2, 0, 300}, // mixed, incl a zero-count channel and a 2-byte value
		{1<<32 + 7},    // > 32-bit
	}
	for _, want := range cases {
		got, err := decodeCountsBlob(encodeCounts(want...))
		if err != nil {
			t.Fatalf("decode %v: %v", want, err)
		}
		if len(want) == 0 && len(got) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("decode round-trip = %v, want %v", got, want)
		}
	}
	// Truncated continuation byte must error, not loop or panic.
	if _, err := decodeCountsBlob([]byte{0x80}); err == nil {
		t.Fatal("decodeCountsBlob([0x80]) = nil err, want truncation error")
	}
}

func TestRebuildHiveKey(t *testing.T) {
	got := rebuildHiveKey("dexory", "london", "r1", "ros-bags", "2026-06-01", "x.mcap")
	want := "customer=dexory/customer_site=london/robot=r1/source=ros-bags/date=2026-06-01/x.mcap"
	if got != want {
		t.Fatalf("rebuildHiveKey = %q, want %q", got, want)
	}
}

// aurynSchemaDDL is the subset of the auryn schema the Go reader queries, used to
// build hermetic test DBs (no Python). The real schema is validated separately by
// the crosslang test; this guards the Go SQL + the cross-language byte contracts.
func aurynSchemaDDL() []string {
	return []string{
		`CREATE TABLE schema_version (id INTEGER PRIMARY KEY CHECK (id=1), version INTEGER NOT NULL)`,
		fmt.Sprintf(`INSERT INTO schema_version(id,version) VALUES (1,%d)`, SchemaVersion),
		`CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE sites (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE robots (id INTEGER PRIMARY KEY, site_id INTEGER NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE sources (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE topic_names (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE schemas (id INTEGER PRIMARY KEY, name TEXT NOT NULL, encoding TEXT NOT NULL)`,
		`CREATE TABLE topic_sets (id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE topic_set_members (set_id INTEGER NOT NULL, topic_id INTEGER NOT NULL, schema_id INTEGER NOT NULL, PRIMARY KEY(set_id,topic_id)) WITHOUT ROWID`,
		`CREATE TABLE files (
			id INTEGER PRIMARY KEY, filename TEXT NOT NULL, etag TEXT NOT NULL, size_bytes INTEGER NOT NULL,
			last_modified_ns INTEGER NOT NULL DEFAULT 0, cataloged_at_ns INTEGER NOT NULL DEFAULT 0,
			customer_id INTEGER NOT NULL, site_id INTEGER NOT NULL, robot_id INTEGER NOT NULL, source_id INTEGER NOT NULL,
			date TEXT NOT NULL, start_time_ns INTEGER NOT NULL, end_time_ns INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL DEFAULT 0, topic_set_id INTEGER NOT NULL, topic_counts BLOB NOT NULL,
			has_error INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE tags_embedded (file_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID`,
		`CREATE TABLE tags_override (file_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT, updated_at INTEGER NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID`,
		`CREATE TABLE catalog_failures (s3_key TEXT NOT NULL PRIMARY KEY, failed_at_ns INTEGER NOT NULL, error_text TEXT NOT NULL)`,
		`CREATE TABLE build_metadata (id INTEGER PRIMARY KEY CHECK (id=1), build_id INTEGER NOT NULL, last_build_ns INTEGER NOT NULL, files_scanned INTEGER NOT NULL, files_failed INTEGER NOT NULL, build_outcome TEXT NOT NULL, builder_version TEXT NOT NULL)`,
		`CREATE VIEW tags_effective AS
			SELECT file_id,key,value,1 AS is_override FROM tags_override WHERE value IS NOT NULL
			UNION ALL
			SELECT e.file_id,e.key,e.value,0 FROM tags_embedded e
			LEFT JOIN tags_override o ON (o.file_id=e.file_id AND o.key=e.key) WHERE o.file_id IS NULL`,
	}
}

// openAurynTestDB opens a fresh DB and applies aurynSchemaDDL; the caller inserts data.
func openAurynTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, s := range aurynSchemaDDL() {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ddl %q: %v", s, err)
		}
	}
	return db
}

// buildMinimalAurynDB writes a one-file DB for the reader tests.
func buildMinimalAurynDB(t *testing.T, path string) {
	t.Helper()
	db := openAurynTestDB(t, path)
	defer db.Close()
	ddl := []string{
		// dimensions
		`INSERT INTO customers(id,name) VALUES (1,'dexory')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'london')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'r1')`,
		`INSERT INTO sources(id,name) VALUES (1,'ros-bags')`,
		// topics /a (id 1) then /b (id 2) -> topic_id ASC == counts order
		`INSERT INTO topic_names(id,name) VALUES (1,'/a'),(2,'/b')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp1')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1),(1,2,1)`,
		// an embedded tag + a NULL mask + an override-wins edit on another key
		`INSERT INTO tags_embedded(file_id,key,value) VALUES (1,'site','london'),(1,'masked','x')`,
		`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (1,'quality','good',1),(1,'masked',NULL,1)`,
	}
	for _, s := range ddl {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ddl %q: %v", s, err)
		}
	}
	// One file: topic_counts [3,2] => message_count 5; chunk_count 7.
	if _, err := db.Exec(`INSERT INTO files
		(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
		VALUES (1,'x.mcap','etag1',999,1,1,1,1,'2026-06-01',1000,2000,7,1,?)`, encodeCounts(3, 2)); err != nil {
		t.Fatalf("insert file: %v", err)
	}
}

// GetFiles resolves ALL ids against ONE pinned db handle (B1): a catalog swap
// mid-request must never mix generations within one OpenSession resolve. An
// unknown id fails the whole batch with ErrFileNotFound naming the id.
func TestGetFiles_BatchResolve(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auryn.db")
	buildMinimalAurynDB(t, path)
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	recs, err := GetFiles(ctx, st, []uint64{1})
	if err != nil {
		t.Fatalf("GetFiles: %v", err)
	}
	if len(recs) != 1 || recs[0].ID != 1 || recs[0].S3ETag != "etag1" {
		t.Fatalf("GetFiles = %+v, want the one fixture file", recs)
	}

	if _, err := GetFiles(ctx, st, []uint64{1, 999}); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("GetFiles with unknown id: err = %v, want ErrFileNotFound", err)
	}
}

func TestAurynReader_Hermetic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auryn.db")
	buildMinimalAurynDB(t, path)

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	// FilterFiles: rebuilt key, summed counts, topic_count, chunk_count, tags.
	files, next, err := FilterFiles(ctx, st, FilterArgs{})
	if err != nil {
		t.Fatalf("FilterFiles: %v", err)
	}
	if next != "" || len(files) != 1 {
		t.Fatalf("FilterFiles = %d files (next=%q), want 1", len(files), next)
	}
	f := files[0]
	wantKey := "customer=dexory/customer_site=london/robot=r1/source=ros-bags/date=2026-06-01/x.mcap"
	if f.S3Key != wantKey {
		t.Fatalf("s3_key = %q, want %q", f.S3Key, wantKey)
	}
	if f.MessageCount != 5 || f.TopicCount != 2 || f.ChunkCount != 7 || f.SizeBytes != 999 {
		t.Fatalf("summary = (msgs=%d topics=%d chunks=%d size=%d), want (5,2,7,999)",
			f.MessageCount, f.TopicCount, f.ChunkCount, f.SizeBytes)
	}
	// tags_effective: quality=good (override), site=london (embedded), masked hidden.
	tagm := f.FlatMetadata()
	if tagm["quality"] != "good" || tagm["site"] != "london" {
		t.Fatalf("tags = %v, want quality=good site=london", tagm)
	}
	if _, masked := tagm["masked"]; masked {
		t.Fatalf("tags = %v, 'masked' should be hidden by NULL override", tagm)
	}

	// GetFile: etag + rebuilt key + counts.
	rec, err := GetFile(ctx, st, f.ID)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if rec.S3Key != wantKey || rec.S3ETag != "etag1" || rec.MessageCount != 5 ||
		rec.StartTimeNs != 1000 || rec.EndTimeNs != 2000 || rec.ChunkCount != 7 {
		t.Fatalf("GetFile = %+v, mismatch", rec)
	}

	// ListTopicsForFile: name-sorted, per-topic counts aligned to topic_id ASC.
	topics, err := ListTopicsForFile(ctx, st, f.ID)
	if err != nil {
		t.Fatalf("ListTopicsForFile: %v", err)
	}
	if len(topics) != 2 || topics[0].Name != "/a" || topics[0].MessageCount != 3 ||
		topics[1].Name != "/b" || topics[1].MessageCount != 2 {
		t.Fatalf("topics = %+v, want [/a:3 /b:2]", topics)
	}
	if topics[0].SchemaName != "S" || topics[0].SchemaEncoding != "ros2msg" {
		t.Fatalf("topic schema = %q/%q, want S/ros2msg", topics[0].SchemaName, topics[0].SchemaEncoding)
	}

	// caps
	hier, err := HasHierarchicalKey(ctx, st)
	if err != nil || !hier {
		t.Fatalf("HasHierarchicalKey = (%v, %v), want (true, nil)", hier, err)
	}
	vocab, err := DistinctMetadataKeys(ctx, st)
	if err != nil {
		t.Fatalf("DistinctMetadataKeys: %v", err)
	}
	if !contains(vocab, "quality") || !contains(vocab, "s3_key") {
		t.Fatalf("vocab = %v, want it to include 'quality' (tag) and 's3_key' (derived)", vocab)
	}

	// Unknown id => ErrFileNotFound (auryn path).
	if _, err := GetFile(ctx, st, 9999); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("GetFile(9999) = %v, want ErrFileNotFound", err)
	}
}

// TestAurynListTopics_CardinalityMismatch: a topic_counts blob whose length does
// not match the topic_set member count is a corrupt catalog — ListTopicsForFile
// must fail fast, not silently mis-count (Codex M2 review).
func TestAurynListTopics_CardinalityMismatch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		blob  []byte // counts for a 2-member topic_set
		wantN int
	}{
		{"short blob (1 count, 2 members)", encodeCounts(3), 1},
		{"long blob (3 counts, 2 members)", encodeCounts(3, 2, 9), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auryn.db")
			buildMinimalAurynDB(t, path) // file id=1 has a 2-member topic_set
			// Overwrite the file's blob with a mismatched one (write via a separate
			// rw handle; the reader opens read-only).
			rw, err := sql.Open("sqlite", fmt.Sprintf("file:%s", path))
			if err != nil {
				t.Fatalf("open rw: %v", err)
			}
			if _, err := rw.Exec(`UPDATE files SET topic_counts = ? WHERE id = 1`, tc.blob); err != nil {
				t.Fatalf("update blob: %v", err)
			}
			rw.Close()

			st, err := OpenReadOnly(context.Background(), path)
			if err != nil {
				t.Fatalf("OpenReadOnly: %v", err)
			}
			defer st.Close()
			if _, err := ListTopicsForFile(context.Background(), st, 1); err == nil {
				t.Fatalf("%s: ListTopicsForFile = nil err, want cardinality-mismatch error", tc.name)
			}
		})
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
