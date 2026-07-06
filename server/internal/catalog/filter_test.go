package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// filterFixtureFile is one file for buildFilterFixtureDB: a filename, a time
// range, optional topics, and optional embedded tags — the auryn-schema
// migration of the deleted (legacy-writer) filter_test.go fixtures. All files
// share one dimension tuple (customer=t/site=t/robot=t/source=t); only
// per-FILE identity (filename, time range, topics, tags) varies, which is all
// FilterFiles's predicates key off.
type filterFixtureFile struct {
	name     string
	startNs  int64
	endNs    int64
	topics   []string
	embedded []TagKV
}

// buildFilterFixtureDB writes a fresh auryn-schema (v3) DB at path containing
// exactly the given files. Every file gets the SAME single topic
// ("/x", schema "X"/"ros2msg", count 10) for every name in topics (a single
// shared topic_names/schemas row, like the legacy fixture's tests only ever
// asserted topic PRESENCE, never distinct per-topic schemas). Returns the
// filename -> assigned file id map (assigned in the given slice order).
func buildFilterFixtureDB(t *testing.T, path string, files []filterFixtureFile) map[string]uint64 {
	t.Helper()
	db := openAurynTestDB(t, path)
	defer db.Close()

	ddl := []string{
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

	// Global topic_id dedup (by name) across every file, so a shared topic set
	// (e.g. two files both carrying "/imu") reuses the same topic_names row —
	// mirroring the real builder's dedup.
	topicID := map[string]int64{}
	nextTopicID := int64(1)
	const schemaID = int64(1)
	if _, err := db.Exec(`INSERT INTO schemas(id,name,encoding) VALUES (1,'X','ros2msg')`); err != nil {
		t.Fatalf("insert schema: %v", err)
	}

	topicSetID := map[string]int64{} // fingerprint(sorted topic ids) -> set id
	nextSetID := int64(1)

	out := make(map[string]uint64, len(files))
	for i, f := range files {
		fileID := int64(i + 1)
		var topicIDs []int64
		for _, tn := range f.topics {
			id, ok := topicID[tn]
			if !ok {
				id = nextTopicID
				nextTopicID++
				topicID[tn] = id
				if _, err := db.Exec(`INSERT INTO topic_names(id,name) VALUES (?,?)`, id, tn); err != nil {
					t.Fatalf("insert topic_name %q: %v", tn, err)
				}
			}
			topicIDs = append(topicIDs, id)
		}
		// Sort ascending (matches decodeCountsBlob's topic_id-ASC contract).
		for a := 0; a < len(topicIDs); a++ {
			for b := a + 1; b < len(topicIDs); b++ {
				if topicIDs[b] < topicIDs[a] {
					topicIDs[a], topicIDs[b] = topicIDs[b], topicIDs[a]
				}
			}
		}
		fp := fmt.Sprint(topicIDs)
		setID, ok := topicSetID[fp]
		if !ok {
			setID = nextSetID
			nextSetID++
			topicSetID[fp] = setID
			if _, err := db.Exec(`INSERT INTO topic_sets(id,fingerprint) VALUES (?,?)`, setID, fp); err != nil {
				t.Fatalf("insert topic_set: %v", err)
			}
			for _, tid := range topicIDs {
				if _, err := db.Exec(`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (?,?,?)`,
					setID, tid, schemaID); err != nil {
					t.Fatalf("insert topic_set_member: %v", err)
				}
			}
		}
		counts := make([]byte, 0, len(topicIDs))
		for range topicIDs {
			counts = append(counts, 10) // message_count=10 per topic (single-byte varint)
		}

		if _, err := db.Exec(`INSERT INTO files
			(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (?,?,?,?,1,1,1,1,'2026-01-01',?,?,1,?,?)`,
			fileID, f.name, "e-"+f.name, 1024, f.startNs, f.endNs, setID, counts); err != nil {
			t.Fatalf("insert file %q: %v", f.name, err)
		}
		for _, tg := range f.embedded {
			if _, err := db.Exec(`INSERT INTO tags_embedded(file_id,key,value) VALUES (?,?,?)`,
				fileID, tg.Key, tg.Value); err != nil {
				t.Fatalf("insert embedded tag: %v", err)
			}
		}
		out[f.name] = uint64(fileID)
	}
	return out
}

// openFilterFixtureStore is buildFilterFixtureDB + OpenReadOnly in one call.
func openFilterFixtureStore(t *testing.T, files []filterFixtureFile) (*Store, map[string]uint64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "filter.db")
	ids := buildFilterFixtureDB(t, path, files)
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, ids
}

// setOverrideRaw inserts a tags_override row directly (the write API is gone;
// tests that need an override tag write it themselves, mirroring what the
// Python builder's tag-edit IPC endpoint does).
func setOverrideRaw(t *testing.T, s *Store, fileID uint64, key, value string) {
	t.Helper()
	// The Store is read-only; open a SEPARATE rw handle on the same file to
	// seed the override, mirroring TestAurynListTopics_CardinalityMismatch's
	// pattern of writing via a second handle.
	rw, err := sql.Open("sqlite", fmt.Sprintf("file:%s", s.dbPath))
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	defer rw.Close()
	if _, err := rw.Exec(`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (?,?,?,1)`,
		fileID, key, value); err != nil {
		t.Fatalf("insert override: %v", err)
	}
}

func TestFilterFiles_All(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{
		{name: "a.mcap", startNs: 100, endNs: 200},
		{name: "b.mcap", startNs: 300, endNs: 400},
	})
	got, _, err := FilterFiles(context.Background(), st, FilterArgs{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("want 2 got %d", len(got))
	}
}

func TestFilterFiles_TimeRange(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{
		{name: "old.mcap", startNs: 100, endNs: 200},
		{name: "mid.mcap", startNs: 1000, endNs: 1500},
		{name: "new.mcap", startNs: 5000, endNs: 6000},
	})
	got, _, err := FilterFiles(context.Background(), st, FilterArgs{
		Limit:           100,
		RecordedBetween: &TimeWindow{StartNs: 900, EndNs: 1600},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !hasSuffixKey(got[0].S3Key, "mid.mcap") {
		t.Errorf("expected only mid.mcap, got %+v", got)
	}
}

func TestFilterFiles_TimeRange_OverlapBoundary(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{
		// touches the window exactly at its start edge -> overlaps (>= / <=).
		{name: "edge.mcap", startNs: 50, endNs: 100},
		{name: "after.mcap", startNs: 101, endNs: 200},
	})
	got, _, _ := FilterFiles(context.Background(), st, FilterArgs{
		Limit:           100,
		RecordedBetween: &TimeWindow{StartNs: 100, EndNs: 150},
	})
	if len(got) != 2 {
		t.Errorf("expected both (boundary inclusive), got %+v", got)
	}
}

func TestFilterFiles_TopicAnyOf(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{
		{name: "a.mcap", startNs: 1, endNs: 2, topics: []string{"/imu", "/gps"}},
		{name: "b.mcap", startNs: 3, endNs: 4, topics: []string{"/lidar"}},
	})
	got, _, _ := FilterFiles(context.Background(), st, FilterArgs{
		Limit:       100,
		TopicsAnyOf: []string{"/imu", "/lidar"},
	})
	if len(got) != 2 {
		t.Errorf("expected 2, got %d", len(got))
	}
	for _, f := range got {
		if f.TopicCount == 0 {
			t.Errorf("topic_count not populated: %+v", f)
		}
	}
}

func TestFilterFiles_TagAll(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{
		{name: "a.mcap", startNs: 1, endNs: 2,
			embedded: []TagKV{{Key: "vehicle", Value: "7"}, {Key: "verified", Value: "yes"}}},
		{name: "b.mcap", startNs: 3, endNs: 4,
			embedded: []TagKV{{Key: "vehicle", Value: "7"}}},
	})
	got, _, _ := FilterFiles(context.Background(), st, FilterArgs{
		Limit: 100,
		TagAll: []TagKV{
			{Key: "vehicle", Value: "7"},
			{Key: "verified", Value: "yes"},
		},
	})
	if len(got) != 1 || !hasSuffixKey(got[0].S3Key, "a.mcap") {
		t.Errorf("expected only a.mcap, got %+v", got)
	}
}

func TestFilterFiles_TagAll_KeyExists(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{
		{name: "a.mcap", startNs: 1, endNs: 2, embedded: []TagKV{{Key: "verified", Value: "yes"}}},
		{name: "b.mcap", startNs: 3, endNs: 4, embedded: []TagKV{{Key: "other", Value: "x"}}},
	})
	// empty value => "key exists".
	got, _, _ := FilterFiles(context.Background(), st, FilterArgs{
		Limit:  100,
		TagAll: []TagKV{{Key: "verified"}},
	})
	if len(got) != 1 || !hasSuffixKey(got[0].S3Key, "a.mcap") {
		t.Errorf("key-exists predicate failed: %+v", got)
	}
}

func TestFilterFiles_TagAny(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{
		{name: "a.mcap", startNs: 1, endNs: 2, embedded: []TagKV{{Key: "color", Value: "red"}}},
		{name: "b.mcap", startNs: 3, endNs: 4, embedded: []TagKV{{Key: "color", Value: "blue"}}},
		{name: "c.mcap", startNs: 5, endNs: 6, embedded: []TagKV{{Key: "color", Value: "green"}}},
	})
	got, _, _ := FilterFiles(context.Background(), st, FilterArgs{
		Limit:  100,
		TagAny: []TagKV{{Key: "color", Value: "red"}, {Key: "color", Value: "blue"}},
	})
	if len(got) != 2 {
		t.Errorf("expected 2 (red OR blue), got %+v", got)
	}
}

func TestFilterFiles_OverrideMakesFileMatch(t *testing.T) {
	// A tag predicate matches a file whose VALUE comes only from an override
	// (tags_effective drives the predicate).
	st, ids := openFilterFixtureStore(t, []filterFixtureFile{
		{name: "a.mcap", startNs: 1, endNs: 2},
	})
	setOverrideRaw(t, st, ids["a.mcap"], "verified", "yes")
	got, _, _ := FilterFiles(context.Background(), st, FilterArgs{
		Limit:  100,
		TagAll: []TagKV{{Key: "verified", Value: "yes"}},
	})
	if len(got) != 1 {
		t.Errorf("override-only tag should match: %+v", got)
	}
	if len(got) == 1 && (len(got[0].Tags) != 1 || !got[0].Tags[0].IsOverride) {
		t.Errorf("expected override tag attached: %+v", got[0].Tags)
	}
}

func TestFilterFiles_Pagination(t *testing.T) {
	var fxs []filterFixtureFile
	for i := 0; i < 5; i++ {
		fxs = append(fxs, filterFixtureFile{
			name: fmt.Sprintf("k-%d.mcap", i), startNs: int64(i * 100), endNs: int64(i*100 + 50),
		})
	}
	st, _ := openFilterFixtureStore(t, fxs)

	page1, next1, _ := FilterFiles(context.Background(), st, FilterArgs{Limit: 2})
	if len(page1) != 2 {
		t.Fatalf("page1 len: %d", len(page1))
	}
	if next1 == "" {
		t.Error("page1 should have a next token")
	}
	page2, next2, _ := FilterFiles(context.Background(), st, FilterArgs{Limit: 2, PageToken: next1})
	if len(page2) != 2 {
		t.Fatalf("page2 len: %d", len(page2))
	}
	page3, next3, _ := FilterFiles(context.Background(), st, FilterArgs{Limit: 2, PageToken: next2})
	if len(page3) != 1 {
		t.Fatalf("page3 len: %d", len(page3))
	}
	if next3 != "" {
		t.Errorf("last page should have empty next, got %q", next3)
	}
}

func TestFilterFiles_BadPageToken(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{{name: "a.mcap", startNs: 1, endNs: 2}})
	_, _, err := FilterFiles(context.Background(), st, FilterArgs{Limit: 2, PageToken: "!!!not-base64!!!"})
	if err == nil {
		t.Error("expected error for malformed page_token")
	}
}

func TestFileSummary_FlatMetadata(t *testing.T) {
	st, _ := openFilterFixtureStore(t, []filterFixtureFile{
		{name: "a.mcap", startNs: 1, endNs: 2,
			embedded: []TagKV{{Key: "robot_id", Value: "r7"}, {Key: "operator", Value: "alice"}}},
	})
	got, _, err := FilterFiles(context.Background(), st, FilterArgs{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	flat := got[0].FlatMetadata()
	if flat["robot_id"] != "r7" || flat["operator"] != "alice" {
		t.Errorf("flat metadata: %+v", flat)
	}
}

// hasSuffixKey reports whether s3Key (the rebuilt Hive key) ends with the
// given bare filename — the filter tests above only care about WHICH file
// (identified by its filename) matched, not the shared dimension prefix.
func hasSuffixKey(s3Key, filename string) bool {
	n := len(s3Key) - len(filename)
	return n >= 0 && s3Key[n:] == filename
}
