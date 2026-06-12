package catalog

import (
	"context"
	"testing"
)

// helper: file with given key, time range, optional topics + embedded tags
type fixtureFile struct {
	key      string
	startNs  int64
	endNs    int64
	topics   []string
	embedded []TagKV
}

func loadFixtures(t *testing.T, s *Store, files []fixtureFile) []uint64 {
	t.Helper()
	ctx := context.Background()
	ids := make([]uint64, len(files))
	for i, f := range files {
		rec := FileRecord{
			S3Key:          f.key,
			S3ETag:         "e-" + f.key,
			S3LastModified: int64(i + 1),
			SizeBytes:      1024,
			StartTimeNs:    f.startNs,
			EndTimeNs:      f.endNs,
		}
		id, _, err := UpsertFile(ctx, s, rec)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
		var topics []TopicRecord
		for _, tn := range f.topics {
			topics = append(topics, TopicRecord{Name: tn, SchemaName: "X", SchemaEncoding: "ros2msg", MessageCount: 10})
		}
		_ = ReplaceTopicsForFile(ctx, s, id, topics)
		_ = ReplaceEmbeddedTagsForFile(ctx, s, id, f.embedded)
	}
	return ids
}

func TestFilterFiles_All(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{
		{key: "a.mcap", startNs: 100, endNs: 200},
		{key: "b.mcap", startNs: 300, endNs: 400},
	})
	got, _, err := FilterFiles(context.Background(), s, FilterArgs{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("want 2 got %d", len(got))
	}
}

func TestFilterFiles_TimeRange(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{
		{key: "old.mcap", startNs: 100, endNs: 200},
		{key: "mid.mcap", startNs: 1000, endNs: 1500},
		{key: "new.mcap", startNs: 5000, endNs: 6000},
	})
	got, _, err := FilterFiles(context.Background(), s, FilterArgs{
		Limit:           100,
		RecordedBetween: &TimeWindow{StartNs: 900, EndNs: 1600},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].S3Key != "mid.mcap" {
		t.Errorf("expected only mid.mcap, got %+v", got)
	}
}

func TestFilterFiles_TimeRange_OverlapBoundary(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{
		// touches the window exactly at its start edge -> overlaps (>= / <=).
		{key: "edge.mcap", startNs: 50, endNs: 100},
		{key: "after.mcap", startNs: 101, endNs: 200},
	})
	got, _, _ := FilterFiles(context.Background(), s, FilterArgs{
		Limit:           100,
		RecordedBetween: &TimeWindow{StartNs: 100, EndNs: 150},
	})
	if len(got) != 2 {
		t.Errorf("expected both (boundary inclusive), got %+v", got)
	}
}

func TestFilterFiles_TopicAnyOf(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{
		{key: "a.mcap", startNs: 1, endNs: 2, topics: []string{"/imu", "/gps"}},
		{key: "b.mcap", startNs: 3, endNs: 4, topics: []string{"/lidar"}},
	})
	got, _, _ := FilterFiles(context.Background(), s, FilterArgs{
		Limit:       100,
		TopicsAnyOf: []string{"/imu", "/lidar"},
	})
	if len(got) != 2 {
		t.Errorf("expected 2, got %d", len(got))
	}
	// topic_count is populated by the SELECT sub-query.
	for _, f := range got {
		if f.TopicCount == 0 {
			t.Errorf("topic_count not populated: %+v", f)
		}
	}
}

func TestFilterFiles_TagAll(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{
		{key: "a.mcap", startNs: 1, endNs: 2,
			embedded: []TagKV{{Key: "vehicle", Value: "7"}, {Key: "verified", Value: "yes"}}},
		{key: "b.mcap", startNs: 3, endNs: 4,
			embedded: []TagKV{{Key: "vehicle", Value: "7"}}},
	})
	got, _, _ := FilterFiles(context.Background(), s, FilterArgs{
		Limit: 100,
		TagAll: []TagKV{
			{Key: "vehicle", Value: "7"},
			{Key: "verified", Value: "yes"},
		},
	})
	if len(got) != 1 || got[0].S3Key != "a.mcap" {
		t.Errorf("expected only a.mcap, got %+v", got)
	}
}

func TestFilterFiles_TagAll_KeyExists(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{
		{key: "a.mcap", startNs: 1, endNs: 2, embedded: []TagKV{{Key: "verified", Value: "yes"}}},
		{key: "b.mcap", startNs: 3, endNs: 4, embedded: []TagKV{{Key: "other", Value: "x"}}},
	})
	// empty value => "key exists".
	got, _, _ := FilterFiles(context.Background(), s, FilterArgs{
		Limit:  100,
		TagAll: []TagKV{{Key: "verified"}},
	})
	if len(got) != 1 || got[0].S3Key != "a.mcap" {
		t.Errorf("key-exists predicate failed: %+v", got)
	}
}

func TestFilterFiles_TagAny(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{
		{key: "a.mcap", startNs: 1, endNs: 2, embedded: []TagKV{{Key: "color", Value: "red"}}},
		{key: "b.mcap", startNs: 3, endNs: 4, embedded: []TagKV{{Key: "color", Value: "blue"}}},
		{key: "c.mcap", startNs: 5, endNs: 6, embedded: []TagKV{{Key: "color", Value: "green"}}},
	})
	got, _, _ := FilterFiles(context.Background(), s, FilterArgs{
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
	s := newTestStore(t)
	ids := loadFixtures(t, s, []fixtureFile{
		{key: "a.mcap", startNs: 1, endNs: 2},
	})
	_ = SetOverride(context.Background(), s, ids[0], "verified", "yes")
	got, _, _ := FilterFiles(context.Background(), s, FilterArgs{
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
	s := newTestStore(t)
	var fxs []fixtureFile
	for i := 0; i < 5; i++ {
		fxs = append(fxs, fixtureFile{
			key: stringID("k-", i), startNs: int64(i * 100), endNs: int64(i*100 + 50),
		})
	}
	loadFixtures(t, s, fxs)

	page1, next1, _ := FilterFiles(context.Background(), s, FilterArgs{Limit: 2})
	if len(page1) != 2 {
		t.Fatalf("page1 len: %d", len(page1))
	}
	if next1 == "" {
		t.Error("page1 should have a next token")
	}
	page2, next2, _ := FilterFiles(context.Background(), s, FilterArgs{Limit: 2, PageToken: next1})
	if len(page2) != 2 {
		t.Fatalf("page2 len: %d", len(page2))
	}
	page3, next3, _ := FilterFiles(context.Background(), s, FilterArgs{Limit: 2, PageToken: next2})
	if len(page3) != 1 {
		t.Fatalf("page3 len: %d", len(page3))
	}
	if next3 != "" {
		t.Errorf("last page should have empty next, got %q", next3)
	}
}

func TestFilterFiles_BadPageToken(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{{key: "a.mcap", startNs: 1, endNs: 2}})
	_, _, err := FilterFiles(context.Background(), s, FilterArgs{Limit: 2, PageToken: "!!!not-base64!!!"})
	if err == nil {
		t.Error("expected error for malformed page_token")
	}
}

func TestFileSummary_FlatMetadata(t *testing.T) {
	s := newTestStore(t)
	loadFixtures(t, s, []fixtureFile{
		{key: "a.mcap", startNs: 1, endNs: 2,
			embedded: []TagKV{{Key: "robot_id", Value: "r7"}, {Key: "operator", Value: "alice"}}},
	})
	got, _, err := FilterFiles(context.Background(), s, FilterArgs{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	flat := got[0].FlatMetadata()
	if flat["robot_id"] != "r7" || flat["operator"] != "alice" {
		t.Errorf("flat metadata: %+v", flat)
	}
}

func stringID(prefix string, n int) string {
	return prefix + string(rune('a'+n)) + ".mcap"
}
