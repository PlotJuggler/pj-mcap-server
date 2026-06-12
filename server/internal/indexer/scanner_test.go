package indexer

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/format"
)

func openStore(t *testing.T) *catalog.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := catalog.Open(context.Background(), filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// fakeLister returns a fixed list of object summaries.
type fakeLister struct{ objs []S3Object }

func (f *fakeLister) List(ctx context.Context, prefix string) ([]S3Object, error) {
	return f.objs, nil
}

// fakeExtractor returns a fixed FileSummary per key and counts calls.
type fakeExtractor struct {
	byKey map[string]format.FileSummary
	calls int64
	err   error
}

func (f *fakeExtractor) Extract(ctx context.Context, key string) (format.FileSummary, error) {
	atomic.AddInt64(&f.calls, 1)
	if f.err != nil {
		return format.FileSummary{}, f.err
	}
	return f.byKey[key], nil
}

func sampleSummary() format.FileSummary {
	return format.FileSummary{
		Key: "run.mcap", Size: 4096, StartNs: 100, EndNs: 200,
		MessageCount: 10, ChunkCount: 2, TopicCount: 1,
		Topics:   []format.TopicInfo{{Name: "/x", SchemaName: "X", SchemaEncoding: "ros2msg", MessageCount: 10}},
		Metadata: map[string]string{"robot_id": "r7"},
	}
}

func TestScanner_IndexesNewFile(t *testing.T) {
	store := openStore(t)
	scanner := &Scanner{
		Store:  store,
		Lister: &fakeLister{objs: []S3Object{{Key: "run.mcap", ETag: "e1", Size: 4096, LastModified: time.Unix(0, 100)}}},
		Extractor: &fakeExtractor{byKey: map[string]format.FileSummary{
			"run.mcap": sampleSummary(),
		}},
	}
	stats, err := scanner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.NewFiles != 1 || stats.Unchanged != 0 || stats.Reindexed != 0 {
		t.Errorf("stats: %+v", stats)
	}
	files, _, _ := catalog.FilterFiles(context.Background(), store, catalog.FilterArgs{Limit: 100})
	if len(files) != 1 || files[0].S3Key != "run.mcap" {
		t.Errorf("file not cataloged: %+v", files)
	}
	tags, _ := catalog.EffectiveTags(context.Background(), store, files[0].ID)
	if len(tags) != 1 || tags[0].Key != "robot_id" || tags[0].Value != "r7" {
		t.Errorf("embedded tag not stored: %+v", tags)
	}
}

func TestScanner_SkipsUnchanged(t *testing.T) {
	store := openStore(t)
	ex := &fakeExtractor{byKey: map[string]format.FileSummary{"run.mcap": sampleSummary()}}
	scanner := &Scanner{
		Store:     store,
		Lister:    &fakeLister{objs: []S3Object{{Key: "run.mcap", ETag: "e1", Size: 4096, LastModified: time.Unix(0, 100)}}},
		Extractor: ex,
	}
	if _, err := scanner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := atomic.LoadInt64(&ex.calls)
	if first != 1 {
		t.Fatalf("first pass should extract once, got %d", first)
	}
	// Second pass: same signature -> no extract, counted unchanged.
	stats, err := scanner.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Unchanged != 1 || stats.NewFiles != 0 || stats.Reindexed != 0 {
		t.Errorf("second pass stats: %+v", stats)
	}
	if atomic.LoadInt64(&ex.calls) != first {
		t.Errorf("unchanged file should NOT be re-extracted: calls=%d", ex.calls)
	}
}

func TestScanner_ReindexPreservesOverride(t *testing.T) {
	store := openStore(t)
	ex := &fakeExtractor{byKey: map[string]format.FileSummary{"run.mcap": sampleSummary()}}
	scanner := &Scanner{
		Store:     store,
		Lister:    &fakeLister{objs: []S3Object{{Key: "run.mcap", ETag: "e1", Size: 4096, LastModified: time.Unix(0, 100)}}},
		Extractor: ex,
	}
	if _, err := scanner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	files, _, _ := catalog.FilterFiles(context.Background(), store, catalog.FilterArgs{Limit: 100})
	id := files[0].ID
	// User overrides a tag.
	if err := catalog.SetOverride(context.Background(), store, id, "verified", "yes"); err != nil {
		t.Fatal(err)
	}
	// Forced reindex: bump last_modified so the change-detect fires.
	scanner.Lister = &fakeLister{objs: []S3Object{{Key: "run.mcap", ETag: "e2", Size: 4096, LastModified: time.Unix(0, 999)}}}
	stats, err := scanner.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Reindexed != 1 {
		t.Errorf("expected reindexed=1, got %+v", stats)
	}
	tags, _ := catalog.EffectiveTags(context.Background(), store, id)
	var foundOverride bool
	for _, tg := range tags {
		if tg.Key == "verified" && tg.Value == "yes" && tg.IsOverride {
			foundOverride = true
		}
	}
	if !foundOverride {
		t.Errorf("override did NOT survive reindex: %+v", tags)
	}
}

func TestScanner_RecordsFailure(t *testing.T) {
	store := openStore(t)
	scanner := &Scanner{
		Store:     store,
		Lister:    &fakeLister{objs: []S3Object{{Key: "bad.mcap", ETag: "e1", Size: 1, LastModified: time.Unix(0, 1)}}},
		Extractor: &fakeExtractor{err: errors.New("no statistics record")},
	}
	stats, err := scanner.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Failed != 1 {
		t.Errorf("expected failed=1, got %+v", stats)
	}
	var key, errText string
	row := store.DB().QueryRow(`SELECT s3_key, error_text FROM indexer_failures WHERE s3_key = 'bad.mcap'`)
	if err := row.Scan(&key, &errText); err != nil {
		t.Fatalf("failure not recorded: %v", err)
	}
	if key != "bad.mcap" {
		t.Errorf("failure key: %q", key)
	}
}
