package indexer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/format"
)

func TestLoop_WarmStartThenPolls(t *testing.T) {
	store := openStore(t)
	scanner := &Scanner{
		Store:     store,
		Lister:    &fakeLister{objs: []S3Object{{Key: "run.mcap", ETag: "e1", Size: 4096, LastModified: time.Unix(0, 1)}}},
		Extractor: &fakeExtractor{byKey: map[string]format.FileSummary{"run.mcap": sampleSummary()}},
	}
	loop := &Loop{Scanner: scanner, Interval: 25 * time.Millisecond, StartupScan: true}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := loop.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// warm-start ran synchronously; one indexed file present.
	if got := atomic.LoadInt64(&loop.RunCountForTest); got < 1 {
		t.Errorf("warm-start did not run: count=%d", got)
	}
	files, _, _ := catalog.FilterFiles(context.Background(), store, catalog.FilterArgs{Limit: 100})
	if len(files) != 1 {
		t.Errorf("warm-start did not index: %d files", len(files))
	}

	<-ctx.Done() // wait for some polls
	if got := atomic.LoadInt64(&loop.RunCountForTest); got < 2 {
		t.Errorf("expected >= 2 runs (warm + poll), got %d", got)
	}
}

// TestLoop_WarmStartOnExistingDB proves the persistence contract: a second Loop
// over the SAME store re-serves existing rows with new=0, reindexed=0,
// unchanged=N (no re-extraction). This is the Go twin of smoke step g.
func TestLoop_WarmStartOnExistingDB(t *testing.T) {
	store := openStore(t)
	obj := S3Object{Key: "run.mcap", ETag: "e1", Size: 4096, LastModified: time.Unix(0, 1)}
	ex := &fakeExtractor{byKey: map[string]format.FileSummary{"run.mcap": sampleSummary()}}

	cold := &Scanner{Store: store, Lister: &fakeLister{objs: []S3Object{obj}}, Extractor: ex}
	coldStats, err := cold.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if coldStats.NewFiles != 1 {
		t.Fatalf("cold run should be new=1: %+v", coldStats)
	}
	callsAfterCold := atomic.LoadInt64(&ex.calls)

	// "Restart": a fresh Scanner over the same store, same object signature.
	warm := &Scanner{Store: store, Lister: &fakeLister{objs: []S3Object{obj}}, Extractor: ex}
	warmStats, err := warm.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if warmStats.NewFiles != 0 || warmStats.Reindexed != 0 || warmStats.Unchanged != 1 {
		t.Errorf("warm-start should be unchanged=1 new=0 reindexed=0: %+v", warmStats)
	}
	if atomic.LoadInt64(&ex.calls) != callsAfterCold {
		t.Errorf("warm-start re-extracted: calls grew from %d to %d", callsAfterCold, ex.calls)
	}
}
