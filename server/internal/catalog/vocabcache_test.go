package catalog

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The problem this cache exists to solve, in the user's words: "the first browse
// after any idle period pays the full scan, and there is no cache — the whole
// aggregation is recomputed on every single call. That is why it feels random:
// it depends entirely on whether someone browsed recently."
//
// So a STRICT cache is not enough. The builder commits continuously, so by the
// time anyone browses again the catalog revision has usually moved — a
// strictly-fresh cache would miss exactly when it matters. The contract is:
// serve the known-good tree IMMEDIATELY and refresh behind the caller, but never
// across a generation boundary (dimension ids renumber on republish).

func TestVocabCache_SecondCallAtTheSameRevisionDoesNotRecompute(t *testing.T) {
	var calls atomic.Int64
	c := NewVocabularyCache(time.Minute)
	compute := func(context.Context) (*Vocabulary, error) {
		calls.Add(1)
		return &Vocabulary{}, nil
	}
	rev := Revision{Generation: "g1", DataVersion: 1}

	for i := 0; i < 5; i++ {
		if _, err := c.get(context.Background(), rev, VocabOptions{}, compute); err != nil {
			t.Fatal(err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("expected 1 computation for 5 calls at one revision, got %d", n)
	}
}

// THE case the user described. The catalog moved while nobody was browsing; the
// next browse must NOT pay the full scan.
func TestVocabCache_IdleThenBrowseServesImmediatelyAndRefreshesBehind(t *testing.T) {
	var calls atomic.Int64
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	c := NewVocabularyCache(time.Minute)
	compute := func(context.Context) (*Vocabulary, error) {
		n := calls.Add(1)
		started <- struct{}{}
		if n > 1 {
			<-release // a refresh is slow; the caller must NOT be waiting on it
		}
		return &Vocabulary{Customers: []VocabCustomer{{ID: uint64(n), Name: "c"}}}, nil
	}

	ctx := context.Background()
	if _, err := c.get(ctx, Revision{Generation: "g1", DataVersion: 1}, VocabOptions{}, compute); err != nil {
		t.Fatal(err)
	}
	<-started

	// Builder committed while idle: same generation, newer data_version.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := c.get(ctx, Revision{Generation: "g1", DataVersion: 2}, VocabOptions{}, compute); err != nil {
			t.Errorf("stale-serve get: %v", err)
		}
	}()
	select {
	case <-done: // served from cache without waiting for the slow recompute
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("a browse after an idle period BLOCKED on the recompute — this is the exact " +
			"stall the cache exists to remove")
	}
	close(release)
}

// Ids are generation-scoped: a republish renumbers them, so a stale tree from a
// previous generation would hand the client ids naming different rows.
func TestVocabCache_NeverServesStaleAcrossAGeneration(t *testing.T) {
	var calls atomic.Int64
	c := NewVocabularyCache(time.Hour) // generous window; generation must still win
	compute := func(context.Context) (*Vocabulary, error) {
		n := calls.Add(1)
		return &Vocabulary{Customers: []VocabCustomer{{ID: uint64(n)}}}, nil
	}
	ctx := context.Background()
	if _, err := c.get(ctx, Revision{Generation: "g1", DataVersion: 1}, VocabOptions{}, compute); err != nil {
		t.Fatal(err)
	}
	v, err := c.get(ctx, Revision{Generation: "g2", DataVersion: 1}, VocabOptions{}, compute)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Customers) == 0 || v.Customers[0].ID != 2 {
		t.Fatal("a new generation must BLOCK for a freshly computed tree, never serve the previous one")
	}
}

// Full and lean responses differ in content, so they must not share a slot.
func TestVocabCache_OptionsArePartOfTheIdentity(t *testing.T) {
	var calls atomic.Int64
	c := NewVocabularyCache(time.Minute)
	compute := func(context.Context) (*Vocabulary, error) {
		calls.Add(1)
		return &Vocabulary{}, nil
	}
	rev := Revision{Generation: "g1", DataVersion: 1}
	ctx := context.Background()
	if _, err := c.get(ctx, rev, VocabOptions{}, compute); err != nil {
		t.Fatal(err)
	}
	if _, err := c.get(ctx, rev, VocabOptions{OmitSources: true, OmitTagFacets: true, OmitSiteRobotCounts: true}, compute); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("full and lean must not share a cache slot: %d computations (want 2)", n)
	}
}

// k clients connecting at once must not queue k multi-second aggregations on the
// single pinned SQLite connection.
func TestVocabCache_ConcurrentMissesCollapse(t *testing.T) {
	var calls atomic.Int64
	c := NewVocabularyCache(time.Minute)
	release := make(chan struct{})
	compute := func(context.Context) (*Vocabulary, error) {
		calls.Add(1)
		<-release
		return &Vocabulary{}, nil
	}
	rev := Revision{Generation: "g1", DataVersion: 1}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.get(context.Background(), rev, VocabOptions{}, compute); err != nil {
				t.Errorf("get: %v", err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("expected 8 concurrent misses to collapse into 1 computation, got %d", n)
	}
}
