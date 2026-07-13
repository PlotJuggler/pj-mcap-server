package ws

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/session"
	"pj-cloud/server/internal/storage"
)

// blockingCodec blocks every ChunkIndex call on `release` (so the test can hold
// the leader in-flight while waiters attach), counts calls, and can fail.
type blockingCodec struct {
	mu      sync.Mutex
	calls   int
	started chan struct{} // one signal per call entering
	release chan struct{} // closed/sent to let a blocked call proceed
	fail    bool
}

func (c *blockingCodec) Extract(context.Context, storage.BlobStore, string) (format.FileSummary, error) {
	return format.FileSummary{}, nil
}
func (c *blockingCodec) ExtractAndIndex(context.Context, storage.BlobStore, string, uint64) (format.FileSummary, format.FileChunkIndex, error) {
	return format.FileSummary{}, format.FileChunkIndex{}, nil
}
func (c *blockingCodec) ChunkIndex(ctx context.Context, _ storage.BlobStore, _ string, fileID uint64) (format.FileChunkIndex, error) {
	c.mu.Lock()
	c.calls++
	fail := c.fail
	c.mu.Unlock()
	select {
	case c.started <- struct{}{}:
	default:
	}
	select {
	case <-c.release:
	case <-ctx.Done():
		return format.FileChunkIndex{}, ctx.Err()
	}
	if fail {
		return format.FileChunkIndex{}, errors.New("boom")
	}
	return format.FileChunkIndex{FileID: fileID}, nil
}
func (c *blockingCodec) callCount() int { c.mu.Lock(); defer c.mu.Unlock(); return c.calls }

// TestCachedChunkIndex_SingleflightDedup: N concurrent cold misses of the same
// (key,etag) collapse to exactly ONE codec load; each caller gets the result
// restamped to its own fileID.
func TestCachedChunkIndex_SingleflightDedup(t *testing.T) {
	codec := &blockingCodec{started: make(chan struct{}, 1), release: make(chan struct{})}
	d := &SessionDeps{Codec: codec, IdxCache: format.NewChunkIndexCache(16)}

	const N = 6
	var wg sync.WaitGroup
	results := make([]session.FileChunkIndex, N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = d.cachedChunkIndex(context.Background(), "k", "e", uint64(i+1))
		}(i)
	}

	<-codec.started                    // the leader is in-flight
	time.Sleep(100 * time.Millisecond) // let the other callers attach as waiters
	close(codec.release)               // release the leader
	wg.Wait()

	if got := codec.callCount(); got != 1 {
		t.Fatalf("codec calls = %d, want 1 (singleflight dedup)", got)
	}
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d err: %v", i, errs[i])
		}
		if results[i].FileID != uint64(i+1) {
			t.Fatalf("caller %d FileID = %d, want %d (per-caller restamp)", i, results[i].FileID, i+1)
		}
	}
}

// TestCachedChunkIndex_LeaderErrorThenRetry: a leader error propagates to all
// in-flight waiters, and a SUBSEQUENT call starts a fresh load (no poisoned entry).
func TestCachedChunkIndex_LeaderErrorThenRetry(t *testing.T) {
	codec := &blockingCodec{started: make(chan struct{}, 1), release: make(chan struct{}), fail: true}
	d := &SessionDeps{Codec: codec, IdxCache: format.NewChunkIndexCache(16)}

	const N = 4
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _, errs[i] = d.cachedChunkIndex(context.Background(), "k", "e", 1) }(i)
	}
	<-codec.started
	time.Sleep(100 * time.Millisecond)
	close(codec.release)
	wg.Wait()

	for i := 0; i < N; i++ {
		if errs[i] == nil {
			t.Fatalf("caller %d: want error, got nil", i)
		}
	}
	if got := codec.callCount(); got != 1 {
		t.Fatalf("first round codec calls = %d, want 1", got)
	}

	// A fresh call retries (the failed singleflight entry was not cached).
	codec.mu.Lock()
	codec.fail = false
	codec.mu.Unlock()
	codec.release = make(chan struct{})
	close(codec.release)
	if _, err := d.cachedChunkIndex(context.Background(), "k", "e", 9); err != nil {
		t.Fatalf("retry after leader error: %v", err)
	}
	if got := codec.callCount(); got != 2 {
		t.Fatalf("after retry codec calls = %d, want 2 (fresh load)", got)
	}
}

// TestCachedChunkIndex_WaiterBailsLeaderCompletes: a waiter that cancels its OWN
// ctx returns ctx.Err() promptly, while the detached leader still completes and
// caches (so a later opener hits).
func TestCachedChunkIndex_WaiterBailsLeaderCompletes(t *testing.T) {
	codec := &blockingCodec{started: make(chan struct{}, 1), release: make(chan struct{})}
	d := &SessionDeps{Codec: codec, IdxCache: format.NewChunkIndexCache(16)}

	// Leader (background ctx) — will block on release.
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _ = d.cachedChunkIndex(context.Background(), "k", "e", 1)
	}()
	<-codec.started

	// A waiter on the SAME key with a cancellable ctx — attaches, then cancels.
	wctx, cancel := context.WithCancel(context.Background())
	waiterErr := make(chan error, 1)
	go func() { _, err := d.cachedChunkIndex(wctx, "k", "e", 2); waiterErr <- err }()
	time.Sleep(50 * time.Millisecond) // let the waiter attach
	cancel()

	select {
	case err := <-waiterErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not return after its ctx was cancelled (leader cancellation leaked to it)")
	}

	// The leader still completes + caches despite the waiter bailing.
	close(codec.release)
	<-leaderDone
	if _, ok := d.IdxCache.Get("k", "e", 7); !ok {
		t.Fatal("leader should have cached the result despite the waiter bailing")
	}
}
