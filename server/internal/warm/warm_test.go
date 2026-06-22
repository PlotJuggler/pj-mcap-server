package warm

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/storage"
)

// fakeCodec counts ChunkIndex calls per key and can fail selected keys.
type fakeCodec struct {
	mu       sync.Mutex
	calls    map[string]int
	failKeys map[string]bool
}

func newFakeCodec(fail ...string) *fakeCodec {
	fc := &fakeCodec{calls: map[string]int{}, failKeys: map[string]bool{}}
	for _, k := range fail {
		fc.failKeys[k] = true
	}
	return fc
}
func (f *fakeCodec) callsFor(key string) int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls[key] }

func (f *fakeCodec) Extract(context.Context, storage.BlobStore, string) (format.FileSummary, error) {
	return format.FileSummary{}, nil
}
func (f *fakeCodec) ChunkIndex(_ context.Context, _ storage.BlobStore, key string, fileID uint64) (format.FileChunkIndex, error) {
	f.mu.Lock()
	f.calls[key]++
	fail := f.failKeys[key]
	f.mu.Unlock()
	if fail {
		return format.FileChunkIndex{}, errors.New("boom")
	}
	return format.FileChunkIndex{FileID: fileID}, nil
}
func (f *fakeCodec) ExtractAndIndex(context.Context, storage.BlobStore, string, uint64) (format.FileSummary, format.FileChunkIndex, error) {
	return format.FileSummary{}, format.FileChunkIndex{}, nil
}

// seedStore creates a legacy catalog store with n files (keys k0..k{n-1}).
func seedStore(t *testing.T, n int) *catalog.Store {
	t.Helper()
	s, err := catalog.Open(context.Background(), filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	for i := 0; i < n; i++ {
		if _, _, err := catalog.UpsertFile(context.Background(), s, catalog.FileRecord{
			S3Key: fmt.Sprintf("k%d", i), S3ETag: "e", S3LastModified: 1, SizeBytes: 1,
			StartTimeNs: 1, EndTimeNs: 2, MessageCount: 1,
		}); err != nil {
			t.Fatalf("UpsertFile %d: %v", i, err)
		}
	}
	return s
}

func TestWarmer_WarmsAllThenSkips(t *testing.T) {
	store := seedStore(t, 5)
	codec := newFakeCodec()
	cache := format.NewChunkIndexCache(64)
	w := &Warmer{Store: store, Codec: codec, Blob: nil, Cache: cache, Concurrency: 3, Metrics: metrics.New()}

	// First sweep: warm all 5.
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cache.Len() != 5 {
		t.Fatalf("cache len = %d, want 5", cache.Len())
	}
	for i := 0; i < 5; i++ {
		if c := codec.callsFor(fmt.Sprintf("k%d", i)); c != 1 {
			t.Fatalf("k%d codec calls = %d, want 1", i, c)
		}
	}

	// Second sweep: all cached => zero new codec calls (skipped).
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	for i := 0; i < 5; i++ {
		if c := codec.callsFor(fmt.Sprintf("k%d", i)); c != 1 {
			t.Fatalf("k%d codec calls after re-warm = %d, want still 1 (skipped)", i, c)
		}
	}
}

func TestWarmer_PoisonFileDoesNotAbort(t *testing.T) {
	store := seedStore(t, 4)
	codec := newFakeCodec("k2") // k2 always fails
	cache := format.NewChunkIndexCache(64)
	w := &Warmer{Store: store, Codec: codec, Blob: nil, Cache: cache, Concurrency: 2}

	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The 3 healthy files warmed despite k2 failing — the sweep did not abort.
	if cache.Len() != 3 {
		t.Fatalf("cache len = %d, want 3 (k2 failed, others warmed)", cache.Len())
	}
	if _, ok := cache.Get("k2", "e", 0); ok {
		t.Fatal("k2 should NOT be cached (it failed)")
	}
	for _, k := range []string{"k0", "k1", "k3"} {
		if _, ok := cache.Get(k, "e", 0); !ok {
			t.Fatalf("%s should be cached", k)
		}
	}
}

func TestWarmer_NilCacheNoop(t *testing.T) {
	w := &Warmer{Store: seedStore(t, 2), Codec: newFakeCodec(), Cache: nil}
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run with nil cache: %v", err)
	}
}

func TestWarmer_CancelledContext(t *testing.T) {
	store := seedStore(t, 3)
	codec := newFakeCodec()
	cache := format.NewChunkIndexCache(64)
	w := &Warmer{Store: store, Codec: codec, Cache: cache, Concurrency: 2}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	// Must return promptly without panicking. A cancelled ctx may fail the catalog
	// list (propagated as context.Canceled) or, if the list slipped through, warm
	// nothing — both are acceptable; a panic or hang is not.
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(cancelled): unexpected err %v", err)
	}
}
