package format

import (
	"context"
	"sync/atomic"
	"testing"

	"pj-cloud/server/internal/storage"
)

// countingStore wraps a BlobStore and counts the round trips. Over WAN every
// GetRange/Head is one HTTPS request (~tens of ms RTT), so these counts ARE the
// latency model: OpenSession plan-building was measured at 3m24s/file on the
// real Dexory staging bucket purely from per-(chunk x channel) GETs.
type countingStore struct {
	inner    storage.BlobStore
	getRange atomic.Int64
	head     atomic.Int64
}

func (c *countingStore) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	c.getRange.Add(1)
	return c.inner.GetRange(ctx, key, off, length)
}

func (c *countingStore) Head(ctx context.Context, key string) (storage.ObjectInfo, error) {
	c.head.Add(1)
	return c.inner.Head(ctx, key)
}

func (c *countingStore) List(ctx context.Context, prefix, token string) ([]storage.ObjectInfo, string, error) {
	return c.inner.List(ctx, prefix, token)
}

// Extract must be O(1) round trips: Head + tail probe + (summary if it
// outgrows the probe) + the magic read — NOT a 4KiB-granular scan of the
// summary section.
func TestExtractGetRangeBudget(t *testing.T) {
	const key = "nissan_zala_50_zeg_1_0.mcap"
	bs := &countingStore{inner: fileStore{root: "testdata"}}
	codec, err := NewCodec("mcap")
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}

	fs, err := codec.Extract(context.Background(), bs, key)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if fs.TopicCount != 6 {
		t.Fatalf("sanity: topic count = %d, want 6", fs.TopicCount)
	}

	if got := bs.getRange.Load(); got > 5 {
		t.Errorf("Extract used %d GetRange calls, want <= 5 (each is one WAN round trip)", got)
	}
	if got := bs.head.Load(); got > 1 {
		t.Errorf("Extract used %d Head calls, want <= 1", got)
	}
}

// ChunkIndex must be O(1) round trips AND O(summary) transfer: the summary
// tail read only — NO MessageIndex reads at all. The MessageIndex region is
// ~16 bytes per message (~10MB on a real 630k-msg staging file): reading it
// just to compute the spec-approximate message estimates made a 0.5MB
// windowed download pay a ~10MB / ~12s plan-build tax over WAN. Per-chunk
// counts are instead derived from the summary's file-level Statistics.
func TestChunkIndexGetRangeBudget(t *testing.T) {
	const key = "nissan_zala_50_zeg_1_0.mcap"
	bs := &countingStore{inner: fileStore{root: "testdata"}}
	codec, err := NewCodec("mcap")
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}

	idx, err := codec.ChunkIndex(context.Background(), bs, key, 7)
	if err != nil {
		t.Fatalf("ChunkIndex: %v", err)
	}
	if len(idx.Chunks) == 0 {
		t.Fatal("sanity: no chunks in index")
	}

	if got := bs.getRange.Load(); got > 5 {
		t.Errorf("ChunkIndex used %d GetRange calls over %d chunks, want <= 5 (summary tail only, no MessageIndex reads)",
			got, len(idx.Chunks))
	}
	if got := bs.head.Load(); got > 1 {
		t.Errorf("ChunkIndex used %d Head calls, want <= 1", got)
	}

	// The exact-count contract must survive the rewrite: per-chunk counts come
	// from MessageIndex records, summing to the file's known message total.
	var total uint64
	for _, c := range idx.Chunks {
		total += c.MessageCount
	}
	if total != 33670 {
		t.Errorf("sum of per-chunk MessageCount = %d, want 33670 (file ground truth)", total)
	}
}
