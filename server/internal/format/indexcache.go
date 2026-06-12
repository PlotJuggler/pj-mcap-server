// indexcache.go is the shared per-file chunk-index cache. Building a file's
// chunk index is a WAN cost (Head + the summary section — ~hundreds of KB of
// schemas for a many-topic file, bandwidth-bound). Two callers want the result:
// the session handler (every OpenSession) and the indexer (which ALREADY reads
// the same summary during its background scan). Caching here lets the indexer
// pre-warm the index for free, so a download never pays the read again.
package format

import "sync"

// ChunkIndexCache maps (s3_key | etag) -> FileChunkIndex. The etag in the key
// makes an overwritten object miss naturally (no stale plan after a reindex).
// A cached FileChunkIndex is read-only after construction (PlanChunks copies
// refs; the channel table is only read), so one value is safely shared across
// concurrent sessions; Get restamps FileID per caller.
type ChunkIndexCache struct {
	mu    sync.Mutex
	max   int
	items map[string]FileChunkIndex
}

// NewChunkIndexCache builds a cache capped at max entries (each is small —
// chunk refs + the schema/channel tables; a crude full reset past the cap
// bounds growth over a long-lived server with a churning bucket).
func NewChunkIndexCache(max int) *ChunkIndexCache {
	if max <= 0 {
		max = 1024
	}
	return &ChunkIndexCache{max: max, items: make(map[string]FileChunkIndex, 64)}
}

func cacheKey(key, etag string) string { return key + "|" + etag }

// Clear empties the cache. Used by tests that need a guaranteed cold plan.
func (c *ChunkIndexCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.items = make(map[string]FileChunkIndex, 64)
	c.mu.Unlock()
}

// Get returns the cached index for (key,etag) with FileID restamped to fileID,
// or ok=false on a miss.
func (c *ChunkIndexCache) Get(key, etag string, fileID uint64) (FileChunkIndex, bool) {
	if c == nil {
		return FileChunkIndex{}, false
	}
	c.mu.Lock()
	idx, ok := c.items[cacheKey(key, etag)]
	c.mu.Unlock()
	if !ok {
		return FileChunkIndex{}, false
	}
	return reFileID(idx, fileID), true
}

// Put stores idx under (key,etag). Safe to call concurrently and repeatedly
// (re-storing an identical value is a no-op). The stored FileID is irrelevant —
// Get restamps it.
func (c *ChunkIndexCache) Put(key, etag string, idx FileChunkIndex) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if len(c.items) >= c.max {
		c.items = make(map[string]FileChunkIndex, 64)
	}
	c.items[cacheKey(key, etag)] = idx
	c.mu.Unlock()
}

// reFileID returns a copy of idx with FileID (and every ChunkRef's FileID)
// restamped — the cached value may have been built under a different catalog
// row id (or the placeholder 0 the indexer caches with). Chunk refs are
// value-copied; the shared maps stay read-only.
func reFileID(idx FileChunkIndex, fileID uint64) FileChunkIndex {
	if idx.FileID == fileID {
		return idx
	}
	out := idx
	out.FileID = fileID
	out.Chunks = make([]ChunkRef, len(idx.Chunks))
	copy(out.Chunks, idx.Chunks)
	for i := range out.Chunks {
		out.Chunks[i].FileID = fileID
	}
	return out
}
