// indexcache.go is the shared per-file chunk-index cache. Building a file's
// chunk index is a WAN cost (Head + the summary section — ~hundreds of KB of
// schemas for a many-topic file, bandwidth-bound). Two callers want the result:
// the session handler (every OpenSession) and the background warmer / indexer
// (which ALREADY reads the same summary). Caching here lets a download skip the
// read once the file is warm.
package format

import (
	"container/list"
	"sync"
)

// ChunkIndexCache maps (s3_key | etag) -> FileChunkIndex with LRU eviction. The
// etag in the key makes an overwritten object miss naturally (no stale plan after
// a reindex). A cached FileChunkIndex is read-only after construction (PlanChunks
// copies refs; the channel table is only read), so one value is safely shared
// across concurrent sessions; Get restamps FileID per caller.
//
// Eviction is LRU by entry count (each entry is small — chunk refs + the
// schema/channel tables): over `max`, the least-recently-used entries are dropped
// one at a time, so a warm working set survives a churning lake (unlike the prior
// crude full-reset, which threw away EVERYTHING the moment the cap was reached —
// catastrophic right after a warm-up at the cap).
type ChunkIndexCache struct {
	mu    sync.Mutex
	max   int
	ll    *list.List               // front = most-recently used, back = LRU victim
	items map[string]*list.Element // cacheKey -> *list.Element(*cacheEntry)
}

type cacheEntry struct {
	key string
	idx FileChunkIndex
}

// NewChunkIndexCache builds a cache capped at max entries (default 1024). The
// ChunkIndexCache MUST be built via this constructor (a zero-value struct has a
// nil list and would panic on Put). When max is below the catalog's file count,
// the cache churns (LRU still preserves the hot set); size it >= the corpus.
func NewChunkIndexCache(max int) *ChunkIndexCache {
	if max <= 0 {
		max = 1024
	}
	return &ChunkIndexCache{max: max, ll: list.New(), items: make(map[string]*list.Element, 64)}
}

func cacheKey(key, etag string) string { return key + "|" + etag }

// Clear empties the cache. Used by tests that need a guaranteed cold plan.
func (c *ChunkIndexCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.ll.Init()
	c.items = make(map[string]*list.Element, 64)
	c.mu.Unlock()
}

// Len returns the number of cached entries (for tests / metrics).
func (c *ChunkIndexCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Get returns the cached index for (key,etag) with FileID restamped to fileID,
// or ok=false on a miss. A hit moves the entry to most-recently-used.
func (c *ChunkIndexCache) Get(key, etag string, fileID uint64) (FileChunkIndex, bool) {
	if c == nil {
		return FileChunkIndex{}, false
	}
	ck := cacheKey(key, etag)
	c.mu.Lock()
	el, ok := c.items[ck]
	if !ok {
		c.mu.Unlock()
		return FileChunkIndex{}, false
	}
	c.ll.MoveToFront(el)
	// Copy the FileChunkIndex VALUE out under the lock (a re-Put could reassign the
	// entry's idx field concurrently); reFileID then deep-copies Chunks as needed.
	idx := el.Value.(*cacheEntry).idx
	c.mu.Unlock()
	return reFileID(idx, fileID), true
}

// Put stores idx under (key,etag) as most-recently-used, evicting LRU entries over
// the cap. Safe to call concurrently and repeatedly (re-storing refreshes recency
// + value). The stored FileID is irrelevant — Get restamps it.
func (c *ChunkIndexCache) Put(key, etag string, idx FileChunkIndex) {
	if c == nil {
		return
	}
	ck := cacheKey(key, etag)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[ck]; ok {
		el.Value.(*cacheEntry).idx = idx
		c.ll.MoveToFront(el)
		return
	}
	c.items[ck] = c.ll.PushFront(&cacheEntry{key: ck, idx: idx})
	for len(c.items) > c.max { // evict the least-recently-used (back) until in-bounds
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.ll.Remove(back)
		delete(c.items, back.Value.(*cacheEntry).key)
	}
}

// reFileID returns a copy of idx with FileID (and every ChunkRef's FileID)
// restamped — the cached value may have been built under a different catalog
// row id (or the placeholder 0 the warmer caches with). Chunk refs are
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
