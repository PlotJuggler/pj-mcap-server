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
// Eviction is LRU, bounded by TWO caps (either can be disabled with 0):
//   - maxBytes: total estimated bytes of the cached indexes. This is the memory
//     bound that matters — a FileChunkIndex is dominated by its per-topic
//     SchemaData, so a many-topic file (hundreds of KB–MBs of schema text) is
//     ~100x a tiny fixture. An entry-count cap alone let 4096 real Dexory files
//     pin ~10 GB; a byte cap bounds RSS regardless of per-file size.
//   - max: entry count (a coarse safety cap; 0 = unbounded by count).
//
// Over either cap, the least-recently-used entries are dropped one at a time (the
// most-recently-used is always kept), so a warm working set survives a churning
// lake — unlike a crude full-reset that throws away everything at the cap.
type ChunkIndexCache struct {
	mu       sync.Mutex
	max      int   // entry-count cap; 0 = unbounded by count
	maxBytes int64 // estimated-byte cap; 0 = unbounded by bytes
	curBytes int64
	ll       *list.List               // front = most-recently used, back = LRU victim
	items    map[string]*list.Element // cacheKey -> *list.Element(*cacheEntry)
}

type cacheEntry struct {
	key   string
	idx   FileChunkIndex
	bytes int // approxBytes(idx) at insert time
}

// NewChunkIndexCache builds a cache capped at max ENTRIES (default 1024), with no
// byte cap. Kept for tests and callers that reason in entry counts. The cache MUST
// be built via a constructor (a zero-value struct has a nil list and panics on Put).
func NewChunkIndexCache(max int) *ChunkIndexCache {
	if max <= 0 {
		max = 1024
	}
	return NewChunkIndexCacheSized(max, 0)
}

// NewChunkIndexCacheSized builds a cache bounded by maxEntries AND/OR maxBytes
// (each 0 = that cap disabled). Production uses (0, budget): byte-bounded only, so
// memory is capped no matter how large individual files' schema tables are.
func NewChunkIndexCacheSized(maxEntries int, maxBytes int64) *ChunkIndexCache {
	return &ChunkIndexCache{
		max: maxEntries, maxBytes: maxBytes,
		ll: list.New(), items: make(map[string]*list.Element, 64),
	}
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
	c.curBytes = 0
	c.mu.Unlock()
}

// Bytes returns the cache's current estimated byte size (for metrics / the warmer).
func (c *ChunkIndexCache) Bytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// evictLocked drops LRU entries until both caps are satisfied, always keeping at
// least the most-recently-used entry — so a single entry larger than the whole
// budget is still cached (memory then exceeds the cap by that one entry) instead
// of evicting itself in an infinite loop. Caller holds c.mu.
func (c *ChunkIndexCache) evictLocked() {
	for c.ll.Len() > 1 &&
		((c.max > 0 && len(c.items) > c.max) ||
			(c.maxBytes > 0 && c.curBytes > c.maxBytes)) {
		back := c.ll.Back()
		if back == nil {
			break
		}
		ent := back.Value.(*cacheEntry)
		c.ll.Remove(back)
		delete(c.items, ent.key)
		c.curBytes -= int64(ent.bytes)
	}
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
	b := idx.ApproxBytes()
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[ck]; ok {
		ent := el.Value.(*cacheEntry)
		c.curBytes += int64(b) - int64(ent.bytes) // adjust for the value swap
		ent.idx = idx
		ent.bytes = b
		c.ll.MoveToFront(el)
		c.evictLocked()
		return
	}
	c.items[ck] = c.ll.PushFront(&cacheEntry{key: ck, idx: idx, bytes: b})
	c.curBytes += int64(b)
	c.evictLocked()
}

// ApproxBytes estimates a cached index's heap footprint. It is dominated by the
// per-topic SchemaData (the ROS2 message-definition text), which is why entries
// vary ~100x in size; the rest is coarse fixed overhead per chunk/channel. Exact
// accounting is unnecessary — this only has to bound RSS, not measure it. Exported
// so the warmer can budget its sweep by the same estimate the cache evicts on.
func (idx FileChunkIndex) ApproxBytes() int {
	n := 512 // slice/map headers + struct base
	for i := range idx.Schemas {
		s := &idx.Schemas[i]
		n += len(s.SchemaData) + len(s.TopicName) + len(s.SchemaName) +
			len(s.SchemaEncoding) + len(s.MessageEncoding) + 96
	}
	for i := range idx.Chunks {
		n += 128 // ChunkRef fixed fields
		for t := range idx.Chunks[i].ChannelTopics {
			n += len(t) + 24
		}
	}
	n += len(idx.channels) * 96
	for i := range idx.chunkCounts {
		n += 48 + len(idx.chunkCounts[i].perTopic)*48 // per-topic map entries
	}
	return n
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
