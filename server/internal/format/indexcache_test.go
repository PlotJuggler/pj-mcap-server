package format

import (
	"fmt"
	"testing"
)

func TestChunkIndexCache_PutGetRestamp(t *testing.T) {
	c := NewChunkIndexCache(8)
	c.Put("k", "e1", FileChunkIndex{FileID: 0, Chunks: []ChunkRef{{FileID: 0, Offset: 10}}})

	// Hit restamps FileID on the index AND every chunk ref.
	idx, ok := c.Get("k", "e1", 42)
	if !ok {
		t.Fatal("expected hit")
	}
	if idx.FileID != 42 || len(idx.Chunks) != 1 || idx.Chunks[0].FileID != 42 {
		t.Fatalf("restamp failed: %+v", idx)
	}

	// Different etag => miss (an overwritten object never serves a stale plan).
	if _, ok := c.Get("k", "e2", 1); ok {
		t.Fatal("different etag should miss")
	}
}

func TestChunkIndexCache_LRUEviction(t *testing.T) {
	c := NewChunkIndexCache(3)
	put := func(n int) { c.Put(fmt.Sprintf("k%d", n), "e", FileChunkIndex{FileID: uint64(n)}) }
	has := func(n int) bool { _, ok := c.Get(fmt.Sprintf("k%d", n), "e", 1); return ok }

	put(1)
	put(2)
	put(3)
	// Touch k1 so it is most-recently-used; k2 is now the LRU.
	if !has(1) {
		t.Fatal("k1 should be present")
	}
	put(4) // over cap (3) => evict the LRU, which is k2 (NOT a full reset).

	if c.Len() != 3 {
		t.Fatalf("len = %d, want 3 (one eviction, not a full reset)", c.Len())
	}
	if has(2) {
		t.Fatal("k2 (LRU) should have been evicted")
	}
	// k1 (recently touched), k3, k4 survive — the warm set is preserved.
	for _, n := range []int{1, 3, 4} {
		if !has(n) {
			t.Fatalf("k%d should survive eviction", n)
		}
	}
}

// idxOfSchemaBytes builds a FileChunkIndex whose approxBytes is ~schemaLen (the
// per-topic SchemaData dominates the footprint).
func idxOfSchemaBytes(schemaLen int) FileChunkIndex {
	return FileChunkIndex{Schemas: []TopicSchemaInfo{{
		TopicName: "/t", SchemaName: "S", SchemaEncoding: "ros2msg",
		MessageEncoding: "cdr", SchemaData: make([]byte, schemaLen),
	}}}
}

func TestChunkIndexCache_ByteEviction(t *testing.T) {
	const entry = 10 * 1024
	// Byte cap ~25 KB, no entry cap: holds ~2 of the ~10 KB entries.
	c := NewChunkIndexCacheSized(0, 25*1024)
	for i := 0; i < 6; i++ {
		c.Put(fmt.Sprintf("k%d", i), "e", idxOfSchemaBytes(entry))
	}
	// Memory is bounded to the budget plus at most one MRU entry (never unbounded).
	if c.Bytes() > 25*1024+2*entry {
		t.Fatalf("curBytes=%d exceeds budget + one entry", c.Bytes())
	}
	if c.Len() > 3 {
		t.Fatalf("len=%d too large for a %d-byte budget", c.Len(), 25*1024)
	}
	if _, ok := c.Get("k0", "e", 0); ok {
		t.Fatal("k0 (LRU) should have been byte-evicted")
	}
	if _, ok := c.Get("k5", "e", 0); !ok {
		t.Fatal("k5 (MRU) must survive")
	}

	// A single entry larger than the whole budget is still kept (MRU never evicts
	// itself) — it's cached, not looped-away.
	c.Clear()
	c.Put("huge", "e", idxOfSchemaBytes(200*1024))
	if _, ok := c.Get("huge", "e", 0); !ok || c.Len() != 1 {
		t.Fatalf("an oversized single entry should be kept; len=%d", c.Len())
	}
}

func TestChunkIndexCache_RePutRefreshes(t *testing.T) {
	c := NewChunkIndexCache(2)
	c.Put("a", "e", FileChunkIndex{FileID: 1})
	c.Put("b", "e", FileChunkIndex{FileID: 2})
	c.Put("a", "e", FileChunkIndex{FileID: 11}) // re-Put 'a' => MRU + new value
	c.Put("c", "e", FileChunkIndex{FileID: 3})  // evicts 'b' (now LRU), not 'a'

	if _, ok := c.Get("b", "e", 1); ok {
		t.Fatal("b should be evicted")
	}
	a, ok := c.Get("a", "e", 99)
	if !ok || a.FileID != 99 {
		t.Fatalf("a should survive + restamp, got ok=%v %+v", ok, a)
	}
}

func TestChunkIndexCache_NilSafe(t *testing.T) {
	var c *ChunkIndexCache
	c.Put("k", "e", FileChunkIndex{})
	if _, ok := c.Get("k", "e", 1); ok {
		t.Fatal("nil cache must always miss")
	}
	if c.Len() != 0 {
		t.Fatal("nil cache len must be 0")
	}
}
