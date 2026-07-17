package session

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// recordingGetter records plain ranged reads.
type recordingGetter struct {
	mu    sync.Mutex
	plain []string
}

func (g *recordingGetter) GetRange(_ context.Context, key string, off, length int64) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.plain = append(g.plain, fmt.Sprintf("%s@%d+%d", key, off, length))
	return []byte{1}, nil
}

// recordingVersionedGetter additionally records version-pinned reads.
type recordingVersionedGetter struct {
	recordingGetter
	versioned []string
}

func (g *recordingVersionedGetter) GetRangeVersioned(_ context.Context, key, version string, off, length int64) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.versioned = append(g.versioned, fmt.Sprintf("%s@%s@%d+%d", key, version, off, length))
	return []byte{1}, nil
}

// A store that can pin a read to one object version MUST be asked to: an
// overwrite of the key mid-session must fail the read instead of silently
// mixing version A's chunk offsets with version B's bytes (pre-merge review,
// Codex architectural finding #5).
func TestChunkReader_UsesVersionPinnedReadWhenAvailable(t *testing.T) {
	g := &recordingVersionedGetter{}
	r := NewChunkReaderVersioned(g, FileKeys{7: "k.mcap"}, map[uint64]string{7: "etag-7"})
	if _, err := r.ReadChunk(context.Background(), ChunkRef{FileID: 7, Offset: 10, Length: 5}); err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if len(g.versioned) != 1 || g.versioned[0] != "k.mcap@etag-7@10+5" {
		t.Fatalf("versioned calls = %v, want the pinned read", g.versioned)
	}
	if len(g.plain) != 0 {
		t.Fatalf("plain calls = %v, want none (versioned path must be used)", g.plain)
	}
}

// Fallbacks: a store without the versioned extension, or a file with no known
// version, reads unconditionally (fakes and older stores keep working).
func TestChunkReader_FallsBackToPlainRead(t *testing.T) {
	plainOnly := &recordingGetter{}
	r := NewChunkReaderVersioned(plainOnly, FileKeys{7: "k.mcap"}, map[uint64]string{7: "etag-7"})
	if _, err := r.ReadChunk(context.Background(), ChunkRef{FileID: 7, Offset: 0, Length: 4}); err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if len(plainOnly.plain) != 1 {
		t.Fatalf("plain calls = %v, want 1 (store has no versioned read)", plainOnly.plain)
	}

	vg := &recordingVersionedGetter{}
	r = NewChunkReaderVersioned(vg, FileKeys{7: "k.mcap"}, nil) // no versions known
	if _, err := r.ReadChunk(context.Background(), ChunkRef{FileID: 7, Offset: 0, Length: 4}); err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if len(vg.versioned) != 0 || len(vg.plain) != 1 {
		t.Fatalf("calls = versioned:%v plain:%v, want plain only (no version to pin)", vg.versioned, vg.plain)
	}
}
