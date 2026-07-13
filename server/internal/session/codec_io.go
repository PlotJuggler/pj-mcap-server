package session

import (
	"context"
	"fmt"

	"pj-cloud/server/internal/format"
)

// RangeGetter is the minimal storage view the production ChunkReader needs: a
// ranged read of an object by key. storage.BlobStore.GetRange satisfies this
// directly, so the WS/main wiring passes the BlobStore in without an adapter.
// (session must not import storage; this narrow interface is the seam.)
type RangeGetter interface {
	GetRange(ctx context.Context, key string, off, length int64) ([]byte, error)
}

// FileKeys maps a catalog file id to its object key. The session opener builds
// it from the selected catalog entries.
type FileKeys map[uint64]string

// blobChunkReader is the production ChunkReader: Range-GETs the chunk record
// bytes for ref via the per-file object key.
type blobChunkReader struct {
	get  RangeGetter
	keys FileKeys
}

// NewChunkReader returns a production ChunkReader over a RangeGetter and a
// fileID->key map.
func NewChunkReader(get RangeGetter, keys FileKeys) ChunkReader {
	return &blobChunkReader{get: get, keys: keys}
}

func (r *blobChunkReader) ReadChunk(ctx context.Context, ref ChunkRef) ([]byte, error) {
	key, ok := r.keys[ref.FileID]
	if !ok {
		return nil, fmt.Errorf("session: no object key for file %d", ref.FileID)
	}
	return r.get.GetRange(ctx, key, ref.Offset, ref.Length)
}

// indexChunkIterator is the production ChunkIterator: it dispatches to the
// correct per-file format.FileChunkIndex (which carries that file's channel/
// schema table) by ref.FileID. This is what makes multi-file stitching work
// through a single Producer.
type indexChunkIterator struct {
	byFile map[uint64]format.FileChunkIndex
}

// NewChunkIterator returns a production ChunkIterator over the per-file chunk
// indexes (keyed by FileID).
func NewChunkIterator(indexes []FileChunkIndex) ChunkIterator {
	byFile := make(map[uint64]format.FileChunkIndex, len(indexes))
	for _, ix := range indexes {
		byFile[ix.FileID] = ix
	}
	return &indexChunkIterator{byFile: byFile}
}

func (it *indexChunkIterator) Iterate(chunkBytes []byte, ref ChunkRef, tr *TimeWindow, emit func(RawMessage) error) error {
	idx, ok := it.byFile[ref.FileID]
	if !ok {
		return fmt.Errorf("session: no chunk index for file %d", ref.FileID)
	}
	return idx.Iterate(chunkBytes, ref, tr, emit)
}
