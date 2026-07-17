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

// versionedRangeGetter is the OPTIONAL conditional-read extension of the
// RangeGetter seam (structural, like RangeGetter itself, so session still does
// not import storage): a ranged read pinned to one object VERSION — the
// catalog's change token (S3 ETag / GCS generation). An overwrite of the key
// mid-session then FAILS the read instead of silently mixing version A's chunk
// offsets with version B's bytes.
type versionedRangeGetter interface {
	GetRangeVersioned(ctx context.Context, key, version string, off, length int64) ([]byte, error)
}

// FileKeys maps a catalog file id to its object key. The session opener builds
// it from the selected catalog entries.
type FileKeys map[uint64]string

// blobChunkReader is the production ChunkReader: Range-GETs the chunk record
// bytes for ref via the per-file object key, version-pinned when both the
// store and the catalog know the file's version.
type blobChunkReader struct {
	get      RangeGetter
	keys     FileKeys
	versions map[uint64]string // fileID -> catalog change token; "" / absent = unpinned
}

// NewChunkReader returns a production ChunkReader over a RangeGetter and a
// fileID->key map (no version pinning — legacy/fake stores).
func NewChunkReader(get RangeGetter, keys FileKeys) ChunkReader {
	return &blobChunkReader{get: get, keys: keys}
}

// NewChunkReaderVersioned additionally carries each file's catalog change token
// so chunk reads are version-pinned whenever the store supports it. A store
// without the extension (or a file with no known version) reads plain.
func NewChunkReaderVersioned(get RangeGetter, keys FileKeys, versions map[uint64]string) ChunkReader {
	return &blobChunkReader{get: get, keys: keys, versions: versions}
}

func (r *blobChunkReader) ReadChunk(ctx context.Context, ref ChunkRef) ([]byte, error) {
	key, ok := r.keys[ref.FileID]
	if !ok {
		return nil, fmt.Errorf("session: no object key for file %d", ref.FileID)
	}
	if v := r.versions[ref.FileID]; v != "" {
		if vg, ok := r.get.(versionedRangeGetter); ok {
			return vg.GetRangeVersioned(ctx, key, v, ref.Offset, ref.Length)
		}
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
