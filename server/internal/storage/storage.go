// Package storage is the BlobStore seam: an object-store abstraction the rest
// of the server reads through, with an S3 implementation (used here against
// Minio). It is the sole credentials boundary — nothing else imports a cloud
// SDK (Plan A "Seam name discipline", 2026-05-28-pj-cloud-server-v1.md).
//
// Seam shape (kept identical to Plan A so later subsystems drop in unchanged):
//
//	GetRange(ctx,key,off,length) ([]byte,error)
//	Head(ctx,key) (ObjectInfo,error)
//	List(ctx,prefix,token) ([]ObjectInfo, nextToken, error)
//
// plus ReaderAt(ctx,key,size) which adapts GetRange into an io.ReaderSeeker for
// the MCAP footer/summary reads in internal/format. Sentinel errors ErrTransient
// / ErrPermanent classify failures for the (future) session resume logic.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"pj-cloud/server/internal/config"
)

var (
	// ErrTransient marks a retryable failure (network blip, 5xx, throttling).
	ErrTransient = errors.New("storage: transient error")
	// ErrPermanent marks a non-retryable failure (404, auth, malformed key).
	ErrPermanent = errors.New("storage: permanent error")
)

// ObjectInfo is the listing/head view of one object. LastModifiedNs is unix nanos.
type ObjectInfo struct {
	Key            string
	ETag           string
	Size           int64
	LastModifiedNs int64
}

// BlobStore is the read-only object-store seam used by this slice.
type BlobStore interface {
	// GetRange returns [off, off+length) of the object. A length <= 0 means
	// "from off to end of object".
	GetRange(ctx context.Context, key string, off, length int64) ([]byte, error)
	// Head returns object metadata without fetching the body.
	Head(ctx context.Context, key string) (ObjectInfo, error)
	// List returns one page of objects under prefix. token is the opaque
	// continuation token ("" for the first page); the returned next token is
	// "" when the listing is exhausted.
	List(ctx context.Context, prefix, token string) ([]ObjectInfo, string, error)
}

// New is the storage-union dispatcher: it builds the BlobStore for whichever arm
// of the StorageConfig tagged union is set (config.Validate guarantees exactly
// one). This is the single selection point — main.go calls New(ctx, cfg.Storage)
// and nothing outside this package picks a backend. (config.Validate runs first
// in Load; New re-checks defensively so a programmatic Config is also safe.)
func New(ctx context.Context, cfg config.StorageConfig) (BlobStore, error) {
	switch {
	case cfg.S3 != nil && cfg.GCS != nil:
		return nil, fmt.Errorf("storage: exactly one of s3/gcs must be set, not both")
	case cfg.S3 != nil:
		return NewS3(ctx, *cfg.S3)
	case cfg.GCS != nil:
		return NewGCS(ctx, *cfg.GCS)
	default:
		return nil, fmt.Errorf("storage: no backend configured (set storage.s3 or storage.gcs)")
	}
}

// rangeReaderAt adapts a BlobStore + key into an io.ReaderAt / io.ReadSeeker so
// the MCAP reader can do indexed (footer/summary) reads without downloading the
// whole object. Size is required so SeekEnd works.
type rangeReaderAt struct {
	ctx  context.Context
	bs   BlobStore
	key  string
	size int64
	pos  int64
}

// ReaderAt returns an io.ReadSeeker (also an io.ReaderAt) over key, backed by
// ranged GetRange calls. size must be the object's full size (from Head).
func ReaderAt(ctx context.Context, bs BlobStore, key string, size int64) *rangeReaderAt {
	return &rangeReaderAt{ctx: ctx, bs: bs, key: key, size: size}
}

func (r *rangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	want := int64(len(p))
	if off+want > r.size {
		want = r.size - off
	}
	data, err := r.bs.GetRange(r.ctx, r.key, off, want)
	if err != nil {
		return 0, err
	}
	n := copy(p, data)
	if int64(n) < int64(len(p)) {
		return n, io.EOF
	}
	return n, nil
}

func (r *rangeReaderAt) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *rangeReaderAt) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, fmt.Errorf("storage: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("storage: negative position")
	}
	r.pos = abs
	return abs, nil
}
