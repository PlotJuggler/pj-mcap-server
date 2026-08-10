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
	"time"

	"pj-cloud/server/internal/config"
)

var (
	// ErrTransient marks a retryable failure (network blip, 5xx, throttling).
	ErrTransient = errors.New("storage: transient error")
	// ErrPermanent marks a non-retryable failure (404, auth, malformed key).
	ErrPermanent = errors.New("storage: permanent error")
	// ErrNotFound marks the OBJECT-absent subset of ErrPermanent. Retry logic
	// keys on ErrPermanent as before; the session plan-build additionally keys
	// on this to report a vanished recording as ERROR_NOT_FOUND instead of a
	// generic bucket outage (event-discovery design 2026-07-30 §7.1).
	//
	// Attachment is deliberately conservative: at the classifier level only
	// the GET-shaped, unambiguous NoSuchKey carries it. A HEAD 404 cannot
	// name its cause (S3 HeadObject returns bare NotFound for a missing
	// OBJECT *and* a missing BUCKET; GCS object Attrs likewise normalize a
	// bucket-level 404 to ErrObjectNotExist), so the store Head methods
	// upgrade a 404 via disambiguate404 — probing bucket existence on the
	// error path — and anything ambiguous stays plain ErrPermanent. Bucket
	// absence and auth-shaped permanents (403 — which S3 also returns for
	// missing objects without s3:ListBucket) never carry it: claiming
	// "recording deleted" on config breakage would misdirect the operator.
	ErrNotFound = errors.New("storage: object not found")
)

// bucketProbeTimeout bounds the disambiguation probe below — an error-path
// nicety must never hang a failed Head for long.
const bucketProbeTimeout = 10 * time.Second

// disambiguate404 upgrades an object-level 404 from a Head call to carry
// ErrNotFound (§7.1) ONLY when the bucket itself is confirmed to exist.
// probeBucket (nil error = bucket exists) is invoked solely on this error
// path — one probe per failed Head, never on the happy path — under a
// context detached from the caller's cancellation (a bailing caller must not
// abort the classification) but capped at bucketProbeTimeout. A
// failed/uncertain probe keeps the error as-is — fail-closed to the
// outage-shaped code.
func disambiguate404(ctx context.Context, err error, is404 bool,
	probeBucket func(context.Context) error) error {
	if err == nil || !is404 {
		return err
	}
	if errors.Is(err, ErrNotFound) {
		return err // already precise (GET-shape NoSuchKey via classify)
	}
	probeCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), bucketProbeTimeout)
	defer cancel()
	if probeBucket(probeCtx) != nil {
		return err
	}
	return fmt.Errorf("%w: %w", ErrNotFound, err)
}

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
