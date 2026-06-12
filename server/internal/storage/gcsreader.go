package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"pj-cloud/server/internal/config"
)

// gcsStore is the Google Cloud Storage BlobStore implementation (Plan A Task
// 14b, Asensus M1b). It is a drop-in behind the IDENTICAL interface as the S3
// arm and, like s3Store, wraps each GetRange/Head/List SDK call in the shared
// retryWith (Plan A Task 14: ONE backoff schedule, classifyGCS deciding
// transient-vs-permanent). This is the only place the GCS SDK is touched (the
// credentials boundary).
type gcsStore struct {
	client *gcs.Client
	bucket string
	prefix string
}

// NewGCS builds a GCS-backed BlobStore. Auth baseline is ADC / Workload Identity
// (the attached service account, no key on disk); option.WithCredentialsFile is
// DEV ONLY. When STORAGE_EMULATOR_HOST is set, cloud.google.com/go/storage
// auto-points at the emulator and skips authentication, so the dev/test
// (fake-gcs) path needs no explicit WithoutAuthentication option here. Mirrors
// the NewS3 signature shape.
func NewGCS(ctx context.Context, cfg config.GCSConfig) (BlobStore, error) {
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile)) // dev only
	}
	client, err := gcs.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcs client: %w", err)
	}
	return &gcsStore{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

func (g *gcsStore) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	// GCS NewRangeReader: length < 0 means "to end of object" (mirror the S3 arm's
	// length<=0 => off..EOF contract).
	readLen := length
	if length <= 0 {
		readLen = -1
	}
	var data []byte
	err := retryWith(ctx, func(ctx context.Context) error {
		r, rErr := g.client.Bucket(g.bucket).Object(key).NewRangeReader(ctx, off, readLen)
		if rErr != nil {
			return classifyGCS(fmt.Errorf("gcs get %q range [%d,%d): %w", key, off, off+length, rErr))
		}
		defer r.Close()
		body, readErr := io.ReadAll(r)
		if readErr != nil {
			return classifyGCS(fmt.Errorf("gcs read %q body: %w", key, readErr))
		}
		data = body
		return nil
	}, classifyGCS)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (g *gcsStore) Head(ctx context.Context, key string) (ObjectInfo, error) {
	var info ObjectInfo
	err := retryWith(ctx, func(ctx context.Context) error {
		attrs, aErr := g.client.Bucket(g.bucket).Object(key).Attrs(ctx)
		if aErr != nil {
			return classifyGCS(fmt.Errorf("gcs attrs %q: %w", key, aErr))
		}
		info = objectInfoFromAttrs(attrs)
		return nil
	}, classifyGCS)
	if err != nil {
		return ObjectInfo{}, err
	}
	return info, nil
}

func (g *gcsStore) List(ctx context.Context, prefix, token string) ([]ObjectInfo, string, error) {
	fullPrefix := g.prefix + prefix
	var objs []ObjectInfo
	var next string
	err := retryWith(ctx, func(ctx context.Context) error {
		// Rebuild the iterator+pager per attempt so a retried List starts the page
		// fresh from token (the previous attempt's failed pager state is discarded).
		it := g.client.Bucket(g.bucket).Objects(ctx, &gcs.Query{Prefix: fullPrefix})
		pager := iterator.NewPager(it, 1000, token)
		var attrsList []*gcs.ObjectAttrs
		n, pErr := pager.NextPage(&attrsList)
		if pErr != nil {
			return classifyGCS(fmt.Errorf("gcs list %q: %w", fullPrefix, pErr))
		}
		objs = make([]ObjectInfo, 0, len(attrsList))
		for _, a := range attrsList {
			objs = append(objs, objectInfoFromAttrs(a))
		}
		next = n
		return nil
	}, classifyGCS)
	if err != nil {
		return nil, "", err
	}
	return objs, next, nil
}

// objectInfoFromAttrs is the ETag-mapping pin (Plan A Task 14b): the
// change-detect identity is Generation (a monotonic int64 rendered as a decimal
// string) + Updated, NOT the MD5/CRC32C ETag (unstable for composed/rewritten
// objects). The indexer treats ObjectInfo.ETag as an opaque string and compares
// it by equality, so a decimal Generation slots into the s3_etag TEXT column and
// compares identically across reindex passes for an unmodified object.
func objectInfoFromAttrs(a *gcs.ObjectAttrs) ObjectInfo {
	return ObjectInfo{
		Key:            a.Name,
		ETag:           strconv.FormatInt(a.Generation, 10),
		Size:           a.Size,
		LastModifiedNs: a.Updated.UnixNano(),
	}
}

// classifyGCS maps a GCS SDK error onto ErrTransient / ErrPermanent so the caller
// can decide whether to retry, preserving the original message. It mirrors the
// s3.go classify shape: permanent sentinels first, then a structural HTTP-status
// switch (404/403/400 permanent; 429/5xx transient), then transient by default
// for network/timeout/unknown. The status is read via *googleapi.Error and,
// failing that, via a structural HTTPCode() interface so non-test code never
// names a test-only error type.
func classifyGCS(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gcs.ErrObjectNotExist) || errors.Is(err, gcs.ErrBucketNotExist) {
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	code := 0
	var gae *googleapi.Error
	if errors.As(err, &gae) {
		code = gae.Code
	}
	if code == 0 {
		var hc interface{ HTTPCode() int }
		if errors.As(err, &hc) {
			code = hc.HTTPCode()
		}
	}
	switch {
	case code == http.StatusNotFound, code == http.StatusForbidden, code == http.StatusBadRequest:
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	case code == http.StatusTooManyRequests, code >= 500:
		return fmt.Errorf("%w: %v", ErrTransient, err)
	default:
		// Network/timeout/unknown -> transient by default (mirrors s3.go).
		return fmt.Errorf("%w: %v", ErrTransient, err)
	}
}
