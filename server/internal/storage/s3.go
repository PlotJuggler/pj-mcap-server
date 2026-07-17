package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"pj-cloud/server/internal/config"
)

// s3Store is the S3/Minio BlobStore implementation.
type s3Store struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3 builds an S3-backed BlobStore. Endpoint override + path-style addressing
// make it work against Minio; static creds are used when supplied (dev), else the
// default AWS credential chain. This is the only place the cloud SDK is touched.
func NewS3(ctx context.Context, cfg config.S3Config) (BlobStore, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error
	loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	if cfg.AccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			// Custom endpoint (Minio / S3-compatible): path-style addressing is
			// required. Real AWS S3 (empty endpoint) keeps the modern
			// virtual-hosted default — path-style is deprecated there and is
			// rejected for buckets created in newer regions.
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})
	return &s3Store{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

func (s *s3Store) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	return s.getRange(ctx, key, "", off, length)
}

// GetRangeVersioned is the conditional-read arm of the seam: the ranged GET
// carries If-Match on the catalog's ETag, so an overwrite of the key mid-session
// fails the read (412 => ErrPermanent) instead of serving bytes from a different
// object version than the chunk index was built from. The catalog stores the
// ETag UNQUOTED (the Python builder strips the quotes); If-Match wants the
// quoted wire form, so it is re-quoted here.
func (s *s3Store) GetRangeVersioned(ctx context.Context, key, version string, off, length int64) ([]byte, error) {
	return s.getRange(ctx, key, version, off, length)
}

func (s *s3Store) getRange(ctx context.Context, key, ifMatchETag string, off, length int64) ([]byte, error) {
	var rng *string
	if length > 0 {
		rng = aws.String(fmt.Sprintf("bytes=%d-%d", off, off+length-1))
	} else if off > 0 {
		rng = aws.String(fmt.Sprintf("bytes=%d-", off))
	}
	var ifMatch *string
	if ifMatchETag != "" {
		if ifMatchETag[0] != '"' {
			ifMatchETag = `"` + ifMatchETag + `"`
		}
		ifMatch = aws.String(ifMatchETag)
	}
	var data []byte
	err := retryWith(ctx, func(ctx context.Context) error {
		out, gErr := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:  aws.String(s.bucket),
			Key:     aws.String(key),
			Range:   rng,
			IfMatch: ifMatch,
		})
		if gErr != nil {
			return classify(fmt.Errorf("get %q range %v: %w", key, rng, gErr))
		}
		defer out.Body.Close()
		body, rErr := io.ReadAll(out.Body)
		if rErr != nil {
			return classify(fmt.Errorf("read %q body: %w", key, rErr))
		}
		data = body
		return nil
	}, classify)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *s3Store) Head(ctx context.Context, key string) (ObjectInfo, error) {
	var info ObjectInfo
	err := retryWith(ctx, func(ctx context.Context) error {
		out, hErr := s.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if hErr != nil {
			return classify(fmt.Errorf("head %q: %w", key, hErr))
		}
		info = ObjectInfo{Key: key, ETag: deref(out.ETag)}
		if out.ContentLength != nil {
			info.Size = *out.ContentLength
		}
		if out.LastModified != nil {
			info.LastModifiedNs = out.LastModified.UnixNano()
		}
		return nil
	}, classify)
	if err != nil {
		return ObjectInfo{}, err
	}
	return info, nil
}

func (s *s3Store) List(ctx context.Context, prefix, token string) ([]ObjectInfo, string, error) {
	fullPrefix := s.prefix + prefix
	in := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	}
	if fullPrefix != "" {
		in.Prefix = aws.String(fullPrefix)
	}
	if token != "" {
		in.ContinuationToken = aws.String(token)
	}
	var objs []ObjectInfo
	var next string
	err := retryWith(ctx, func(ctx context.Context) error {
		out, lErr := s.client.ListObjectsV2(ctx, in)
		if lErr != nil {
			return classify(fmt.Errorf("list prefix %q: %w", fullPrefix, lErr))
		}
		objs = make([]ObjectInfo, 0, len(out.Contents))
		for _, o := range out.Contents {
			oi := ObjectInfo{Key: deref(o.Key), ETag: deref(o.ETag)}
			if o.Size != nil {
				oi.Size = *o.Size
			}
			if o.LastModified != nil {
				oi.LastModifiedNs = o.LastModified.UnixNano()
			}
			objs = append(objs, oi)
		}
		next = ""
		if out.IsTruncated != nil && *out.IsTruncated {
			next = deref(out.NextContinuationToken)
		}
		return nil
	}, classify)
	if err != nil {
		return nil, "", err
	}
	return objs, next, nil
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// classify maps an S3/smithy error onto ErrTransient / ErrPermanent so the
// caller can decide whether to retry, while preserving the original message.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var nf *types.NoSuchKey
	var nb *types.NoSuchBucket
	if errors.As(err, &nf) || errors.As(err, &nb) {
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch code := apiErr.ErrorCode(); {
		case code == "NoSuchKey" || code == "NotFound" || code == "NoSuchBucket" ||
			code == "AccessDenied" || code == "Forbidden" || strings.HasPrefix(code, "InvalidAccessKeyId") ||
			// PreconditionFailed = a GetRangeVersioned If-Match miss: the object
			// was overwritten (a new version). Retrying can NEVER succeed against
			// this ETag, so it is PERMANENT — retrying just delays the clean
			// "object changed mid-session" failure.
			code == "PreconditionFailed" || code == "412":
			return fmt.Errorf("%w: %v", ErrPermanent, err)
		default:
			return fmt.Errorf("%w: %v", ErrTransient, err)
		}
	}
	// Network/timeout/unknown -> transient by default.
	return fmt.Errorf("%w: %v", ErrTransient, err)
}
