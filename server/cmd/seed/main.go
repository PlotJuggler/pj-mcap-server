// Command seed uploads synthetic MCAP fixtures into a (local Minio) bucket so a
// fresh checkout has something to browse — the quickstart's data step. It is
// IDEMPOTENT and SAFE: if the bucket already holds any .mcap object it does
// nothing (your own corpus is never clobbered) unless -force is given. Defaults
// target the dev Minio from infra/minio (path-style, admin/password123, bucket
// "recordings"), matching config.Default().
//
// Modes:
//
//	seed -check            # exit 0 if the bucket is EMPTY (i.e. seeding is needed),
//	                       # exit 3 if it already has .mcap data (skip). For run.sh.
//	seed -dir <fixtures>   # upload every *.mcap under <fixtures> (skips if non-empty)
//
// -dir walks RECURSIVELY and uploads each *.mcap with an S3 key equal to its
// path relative to <fixtures> (POSIX-separated) — so a Hive-partitioned tree
// (customer=.../customer_site=.../robot=.../source=.../date=.../name.mcap, as
// produced by `gen-ci-fixtures -hive`) uploads with its full partitioned key
// intact, not flattened to its basename. A flat (non-Hive) fixture directory
// behaves exactly as before: relative-path == basename. The target bucket is
// created if it does not already exist (idempotent; errors mapping to
// "already own it" / "already exists" are swallowed) — the catalog-migration
// smoke harness targets a fresh dedicated bucket that infra/minio's
// docker-compose init never creates.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func main() {
	dir := flag.String("dir", "", "directory of *.mcap fixtures to upload")
	check := flag.Bool("check", false, "probe only: exit 0 if the bucket is empty (seed needed), 3 if it already has .mcap data")
	bucket := flag.String("bucket", "recordings", "target bucket")
	endpoint := flag.String("endpoint", "http://localhost:9000", "S3 endpoint (empty = real AWS)")
	accessKey := flag.String("access-key", "admin", "access key (empty = AWS default credential chain)")
	secretKey := flag.String("secret-key", "password123", "secret key")
	region := flag.String("region", "us-east-1", "region")
	force := flag.Bool("force", false, "upload even if the bucket already has .mcap objects")
	flag.Parse()

	code, err := run(*dir, *check, *bucket, *endpoint, *accessKey, *secretKey, *region, *force)
	if err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(dir string, check bool, bucket, endpoint, accessKey, secretKey, region string, force bool) (int, error) {
	ctx := context.Background()

	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if accessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return 1, fmt.Errorf("aws config: %w", err)
	}
	cl := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" { // Minio / S3-compatible needs path-style (see storage/s3.go)
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	if check {
		// Probe-only: NEVER create the bucket here (that would make -check
		// itself a mutating call). A bucket that does not exist yet is
		// indistinguishable, for -check's purposes, from an empty one —
		// both mean "seeding is needed" (exit 0). Bucket auto-creation only
		// happens below, on the actual upload path.
		hasData, err := bucketHasMcap(ctx, cl, bucket)
		if err != nil {
			if isNoSuchBucket(err) {
				fmt.Printf("seed: bucket %q does not exist yet\n", bucket)
				return 0, nil
			}
			return 1, fmt.Errorf("list %q (is Minio up?): %w", bucket, err)
		}
		if hasData {
			fmt.Printf("seed: bucket %q already has .mcap data\n", bucket)
			return 3, nil
		}
		fmt.Printf("seed: bucket %q is empty\n", bucket)
		return 0, nil
	}

	// Idempotent bucket creation: the smoke harness targets a fresh dedicated
	// bucket (e.g. "smoke-hive") that infra/minio's compose init never creates,
	// unlike "recordings". A bucket that already exists (the common case, incl.
	// every existing caller) is a silent no-op — "already own it" / "already
	// exists" are the only errors swallowed; anything else propagates. This
	// only runs on the actual seeding path — never for -check.
	if err := ensureBucket(ctx, cl, bucket, region); err != nil {
		return 1, fmt.Errorf("ensure bucket %q exists: %w", bucket, err)
	}

	hasData, err := bucketHasMcap(ctx, cl, bucket)
	if err != nil {
		return 1, fmt.Errorf("list %q (is Minio up?): %w", bucket, err)
	}

	if hasData && !force {
		fmt.Printf("seed: bucket %q already has .mcap data — skipping (use -force to override)\n", bucket)
		return 0, nil
	}
	if dir == "" {
		return 2, fmt.Errorf("-dir is required to upload (or pass -check to probe)")
	}

	keys, paths, err := collectFixtures(dir)
	if err != nil {
		return 1, fmt.Errorf("walk %q: %w", dir, err)
	}
	if len(keys) == 0 {
		return 1, fmt.Errorf("no *.mcap fixtures found under %q", dir)
	}
	for i, key := range keys {
		data, err := os.ReadFile(paths[i])
		if err != nil {
			return 1, err
		}
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket, Key: &key, Body: bytes.NewReader(data),
		}); err != nil {
			return 1, fmt.Errorf("put %q: %w", key, err)
		}
		fmt.Printf("seed: uploaded %s (%d bytes)\n", key, len(data))
	}
	fmt.Printf("seed: %d fixture(s) -> s3://%s\n", len(keys), bucket)
	return 0, nil
}

// collectFixtures walks dir RECURSIVELY and returns, in matching sortstable
// order, the S3 key (path relative to dir, POSIX-separated) and absolute
// on-disk path of every *.mcap file found. A flat directory (no
// subdirectories) yields key == filepath.Base(path) for every entry — exactly
// the prior non-recursive behavior — so existing (non-Hive) callers are
// unaffected; a Hive-partitioned tree (gen-ci-fixtures -hive) yields the full
// partitioned key (e.g. "customer=test/customer_site=lab/robot=r1/source=
// synthetic/date=2026-06-22/ci_synth_a.mcap").
func collectFixtures(dir string) (keys []string, paths []string, err error) {
	walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".mcap") {
			return nil
		}
		rel, relErr := filepath.Rel(dir, path)
		if relErr != nil {
			return relErr
		}
		keys = append(keys, filepath.ToSlash(rel))
		paths = append(paths, path)
		return nil
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	return keys, paths, nil
}

// ensureBucket creates bucket if it does not already exist. "Already own it" /
// "already exists" responses (from a concurrent creator or a prior run) are
// treated as success, not failure — this call is meant to be idempotent.
func ensureBucket(ctx context.Context, cl *s3.Client, bucket, region string) error {
	input := &s3.CreateBucketInput{Bucket: &bucket}
	// us-east-1 is S3's implicit default region and must NOT be sent as an
	// explicit LocationConstraint (the API rejects that combination); every
	// other region needs it set explicitly. Minio accepts either form.
	if region != "" && region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	_, err := cl.CreateBucket(ctx, input)
	if err == nil {
		return nil
	}
	var alreadyOwned *types.BucketAlreadyOwnedByYou
	var alreadyExists *types.BucketAlreadyExists
	if errors.As(err, &alreadyOwned) || errors.As(err, &alreadyExists) {
		return nil
	}
	// Some S3-compatible backends (older Minio releases) return a generic API
	// error code instead of the typed exceptions above — match on code too.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return nil
		}
	}
	return err
}

// isNoSuchBucket reports whether err is S3's "the bucket does not exist"
// error, matching both the typed exception and the generic API-error-code
// fallback some S3-compatible backends return (mirrors ensureBucket's
// already-exists matching below).
func isNoSuchBucket(err error) bool {
	var noSuchBucket *types.NoSuchBucket
	if errors.As(err, &noSuchBucket) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchBucket" {
		return true
	}
	return false
}

// bucketHasMcap reports whether the bucket already contains any .mcap object.
func bucketHasMcap(ctx context.Context, cl *s3.Client, bucket string) (bool, error) {
	out, err := cl.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket})
	if err != nil {
		return false, err
	}
	for _, o := range out.Contents {
		if o.Key != nil && strings.HasSuffix(*o.Key, ".mcap") {
			return true, nil
		}
	}
	return false, nil
}
