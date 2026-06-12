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
//	seed -dir <fixtures>   # upload every *.mcap in <fixtures> (skips if non-empty)
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	hasData, err := bucketHasMcap(ctx, cl, bucket)
	if err != nil {
		return 1, fmt.Errorf("list %q (is Minio up?): %w", bucket, err)
	}

	if check {
		if hasData {
			fmt.Printf("seed: bucket %q already has .mcap data\n", bucket)
			return 3, nil
		}
		fmt.Printf("seed: bucket %q is empty\n", bucket)
		return 0, nil
	}

	if hasData && !force {
		fmt.Printf("seed: bucket %q already has .mcap data — skipping (use -force to override)\n", bucket)
		return 0, nil
	}
	if dir == "" {
		return 2, fmt.Errorf("-dir is required to upload (or pass -check to probe)")
	}

	files, _ := filepath.Glob(filepath.Join(dir, "*.mcap"))
	if len(files) == 0 {
		return 1, fmt.Errorf("no *.mcap fixtures found in %q", dir)
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return 1, err
		}
		key := filepath.Base(f)
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket, Key: &key, Body: bytes.NewReader(data),
		}); err != nil {
			return 1, fmt.Errorf("put %q: %w", key, err)
		}
		fmt.Printf("seed: uploaded %s (%d bytes)\n", key, len(data))
	}
	fmt.Printf("seed: %d fixture(s) -> s3://%s\n", len(files), bucket)
	return 0, nil
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
