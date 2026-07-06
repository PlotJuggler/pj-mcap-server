package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
)

// TestCollectFixtures_Flat pins the pre-existing (non-Hive) behavior: a flat
// directory of *.mcap files yields key == filepath.Base(path) for every entry,
// matching the historical filepath.Glob("*.mcap") + filepath.Base(f) logic
// exactly (backward compatibility for every existing caller of `seed -dir`).
func TestCollectFixtures_Flat(t *testing.T) {
	dir := t.TempDir()
	names := []string{"a.mcap", "b.mcap"}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A non-.mcap file must never be collected.
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	keys, paths, err := collectFixtures(dir)
	if err != nil {
		t.Fatalf("collectFixtures: %v", err)
	}
	if len(keys) != len(paths) || len(keys) != 2 {
		t.Fatalf("got %d keys / %d paths, want 2/2 (keys=%v)", len(keys), len(paths), keys)
	}
	sort.Strings(keys)
	if keys[0] != "a.mcap" || keys[1] != "b.mcap" {
		t.Fatalf("flat keys = %v, want [a.mcap b.mcap]", keys)
	}
}

// TestCollectFixtures_Hive proves a Hive-partitioned tree (as produced by
// `gen-ci-fixtures -hive`) yields the FULL partitioned relative path as the S3
// key, POSIX-separated — the load-bearing fix for the catalog-migration smoke
// harness (a flattened basename key would not round-trip through
// keyparse.parse_hive_key on the Python builder side).
func TestCollectFixtures_Hive(t *testing.T) {
	dir := t.TempDir()
	rel := filepath.Join("customer=test", "customer_site=lab", "robot=r1", "source=synthetic", "date=2026-06-22")
	if err := os.MkdirAll(filepath.Join(dir, rel), 0o755); err != nil {
		t.Fatal(err)
	}
	full := filepath.Join(dir, rel, "ci_synth_a.mcap")
	if err := os.WriteFile(full, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	keys, paths, err := collectFixtures(dir)
	if err != nil {
		t.Fatalf("collectFixtures: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("got %d keys, want 1 (keys=%v)", len(keys), keys)
	}
	wantKey := "customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-22/ci_synth_a.mcap"
	if keys[0] != wantKey {
		t.Errorf("key = %q, want %q", keys[0], wantKey)
	}
	if paths[0] != full {
		t.Errorf("path = %q, want %q", paths[0], full)
	}
}

// TestCollectFixtures_EmptyDir must not error on a directory with no *.mcap
// files — the caller (run()) is responsible for turning zero results into its
// own "no fixtures found" error, not this pure collector.
func TestCollectFixtures_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	keys, paths, err := collectFixtures(dir)
	if err != nil {
		t.Fatalf("collectFixtures: %v", err)
	}
	if len(keys) != 0 || len(paths) != 0 {
		t.Fatalf("got %d keys / %d paths, want 0/0", len(keys), len(paths))
	}
}

// fakeS3Server simulates the minimal S3 API surface run() exercises:
// CreateBucket (PUT /bucket), ListObjectsV2 (GET /bucket?list-type=2...), and
// PutObject (PUT /bucket/key). Existence is tracked as MUTABLE state seeded
// from bucketExists — a successful CreateBucket flips it true, so the
// upload path's "create then list" sequence sees a consistent bucket across
// both calls. If the bucket does not (yet) exist, ListObjectsV2 always
// answers 404 NoSuchBucket (regardless of mcapKeys). createCalls, if
// non-nil, counts every CreateBucket request received so callers can assert
// -check never triggers bucket creation.
func fakeS3Server(bucket string, bucketExists bool, mcapKeys []string, createCalls *int32) *httptest.Server {
	var exists int32
	if bucketExists {
		exists = 1
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		switch {
		case r.Method == http.MethodPut && path == bucket:
			if createCalls != nil {
				atomic.AddInt32(createCalls, 1)
			}
			atomic.StoreInt32(&exists, 1)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && path == bucket && strings.Contains(r.URL.RawQuery, "list-type=2"):
			if atomic.LoadInt32(&exists) == 0 {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+
					`<Error><Code>NoSuchBucket</Code><Message>no such bucket</Message>`+
					`<BucketName>%s</BucketName><RequestId>1</RequestId><HostId>1</HostId></Error>`, bucket)
				return
			}
			var contents strings.Builder
			for _, k := range mcapKeys {
				fmt.Fprintf(&contents, `<Contents><Key>%s</Key><Size>1</Size></Contents>`, k)
			}
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+
				`<ListBucketResult><Name>%s</Name><KeyCount>%d</KeyCount>%s</ListBucketResult>`,
				bucket, len(mcapKeys), contents.String())
		case r.Method == http.MethodPut && strings.HasPrefix(path, bucket+"/"):
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// TestCheckMode_MissingBucket_NeverCreates is the S4 fix: -check on a bucket
// that does not exist yet must map to "empty/seed-needed" (exit 0) — the
// SAME contract run.sh already relies on for an empty bucket — WITHOUT ever
// issuing a CreateBucket call. -check must stay probe-only; auto-creation is
// reserved for the actual upload path.
func TestCheckMode_MissingBucket_NeverCreates(t *testing.T) {
	var createCalls int32
	srv := fakeS3Server("missing-bucket", false, nil, &createCalls)
	defer srv.Close()

	code, err := run("", true, "missing-bucket", srv.URL, "test", "test", "us-east-1", false)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if code != 0 {
		t.Fatalf("code = %d, want 0 (missing bucket == seed-needed)", code)
	}
	if got := atomic.LoadInt32(&createCalls); got != 0 {
		t.Fatalf("CreateBucket called %d time(s) during -check — must be probe-only", got)
	}
}

// TestCheckMode_EmptyBucket_ExitZero_NeverCreates: an existing-but-empty
// bucket also means "seed-needed" (exit 0), and -check still must not create
// anything (the bucket already exists here, so this mostly guards against a
// regression that makes -check call CreateBucket unconditionally).
func TestCheckMode_EmptyBucket_ExitZero_NeverCreates(t *testing.T) {
	var createCalls int32
	srv := fakeS3Server("recordings", true, nil, &createCalls)
	defer srv.Close()

	code, err := run("", true, "recordings", srv.URL, "test", "test", "us-east-1", false)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if code != 0 {
		t.Fatalf("code = %d, want 0 (empty bucket == seed-needed)", code)
	}
	if got := atomic.LoadInt32(&createCalls); got != 0 {
		t.Fatalf("CreateBucket called %d time(s) during -check — must be probe-only", got)
	}
}

// TestCheckMode_HasData_ExitThree_NeverCreates: a bucket already holding
// .mcap data means "skip" (exit 3), again without any CreateBucket call.
func TestCheckMode_HasData_ExitThree_NeverCreates(t *testing.T) {
	var createCalls int32
	srv := fakeS3Server("recordings", true, []string{"a.mcap"}, &createCalls)
	defer srv.Close()

	code, err := run("", true, "recordings", srv.URL, "test", "test", "us-east-1", false)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if code != 3 {
		t.Fatalf("code = %d, want 3 (bucket already has .mcap data)", code)
	}
	if got := atomic.LoadInt32(&createCalls); got != 0 {
		t.Fatalf("CreateBucket called %d time(s) during -check — must be probe-only", got)
	}
}

// TestUploadPath_CreatesMissingBucket pins the OTHER half of the S4 contract:
// bucket auto-creation is preserved on the real (non-check) seeding path —
// only -check became probe-only, not the uploader.
func TestUploadPath_CreatesMissingBucket(t *testing.T) {
	var createCalls int32
	srv := fakeS3Server("smoke-hive", false, nil, &createCalls)
	defer srv.Close()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.mcap"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	code, err := run(dir, false, "smoke-hive", srv.URL, "test", "test", "us-east-1", false)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if code != 0 {
		t.Fatalf("code = %d, want 0 (upload success)", code)
	}
	if got := atomic.LoadInt32(&createCalls); got != 1 {
		t.Fatalf("CreateBucket called %d time(s) on the upload path, want exactly 1", got)
	}
}
