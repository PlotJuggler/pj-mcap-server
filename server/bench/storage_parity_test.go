//go:build bench

// storage_parity_test.go — the Plan A Task 46a storage-parity microbench: raw
// BlobStore.GetRange throughput, S3/Minio vs GCS/fake-gcs, on ONE ground-truth
// object. It lives behind the SAME `bench` build tag as throughput_test.go, so it is
// invisible to `go test ./...` / `make smoke`; run it with `make bench-storage`
// (= `go test -tags=bench -run TestStorageP-arity ./bench/...`).
//
// WHAT IT MEASURES, AND WHAT IT DOES NOT
//
//	For each backend it issues N full-object GetRange reads of the same MCAP and
//	reports MB/s (object bytes * N / wall seconds). Both backends here are LOCAL and
//	co-resident with this process: Minio on :9000 and an in-memory fake-gcs on :4443.
//	So this is a LOOPBACK + CPU lower bound, NOT a network-link comparison.
//
//	The plan's "~10% parity" figure (GCS within ~10% of S3) is a REFERENCE-MACHINE
//	criterion — real GCS vs real S3 over a real link. This co-resident microbench
//	therefore asserts ONLY a generous, non-flapping FLOOR: gcs >= 25% of s3. It
//	reports both absolute numbers so a human can eyeball parity; it does not gate on
//	the 10% reference figure (that is an M-stage SOW item against real buckets).
//
// PRECONDITIONS (the bench SKIPS, never fails, when unmet — it is opt-in):
//   - Minio up on :9000 with the `recordings` bucket seeded (infra/minio).
//   - fake-gcs reachable: STORAGE_EMULATOR_HOST set (default localhost:4443 if the
//     emulator is up) AND the `recordings` bucket seeded (infra/fake-gcs + seed.sh).
//
// `make bench-storage` brings fake-gcs up + seeds it, sets the env, runs this, and
// tears the emulator down. Run standalone by exporting STORAGE_EMULATOR_HOST yourself.
package bench

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/storage"
)

const (
	// parityKey is the object both backends read. The largest corpus file makes the
	// per-read transfer dominate fixed per-call overhead.
	parityKey = "nissan_zala_90_country_road_2_0.mcap"
	// parityIterations is how many full-object GetRange reads to time per backend.
	parityIterations = 5
	// parityFloorFraction: fail if gcs MB/s < 25% of s3 MB/s on this shared box.
	parityFloorFraction = 0.25
)

// TestStorageParity is a Test (not a Benchmark) so it owns setup once and asserts
// the floor with a normal t.Fatalf; `go test -tags=bench -bench=.` still runs it.
func TestStorageParity(t *testing.T) {
	if !minioReachable() {
		t.Skipf("Minio not reachable at %s — storage-parity bench is opt-in (start infra/minio)", minioHealth)
	}
	emu := os.Getenv("STORAGE_EMULATOR_HOST")
	if emu == "" {
		t.Skip("STORAGE_EMULATOR_HOST not set — storage-parity bench is opt-in (start infra/fake-gcs + export it)")
	}
	if !fakeGCSReachable(emu) {
		t.Skipf("fake-gcs not reachable at %s — start infra/fake-gcs (docker compose up -d --wait) + seed.sh", emu)
	}

	ctx := context.Background()

	// S3/Minio store (same config the server's Default() uses).
	s3cfg := config.Default().Storage.S3
	s3, err := storage.NewS3(ctx, *s3cfg)
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}
	// GCS/fake-gcs store (emulator auto-selected from STORAGE_EMULATOR_HOST).
	gcs, err := storage.NewGCS(ctx, config.GCSConfig{Bucket: "recordings"})
	if err != nil {
		t.Fatalf("NewGCS: %v", err)
	}

	// Sanity: the object exists on both, and is the same size.
	s3info, err := s3.Head(ctx, parityKey)
	if err != nil {
		t.Fatalf("s3 Head %q: %v", parityKey, err)
	}
	gcsInfo, err := gcs.Head(ctx, parityKey)
	if err != nil {
		t.Fatalf("gcs Head %q: %v (is the bucket seeded? run infra/fake-gcs/seed.sh)", parityKey, err)
	}
	if s3info.Size != gcsInfo.Size {
		t.Fatalf("object size mismatch: s3=%d gcs=%d (different corpus on the two backends)", s3info.Size, gcsInfo.Size)
	}
	size := s3info.Size

	// Warm-up read on each (prime caches / decoder pools) — not timed.
	if _, err := s3.GetRange(ctx, parityKey, 0, size); err != nil {
		t.Fatalf("s3 warm-up GetRange: %v", err)
	}
	if _, err := gcs.GetRange(ctx, parityKey, 0, size); err != nil {
		t.Fatalf("gcs warm-up GetRange: %v", err)
	}

	s3MBps := timeFullReads(t, "s3", s3, parityKey, size)
	gcsMBps := timeFullReads(t, "gcs", gcs, parityKey, size)

	t.Logf("STORAGE PARITY (%d full-object reads of %s, %.1f MiB each):", parityIterations, parityKey, float64(size)/(1024*1024))
	t.Logf("  s3/minio   : %.1f MB/s", s3MBps)
	t.Logf("  gcs/fakegcs: %.1f MB/s", gcsMBps)
	if s3MBps > 0 {
		t.Logf("  gcs is %.0f%% of s3 (plan's ~10%% target is a REFERENCE-MACHINE criterion; here both are loopback+CPU-bound)", gcsMBps/s3MBps*100)
	}
	// Machine-readable lines the make target / harness can grep.
	fmt.Printf("STORAGE_PARITY_S3_MBPS=%.1f\n", s3MBps)
	fmt.Printf("STORAGE_PARITY_GCS_MBPS=%.1f\n", gcsMBps)

	floor := s3MBps * parityFloorFraction
	if gcsMBps < floor {
		t.Fatalf("GCS parity below floor: %.1f MB/s < %.1f MB/s (%.0f%% of s3 %.1f MB/s)",
			gcsMBps, floor, parityFloorFraction*100, s3MBps)
	}
}

// timeFullReads issues parityIterations full-object GetRange reads and returns MB/s
// (object bytes * N / wall seconds), asserting each read returns the full object.
func timeFullReads(t *testing.T, label string, bs storage.BlobStore, key string, size int64) float64 {
	t.Helper()
	start := time.Now()
	var total int64
	for i := 0; i < parityIterations; i++ {
		data, err := bs.GetRange(context.Background(), key, 0, size)
		if err != nil {
			t.Fatalf("%s GetRange iter %d: %v", label, i, err)
		}
		if int64(len(data)) != size {
			t.Fatalf("%s GetRange iter %d: got %d bytes, want %d", label, i, len(data), size)
		}
		total += int64(len(data))
	}
	elapsed := time.Since(start)
	return (float64(total) / (1024 * 1024)) / elapsed.Seconds()
}

// fakeGCSReachable probes the emulator's JSON bucket-list endpoint over plain HTTP.
func fakeGCSReachable(host string) bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + host + "/storage/v1/b")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
