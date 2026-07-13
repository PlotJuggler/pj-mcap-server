package storage

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"

	"pj-cloud/server/internal/config"
)

// TestGCS_ObjectInfoUsesGeneration pins the change-detect mapping: ObjectInfo's
// ETag is the GCS Generation rendered as a decimal string (NOT the MD5/CRC32C
// ETag, which is unstable for composed/rewritten objects), Size is Size, and
// LastModifiedNs comes from Updated. CRC32C/MD5Hash must be ignored.
func TestGCS_ObjectInfoUsesGeneration(t *testing.T) {
	attrs := &gcs.ObjectAttrs{
		Name:       "run.mcap",
		Size:       4096,
		Generation: 1717430400000123, // monotonic int64 — the change-detect identity
		CRC32C:     0xdeadbeef,       // must NOT be used as the ETag
		MD5:        []byte{1, 2, 3, 4},
		Updated:    time.Unix(1717430400, 0),
	}
	info := objectInfoFromAttrs(attrs)
	if info.Key != "run.mcap" {
		t.Errorf("key: %q", info.Key)
	}
	if info.ETag != "1717430400000123" {
		t.Errorf("ETag must be the Generation as decimal, got %q", info.ETag)
	}
	if info.Size != 4096 {
		t.Errorf("size: %d", info.Size)
	}
	if info.LastModifiedNs != time.Unix(1717430400, 0).UnixNano() {
		t.Errorf("last_modified must come from Updated, got %d", info.LastModifiedNs)
	}
}

// TestGCS_ClassifyErrors checks the permanent/transient split:
//   - ErrObjectNotExist / ErrBucketNotExist -> permanent
//   - 403 / 404 / 400 -> permanent
//   - 503 / 429 / 5xx / network -> transient
//
// The 403/404/400/503 cases are driven through a structural HTTPCode() stand-in
// so non-test code never names a test-only type (classifyGCS reaches it via the
// errors.As interface branch, exactly like the real *googleapi.Error).
func TestGCS_ClassifyErrors(t *testing.T) {
	if classifyGCS(nil) != nil {
		t.Error("nil must classify to nil")
	}
	if !errors.Is(classifyGCS(gcs.ErrObjectNotExist), ErrPermanent) {
		t.Error("ErrObjectNotExist should be permanent")
	}
	if !errors.Is(classifyGCS(gcs.ErrBucketNotExist), ErrPermanent) {
		t.Error("ErrBucketNotExist should be permanent")
	}
	if !errors.Is(classifyGCS(&googleAPIErr{code: 404}), ErrPermanent) {
		t.Error("404 should be permanent")
	}
	if !errors.Is(classifyGCS(&googleAPIErr{code: 403}), ErrPermanent) {
		t.Error("403 should be permanent")
	}
	if !errors.Is(classifyGCS(&googleAPIErr{code: 400}), ErrPermanent) {
		t.Error("400 should be permanent")
	}
	if !errors.Is(classifyGCS(&googleAPIErr{code: 503}), ErrTransient) {
		t.Error("503 should be transient")
	}
	if !errors.Is(classifyGCS(&googleAPIErr{code: 429}), ErrTransient) {
		t.Error("429 should be transient")
	}
	if !errors.Is(classifyGCS(errors.New("dial tcp: connection refused")), ErrTransient) {
		t.Error("an unclassified network error should default to transient")
	}
}

// googleAPIErr is a minimal stand-in for an HTTP-status-bearing SDK error. It
// implements HTTPCode() so classifyGCS picks it up via its structural-interface
// branch — classifyGCS (non-test code) never names this test-only type.
type googleAPIErr struct{ code int }

func (e *googleAPIErr) Error() string { return "google api error" }
func (e *googleAPIErr) HTTPCode() int { return e.code }

// TestGCS_EmulatorRoundTrip exercises GetRange/Head/List + the rangeReaderAt
// footer read against a real fake-gcs-server. It is HERMETIC by default: it
// SKIPS unless STORAGE_EMULATOR_HOST is set (the matrix leg sets it). Plain
// `go test` never needs docker.
func TestGCS_EmulatorRoundTrip(t *testing.T) {
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("STORAGE_EMULATOR_HOST not set; skipping GCS emulator round-trip")
	}
	bucket := os.Getenv("PJ_CLOUD_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("PJ_CLOUD_GCS_TEST_BUCKET not set; skipping GCS emulator round-trip")
	}
	// The matrix leg pre-seeds at least one object; assert List/Head/GetRange work
	// against it. This test is deliberately data-light — the matrix leg owns the
	// full corpus round-trip; here we only prove the seam boots against fake-gcs.
	ctx := context.Background()
	store, err := NewGCS(ctx, config.GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("newGCS: %v", err)
	}
	objs, _, err := store.List(ctx, "", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(objs) == 0 {
		t.Skip("emulator bucket is empty; matrix leg seeds it")
	}
	key := objs[0].Key
	info, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head %q: %v", key, err)
	}
	if info.Size <= 0 {
		t.Fatalf("Head %q: non-positive size %d", key, info.Size)
	}
	// Footer-style ranged read through the rangeReaderAt adapter.
	rd := ReaderAt(ctx, store, key, info.Size)
	if _, err := rd.Seek(-8, 2); err != nil {
		t.Fatalf("Seek end-8: %v", err)
	}
	buf := make([]byte, 8)
	if _, err := rd.Read(buf); err != nil {
		t.Fatalf("Read footer: %v", err)
	}
}
