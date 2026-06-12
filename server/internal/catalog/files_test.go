package catalog

import (
	"context"
	"testing"
	"time"
)

func TestUpsertFileNewRow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := FileRecord{
		S3Key:           "trip/run.mcap",
		S3ETag:          "etag-1",
		S3LastModified:  unixNs(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)),
		SizeBytes:       1 << 20,
		StartTimeNs:     1_000_000_000,
		EndTimeNs:       2_000_000_000,
		ChunkCount:      5,
		MessageCount:    100,
		HasMessageIndex: true,
	}
	id, created, err := UpsertFile(ctx, s, rec)
	if err != nil {
		t.Fatalf("UpsertFile: %v", err)
	}
	if !created {
		t.Error("expected created=true on first upsert")
	}
	if id == 0 {
		t.Error("expected non-zero id")
	}
}

func TestUpsertFileUnchanged(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := minimalRec("k", "etag-1", 100)
	id1, _, err := UpsertFile(ctx, s, rec)
	if err != nil {
		t.Fatal(err)
	}
	id2, created, err := UpsertFile(ctx, s, rec)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Error("expected created=false on unchanged upsert")
	}
	if id1 != id2 {
		t.Errorf("id changed: %d -> %d", id1, id2)
	}
}

func TestUpsertFileEtagChanged(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	id1, _, _ := UpsertFile(ctx, s, minimalRec("k", "etag-1", 100))
	rec2 := minimalRec("k", "etag-2", 200)
	id2, created, err := UpsertFile(ctx, s, rec2)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Error("created should be false for in-place replace")
	}
	if id1 != id2 {
		t.Errorf("id should stay stable across reindex: %d -> %d", id1, id2)
	}
	got, err := GetFile(ctx, s, id2)
	if err != nil {
		t.Fatal(err)
	}
	if got.S3ETag != "etag-2" {
		t.Errorf("etag not updated: got %q", got.S3ETag)
	}
}

func TestGetFileNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := GetFile(context.Background(), s, 9999)
	if err != ErrFileNotFound {
		t.Errorf("want ErrFileNotFound, got %v", err)
	}
}

func TestGetFileRoundTripsAllFields(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rec := FileRecord{
		S3Key: "round.mcap", S3ETag: "e", S3LastModified: 42, SizeBytes: 4096,
		StartTimeNs: 10, EndTimeNs: 20, ChunkCount: 3, MessageCount: 99, HasMessageIndex: true,
	}
	id, _, err := UpsertFile(ctx, s, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := GetFile(ctx, s, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.ChunkCount != 3 || got.MessageCount != 99 || !got.HasMessageIndex ||
		got.StartTimeNs != 10 || got.EndTimeNs != 20 || got.SizeBytes != 4096 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

func minimalRec(key, etag string, modNs int64) FileRecord {
	return FileRecord{
		S3Key:          key,
		S3ETag:         etag,
		S3LastModified: modNs,
		SizeBytes:      1,
		StartTimeNs:    1,
		EndTimeNs:      2,
	}
}

func unixNs(t time.Time) int64 { return t.UnixNano() }
