package ws

import (
	"context"
	"path/filepath"
	"testing"

	"pj-cloud/server/internal/catalog"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

func openCatalogStore(t *testing.T) *catalog.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := catalog.Open(context.Background(), filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestCatalogHandler_ListFiles(t *testing.T) {
	store := openCatalogStore(t)
	id, _, _ := catalog.UpsertFile(context.Background(), store, catalog.FileRecord{
		S3Key: "a.mcap", S3ETag: "e", S3LastModified: 1, SizeBytes: 100,
		StartTimeNs: 1, EndTimeNs: 2, MessageCount: 10,
	})
	_ = catalog.ReplaceTopicsForFile(context.Background(), store, id, []catalog.TopicRecord{
		{Name: "/x", SchemaName: "X", SchemaEncoding: "ros2msg", MessageCount: 10},
	})

	h := &CatalogHandler{Store: store}
	resp, err := h.ListFiles(context.Background(), &pb.ListFilesRequest{Limit: 100})
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(resp.Files) != 1 {
		t.Fatalf("files: got %d want 1", len(resp.Files))
	}
	if resp.Files[0].S3Key != "a.mcap" {
		t.Errorf("s3_key: got %q", resp.Files[0].S3Key)
	}
	if resp.Files[0].TopicCount != 1 {
		t.Errorf("topic_count: got %d", resp.Files[0].TopicCount)
	}
}

func TestCatalogHandler_GetFileWithTopics(t *testing.T) {
	store := openCatalogStore(t)
	id, _, _ := catalog.UpsertFile(context.Background(), store, catalog.FileRecord{
		S3Key: "b.mcap", S3ETag: "e", S3LastModified: 1, SizeBytes: 100,
		StartTimeNs: 1, EndTimeNs: 2, MessageCount: 10,
	})
	_ = catalog.ReplaceTopicsForFile(context.Background(), store, id, []catalog.TopicRecord{
		{Name: "/imu/data", SchemaName: "I", SchemaEncoding: "ros2msg", MessageCount: 5},
		{Name: "/gps/fix", SchemaName: "G", SchemaEncoding: "ros2msg", MessageCount: 5},
	})

	h := &CatalogHandler{Store: store}
	resp, err := h.GetFile(context.Background(), &pb.GetFileRequest{FileId: id})
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if len(resp.Topics) != 2 {
		t.Errorf("topics: got %d want 2", len(resp.Topics))
	}
}

func TestCatalogHandler_GetFileNotFound(t *testing.T) {
	store := openCatalogStore(t)
	h := &CatalogHandler{Store: store}
	_, err := h.GetFile(context.Background(), &pb.GetFileRequest{FileId: 9999})
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	var nf errFileNotFound
	if !asErrFileNotFound(err, &nf) {
		t.Errorf("expected errFileNotFound, got %T: %v", err, err)
	}
}

func TestCatalogHandler_UpdateTags(t *testing.T) {
	store := openCatalogStore(t)
	id, _, _ := catalog.UpsertFile(context.Background(), store, catalog.FileRecord{
		S3Key: "c.mcap", S3ETag: "e", S3LastModified: 1, SizeBytes: 1,
		StartTimeNs: 1, EndTimeNs: 2,
	})

	h := &CatalogHandler{Store: store}
	resp, err := h.UpdateTags(context.Background(), &pb.UpdateTagsRequest{
		FileId:  id,
		SetTags: []*pb.Tag{{Key: "verified", Value: "yes"}},
	})
	if err != nil {
		t.Fatalf("UpdateTags: %v", err)
	}
	if len(resp.EffectiveTags) != 1 || resp.EffectiveTags[0].Key != "verified" {
		t.Errorf("effective tags: %+v", resp.EffectiveTags)
	}
	if !resp.EffectiveTags[0].IsOverride {
		t.Errorf("should be marked override")
	}
}

func TestCatalogHandler_UpdateTags_UnsetMasksEmbedded(t *testing.T) {
	store := openCatalogStore(t)
	id, _, _ := catalog.UpsertFile(context.Background(), store, catalog.FileRecord{
		S3Key: "d.mcap", S3ETag: "e", S3LastModified: 1, SizeBytes: 1, StartTimeNs: 1, EndTimeNs: 2,
	})
	// Embedded tag present; unset must NULL-mask it (not just delete an override
	// that does not exist), so it does NOT re-surface.
	_ = catalog.ReplaceEmbeddedTagsForFile(context.Background(), store, id,
		[]catalog.TagKV{{Key: "robot_id", Value: "r7"}})

	h := &CatalogHandler{Store: store}
	resp, err := h.UpdateTags(context.Background(), &pb.UpdateTagsRequest{
		FileId: id, UnsetKeys: []string{"robot_id"},
	})
	if err != nil {
		t.Fatalf("UpdateTags: %v", err)
	}
	if len(resp.EffectiveTags) != 0 {
		t.Errorf("embedded tag should be masked, got %+v", resp.EffectiveTags)
	}
}

func TestCatalogHandler_UpdateTagsNotFound(t *testing.T) {
	store := openCatalogStore(t)
	h := &CatalogHandler{Store: store}
	_, err := h.UpdateTags(context.Background(), &pb.UpdateTagsRequest{
		FileId: 4242, SetTags: []*pb.Tag{{Key: "x", Value: "y"}},
	})
	var nf errFileNotFound
	if !asErrFileNotFound(err, &nf) {
		t.Errorf("expected errFileNotFound, got %v", err)
	}
}

// TestFlatMetadata_EffectiveTagsOverlayDerived proves the slice contract: the
// flat metadata map = derived entries OVERLAID by tags_effective, with effective
// tags WINNING on key collision (a user override named "size_bytes" shadows the
// derived value), while derived keys with no tag collision remain.
func TestFlatMetadata_EffectiveTagsOverlayDerived(t *testing.T) {
	store := openCatalogStore(t)
	id, _, _ := catalog.UpsertFile(context.Background(), store, catalog.FileRecord{
		S3Key: "e.mcap", S3ETag: "e", S3LastModified: 1, SizeBytes: 100,
		StartTimeNs: 10, EndTimeNs: 30, MessageCount: 42, ChunkCount: 3,
	})
	_ = catalog.ReplaceTopicsForFile(context.Background(), store, id, []catalog.TopicRecord{
		{Name: "/x", SchemaName: "X", SchemaEncoding: "ros2msg", MessageCount: 42},
	})
	// User override collides with a derived key + adds a fresh one.
	_ = catalog.SetOverride(context.Background(), store, id, "size_bytes", "OVERRIDDEN")
	_ = catalog.SetOverride(context.Background(), store, id, "robot_id", "r7")

	h := &CatalogHandler{Store: store}
	resp, err := h.ListFiles(context.Background(), &pb.ListFilesRequest{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	flat := resp.GetMetadata()["1"].GetEntries() // first/only file id is 1
	if flat["size_bytes"] != "OVERRIDDEN" {
		t.Errorf("effective tag should win on collision: size_bytes=%q", flat["size_bytes"])
	}
	if flat["robot_id"] != "r7" {
		t.Errorf("override tag missing from flat map: %+v", flat)
	}
	// Derived keys without a tag collision survive (Lua/live-test contract).
	if flat["message_count"] != "42" || flat["chunk_count"] != "3" ||
		flat["duration_ns"] != "20" || flat["s3_key"] != "e.mcap" {
		t.Errorf("derived keys clobbered: %+v", flat)
	}
}

// asErrFileNotFound is a tiny errors.As wrapper (kept here so the test file has
// no extra imports beyond the ones above).
func asErrFileNotFound(err error, target *errFileNotFound) bool {
	for err != nil {
		if e, ok := err.(errFileNotFound); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
