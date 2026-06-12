// handlers_catalog.go implements the catalog half of the WS protocol against
// the SQLite-WAL Store (Plan A Task 27): ListFiles (with FileFilter predicates +
// pagination), GetFile (summary + topics + effective tags), and UpdateTags
// (set/unset override rows). The pure CatalogHandler methods return the proto
// responses (unit-testable, lifted from Plan A); connState's handle* methods
// (server.go) call them and route the result/error onto the priority channel.
//
// FLAT METADATA CONTRACT (slice decision, documented): the ListFiles flat
// metadata map per file = DERIVED entries (s3_key, size_bytes, message_count,
// topic_count, chunk_count, duration_ns, start_ns, end_ns — every key the
// GUI/Lua/live tests depend on) OVERLAID by tags_effective. Effective tags WIN
// on key collision (a user override named e.g. "size_bytes" shadows the derived
// value). This REVERSES the catalog-lite precedence (where derived/embedded
// filled only absent keys); it is intentional so user edits are authoritative in
// the client's Lua filter.
package ws

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"pj-cloud/server/internal/catalog"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// CatalogHandler answers the catalog RPCs against the SQLite store.
type CatalogHandler struct {
	Store *catalog.Store
}

// ListFiles applies the FileFilter predicates + pagination and returns the wire
// response including the flat metadata overlay map.
func (h *CatalogHandler) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	args := catalog.FilterArgs{Limit: int(req.GetLimit()), PageToken: req.GetPageToken()}
	if f := req.GetFilter(); f != nil {
		if tr := f.GetRecordedBetween(); tr != nil {
			args.RecordedBetween = &catalog.TimeWindow{StartNs: tr.GetStartNs(), EndNs: tr.GetEndNs()}
		}
		args.TopicsAnyOf = f.GetTopicsAnyOf()
		for _, t := range f.GetTagAll() {
			args.TagAll = append(args.TagAll, catalog.TagKV{Key: t.GetKey(), Value: t.GetValue()})
		}
		for _, t := range f.GetTagAny() {
			args.TagAny = append(args.TagAny, catalog.TagKV{Key: t.GetKey(), Value: t.GetValue()})
		}
	}
	files, next, err := catalog.FilterFiles(ctx, h.Store, args)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}
	resp := &pb.ListFilesResponse{
		NextPageToken: next,
		Metadata:      make(map[string]*pb.FlatMetadata, len(files)),
	}
	for _, f := range files {
		resp.Files = append(resp.Files, fileSummaryToProto(f))
		resp.Metadata[strconv.FormatUint(f.ID, 10)] = &pb.FlatMetadata{Entries: flatMetadata(f)}
	}
	return resp, nil
}

// GetFile returns the summary + topics + effective tags, mapping ErrFileNotFound.
func (h *CatalogHandler) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	rec, err := catalog.GetFile(ctx, h.Store, req.GetFileId())
	if err != nil {
		if errors.Is(err, catalog.ErrFileNotFound) {
			return nil, errFileNotFound{id: req.GetFileId()}
		}
		return nil, err
	}
	topics, err := catalog.ListTopicsForFile(ctx, h.Store, rec.ID)
	if err != nil {
		return nil, err
	}
	tags, err := catalog.EffectiveTags(ctx, h.Store, rec.ID)
	if err != nil {
		return nil, err
	}
	resp := &pb.GetFileResponse{
		Summary: &pb.FileSummary{
			Id:           rec.ID,
			S3Key:        rec.S3Key,
			SizeBytes:    uint64(rec.SizeBytes),
			Recorded:     &pb.TimeRange{StartNs: rec.StartTimeNs, EndNs: rec.EndTimeNs},
			TopicCount:   uint32(len(topics)),
			MessageCount: rec.MessageCount,
			Tags:         tagsToProto(tags),
		},
	}
	for _, t := range topics {
		resp.Topics = append(resp.Topics, &pb.TopicInfo{
			Name: t.Name, SchemaName: t.SchemaName, SchemaEncoding: t.SchemaEncoding,
			MessageCount: t.MessageCount,
		})
	}
	return resp, nil
}

// UpdateTags applies set_tags (SetOverride) and unset_keys (MaskEmbedded when an
// embedded tag exists with that key, else UnsetOverride — the proto's "delete
// override; or NULL-mask if embedded had it" rule), then returns the post-update
// effective view. The file must exist (NOT_FOUND otherwise).
func (h *CatalogHandler) UpdateTags(ctx context.Context, req *pb.UpdateTagsRequest) (*pb.UpdateTagsResponse, error) {
	if _, err := catalog.GetFile(ctx, h.Store, req.GetFileId()); err != nil {
		if errors.Is(err, catalog.ErrFileNotFound) {
			return nil, errFileNotFound{id: req.GetFileId()}
		}
		return nil, err
	}
	for _, t := range req.GetSetTags() {
		if err := catalog.SetOverride(ctx, h.Store, req.GetFileId(), t.GetKey(), t.GetValue()); err != nil {
			return nil, err
		}
	}
	for _, k := range req.GetUnsetKeys() {
		hasEmbedded, err := catalog.HasEmbeddedTag(ctx, h.Store, req.GetFileId(), k)
		if err != nil {
			return nil, err
		}
		if hasEmbedded {
			// NULL-mask so the embedded value does not re-surface.
			if err := catalog.MaskEmbedded(ctx, h.Store, req.GetFileId(), k); err != nil {
				return nil, err
			}
		} else {
			if err := catalog.UnsetOverride(ctx, h.Store, req.GetFileId(), k); err != nil {
				return nil, err
			}
		}
	}
	tags, err := catalog.EffectiveTags(ctx, h.Store, req.GetFileId())
	if err != nil {
		return nil, err
	}
	return &pb.UpdateTagsResponse{EffectiveTags: tagsToProto(tags)}, nil
}

// errFileNotFound carries the file id so the connState layer can map it to
// ERROR_NOT_FOUND with a useful message.
type errFileNotFound struct{ id uint64 }

func (e errFileNotFound) Error() string { return fmt.Sprintf("no file with id %d", e.id) }

func tagsToProto(tags []catalog.EffectiveTag) []*pb.Tag {
	out := make([]*pb.Tag, 0, len(tags))
	for _, t := range tags {
		out = append(out, &pb.Tag{Key: t.Key, Value: t.Value, IsOverride: t.IsOverride})
	}
	return out
}

// fileSummaryToProto builds the wire FileSummary (effective-tags view, with the
// real is_override) from a catalog.FileSummary.
func fileSummaryToProto(s catalog.FileSummary) *pb.FileSummary {
	return &pb.FileSummary{
		Id:           s.ID,
		S3Key:        s.S3Key,
		SizeBytes:    uint64(s.SizeBytes),
		Recorded:     &pb.TimeRange{StartNs: s.StartTimeNs, EndNs: s.EndTimeNs},
		TopicCount:   s.TopicCount,
		MessageCount: s.MessageCount,
		Tags:         tagsToProto(s.Tags),
	}
}

// flatMetadata builds the client-ingest flat map for one file: DERIVED entries
// overlaid by tags_effective (effective tags WIN on collision — see file header).
func flatMetadata(s catalog.FileSummary) map[string]string {
	out := map[string]string{
		"s3_key":        s.S3Key,
		"size_bytes":    strconv.FormatInt(s.SizeBytes, 10),
		"message_count": strconv.FormatUint(s.MessageCount, 10),
		"topic_count":   strconv.FormatUint(uint64(s.TopicCount), 10),
		"chunk_count":   strconv.FormatUint(uint64(s.ChunkCount), 10),
		"duration_ns":   strconv.FormatInt(s.EndTimeNs-s.StartTimeNs, 10),
		"start_ns":      strconv.FormatInt(s.StartTimeNs, 10),
		"end_ns":        strconv.FormatInt(s.EndTimeNs, 10),
	}
	// Effective tags overlay on top (override-wins on key collision).
	for k, v := range s.FlatMetadata() {
		out[k] = v
	}
	return out
}
