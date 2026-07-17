// handlers_catalog.go implements the catalog half of the WS protocol against
// the read-only SQLite-WAL Store (Plan A Task 27): ListFiles (with FileFilter
// predicates + pagination), GetFile (summary + topics + effective tags), and
// GetVocabulary. The pure CatalogHandler methods return the proto responses
// (unit-testable, lifted from Plan A); connState's handle* methods (server.go)
// call them and route the result/error onto the priority channel.
//
// UpdateTags has NO local implementation here (catalog-migration §2.6): the
// catalog is always read-only, so a tag edit can only ever be forwarded to the
// Python builder's tag-edit IPC endpoint — see connState.handleUpdateTags /
// handleUpdateTagsForwarded in server.go.
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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"

	"google.golang.org/protobuf/proto"

	"pj-cloud/server/internal/catalog"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// CatalogHandler answers the catalog RPCs against the SQLite store.
type CatalogHandler struct {
	Store *catalog.Store
}

// ── generation-bound pagination cursors ──────────────────────────────────────
//
// The wire page token is NOT a bare rowid: rowids renumber across a full
// builder rebuild (CATALOG_CONTRACT.md §7), so continuing a listing from an
// old rowid in a renumbered id-space would silently skip or repeat files. The
// cursor binds the position to (a) the catalog GENERATION it was minted from
// and (b) a hash of the filter it belongs to, and the server rejects any
// mismatch: a foreign generation is ERROR_STALE_CATALOG (retryable — re-list
// from page one), a foreign filter or malformed token is
// ERROR_INVALID_REQUEST (a client bug).
//
// Encoding (versioned, explicit — never gob): base64url of
//   [1B version=1][1B genLen][gen][8B BE afterID][8B BE filterHash]

const listCursorVersion = 1

type listCursor struct {
	gen        []byte
	afterID    uint64
	filterHash uint64
}

func encodeListCursor(c listCursor) string {
	buf := make([]byte, 0, 2+len(c.gen)+16)
	buf = append(buf, listCursorVersion, byte(len(c.gen)))
	buf = append(buf, c.gen...)
	buf = binary.BigEndian.AppendUint64(buf, c.afterID)
	buf = binary.BigEndian.AppendUint64(buf, c.filterHash)
	return base64.RawURLEncoding.EncodeToString(buf)
}

func decodeListCursor(tok string) (listCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return listCursor{}, fmt.Errorf("not base64url: %w", err)
	}
	if len(raw) < 2 {
		return listCursor{}, errors.New("truncated")
	}
	if raw[0] != listCursorVersion {
		return listCursor{}, fmt.Errorf("unknown cursor version %d", raw[0])
	}
	genLen := int(raw[1])
	if len(raw) != 2+genLen+16 {
		return listCursor{}, errors.New("wrong length")
	}
	return listCursor{
		gen:        append([]byte(nil), raw[2:2+genLen]...),
		afterID:    binary.BigEndian.Uint64(raw[2+genLen:]),
		filterHash: binary.BigEndian.Uint64(raw[2+genLen+8:]),
	}, nil
}

// filterHash fingerprints the request's FileFilter so a cursor can never be
// replayed against a DIFFERENT query (which would produce a silently wrong
// page). Deterministic proto marshal + FNV-1a; stable within one server build,
// which is all a generation-bound cursor needs (a restart changes the epoch and
// invalidates every outstanding cursor anyway).
func filterHash(f *pb.FileFilter) uint64 {
	h := fnv.New64a()
	if f != nil {
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(f)
		if err == nil {
			_, _ = h.Write(raw)
		}
	}
	return h.Sum64()
}

// filterHasDimensionIDs reports whether the filter carries any dimension id —
// the generation-scoped handles picked from a GetVocabulary response.
func filterHasDimensionIDs(f *pb.FileFilter) bool {
	return f != nil && (f.CustomerId != nil || f.SiteId != nil || f.RobotId != nil || f.SourceId != nil)
}

// ListFiles applies the FileFilter predicates + generation-checked pagination
// and returns the wire response including the flat metadata overlay map and the
// serving generation. The WHOLE request — generation checks, the filter query,
// tag attach, and the next-cursor mint — runs against ONE leased snapshot, so a
// concurrent rebuild can neither mix generations inside the response nor close
// the handle mid-flight.
func (h *CatalogHandler) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	lease := h.Store.Acquire()
	defer lease.Release()
	gen := lease.Generation()

	fhash := filterHash(req.GetFilter())
	args := catalog.FilterArgs{Limit: int(req.GetLimit())}
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
		// Dimension selection (proto3 optional => presence via the pointer field).
		args.CustomerID = f.CustomerId
		args.SiteID = f.SiteId
		args.RobotID = f.RobotId
		args.SourceID = f.SourceId
	}

	// Dimension ids are generation-scoped: a first-page request carrying one
	// MUST also carry the generation the ids came from, or the server cannot
	// detect a stale id silently selecting a renumbered dimension.
	expected := req.GetExpectedCatalogGeneration()
	if filterHasDimensionIDs(req.GetFilter()) && len(expected) == 0 && req.GetPageToken() == "" {
		return nil, errGenerationRequired{}
	}
	if tok := req.GetPageToken(); tok != "" {
		cur, err := decodeListCursor(tok)
		if err != nil {
			return nil, errBadPageToken{reason: err.Error()}
		}
		if len(expected) != 0 && !bytes.Equal(expected, cur.gen) {
			// The client contradicts itself: the cursor and the explicit
			// expectation name different generations. A client bug, not
			// staleness.
			return nil, errBadPageToken{reason: "page_token and expected_catalog_generation disagree"}
		}
		if cur.filterHash != fhash {
			return nil, errBadPageToken{reason: "page_token was minted for a different filter"}
		}
		if !bytes.Equal(cur.gen, gen) {
			return nil, errStaleCatalog{}
		}
		args.AfterID = cur.afterID
	} else if len(expected) != 0 && !bytes.Equal(expected, gen) {
		return nil, errStaleCatalog{}
	}

	files, more, err := catalog.FilterFilesDB(ctx, lease.DB(), args)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}
	resp := &pb.ListFilesResponse{
		Metadata:          make(map[string]*pb.FlatMetadata, len(files)),
		CatalogGeneration: gen,
	}
	if more && len(files) > 0 {
		resp.NextPageToken = encodeListCursor(listCursor{
			gen:        gen,
			afterID:    files[len(files)-1].ID,
			filterHash: fhash,
		})
	}
	for _, f := range files {
		resp.Files = append(resp.Files, fileSummaryToProto(f))
		resp.Metadata[strconv.FormatUint(f.ID, 10)] = &pb.FlatMetadata{Entries: flatMetadata(f)}
	}
	return resp, nil
}

// GetFile returns the summary + topics + effective tags, mapping ErrFileNotFound.
//
// Uses the compound catalog.GetFileDetail (B1 — catalog-migration §6.2a
// review) rather than three separate GetFile/ListTopicsForFile/EffectiveTags
// calls: each of those independently re-fetches Store.DB(), so a
// ReopenIfSwapped landing between them could pair one generation's file
// summary with another generation's topics/tags. GetFileDetail pins the
// handle once and runs all three phases against it.
//
// Addressing: when req.s3_key is PRESENT (proto3 optional), the file is
// resolved by the STABLE object key and file_id is IGNORED — file ids are
// generation-scoped handles that renumber across external-builder rebuilds,
// so a client-held id can silently name the wrong file minutes later; the
// key (from FileSummary.s3_key) cannot. There is deliberately NO id/key
// mismatch validation (surviving a stale id is the point). Present-but-empty
// is an error (never a silent fallback to file_id); absent = the unchanged
// legacy id path. Key resolve + detail read share ONE pinned handle
// (GetFileDetailByKey), keeping the whole response generation-consistent.
func (h *CatalogHandler) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	var (
		rec    catalog.FileRecord
		topics []catalog.TopicRecord
		tags   []catalog.EffectiveTag
		err    error
	)
	if req.S3Key != nil {
		key := req.GetS3Key()
		if key == "" {
			return nil, errEmptyS3Key{}
		}
		rec, topics, tags, err = catalog.GetFileDetailByKey(ctx, h.Store, key)
		if err != nil {
			if errors.Is(err, catalog.ErrFileNotFound) {
				return nil, errFileNotFoundByKey{key: key}
			}
			return nil, err
		}
	} else {
		rec, topics, tags, err = catalog.GetFileDetail(ctx, h.Store, req.GetFileId())
		if err != nil {
			if errors.Is(err, catalog.ErrFileNotFound) {
				return nil, errFileNotFound{id: req.GetFileId()}
			}
			return nil, err
		}
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

// GetVocabulary returns the filter vocabulary (catalog-vocabulary-rpc.md): the
// strict customer->site->robot tree, the flat source dimension, and the tag facets.
// Built from the dimension tables against ONE leased snapshot, stamped with that
// snapshot's generation — the dimension ids are generation-scoped handles, only
// meaningful together with the token (echoed back via
// ListFilesRequest.expected_catalog_generation).
func (h *CatalogHandler) GetVocabulary(ctx context.Context, _ *pb.GetVocabularyRequest) (*pb.GetVocabularyResponse, error) {
	lease := h.Store.Acquire()
	defer lease.Release()
	v, err := catalog.GetVocabularyDB(ctx, lease.DB())
	if err != nil {
		return nil, fmt.Errorf("vocabulary: %w", err)
	}
	resp := &pb.GetVocabularyResponse{CatalogGeneration: lease.Generation()}
	for _, c := range v.Customers {
		pc := &pb.DimCustomer{Id: c.ID, Name: c.Name, FileCount: c.FileCount}
		for _, st := range c.Sites {
			ps := &pb.DimSite{Id: st.ID, Name: st.Name, FileCount: st.FileCount}
			for _, r := range st.Robots {
				ps.Robots = append(ps.Robots, &pb.DimRobot{Id: r.ID, Name: r.Name, FileCount: r.FileCount})
			}
			pc.Sites = append(pc.Sites, ps)
		}
		resp.Customers = append(resp.Customers, pc)
	}
	for _, src := range v.Sources {
		resp.Sources = append(resp.Sources, &pb.DimSource{Id: src.ID, Name: src.Name, FileCount: src.FileCount})
	}
	for _, f := range v.Tags {
		pf := &pb.TagFacet{Key: f.Key}
		for _, val := range f.Values {
			pf.Values = append(pf.Values, &pb.TagFacetValue{Value: val.Value, FileCount: val.FileCount})
		}
		resp.Tags = append(resp.Tags, pf)
	}
	return resp, nil
}

// errFileNotFound carries the file id so the connState layer can map it to
// ERROR_NOT_FOUND with a useful message.
type errFileNotFound struct{ id uint64 }

func (e errFileNotFound) Error() string { return fmt.Sprintf("no file with id %d", e.id) }

// errFileNotFoundByKey is errFileNotFound's key-addressed sibling: it carries
// the s3_key so the connState layer can map it to ERROR_NOT_FOUND with the
// key the client actually asked for.
type errFileNotFoundByKey struct{ key string }

func (e errFileNotFoundByKey) Error() string { return fmt.Sprintf("no file with s3_key %q", e.key) }

// errEmptyS3Key is the present-but-EMPTY s3_key rejection (both GetFile and
// UpdateTags): mapped to ERROR_INVALID_REQUEST at the connState layer —
// NEVER a silent fallback to file_id addressing.
type errEmptyS3Key struct{}

func (errEmptyS3Key) Error() string { return "s3_key must be non-empty" }

// errStaleCatalog: the request carried a generation-scoped handle (dimension id
// or pagination cursor) from a generation the server no longer serves — mapped
// to ERROR_STALE_CATALOG (retryable: re-fetch vocabulary, re-list from page one).
type errStaleCatalog struct{}

func (errStaleCatalog) Error() string {
	return "catalog generation changed (a rebuild renumbered the ids); re-fetch the vocabulary and restart the listing"
}

// errGenerationRequired: a first-page filter carries dimension ids but no
// expected_catalog_generation — the server could not detect a stale id, so the
// request is rejected as invalid (a client bug, not staleness).
type errGenerationRequired struct{}

func (errGenerationRequired) Error() string {
	return "filter carries dimension ids but no expected_catalog_generation (echo GetVocabularyResponse.catalog_generation)"
}

// errBadPageToken: a malformed cursor, a cursor minted for a different filter,
// or a cursor contradicting the explicit generation expectation — invalid
// request, never silently honored.
type errBadPageToken struct{ reason string }

func (e errBadPageToken) Error() string { return "invalid page_token: " + e.reason }

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
