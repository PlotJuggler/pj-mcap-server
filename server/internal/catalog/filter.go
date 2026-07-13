package catalog

import (
	"context"
	"encoding/base64"
	"strconv"
)

// TimeWindow is the recorded-between predicate's [StartNs, EndNs] inclusive
// window; a file matches if its recorded range OVERLAPS the window.
type TimeWindow struct {
	StartNs int64
	EndNs   int64
}

// FilterArgs are the server-side FileFilter predicates (+ pagination) the ws
// layer maps from pb.ListFilesRequest.
type FilterArgs struct {
	RecordedBetween *TimeWindow
	TopicsAnyOf     []string
	TagAll          []TagKV
	TagAny          []TagKV
	Limit           int
	PageToken       string

	// Dimension selection (catalog-vocabulary-rpc.md). nil = unset (proto3
	// optional presence). The strict hierarchy means the deepest set id is
	// sufficient; the server ANDs whatever is present. source_id is an
	// independent flat dimension.
	CustomerID *uint64
	SiteID     *uint64
	RobotID    *uint64
	SourceID   *uint64
}

// FileSummary is the lightweight per-file shape returned by FilterFiles. It
// carries the effective tags (with is_override) so the ws layer can build both
// the FileSummary.tags wire field and the flat metadata overlay.
type FileSummary struct {
	ID           uint64
	S3Key        string
	SizeBytes    int64
	StartTimeNs  int64
	EndTimeNs    int64
	MessageCount uint64
	ChunkCount   uint32
	TopicCount   uint32
	Tags         []EffectiveTag
}

// FilterFiles returns matching file summaries (without per-file topic detail —
// that requires a separate GetFile call), filtered against the auryn schema.
// Cursor pagination is keyed on the row id, which is stable across a builder
// rescan/rebuild (CATALOG_CONTRACT.md §7).
//
// Returns (files, nextPageToken, error). nextPageToken is empty when exhausted.
func FilterFiles(ctx context.Context, s *Store, args FilterArgs) ([]FileSummary, string, error) {
	return aurynFilterFiles(ctx, s.DB(), args)
}

// FlatMetadata renders this file's effective tags as a plain string->string map
// (override-wins is already applied by EffectiveTags). This is the tags layer of
// ListFilesResponse.metadata -> Mosaico SequenceInfo.user_metadata
// (unified-plan §3.1). The ws layer overlays this on TOP of the derived keys
// (effective tags WIN on key collision — slice contract). Last writer wins on
// duplicate keys, which cannot happen because tags_effective is keyed by
// (file_id, key).
func (s FileSummary) FlatMetadata() map[string]string {
	m := make(map[string]string, len(s.Tags))
	for _, t := range s.Tags {
		m[t.Key] = t.Value
	}
	return m
}

func encodeCursor(id uint64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatUint(id, 10)))
}

func decodeCursor(tok string) (uint64, error) {
	b, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(b), 10, 64)
}
