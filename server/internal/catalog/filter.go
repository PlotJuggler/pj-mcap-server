package catalog

import (
	"context"
	"database/sql"
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
	// AfterID is the keyset-pagination position: return rows with id > AfterID
	// (0 = first page). Rowids are generation-scoped (a full rebuild renumbers
	// them, CATALOG_CONTRACT.md §7), so the WIRE page token that carries this
	// value is generation-bound and validated by the ws layer — this package
	// only ever sees a position already checked against the pinned snapshot.
	AfterID uint64

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
// Keyset pagination is on the row id, which is stable across an in-place
// rescan but RENUMBERED by a full rebuild (CATALOG_CONTRACT.md §7) — the ws
// layer binds the wire cursor to the catalog generation for exactly that
// reason.
//
// Returns (files, more, error): more is true when rows beyond this page exist
// (the caller mints the next generation-bound cursor from the last row's ID).
// This convenience wrapper leases the current snapshot itself; callers that
// must pair the rows with the generation token use FilterFilesDB on a
// Snapshot's handle instead.
func FilterFiles(ctx context.Context, s *Store, args FilterArgs) ([]FileSummary, bool, error) {
	lease := s.Acquire()
	defer lease.Release()
	return aurynFilterFiles(ctx, lease.DB(), args)
}

// FilterFilesDB is FilterFiles against an already-leased snapshot handle (B1):
// the caller pins ONE Snapshot for the query, the generation token, and the
// cursor mint so all three describe the same generation.
func FilterFilesDB(ctx context.Context, db *sql.DB, args FilterArgs) ([]FileSummary, bool, error) {
	return aurynFilterFiles(ctx, db, args)
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

