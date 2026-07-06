package catalog

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
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

	// Dimension selection (auryn reader only; catalog-vocabulary-rpc.md). nil =
	// unset (proto3 optional presence). The strict hierarchy means the deepest set
	// id is sufficient; the server ANDs whatever is present. source_id is an
	// independent flat dimension. The legacy Go-schema FilterFiles IGNORES these
	// (it has no dimension columns); only aurynFilterFiles applies them.
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
// that requires a separate GetFile call). Cursor pagination is keyed on the
// row id, which is stable across reindexes (UpsertFile guarantees id stability).
//
// Returns (files, nextPageToken, error). nextPageToken is empty when exhausted.
// A read-only Store (OpenReadOnly) filters against the auryn schema.
//
// Pins db := s.DB() ONCE and threads it through both the summary query and the
// per-row tag-attach loop below (B1 — catalog-migration §6.2a review): a
// mid-loop ReopenIfSwapped swap must never let a later row's tags come from a
// different generation than its summary.
func FilterFiles(ctx context.Context, s *Store, args FilterArgs) ([]FileSummary, string, error) {
	db := s.DB()
	if s.readOnly {
		return aurynFilterFiles(ctx, db, args)
	}
	limit := args.Limit
	if limit <= 0 {
		limit = 200
	}
	if limit > 1000 {
		limit = 1000
	}

	var (
		clauses []string
		params  []interface{}
	)

	// Cursor (id > N).
	if args.PageToken != "" {
		cursor, err := decodeCursor(args.PageToken)
		if err != nil {
			return nil, "", fmt.Errorf("invalid page_token: %w", err)
		}
		clauses = append(clauses, "f.id > ?")
		params = append(params, cursor)
	}

	// Time range — files that overlap the requested window.
	if args.RecordedBetween != nil {
		clauses = append(clauses, "f.end_time_ns >= ? AND f.start_time_ns <= ?")
		params = append(params, args.RecordedBetween.StartNs, args.RecordedBetween.EndNs)
	}

	// Topics any-of (sub-select EXISTS).
	if len(args.TopicsAnyOf) > 0 {
		placeholders := strings.Repeat("?,", len(args.TopicsAnyOf))
		placeholders = placeholders[:len(placeholders)-1]
		clauses = append(clauses, fmt.Sprintf(
			`EXISTS (SELECT 1 FROM topics t WHERE t.file_id = f.id AND t.name IN (%s))`, placeholders,
		))
		for _, tn := range args.TopicsAnyOf {
			params = append(params, tn)
		}
	}

	// Tag predicates use the tags_effective view.
	for _, tag := range args.TagAll {
		if tag.Value == "" {
			clauses = append(clauses,
				`EXISTS (SELECT 1 FROM tags_effective te WHERE te.file_id = f.id AND te.key = ?)`)
			params = append(params, tag.Key)
		} else {
			clauses = append(clauses,
				`EXISTS (SELECT 1 FROM tags_effective te WHERE te.file_id = f.id AND te.key = ? AND te.value = ?)`)
			params = append(params, tag.Key, tag.Value)
		}
	}
	if len(args.TagAny) > 0 {
		var subs []string
		for _, tag := range args.TagAny {
			if tag.Value == "" {
				subs = append(subs, `(te.key = ?)`)
				params = append(params, tag.Key)
			} else {
				subs = append(subs, `(te.key = ? AND te.value = ?)`)
				params = append(params, tag.Key, tag.Value)
			}
		}
		clauses = append(clauses, fmt.Sprintf(
			`EXISTS (SELECT 1 FROM tags_effective te WHERE te.file_id = f.id AND (%s))`,
			strings.Join(subs, " OR "),
		))
	}

	where := ""
	if len(clauses) > 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}

	q := fmt.Sprintf(
		`SELECT f.id, f.s3_key, f.size_bytes, f.start_time_ns, f.end_time_ns,
		        f.message_count, f.chunk_count,
		        (SELECT COUNT(*) FROM topics t WHERE t.file_id = f.id) AS topic_count
		 FROM files f
		 %s
		 ORDER BY f.id ASC
		 LIMIT ?`, where)
	params = append(params, limit+1) // +1 to detect "has more"

	rows, err := db.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, "", fmt.Errorf("filter query: %w", err)
	}
	defer rows.Close()

	var out []FileSummary
	for rows.Next() {
		var f FileSummary
		if err := rows.Scan(&f.ID, &f.S3Key, &f.SizeBytes, &f.StartTimeNs, &f.EndTimeNs,
			&f.MessageCount, &f.ChunkCount, &f.TopicCount); err != nil {
			return nil, "", err
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	next := ""
	if len(out) > limit {
		next = encodeCursor(out[limit-1].ID)
		out = out[:limit]
	}

	// Attach effective tags (N+1 query per returned row — acceptable at v1 scale;
	// flagged for M2 if the corpus grows large, idx_tags_*_kv exist). Uses the
	// pinned db (B1), not the Store-level EffectiveTags, for the same reason as
	// aurynFilterFiles above — this path never swaps today (legacy store), but
	// the code shape should not depend on that.
	for i := range out {
		tags, err := effectiveTagsDB(ctx, db, out[i].ID)
		if err != nil {
			return nil, "", err
		}
		out[i].Tags = tags
	}
	return out, next, nil
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

// Compile-time assertion that sql package is referenced (silences unused import
// when this file is read in isolation by tooling).
var _ = sql.ErrNoRows
