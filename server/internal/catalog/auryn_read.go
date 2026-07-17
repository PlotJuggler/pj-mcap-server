package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// auryn_read.go is the READ-ONLY query side over the auryn (Python-builder)
// schema (catalog-migration plan §2.3/§2.4). The public reader functions
// (FilterFiles, GetFile, ListTopicsForFile, HasHierarchicalKey in files.go /
// filter.go / topics.go / caps.go) delegate straight to these — the M6 cutover
// (§2.6) deleted the legacy Go-writer schema + queries these used to branch
// away from, so OpenReadOnly is now the only way a Store is ever opened.
//
// The auryn schema differs structurally: dimensions are normalized FK ids (the
// object key is REBUILT from them, §5/D1), per-file message counts are a packed
// varint blob (§4), and topics live in a deduped topic_set. See CATALOG_CONTRACT.md.

// rebuildHiveKey is the exact inverse of the Python builder's
// keyparse.rebuild_hive_key — the single source of the object key, rebuilt from
// the file's dimensions for range-GETs (no stored s3_key column).
func rebuildHiveKey(customer, site, robot, source, date, filename string) string {
	return fmt.Sprintf(
		"customer=%s/customer_site=%s/robot=%s/source=%s/date=%s/%s",
		customer, site, robot, source, date, filename)
}

// decodeCountsBlob decodes files.topic_counts: one unsigned-LEB128 varint per
// topic-set member, ordered by topic_id ASC. Port of varint.decode_counts_blob —
// must stay byte-identical to the writer (CATALOG_CONTRACT.md §4).
func decodeCountsBlob(blob []byte) ([]uint64, error) {
	out := make([]uint64, 0, 8)
	i := 0
	for i < len(blob) {
		var v uint64
		var shift uint
		for {
			if i >= len(blob) {
				return nil, errors.New("truncated varint in topic_counts")
			}
			b := blob[i]
			i++
			v |= uint64(b&0x7f) << shift
			if b&0x80 == 0 {
				break
			}
			shift += 7
			if shift >= 64 {
				return nil, errors.New("varint overflow in topic_counts")
			}
		}
		out = append(out, v)
	}
	return out, nil
}

func sumCounts(c []uint64) uint64 {
	var t uint64
	for _, v := range c {
		t += v
	}
	return t
}

// aurynGetFile resolves a FileRecord from the auryn schema: dimensions -> rebuilt
// s3_key, topic_counts -> message_count, etag for the session's If-Match. Fields
// the serving path does not read (s3_last_modified, indexed_at, has_message_index,
// mcap_summary) are left zero.
//
// Takes an already-pinned db (B1 — catalog-migration §6.2a review): callers that
// compose this with another query against the same file (e.g. GetFileDetail's
// summary+topics+tags read) must capture Store.DB() ONCE and pass the SAME
// handle through every phase, or a concurrent ReopenIfSwapped could mix
// generations mid-request (stale file ids paired with a new generation's rows).
func aurynGetFile(ctx context.Context, db *sql.DB, id uint64) (FileRecord, error) {
	var (
		rec                                       FileRecord
		customer, site, robot, source, date, name string
		blob                                      []byte
	)
	rec.ID = id
	row := db.QueryRowContext(ctx, `
		SELECT c.name, st.name, r.name, src.name, f.date, f.filename,
		       f.etag, f.size_bytes, f.start_time_ns, f.end_time_ns, f.chunk_count, f.topic_counts
		FROM files f
		JOIN customers c   ON c.id  = f.customer_id
		JOIN sites st      ON st.id = f.site_id
		JOIN robots r      ON r.id  = f.robot_id
		JOIN sources src   ON src.id = f.source_id
		WHERE f.id = ?`, id)
	err := row.Scan(&customer, &site, &robot, &source, &date, &name,
		&rec.S3ETag, &rec.SizeBytes, &rec.StartTimeNs, &rec.EndTimeNs, &rec.ChunkCount, &blob)
	switch {
	case err == nil:
	case errors.Is(err, sql.ErrNoRows):
		return FileRecord{}, ErrFileNotFound
	default:
		return FileRecord{}, fmt.Errorf("auryn get file %d: %w", id, err)
	}
	rec.S3Key = rebuildHiveKey(customer, site, robot, source, date, name)
	counts, derr := decodeCountsBlob(blob)
	if derr != nil {
		return FileRecord{}, fmt.Errorf("auryn get file %d: %w", id, derr)
	}
	rec.MessageCount = sumCounts(counts)
	return rec, nil
}

// aurynListTopicsForFile reconstructs the file's topics from its deduped
// topic_set, zipping the topic_id-ASC members with the aligned topic_counts varints,
// then returns them name-sorted (matching the legacy ListTopicsForFile contract).
//
// Takes an already-pinned db (B1): this function itself runs TWO queries (the
// files row, then the topic_set_members join) — both must land on the SAME
// generation, and a caller composing this with a sibling summary/tags query
// must pin once and pass that same handle down (see aurynGetFile's doc).
func aurynListTopicsForFile(ctx context.Context, db *sql.DB, fileID uint64) ([]TopicRecord, error) {
	var (
		setID int64
		blob  []byte
	)
	row := db.QueryRowContext(ctx,
		`SELECT topic_set_id, topic_counts FROM files WHERE id = ?`, fileID)
	switch err := row.Scan(&setID, &blob); {
	case err == nil:
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil // unknown file => no topics (matches legacy: empty slice)
	default:
		return nil, fmt.Errorf("auryn list topics %d: %w", fileID, err)
	}
	counts, err := decodeCountsBlob(blob)
	if err != nil {
		return nil, fmt.Errorf("auryn list topics %d: %w", fileID, err)
	}

	// Members ORDERED BY topic_id ASC == the topic_counts ordering (the contract).
	rows, err := db.QueryContext(ctx, `
		SELECT tn.name, sc.name, sc.encoding
		FROM topic_set_members tsm
		JOIN topic_names tn ON tn.id = tsm.topic_id
		JOIN schemas sc     ON sc.id = tsm.schema_id
		WHERE tsm.set_id = ?
		ORDER BY tsm.topic_id ASC`, setID)
	if err != nil {
		return nil, fmt.Errorf("auryn list topics %d: %w", fileID, err)
	}
	defer rows.Close()
	var out []TopicRecord
	i := 0
	for rows.Next() {
		var t TopicRecord
		if err := rows.Scan(&t.Name, &t.SchemaName, &t.SchemaEncoding); err != nil {
			return nil, err
		}
		if i < len(counts) {
			t.MessageCount = counts[i]
		}
		out = append(out, t)
		i++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Cardinality MUST match: one varint per topic-set member (CATALOG_CONTRACT.md
	// §4). A mismatch is a corrupt/contract-violating catalog — fail fast rather
	// than silently assign 0 to suffix topics (short blob) or drop counts (long blob).
	if i != len(counts) {
		return nil, fmt.Errorf(
			"auryn list topics %d: topic_set has %d members but topic_counts decodes %d entries (corrupt catalog)",
			fileID, i, len(counts))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out, nil
}

// aurynHasHierarchicalKey: every auryn object key is Hive-partitioned (contains
// '/'), so the hierarchy flag is simply "are there any files". (M3's GetVocabulary
// replaces this boolean with the real dimension tree.)
func aurynHasHierarchicalKey(ctx context.Context, db *sql.DB) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM files LIMIT 1`).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, err
	}
}

// aurynFilterFiles mirrors FilterFiles over the auryn schema: keyset pagination on
// files.id, the time-overlap predicate, topics-any-of via the topic_set, and tag
// predicates via the tags_effective view. s3_key is rebuilt from dimensions and
// message_count is summed from the topic_counts blob per row.
//
// Takes an already-pinned db (B1): the summary query and the per-row tag-attach
// phase below MUST run against the SAME generation — a mid-loop ReopenIfSwapped
// swap must never let row N's tags come from a different generation than row N's
// summary (old file ids reused/renumbered across a full rebuild would then pair
// with the WRONG file's tags).
func aurynFilterFiles(ctx context.Context, db *sql.DB, args FilterArgs) ([]FileSummary, bool, error) {
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
	if args.AfterID != 0 {
		clauses = append(clauses, "f.id > ?")
		params = append(params, args.AfterID)
	}
	if args.RecordedBetween != nil {
		clauses = append(clauses, "f.end_time_ns >= ? AND f.start_time_ns <= ?")
		params = append(params, args.RecordedBetween.StartNs, args.RecordedBetween.EndNs)
	}
	// Dimension predicates (direct indexed lookups on the denormalized FK columns).
	// The strict hierarchy makes the deepest set id sufficient, but the server ANDs
	// every present id (redundant-but-harmless); source_id is independent.
	if args.CustomerID != nil {
		clauses = append(clauses, "f.customer_id = ?")
		params = append(params, *args.CustomerID)
	}
	if args.SiteID != nil {
		clauses = append(clauses, "f.site_id = ?")
		params = append(params, *args.SiteID)
	}
	if args.RobotID != nil {
		clauses = append(clauses, "f.robot_id = ?")
		params = append(params, *args.RobotID)
	}
	if args.SourceID != nil {
		clauses = append(clauses, "f.source_id = ?")
		params = append(params, *args.SourceID)
	}
	if len(args.TopicsAnyOf) > 0 {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(args.TopicsAnyOf)), ",")
		clauses = append(clauses, fmt.Sprintf(`EXISTS (
			SELECT 1 FROM topic_set_members tsm
			JOIN topic_names tn ON tn.id = tsm.topic_id
			WHERE tsm.set_id = f.topic_set_id AND tn.name IN (%s))`, ph))
		for _, tn := range args.TopicsAnyOf {
			params = append(params, tn)
		}
	}
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
			strings.Join(subs, " OR ")))
	}

	where := ""
	if len(clauses) > 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}
	q := fmt.Sprintf(`
		SELECT f.id, c.name, st.name, r.name, src.name, f.date, f.filename,
		       f.size_bytes, f.start_time_ns, f.end_time_ns, f.chunk_count, f.topic_counts,
		       (SELECT COUNT(*) FROM topic_set_members WHERE set_id = f.topic_set_id) AS topic_count
		FROM files f
		JOIN customers c ON c.id = f.customer_id
		JOIN sites st    ON st.id = f.site_id
		JOIN robots r    ON r.id  = f.robot_id
		JOIN sources src ON src.id = f.source_id
		%s
		ORDER BY f.id ASC
		LIMIT ?`, where)
	params = append(params, limit+1)

	rows, err := db.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, false, fmt.Errorf("auryn filter query: %w", err)
	}
	defer rows.Close()

	var out []FileSummary
	for rows.Next() {
		var (
			f                                         FileSummary
			customer, site, robot, source, date, name string
			blob                                      []byte
		)
		if err := rows.Scan(&f.ID, &customer, &site, &robot, &source, &date, &name,
			&f.SizeBytes, &f.StartTimeNs, &f.EndTimeNs, &f.ChunkCount, &blob, &f.TopicCount); err != nil {
			return nil, false, err
		}
		f.S3Key = rebuildHiveKey(customer, site, robot, source, date, name)
		counts, derr := decodeCountsBlob(blob)
		if derr != nil {
			return nil, false, fmt.Errorf("auryn filter (file %d): %w", f.ID, derr)
		}
		f.MessageCount = sumCounts(counts)
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	more := false
	if len(out) > limit {
		more = true
		out = out[:limit]
	}
	// Attach effective tags (tags_effective exists in both schemas). Uses the
	// SAME pinned db as the summary query above (B1) via effectiveTagsDB, not
	// the Store-level EffectiveTags (which would re-fetch Store.DB() and could
	// observe a different generation if a swap landed mid-loop).
	for i := range out {
		tags, err := effectiveTagsDB(ctx, db, out[i].ID)
		if err != nil {
			return nil, false, err
		}
		out[i].Tags = tags
	}
	return out, more, nil
}
