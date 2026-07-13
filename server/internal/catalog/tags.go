package catalog

import (
	"context"
	"database/sql"
)

// TagKV is a plain embedded key/value tag (the codec-extracted layer).
type TagKV struct {
	Key   string
	Value string
}

// EffectiveTag is one row of the tags_effective view (override-wins merge).
// IsOverride marks which layer the row came from.
type EffectiveTag struct {
	Key        string
	Value      string
	IsOverride bool
}

// EffectiveTags returns the merged view (override ∪ embedded with override-wins).
// IsOverride indicates which layer the row came from. Sorted by key.
//
// This is the public, single-call entry point (fetches Store.DB() itself); a
// caller composing this with a sibling summary/topics query in one logical
// operation (B1 — catalog-migration §6.2a review) must instead pin
// db := s.DB() once and call effectiveTagsDB(ctx, db, fileID) directly, so all
// phases land on the same generation. See aurynFilterFiles and GetFileDetail.
func EffectiveTags(ctx context.Context, s *Store, fileID uint64) ([]EffectiveTag, error) {
	return effectiveTagsDB(ctx, s.DB(), fileID)
}

// effectiveTagsDB is EffectiveTags over an already-pinned db handle.
func effectiveTagsDB(ctx context.Context, db *sql.DB, fileID uint64) ([]EffectiveTag, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT key, value, is_override FROM tags_effective WHERE file_id = ? ORDER BY key`,
		fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EffectiveTag
	for rows.Next() {
		var t EffectiveTag
		var isOv int
		if err := rows.Scan(&t.Key, &t.Value, &isOv); err != nil {
			return nil, err
		}
		t.IsOverride = isOv != 0
		out = append(out, t)
	}
	return out, rows.Err()
}
