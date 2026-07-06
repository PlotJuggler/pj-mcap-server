package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"time"
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

// ReplaceEmbeddedTagsForFile atomically replaces the embedded tag set for the
// file. Override tags (in tags_override) are NOT touched — that is the point of
// the two-layer model: a forced reindex re-extracts embedded tags but preserves
// every user override. See spec §5.1 for rationale.
func ReplaceEmbeddedTagsForFile(ctx context.Context, s *Store, fileID uint64, tags []TagKV) error {
	return s.Write(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM tags_embedded WHERE file_id = ?`, fileID); err != nil {
			return fmt.Errorf("delete embedded: %w", err)
		}
		if len(tags) == 0 {
			return nil
		}
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO tags_embedded (file_id, key, value) VALUES (?, ?, ?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, t := range tags {
			if _, err := stmt.ExecContext(ctx, fileID, t.Key, t.Value); err != nil {
				return fmt.Errorf("insert embedded %q: %w", t.Key, err)
			}
		}
		return nil
	})
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

// HasEmbeddedTag reports whether the file has an embedded tag with the given
// key. The UpdateTags handler uses this to decide between MaskEmbedded (NULL
// mask when an embedded tag exists) and UnsetOverride (plain delete otherwise).
func HasEmbeddedTag(ctx context.Context, s *Store, fileID uint64, key string) (bool, error) {
	var one int
	row := s.DB().QueryRowContext(ctx,
		`SELECT 1 FROM tags_embedded WHERE file_id = ? AND key = ?`, fileID, key)
	switch err := row.Scan(&one); err {
	case nil:
		return true, nil
	case sql.ErrNoRows:
		return false, nil
	default:
		return false, err
	}
}

// SetOverride upserts a non-NULL override row. The override wins over any
// embedded value for the same key. Idempotent.
func SetOverride(ctx context.Context, s *Store, fileID uint64, key, value string) error {
	now := time.Now().UnixNano()
	return s.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO tags_override (file_id, key, value, updated_at)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(file_id, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
			fileID, key, value, now)
		return err
	})
}

// UnsetOverride removes any override row for (fileID, key). If an embedded
// tag with the same key exists, it will become effective again.
func UnsetOverride(ctx context.Context, s *Store, fileID uint64, key string) error {
	return s.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`DELETE FROM tags_override WHERE file_id = ? AND key = ?`, fileID, key)
		return err
	})
}

// MaskEmbedded inserts a NULL-valued override row, which hides any embedded
// tag with the same key from the effective view (see schema view).
func MaskEmbedded(ctx context.Context, s *Store, fileID uint64, key string) error {
	now := time.Now().UnixNano()
	return s.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO tags_override (file_id, key, value, updated_at)
			 VALUES (?, ?, NULL, ?)
			 ON CONFLICT(file_id, key) DO UPDATE SET value = NULL, updated_at = excluded.updated_at`,
			fileID, key, now)
		return err
	})
}
