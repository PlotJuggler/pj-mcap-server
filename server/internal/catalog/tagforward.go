package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// tagforward.go implements the two reads the tag-IPC forwarder (D2, the WS
// UpdateTags handler over a read-only/external-builder Store) needs around
// the actual Python-side edit:
//
//   - ObjectKeyForFile — resolve the wire file_id to the object key BEFORE
//     forwarding (the endpoint addresses files by key, not file_id).
//   - EffectiveTagsByKey — re-resolve key -> CURRENT file_id -> tags_effective
//     AFTER the edit lands (design-review finding A1): the file_id used to
//     address the request must NEVER be reused for this re-read, because a
//     catalog rebuild between the two phases can renumber ids (§7), so the
//     old id may by then name a DIFFERENT file. Re-deriving from the stable
//     key is the only generation-safe answer.
//
// Both helpers only make sense against the auryn schema (dimensions + the
// tags_effective view) — the only shape a read-only Store ever opens.

// ObjectKeyForFile resolves a file's rebuilt object key by id, on an
// already-pinned db handle (B1 pattern — see aurynGetFile/detail.go). Returns
// ErrFileNotFound for an unknown id.
func ObjectKeyForFile(ctx context.Context, db *sql.DB, id uint64) (string, error) {
	rec, err := aurynGetFile(ctx, db, id)
	if err != nil {
		return "", err
	}
	return rec.S3Key, nil
}

// FileIDForKey resolves an object key to its CURRENT file id, on an
// already-pinned db handle (B1 pattern — callers composing this with further
// reads of the same file MUST pass the SAME handle through every phase, or a
// concurrent ReopenIfSwapped could mix generations mid-request). The key is
// Hive-parsed to dimension NAMES and looked up by joining on them —
// lookup-only, NEVER creating rows (mirrors the Python db.lookup_file_id
// contract: an unknown customer/site/robot/source is a miss, unlike the
// builder's own resolve_customer/resolve_site/...).
//
// Returns ErrFileNotFound if key does not parse as a Hive key, or parses but
// names no cataloged file.
func FileIDForKey(ctx context.Context, db *sql.DB, key string) (uint64, error) {
	dims, ok := parseHiveKey(key)
	if !ok {
		return 0, ErrFileNotFound
	}
	var id uint64
	err := db.QueryRowContext(ctx, `
		SELECT f.id FROM files f
		JOIN customers c ON c.id = f.customer_id
		JOIN sites st    ON st.id = f.site_id
		JOIN robots r    ON r.id  = f.robot_id
		JOIN sources src ON src.id = f.source_id
		WHERE c.name = ? AND st.name = ? AND r.name = ? AND src.name = ? AND f.date = ? AND f.filename = ?`,
		dims.Customer, dims.Site, dims.Robot, dims.Source, dims.Date, dims.Filename).Scan(&id)
	switch {
	case err == nil:
		return id, nil
	case errors.Is(err, sql.ErrNoRows):
		return 0, ErrFileNotFound
	default:
		return 0, fmt.Errorf("file id for key %q: %w", key, err)
	}
}

// EffectiveTagsByKey resolves an object key to its CURRENT file_id and
// returns its tags_effective, both against ONE freshly-pinned db handle
// (s.DB() is called exactly once here) — this is the forwarder's response
// phase (see file header, finding A1).
//
// Returns ErrFileNotFound if key does not parse as a Hive key, or parses but
// names no cataloged file (see FileIDForKey).
func EffectiveTagsByKey(ctx context.Context, s *Store, key string) ([]EffectiveTag, error) {
	lease := s.Acquire()
	defer lease.Release()
	db := lease.DB()
	id, err := FileIDForKey(ctx, db, key)
	if err != nil {
		return nil, err
	}
	return effectiveTagsDB(ctx, db, id)
}
