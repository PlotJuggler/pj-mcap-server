package catalog

import (
	"context"
	"database/sql"
	"fmt"
)

// TopicRecord is one topic row (maps onto wire TopicInfo + format.TopicInfo).
type TopicRecord struct {
	Name           string
	SchemaName     string
	SchemaEncoding string
	MessageCount   uint64
}

// ReplaceTopicsForFile atomically deletes existing topic rows for the file
// and inserts the new set. Called by the indexer when a file is (re)indexed.
func ReplaceTopicsForFile(ctx context.Context, s *Store, fileID uint64, topics []TopicRecord) error {
	return s.Write(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM topics WHERE file_id = ?`, fileID); err != nil {
			return fmt.Errorf("delete topics: %w", err)
		}
		if len(topics) == 0 {
			return nil
		}
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO topics (file_id, name, schema_name, schema_encoding, message_count)
			 VALUES (?, ?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("prepare insert: %w", err)
		}
		defer stmt.Close()
		for _, t := range topics {
			if _, err := stmt.ExecContext(ctx, fileID, t.Name, t.SchemaName, t.SchemaEncoding, t.MessageCount); err != nil {
				return fmt.Errorf("insert topic %q: %w", t.Name, err)
			}
		}
		return nil
	})
}

// ListTopicsForFile returns all topics for the given file in name-sorted order.
// A read-only Store (OpenReadOnly) reconstructs them from the auryn topic_set.
//
// This is the public, single-call entry point (fetches Store.DB() itself); a
// caller composing this with a sibling summary/tags query in one logical
// operation (B1 — catalog-migration §6.2a review) must instead pin
// db := s.DB() once and call listTopicsLegacy/aurynListTopicsForFile directly
// against that SAME handle. See GetFileDetail.
func ListTopicsForFile(ctx context.Context, s *Store, fileID uint64) ([]TopicRecord, error) {
	db := s.DB()
	if s.readOnly {
		return aurynListTopicsForFile(ctx, db, fileID)
	}
	return listTopicsLegacy(ctx, db, fileID)
}

// listTopicsLegacy is ListTopicsForFile's legacy-schema branch over an
// already-pinned db handle.
func listTopicsLegacy(ctx context.Context, db *sql.DB, fileID uint64) ([]TopicRecord, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT name, schema_name, schema_encoding, message_count
		 FROM topics WHERE file_id = ? ORDER BY name`, fileID)
	if err != nil {
		return nil, fmt.Errorf("list topics %d: %w", fileID, err)
	}
	defer rows.Close()
	var out []TopicRecord
	for rows.Next() {
		var t TopicRecord
		if err := rows.Scan(&t.Name, &t.SchemaName, &t.SchemaEncoding, &t.MessageCount); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
