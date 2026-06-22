package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// FileRecord is the catalog representation of one MCAP file in S3.
//
// McapSummary is part of the schema (Plan A) but this slice always stores NULL;
// HasMessageIndex is derived from "summary present / chunk_count > 0" by the
// indexer — neither is load-bearing in this slice (chunk index is rebuilt
// on-demand per OpenSession). See store.go's DESIGN NOTE.
type FileRecord struct {
	ID              uint64
	S3Key           string
	S3ETag          string
	S3LastModified  int64 // unix ns from S3 metadata
	SizeBytes       int64
	IndexedAt       int64 // unix ns; set by UpsertFile when committing
	StartTimeNs     int64
	EndTimeNs       int64
	ChunkCount      uint32
	MessageCount    uint64
	HasMessageIndex bool
	McapSummary     []byte
}

// ErrFileNotFound is returned by GetFile when no row matches.
var ErrFileNotFound = errors.New("file not found")

// UpsertFile inserts a new row or updates an existing one keyed on S3Key.
// Returns the row id and a "created" flag (true if this was a new row).
// On in-place replace (changed ETag / size / last_modified), the id is preserved
// so foreign keys (topics, tags_embedded, tags_override) remain valid — this is
// what makes tag overrides SURVIVE a forced reindex.
//
// If all observable fields match (etag + size + last_modified), this is a no-op
// and returns (id, false, nil) without writing.
func UpsertFile(ctx context.Context, s *Store, rec FileRecord) (uint64, bool, error) {
	// First, read the existing row (if any) using the read pool.
	var (
		existingID   uint64
		existingEtag string
		existingSize int64
		existingMod  int64
	)
	row := s.DB().QueryRowContext(ctx,
		`SELECT id, s3_etag, size_bytes, s3_last_modified FROM files WHERE s3_key = ?`,
		rec.S3Key,
	)
	switch err := row.Scan(&existingID, &existingEtag, &existingSize, &existingMod); err {
	case nil:
		if existingEtag == rec.S3ETag && existingSize == rec.SizeBytes && existingMod == rec.S3LastModified {
			return existingID, false, nil
		}
	case sql.ErrNoRows:
		// new file
	default:
		return 0, false, fmt.Errorf("look up existing file: %w", err)
	}

	indexedAt := time.Now().UnixNano()
	var newID int64
	created := existingID == 0

	err := s.Write(ctx, func(tx *sql.Tx) error {
		if created {
			res, err := tx.ExecContext(ctx,
				`INSERT INTO files
				 (s3_key, s3_etag, s3_last_modified, size_bytes, indexed_at,
				  start_time_ns, end_time_ns, chunk_count, message_count, has_message_index, mcap_summary)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				rec.S3Key, rec.S3ETag, rec.S3LastModified, rec.SizeBytes, indexedAt,
				rec.StartTimeNs, rec.EndTimeNs, rec.ChunkCount, rec.MessageCount,
				boolToInt(rec.HasMessageIndex), rec.McapSummary,
			)
			if err != nil {
				return fmt.Errorf("insert files: %w", err)
			}
			id, err := res.LastInsertId()
			if err != nil {
				return fmt.Errorf("last insert id: %w", err)
			}
			newID = id
			return nil
		}
		// in-place update (id preserved)
		_, err := tx.ExecContext(ctx,
			`UPDATE files SET
			   s3_etag = ?, s3_last_modified = ?, size_bytes = ?, indexed_at = ?,
			   start_time_ns = ?, end_time_ns = ?, chunk_count = ?, message_count = ?,
			   has_message_index = ?, mcap_summary = ?
			 WHERE id = ?`,
			rec.S3ETag, rec.S3LastModified, rec.SizeBytes, indexedAt,
			rec.StartTimeNs, rec.EndTimeNs, rec.ChunkCount, rec.MessageCount,
			boolToInt(rec.HasMessageIndex), rec.McapSummary,
			existingID,
		)
		if err != nil {
			return fmt.Errorf("update files: %w", err)
		}
		newID = int64(existingID)
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return uint64(newID), created, nil
}

// GetFile returns the FileRecord with the given id, or ErrFileNotFound. A
// read-only Store (OpenReadOnly) resolves against the auryn schema.
func GetFile(ctx context.Context, s *Store, id uint64) (FileRecord, error) {
	if s.readOnly {
		return aurynGetFile(ctx, s, id)
	}
	var (
		rec FileRecord
		has int
	)
	rec.ID = id
	row := s.DB().QueryRowContext(ctx,
		`SELECT s3_key, s3_etag, s3_last_modified, size_bytes, indexed_at,
		        start_time_ns, end_time_ns, chunk_count, message_count, has_message_index, mcap_summary
		 FROM files WHERE id = ?`, id)
	err := row.Scan(&rec.S3Key, &rec.S3ETag, &rec.S3LastModified, &rec.SizeBytes, &rec.IndexedAt,
		&rec.StartTimeNs, &rec.EndTimeNs, &rec.ChunkCount, &rec.MessageCount, &has, &rec.McapSummary)
	switch err {
	case nil:
		rec.HasMessageIndex = has != 0
		return rec, nil
	case sql.ErrNoRows:
		return FileRecord{}, ErrFileNotFound
	default:
		return FileRecord{}, fmt.Errorf("get file %d: %w", id, err)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
