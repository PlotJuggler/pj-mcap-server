package catalog

import (
	"context"
	"errors"
	"fmt"
)

// FileRecord is the catalog representation of one MCAP file in S3.
//
// HasMessageIndex / McapSummary are not populated by the auryn reader (chunk
// index is rebuilt on-demand per OpenSession from storage, never stored in the
// catalog) — left zero-valued.
type FileRecord struct {
	ID              uint64
	S3Key           string
	S3ETag          string
	S3LastModified  int64 // unix ns from S3 metadata
	SizeBytes       int64
	IndexedAt       int64 // unix ns; the Python builder's files.cataloged_at_ns
	StartTimeNs     int64
	EndTimeNs       int64
	ChunkCount      uint32
	MessageCount    uint64
	HasMessageIndex bool
	McapSummary     []byte
}

// ErrFileNotFound is returned by GetFile when no row matches.
var ErrFileNotFound = errors.New("file not found")

// GetFile returns the FileRecord with the given id, or ErrFileNotFound.
//
// This is the public, single-call entry point (fetches Store.DB() itself); a
// caller composing this with a sibling topics/tags query in one logical
// operation (B1 — catalog-migration §6.2a review) must instead pin
// db := s.DB() once and call aurynGetFile directly against that SAME handle.
// See GetFileDetail.
func GetFile(ctx context.Context, s *Store, id uint64) (FileRecord, error) {
	return aurynGetFile(ctx, s.DB(), id)
}

// GetFiles resolves every id against ONE pinned db handle (B1): all records in
// an OpenSession selection come from the SAME catalog generation, so a
// ReopenIfSwapped landing mid-request can never pair one generation's
// (renumbered) ids with another generation's records. An unknown id fails the
// whole batch with a wrapped ErrFileNotFound naming the id.
func GetFiles(ctx context.Context, s *Store, ids []uint64) ([]FileRecord, error) {
	db := s.DB() // pinned once for the whole batch
	out := make([]FileRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := aurynGetFile(ctx, db, id)
		if err != nil {
			return nil, fmt.Errorf("file id %d: %w", id, err)
		}
		out = append(out, rec)
	}
	return out, nil
}
