package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"pj-cloud/server/internal/catalog"
)

// Scanner walks the bucket once per RunOnce call and reconciles with the
// catalog: unchanged objects (same etag+size+last_modified) are SKIPPED with no
// fetch/extract (warm-start), new/changed objects are re-extracted (overrides
// preserved by UpsertFile's id-stable in-place update + ReplaceEmbeddedTags not
// touching tags_override). Failures are recorded in indexer_failures.
type Scanner struct {
	Store     *catalog.Store
	Lister    Lister
	Extractor SummaryExtractor
	Prefix    string
	Log       *slog.Logger
}

// RunStats summarizes a single pass. Surfaced in the "run complete" log line.
type RunStats struct {
	Scanned   int
	NewFiles  int
	Reindexed int
	Unchanged int
	Failed    int
	Duration  time.Duration
}

func (s *Scanner) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// RunOnce performs one full reconcile pass.
func (s *Scanner) RunOnce(ctx context.Context) (RunStats, error) {
	start := time.Now()
	stats := RunStats{}

	objs, err := s.Lister.List(ctx, s.Prefix)
	if err != nil {
		return stats, fmt.Errorf("list bucket: %w", err)
	}
	stats.Scanned = len(objs)

	for _, obj := range objs {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if err := s.processOne(ctx, obj, &stats); err != nil {
			stats.Failed++
			s.log().Warn("indexer: extract failed", "key", obj.Key, "err", err)
			_ = recordFailure(ctx, s.Store, obj.Key, err.Error())
		}
	}
	stats.Duration = time.Since(start)
	return stats, nil
}

func (s *Scanner) processOne(ctx context.Context, obj S3Object, stats *RunStats) error {
	// Fast-path: if catalog has the file with identical (etag, size,
	// last_modified), skip entirely — warm-start serves the existing row with NO
	// re-extract. The log line is the smoke step-g contract.
	if existing, err := lookupBySignature(ctx, s.Store, obj); err != nil {
		return err
	} else if existing {
		stats.Unchanged++
		s.log().Info("indexer: skip-unchanged", "key", obj.Key)
		return nil
	}

	// Extract happens OUTSIDE the writer tx (ranged summary reads / network I/O).
	fs, err := s.Extractor.Extract(ctx, obj.Key)
	if err != nil {
		return fmt.Errorf("extract: %w", err)
	}
	result := summaryToResult(fs)
	result.File.S3Key = obj.Key
	result.File.S3ETag = obj.ETag
	result.File.S3LastModified = obj.LastModified.UnixNano()

	id, created, err := catalog.UpsertFile(ctx, s.Store, result.File)
	if err != nil {
		return fmt.Errorf("upsert file: %w", err)
	}
	if err := catalog.ReplaceTopicsForFile(ctx, s.Store, id, result.Topics); err != nil {
		return fmt.Errorf("topics: %w", err)
	}
	if err := catalog.ReplaceEmbeddedTagsForFile(ctx, s.Store, id, result.EmbeddedTags); err != nil {
		return fmt.Errorf("tags: %w", err)
	}
	if created {
		stats.NewFiles++
		s.log().Info("indexer: extracted", "key", obj.Key, "kind", "new",
			"size_bytes", result.File.SizeBytes, "messages", result.File.MessageCount,
			"topics", len(result.Topics), "chunks", result.File.ChunkCount)
	} else {
		stats.Reindexed++
		s.log().Info("indexer: extracted", "key", obj.Key, "kind", "reindexed",
			"size_bytes", result.File.SizeBytes, "messages", result.File.MessageCount,
			"topics", len(result.Topics), "chunks", result.File.ChunkCount)
	}
	return nil
}

// lookupBySignature returns true if a row already exists with the same
// (etag, size, last_modified). Faster than UpsertFile for the unchanged case.
func lookupBySignature(ctx context.Context, store *catalog.Store, obj S3Object) (bool, error) {
	var (
		etag string
		size int64
		mod  int64
	)
	row := store.DB().QueryRowContext(ctx,
		`SELECT s3_etag, size_bytes, s3_last_modified FROM files WHERE s3_key = ?`, obj.Key)
	switch err := row.Scan(&etag, &size, &mod); err {
	case nil:
		return etag == obj.ETag && size == obj.Size && mod == obj.LastModified.UnixNano(), nil
	case sql.ErrNoRows:
		return false, nil
	default:
		return false, err
	}
}

func recordFailure(ctx context.Context, store *catalog.Store, key, msg string) error {
	now := time.Now().UnixNano()
	return store.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO indexer_failures (s3_key, failed_at, error_text)
			 VALUES (?, ?, ?)
			 ON CONFLICT(s3_key) DO UPDATE SET failed_at = excluded.failed_at, error_text = excluded.error_text`,
			key, now, msg)
		return err
	})
}
