package catalog

import (
	"context"
	"database/sql"
	"errors"
)

// BuildInfo is the catalog-freshness snapshot the Python builder stamps at the end
// of each reconcile (catalog-migration plan §6.5). It replaces the in-process
// indexer-run signals orphaned by moving the writer out of process. Present is
// false on the legacy (read-write Go) store or before the first build.
type BuildInfo struct {
	Present        bool
	BuildID        int64 // monotonic; bumps each completed build (swap-detection §6.2a)
	LastBuildNs    int64
	FilesScanned   int64
	FilesFailed    int64
	Outcome        string // "ok" | "partial"
	BuilderVersion string
}

// GetBuildInfo reads build_metadata from the auryn catalog. On the legacy Go store
// (no build_metadata) or before the first build it returns Present=false (not an
// error), so callers (dashboard, metrics) work on both paths.
func GetBuildInfo(ctx context.Context, s *Store) (BuildInfo, error) {
	if !s.readOnly {
		return BuildInfo{}, nil // legacy store has no build_metadata
	}
	var bi BuildInfo
	err := s.DB().QueryRowContext(ctx,
		`SELECT build_id, last_build_ns, files_scanned, files_failed, build_outcome, builder_version
		 FROM build_metadata WHERE id = 1`).
		Scan(&bi.BuildID, &bi.LastBuildNs, &bi.FilesScanned, &bi.FilesFailed, &bi.Outcome, &bi.BuilderVersion)
	switch {
	case err == nil:
		bi.Present = true
		return bi, nil
	case errors.Is(err, sql.ErrNoRows):
		return BuildInfo{}, nil // table exists, no build yet
	default:
		return BuildInfo{}, err
	}
}
