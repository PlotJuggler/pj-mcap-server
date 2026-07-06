package catalog

import (
	"context"
	"database/sql"
	"errors"
)

// BuildInfo is the catalog-freshness snapshot the Python builder stamps at the end
// of each reconcile (catalog-migration plan §6.5). It replaces the in-process
// indexer-run signals orphaned by moving the writer out of process. Present is
// false before the first build (build_metadata exists but has no row=1 yet).
type BuildInfo struct {
	Present bool
	// BuildID is a monotonic freshness/confirmation counter (bumps each completed
	// build). It is NOT the swap-detection trigger for §6.2a — that is file
	// identity ((dev, inode) of the served path; see ReopenIfSwapped in
	// reopen.go and CATALOG_CONTRACT.md §9). BuildID is not even guaranteed
	// comparable across a rebuild's best-effort seeding; use it only to display
	// "which build is being served", never to decide whether to reopen.
	BuildID        int64
	LastBuildNs    int64
	FilesScanned   int64
	FilesFailed    int64
	Outcome        string // "ok" | "partial"
	BuilderVersion string
}

// GetBuildInfo reads build_metadata from the auryn catalog. Before the first
// build it returns Present=false (not an error), so callers (dashboard,
// metrics) can render a "no build yet" state.
func GetBuildInfo(ctx context.Context, s *Store) (BuildInfo, error) {
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
