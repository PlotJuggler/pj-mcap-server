package catalog

import "context"

// Failure is one quarantined file: the object key, when it failed, and why. It
// surfaces on the dashboard so operators see WHICH files could not be cataloged
// and WHY (catalog-migration plan §4.5) — distinct from a cataloged file's
// has_error health flag.
type Failure struct {
	Key     string
	WhenNs  int64
	ErrText string
}

// RecentFailures returns the most recent quarantine rows, newest first, reading
// the builder's `catalog_failures` (key, when, error).
func RecentFailures(ctx context.Context, s *Store, limit int) ([]Failure, error) {
	if limit <= 0 {
		limit = 20
	}
	q := `SELECT s3_key, failed_at_ns, error_text FROM catalog_failures ORDER BY failed_at_ns DESC LIMIT ?`
	rows, err := s.DB().QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Failure
	for rows.Next() {
		var f Failure
		if err := rows.Scan(&f.Key, &f.WhenNs, &f.ErrText); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
