package catalog

import (
	"context"
	"database/sql"
)

// WarmEntry is the (object key, change-detect etag) a chunk-index warmer needs to
// pre-fill the cache: the etag is part of the cache key, so a warmed entry is
// invalidated automatically when the object is overwritten.
type WarmEntry struct {
	Key  string
	ETag string
}

// WarmEntries lists every cataloged file's (key, etag) for the background warmer
// (catalog-migration plan §3.2). Read-only. On the auryn (read-only) store the key
// is rebuilt from dimensions; on the legacy Go-schema store it is the stored
// s3_key. Ordered by id for a stable, resumable warm sweep.
func WarmEntries(ctx context.Context, s *Store) ([]WarmEntry, error) {
	db := s.DB()
	if s.readOnly {
		return aurynWarmEntries(ctx, db)
	}
	return legacyWarmEntries(ctx, db)
}

func aurynWarmEntries(ctx context.Context, db *sql.DB) ([]WarmEntry, error) {
	var out []WarmEntry
	err := queryRows(ctx, db, `
		SELECT c.name, st.name, r.name, src.name, f.date, f.filename, f.etag
		FROM files f
		JOIN customers c ON c.id = f.customer_id
		JOIN sites st    ON st.id = f.site_id
		JOIN robots r    ON r.id  = f.robot_id
		JOIN sources src ON src.id = f.source_id
		ORDER BY f.id`,
		func(scan func(...any) error) error {
			var customer, site, robot, source, date, name, etag string
			if err := scan(&customer, &site, &robot, &source, &date, &name, &etag); err != nil {
				return err
			}
			out = append(out, WarmEntry{
				Key:  rebuildHiveKey(customer, site, robot, source, date, name),
				ETag: etag,
			})
			return nil
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func legacyWarmEntries(ctx context.Context, db *sql.DB) ([]WarmEntry, error) {
	var out []WarmEntry
	err := queryRows(ctx, db, `SELECT s3_key, s3_etag FROM files ORDER BY id`,
		func(scan func(...any) error) error {
			var key, etag string
			if err := scan(&key, &etag); err != nil {
				return err
			}
			out = append(out, WarmEntry{Key: key, ETag: etag})
			return nil
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}
