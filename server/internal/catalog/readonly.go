package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// SchemaVersion pins the cross-language catalog contract (see CATALOG_CONTRACT.md).
// It MUST equal the Python builder's SCHEMA_VERSION
// (mcap_catalog/mcap_catalog_builder/db.py). OpenReadOnly fails fast when the DB it
// is handed carries a different version, so a catalog written by an incompatible
// builder can never be silently mis-served. BUMP BOTH SIDES IN LOCKSTEP on any
// change to a table/column the Go reader reads.
//
// v2 (M2): tags_embedded/tags_override/tags_effective override layer +
// files.chunk_count.
const SchemaVersion = 2

// ErrReadOnly is returned by Write on a Store opened via OpenReadOnly. Under the
// auryn migration the Python builder is the sole writer; the Go server reads.
var ErrReadOnly = errors.New("catalog: store is read-only (opened via OpenReadOnly)")

// SchemaVersionError reports a cross-language catalog schema-version mismatch (or a
// DB with no schema_version row at all — an unstamped / pre-contract DB). It is
// fatal at open: the reader refuses to serve a catalog it cannot interpret.
type SchemaVersionError struct {
	Path   string // DB path
	Got    int64  // version found in the DB (0 if no row / no table)
	Want   int    // version this reader speaks (SchemaVersion)
	Reason string // human detail (missing table, missing row, mismatch)
}

func (e *SchemaVersionError) Error() string {
	return fmt.Sprintf(
		"catalog: schema_version mismatch at %q: reader speaks %d, DB has %s (%s); "+
			"rebuild the catalog with a matching mcap_catalog builder",
		e.Path, e.Want, gotStr(e.Got), e.Reason)
}

func gotStr(got int64) string {
	if got == 0 {
		return "no version"
	}
	return fmt.Sprintf("%d", got)
}

// OpenReadOnly opens an EXISTING catalog DB written by the Python mcap_catalog
// builder, read-only, and verifies the cross-language contract version.
//
// Unlike Open it does NOT apply the embedded Go schema: the auryn schema is
// authoritative, and running `CREATE TABLE IF NOT EXISTS` here would silently
// diverge the two `files` definitions (the Go writer's columns differ from the
// auryn writer's). The DB must already exist (mode=ro errors otherwise — that is
// the intended fail-fast: the builder must have run first).
//
// The returned Store has no writer goroutine; Write returns ErrReadOnly and Close
// only closes the DB handle.
func OpenReadOnly(ctx context.Context, dbPath string) (*Store, error) {
	// mode=ro: open an existing DB read-only (no create, no schema mutation). WAL
	// readers still see committed writes from the external Python writer; foreign_keys
	// is harmless for reads. busy_timeout guards against a checkpoint contending.
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite (ro): %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite (ro) %q (did the builder run?): %w", dbPath, err)
	}
	if err := checkSchemaVersion(ctx, db, dbPath); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, readOnly: true}, nil
}

// checkSchemaVersion reads schema_version.version and fails fast unless it equals
// SchemaVersion. A missing table or missing row is itself a mismatch (an unstamped
// / pre-contract DB this reader must not serve).
func checkSchemaVersion(ctx context.Context, db *sql.DB, dbPath string) error {
	var got int64
	err := db.QueryRowContext(ctx, "SELECT version FROM schema_version WHERE id = 1").Scan(&got)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return &SchemaVersionError{Path: dbPath, Got: 0, Want: SchemaVersion, Reason: "no schema_version row"}
	case err != nil:
		// "no such table: schema_version" lands here — an unstamped/legacy DB.
		return &SchemaVersionError{Path: dbPath, Got: 0, Want: SchemaVersion, Reason: err.Error()}
	case got != SchemaVersion:
		return &SchemaVersionError{Path: dbPath, Got: got, Want: SchemaVersion, Reason: "version differs"}
	}
	return nil
}
