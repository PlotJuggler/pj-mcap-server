package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
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
// v3 (M6): build_metadata table (catalog-freshness / swap-detection).
const SchemaVersion = 3

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
// only closes the DB handle. The Store also records the served path + the opened
// file's (dev, inode) identity so ReopenIfSwapped (reopen.go) can later detect an
// atomic-publish rebuild (CATALOG_CONTRACT.md §9) and reopen transparently.
func OpenReadOnly(ctx context.Context, dbPath string) (*Store, error) {
	db, ident, err := openVerified(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	s := &Store{readOnly: true, dbPath: dbPath}
	s.dbPtr.Store(db)
	s.identity.Store(&ident)
	return s, nil
}

// fileIdentity is the (device, inode) pair identifying a physical file on disk —
// stable across in-place WAL mutation, but DIFFERENT after an os.replace (the
// catalog builder's atomic-publish rebuild swaps in a new inode at the same
// path). This is the swap-detection primitive per CATALOG_CONTRACT.md §9: the
// trigger is file identity, never build_metadata.build_id.
//
// Linux-only (this server is linux-deployed): extracted via syscall.Stat_t, kept
// to this one helper so a future portability need only touches one spot.
type fileIdentity struct {
	Dev uint64
	Ino uint64
}

// statIdentity stats path and extracts its (dev, inode) identity.
func statIdentity(path string) (fileIdentity, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return fileIdentity{}, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, fmt.Errorf("statIdentity: no syscall.Stat_t for %q (unsupported platform)", path)
	}
	return fileIdentity{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

// errIdentityRace is the internal sentinel openVerified uses to distinguish "the
// file was concurrently replaced while we were opening it" (retryable, bounded)
// from a real verification failure such as a schema-version mismatch (NOT
// retried here — the caller, e.g. ReopenIfSwapped, decides what a persistent
// verify failure means; retrying it internally would just mask it under the
// same "still racing" story).
var errIdentityRace = errors.New("catalog: file identity changed during open (concurrent replace)")

// identityRaceAttempts bounds the C2 stat-open-stat retry loop. Three attempts
// with a short sleep is generous for a rename that completes in microseconds;
// it exists only to defeat the pathological case of opening exactly astride an
// os.replace.
const identityRaceAttempts = 3

// identityRaceBackoff is the sleep between identity-race retries.
const identityRaceBackoff = 20 * time.Millisecond

// openVerified opens dbPath read-only, pins the connection pool to a single
// physical connection (C1), verifies the schema_version contract, and confirms
// the opened handle actually corresponds to the file it stat'd (C2) — all
// before returning. It is the shared core of OpenReadOnly and ReopenIfSwapped so
// both go through the identical open+verify+identity sequence.
//
// C1 — pin the pool: database/sql may lazily open NEW physical connections by
// path at any time; if the pool were unpinned, a query after the served file was
// replaced could silently open the NEW file on a fresh connection with NO
// schema/build verification, mixing generations within a single Store. Catalog
// queries are all sub-ms, so serializing them behind one physical connection is
// an accepted cost, not a scalability problem.
//
// C2 — identity belongs to the opened handle: a bare stat before Open (with no
// recheck after) would race a concurrent os.replace — the stat could observe the
// OLD file while Open/verify actually lands on a half-published NEW one, or vice
// versa. The stat-open-verify-stat-compare sequence closes that window: if the
// two stats disagree, the file changed under us mid-open and we retry from
// scratch (bounded) rather than trust a possibly-mismatched identity.
func openVerified(ctx context.Context, dbPath string) (*sql.DB, fileIdentity, error) {
	var lastErr error
	for attempt := 1; attempt <= identityRaceAttempts; attempt++ {
		db, ident, err := openVerifiedOnce(ctx, dbPath)
		if err == nil {
			return db, ident, nil
		}
		lastErr = err
		if !errors.Is(err, errIdentityRace) {
			// A real verification failure (bad schema_version, ping failure, missing
			// file): propagate immediately. Retrying it here would just re-observe
			// the same failure under a misleading "race" story.
			return nil, fileIdentity{}, err
		}
		if attempt < identityRaceAttempts {
			time.Sleep(identityRaceBackoff)
		}
	}
	return nil, fileIdentity{}, fmt.Errorf("openVerified %q: identity race persisted after %d attempts: %w",
		dbPath, identityRaceAttempts, lastErr)
}

// openVerifiedOnce is one stat-open-verify-stat attempt (see openVerified for the
// C1/C2 rationale). Returns errIdentityRace (wrapped) when the pre/post stat
// disagree; any other error is a genuine, non-retryable verification failure.
func openVerifiedOnce(ctx context.Context, dbPath string) (*sql.DB, fileIdentity, error) {
	s1, err := statIdentity(dbPath)
	if err != nil {
		return nil, fileIdentity{}, fmt.Errorf("stat %q: %w", dbPath, err)
	}

	// mode=ro: open an existing DB read-only (no create, no schema mutation). WAL
	// readers still see committed writes from the external Python writer; foreign_keys
	// is harmless for reads. busy_timeout guards against a checkpoint contending.
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fileIdentity{}, fmt.Errorf("open sqlite (ro): %w", err)
	}
	// C1: pin to exactly one physical connection, held open indefinitely, so no
	// later query can silently open a second connection against a since-replaced
	// file without going through this verification.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	// Force the single physical connection open now (not lazily on first query),
	// so verification below runs against the connection we are about to pin.
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fileIdentity{}, fmt.Errorf("ping sqlite (ro) %q (did the builder run?): %w", dbPath, err)
	}
	if err := checkSchemaVersion(ctx, db, dbPath); err != nil {
		_ = db.Close()
		return nil, fileIdentity{}, err
	}

	s2, err := statIdentity(dbPath)
	if err != nil {
		_ = db.Close()
		return nil, fileIdentity{}, fmt.Errorf("stat %q (post-verify): %w", dbPath, err)
	}
	if s1 != s2 {
		_ = db.Close()
		return nil, fileIdentity{}, fmt.Errorf("%w: %+v -> %+v", errIdentityRace, s1, s2)
	}
	return db, s1, nil
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
