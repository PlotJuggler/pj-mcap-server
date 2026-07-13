// Package catalog implements the read-only SQLite-backed file/topic/tag
// catalog reader. The Python mcap_catalog builder (mcap_catalog/) is the SOLE
// writer of the catalog DB (catalog-migration plan; CATALOG_CONTRACT.md); this
// package only ever opens it via OpenReadOnly and never mutates it. The
// former Go writer (catalog.Open, the write API, and the embedded legacy
// schema) was deleted in the M6 cutover (§2.6) once the harness fully moved to
// the Python builder — see git history for the removed writer implementation.
//
// File ids are the auryn schema's SQLite rowids, stable across a builder
// rescan/rebuild for unchanged keys (CATALOG_CONTRACT.md §7), which is the
// cursor-pagination + session-resolve contract.
package catalog

import (
	"database/sql"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no cgo)
)

// Store wraps the SQLite read handle (opened via OpenReadOnly) + the
// atomic-publish reopen-on-swap plumbing (reopen.go).
type Store struct {
	// dbPtr holds the current *sql.DB. It is an atomic.Pointer (not a bare field)
	// because the Store can be SWAPPED underneath in-flight readers when the
	// Python builder atomically replaces the served catalog file (catalog-migration
	// §6.2a / CATALOG_CONTRACT.md §9 "Publish & reopen protocol"): ReopenIfSwapped
	// stores a freshly opened+verified handle here and closes the old one. DB() is
	// the single public read accessor; every catalog read in the codebase goes
	// through it, so a swap is transparent to callers that re-fetch DB() per query
	// (which they all do — no caller holds a *sql.DB across a query boundary).
	//
	// A goroutine that already grabbed the old *sql.DB from DB() just before a swap
	// may see a "sql: database is closed" (or similar) error on that in-flight
	// query once the old handle is Close()d below — this is an accepted, transient
	// race: the caller's NEXT query calls DB() again and lands on the new handle.
	dbPtr atomic.Pointer[sql.DB]

	// closeMu guards the closed flag so Close is idempotent: a second Close (a
	// shutdown race) is a no-op rather than a double-close panic/error.
	closeMu sync.Mutex
	closed  bool

	// dbPath is the served catalog path this Store was opened from. Used by
	// ReopenIfSwapped to re-stat/reopen the same path.
	dbPath string

	// identity is the (dev, inode) of the file backing the current dbPtr handle,
	// captured atomically with verification (openVerified). A pointer (not a bare
	// struct) so it can be stored/loaded atomically alongside dbPtr.
	identity atomic.Pointer[fileIdentity]

	// reopenMu serializes ReopenIfSwapped calls so at most one swap is ever
	// in-flight (the caller — the freshness-updater goroutine — already calls it
	// serially off a single ticker, but the mutex makes that a documented
	// invariant rather than an assumption).
	reopenMu sync.Mutex
}

// DB returns the underlying *sql.DB for queries. Callers must not hold the
// returned handle across a call to ReopenIfSwapped — always re-call DB() to
// get the current handle (every query in this codebase already does: none
// stash a *sql.DB across an await point).
func (s *Store) DB() *sql.DB { return s.dbPtr.Load() }

// Close closes the database handle. It is idempotent: a second Close (a
// shutdown race) is a no-op rather than a double-close panic.
func (s *Store) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.DB().Close()
}
