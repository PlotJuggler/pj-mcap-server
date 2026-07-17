// Package catalog implements the read-only SQLite-backed file/topic/tag
// catalog reader. The Python mcap_catalog builder (mcap_catalog/) is the SOLE
// writer of the catalog DB (catalog-migration plan; CATALOG_CONTRACT.md); this
// package only ever opens it via OpenReadOnly and never mutates it. The
// former Go writer (catalog.Open, the write API, and the embedded legacy
// schema) was deleted in the M6 cutover (§2.6) once the harness fully moved to
// the Python builder — see git history for the removed writer implementation.
//
// File ids are the auryn schema's SQLite rowids, stable across an in-place
// builder rescan for unchanged keys but RENUMBERED by a full rebuild
// (CATALOG_CONTRACT.md §7) — which is why anything that carries an id across a
// request boundary must be generation-checked (the Snapshot lease below).
package catalog

import (
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no cgo)
)

// generationTokenLen is the wire size of a generation token: a 16-byte random
// server epoch + an 8-byte big-endian swap ordinal. Opaque bytes on the wire —
// clients only ever compare for equality.
const generationTokenLen = 16 + 8

// snapshot is one served catalog generation: the open handle, the (dev,inode)
// identity it was verified against, and the generation token — published and
// retired as ONE unit so no reader can ever observe a mixed pair (e.g. the new
// db with the old token). refs counts active leases plus one "current" ref held
// by the Store; the db closes when the last ref drops (drain-then-close — an
// in-flight leased request never sees "sql: database is closed" because of a
// swap).
type snapshot struct {
	db    *sql.DB
	ident fileIdentity
	gen   []byte // immutable once published
	refs  atomic.Int64
}

// release drops one reference; the last one out closes the db.
func (sn *snapshot) release() {
	if sn.refs.Add(-1) == 0 {
		_ = sn.db.Close()
	}
}

// Store wraps the SQLite read handle (opened via OpenReadOnly) + the
// atomic-publish reopen-on-swap plumbing (reopen.go). All generation-sensitive
// reads go through Acquire (a refcounted lease on one snapshot); DB() remains
// for single-shot reads (health ping, one-query lookups) where a transient
// closed-handle error on a swap boundary is acceptable.
type Store struct {
	// mu guards cur (the published snapshot pointer), closed, and curRefLive.
	// Acquire/swap/Close are all sub-microsecond critical sections.
	mu  sync.Mutex
	cur *snapshot
	// curRefLive is true while s.cur still holds an un-dropped "current" ref.
	// publish (on swap) and Close both want to drop that ref exactly once;
	// whichever flips this from true under s.mu does the single drop, so a
	// Close racing a ReopenIfSwapped can neither double-drop (closing a
	// still-leased db) nor leak the retired snapshot.
	curRefLive bool
	closed     bool

	// epoch is this server process's random 128-bit generation namespace; the
	// token is epoch||ordinal, so tokens can never collide across restarts and
	// every client-held token from a previous server run reads as stale.
	epoch [16]byte
	// ordinal counts verified swaps (starts at 1). Guarded by reopenMu (only the
	// swap path writes it).
	ordinal uint64

	// dbPath is the served catalog path this Store was opened from. Used by
	// ReopenIfSwapped to re-stat/reopen the same path.
	dbPath string

	// reopenMu serializes ReopenIfSwapped calls so at most one swap is ever
	// in-flight (the caller — the freshness-updater goroutine — already calls it
	// serially off a single ticker, but the mutex makes that a documented
	// invariant rather than an assumption).
	reopenMu sync.Mutex
}

// newGenToken renders epoch||big-endian(ordinal) as the opaque wire token.
func (s *Store) newGenToken(ordinal uint64) []byte {
	tok := make([]byte, generationTokenLen)
	copy(tok, s.epoch[:])
	binary.BigEndian.PutUint64(tok[16:], ordinal)
	return tok
}

// publish installs sn as the current snapshot (holding its "current" ref) and
// retires the previous one, whose db then closes once its last lease releases.
// If the Store has already been Closed, sn is NOT installed — its current ref is
// dropped so it closes cleanly (a ReopenIfSwapped that raced shutdown must not
// leak the db it just opened).
func (s *Store) publish(sn *snapshot) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		sn.release() // never became current; drop its ref so it closes
		return
	}
	old := s.cur
	oldLive := s.curRefLive
	s.cur = sn
	s.curRefLive = true
	s.mu.Unlock()
	if old != nil && oldLive {
		old.release() // drop the retired "current" ref; leases keep it alive until they finish
	}
}

// Snapshot is a refcounted lease on ONE catalog generation: its DB and
// Generation are guaranteed to describe the same served file, and the handle
// stays open (even across a ReopenIfSwapped) until Release. Always Release
// (defer it); Release is idempotent.
type Snapshot struct {
	sn       *snapshot
	released atomic.Bool
}

// DB returns the leased generation's handle. Valid until Release.
func (l *Snapshot) DB() *sql.DB { return l.sn.db }

// Generation returns the leased generation's opaque token (a copy; callers may
// retain it).
func (l *Snapshot) Generation() []byte {
	return append([]byte(nil), l.sn.gen...)
}

// Release drops the lease. Idempotent; after the last lease on a retired
// generation releases, its db handle is closed.
func (l *Snapshot) Release() {
	if l.released.Swap(true) {
		return
	}
	l.sn.release()
}

// Acquire leases the CURRENT snapshot: an atomic (db, generation) pair that a
// multi-query request pins once so a concurrent swap can neither mix
// generations within the request nor close the handle mid-flight. Callers MUST
// Release (defer immediately).
func (s *Store) Acquire() *Snapshot {
	s.mu.Lock()
	sn := s.cur
	if s.closed {
		// Shutting down: do NOT resurrect a possibly-zero-ref snapshot. Hand back
		// a pre-released lease over the (closed) handle — DB() then yields the
		// closed *sql.DB and the caller's query fails cleanly, the accepted
		// shutdown transient. No refcount is touched.
		s.mu.Unlock()
		l := &Snapshot{sn: sn}
		l.released.Store(true)
		return l
	}
	sn.refs.Add(1)
	s.mu.Unlock()
	return &Snapshot{sn: sn}
}

// Generation returns the CURRENT generation token (a copy). For a token that is
// guaranteed consistent with the queries of a multi-query request, use
// Acquire and read both from the lease instead.
func (s *Store) Generation() []byte {
	s.mu.Lock()
	gen := append([]byte(nil), s.cur.gen...)
	s.mu.Unlock()
	return gen
}

// DB returns the current *sql.DB for SINGLE-SHOT queries. Callers must not hold
// the returned handle across a query boundary (a swap may retire and close it —
// re-call DB(), or better, Acquire a Snapshot for multi-query reads).
func (s *Store) DB() *sql.DB {
	s.mu.Lock()
	db := s.cur.db
	s.mu.Unlock()
	return db
}

// identitySnapshot returns the current snapshot's verified (dev,inode).
func (s *Store) identitySnapshot() fileIdentity {
	s.mu.Lock()
	ident := s.cur.ident
	s.mu.Unlock()
	return ident
}

// Close retires the current snapshot. Idempotent: a second Close (a shutdown
// race) is a no-op. Held leases keep their handle alive until they Release
// (drain-then-close); the current ref is dropped exactly once (curRefLive),
// so a concurrent ReopenIfSwapped can't also drop it.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cur := s.cur
	live := s.curRefLive
	s.curRefLive = false
	s.mu.Unlock()
	if cur != nil && live {
		cur.release() // drop the "current" ref
	}
	return nil
}

// initSnapshots seeds the Store's epoch + first snapshot (ordinal 1) from the
// verified open. Called only by OpenReadOnly.
func (s *Store) initSnapshots(db *sql.DB, ident fileIdentity) error {
	if _, err := rand.Read(s.epoch[:]); err != nil {
		return fmt.Errorf("catalog: generation epoch: %w", err)
	}
	s.ordinal = 1
	first := &snapshot{db: db, ident: ident, gen: s.newGenToken(s.ordinal)}
	first.refs.Store(1) // the "current" ref
	s.cur = first
	s.curRefLive = true
	return nil
}
