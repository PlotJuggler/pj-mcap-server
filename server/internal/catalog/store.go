// Package catalog implements the SQLite-backed file/topic/tag catalog.
//
// This REPLACES the former in-memory "catalog-lite" (deleted) with the Plan A
// SQLite-WAL catalog (2026-05-28-pj-cloud-server-v1.md Tasks 7-12). Reads happen
// on the connection pool (Store.DB()); writes are funneled through a single
// goroutine that runs each job inside a transaction — this eliminates SQLite
// writer contention without app-level locks. File ids are SQLite rowids and are
// STABLE across reindexes for unchanged keys (UpsertFile does an in-place UPDATE
// preserving id), which is the cursor-pagination + session-resolve contract.
//
// DESIGN NOTE (mcap_summary blob): the schema keeps the column (Plan A defines
// it) but this slice WRITES NULL. Chunk indexes stay ON-DEMAND via
// format.Codec.ChunkIndex per OpenSession — the DB never stored them before, a
// warm GetFile only needs files+topics+tags rows, and the session path rebuilds
// the chunk index per OpenSession. Storing the summary blob now buys nothing and
// risks staleness; the column is left for a later warm-GetFile slice.
package catalog

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"pj-cloud/server/internal/metrics"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no cgo)
)

//go:embed schema.sql
var schemaSQL string

// ErrStoreClosed is returned by Write when the Store has been Closed. A late
// Write can race shutdown (an indexer iteration finishing its extract just as
// Close fires); it must fail gracefully rather than panic on a send to the
// closed writer channel.
var ErrStoreClosed = errors.New("catalog: store is closed")

// errWriterPanic is returned to the caller whose WriteFn panicked. The panic is
// recovered per-job so it neither kills the single writer goroutine nor
// deadlocks the waiting caller (spec §8.1).
var errWriterPanic = errors.New("catalog: write job panicked (recovered; see logs)")

// Store wraps the SQLite handle + the catalog-writer goroutine plumbing.
type Store struct {
	db        *sql.DB
	writeCh   chan writeJob
	closeDone chan struct{}

	// closeMu guards the closed flag so Write can take a read lock to safely
	// enqueue while Close takes the write lock to flip closed + shut the channel.
	// This makes Write-after-Close return ErrStoreClosed (no panic) and Close
	// idempotent.
	closeMu sync.RWMutex
	closed  bool

	// readOnly marks a Store opened via OpenReadOnly: no writer goroutine runs
	// (writeCh/closeDone are nil), Write fails fast with ErrReadOnly, and Close
	// only closes the DB handle. This is the auryn-migration read path — the
	// Python builder is the sole writer, the Go server reads (catalog-migration
	// plan §2.1).
	readOnly bool

	// metrics + log, if set, drive per-write-job panic recovery (spec §8.1).
	// Set once via SetObservability right after Open; nil-safe (the Guard skips
	// the counter when metrics is nil and logs to slog.Default() when log is nil).
	metrics *metrics.Metrics
	log     *slog.Logger
}

// SetObservability wires the panic-recovery metrics + logger into the catalog
// writer goroutine. Call once after Open, before traffic. Optional: an unset
// Store still recovers writer-job panics (it just logs to slog.Default() and
// skips the counter).
func (s *Store) SetObservability(m *metrics.Metrics, log *slog.Logger) {
	s.metrics = m
	s.log = log
}

// WriteFn is a unit of work that runs inside a write transaction.
// Implementations must NOT spawn goroutines that touch tx, and must not call
// any Store.* method (that would deadlock on the single writer).
type WriteFn func(tx *sql.Tx) error

type writeJob struct {
	fn   WriteFn
	done chan error
}

// Open creates the SQLite file (if missing), applies the schema, and starts
// the catalog-writer goroutine. The returned Store must be Close()d.
func Open(ctx context.Context, dbPath string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	s := &Store{
		db:        db,
		writeCh:   make(chan writeJob, 64),
		closeDone: make(chan struct{}),
	}
	go s.runWriter()
	return s, nil
}

func (s *Store) runWriter() {
	defer close(s.closeDone)
	for job := range s.writeCh {
		s.runJob(job)
	}
}

// runJob executes one write job inside its own transaction with per-job panic
// recovery: a panicking WriteFn must NOT kill the single writer goroutine (that
// would deadlock every subsequent caller waiting on done) nor leave its own
// caller hanging — the recovered panic rolls the tx back and replies on done
// with errWriterPanic (spec §8.1).
func (s *Store) runJob(job writeJob) {
	var tx *sql.Tx
	panicked := metrics.Guard(s.metrics, s.log, "catalog-writer", func() {
		var err error
		tx, err = s.db.Begin()
		if err != nil {
			job.done <- fmt.Errorf("begin tx: %w", err)
			return
		}
		if err := job.fn(tx); err != nil {
			_ = tx.Rollback()
			tx = nil
			job.done <- err
			return
		}
		if err := tx.Commit(); err != nil {
			tx = nil
			job.done <- fmt.Errorf("commit tx: %w", err)
			return
		}
		tx = nil
		job.done <- nil
	})
	if panicked {
		// fn panicked after Begin but before reaching a done send (or even before
		// Begin): roll back any open tx and unblock the caller.
		if tx != nil {
			_ = tx.Rollback()
		}
		// done has buffer 1 and is written at most once; the non-blocking send is
		// a belt-and-braces guard against a panic that somehow happened after a
		// done send (it cannot, given the single-send paths above).
		select {
		case job.done <- errWriterPanic:
		default:
		}
	}
}

// Write enqueues a write job and waits for it to complete (or for ctx to be
// cancelled). All catalog writes go through this entry point. Each job runs in
// its OWN transaction (no batching); a slow extract is NOT inside the tx (the
// indexer extracts before calling Write).
//
// A read-only Store (OpenReadOnly) has no writer goroutine; Write returns
// ErrReadOnly immediately.
func (s *Store) Write(ctx context.Context, fn WriteFn) error {
	if s.readOnly {
		return ErrReadOnly
	}
	// Hold the read lock across the enqueue so Close cannot flip closed + shut the
	// channel between the guard check and the send (which would panic). The lock is
	// released before waiting on done so a slow write never blocks Close.
	s.closeMu.RLock()
	if s.closed {
		s.closeMu.RUnlock()
		return ErrStoreClosed
	}
	done := make(chan error, 1)
	select {
	case s.writeCh <- writeJob{fn: fn, done: done}:
		s.closeMu.RUnlock()
	case <-ctx.Done():
		s.closeMu.RUnlock()
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DB returns the underlying *sql.DB for read-only queries. Callers must NOT
// use this for writes (would race with the writer goroutine).
func (s *Store) DB() *sql.DB { return s.db }

// Close stops the writer and closes the database. It is idempotent: a second
// Close (a shutdown race) is a no-op rather than a double-close panic. After
// Close, Write returns ErrStoreClosed (or ErrReadOnly on a read-only Store,
// which checks readOnly first).
func (s *Store) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	// A read-only Store has no writer goroutine (writeCh/closeDone are nil): just
	// close the DB. Closing a nil channel / receiving from nil would panic/block.
	if s.readOnly {
		s.closeMu.Unlock()
		return s.db.Close()
	}
	close(s.writeCh)
	s.closeMu.Unlock()

	// Wait for the writer goroutine to drain in-flight jobs, then close the DB.
	<-s.closeDone
	return s.db.Close()
}
