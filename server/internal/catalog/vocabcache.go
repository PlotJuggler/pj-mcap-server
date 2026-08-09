package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// Revision identifies the catalog CONTENT a derived result was computed from.
//
// Generation alone is not enough: it changes only when an atomic publish swaps
// the served file by (dev,inode), and a production catalog grows IN PLACE via
// WAL (pj_cloud_catalog_reopens_total stays 0 while files_scanned climbs), so a
// generation-keyed cache would serve stale counts indefinitely.
//
// PRAGMA data_version closes that gap: SQLite bumps it whenever ANOTHER
// connection commits — exactly our topology, since the Python builder is a
// separate process — and deliberately NOT for commits on our own connection, so
// our reads never self-invalidate. Verified 2026-08-09 with modernc v1.34.1, the
// production read-only DSN, MaxOpenConns(1), journal_mode=wal: unchanged across
// our own reads, moved on another connection's commit, moved on a separate
// PROCESS commit, and the reader saw the external rows.
//
// Comparable by design — used directly as a cache key.
type Revision struct {
	Generation  string
	DataVersion int64
}

// ErrNoCatalog is returned when a revision is requested before any catalog has
// been published (degraded start).
var ErrNoCatalog = errors.New("catalog not yet available")

func readDataVersion(ctx context.Context, db *sql.DB) (int64, error) {
	var v int64
	if err := db.QueryRowContext(ctx, `PRAGMA data_version`).Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}

// ReadRevision reads the revision of an ALREADY-LEASED snapshot. The caller owns
// the lease and must use that same lease for the computation, or the result
// could describe a different file than the revision claims.
func ReadRevision(ctx context.Context, lease *Snapshot) (Revision, error) {
	db := lease.DB()
	if db == nil {
		return Revision{}, ErrNoCatalog
	}
	dv, err := readDataVersion(ctx, db)
	if err != nil {
		return Revision{}, err
	}
	return Revision{Generation: string(lease.Generation()), DataVersion: dv}, nil
}

// VocabularyCache memoizes GetVocabulary per (Revision, VocabOptions).
//
// The uncached call is a whole-catalog aggregation — measured 2.54 s full /
// 0.43 s picker-only at 8.8M files — and it ran on EVERY request. That is why
// browse latency felt random: it depended entirely on whether anyone had
// browsed recently.
//
// A strictly-fresh cache would not fix that. The builder commits continuously,
// so after any idle period the revision has almost always moved and the next
// browse would miss precisely when it matters. So within ONE generation a stale
// entry is served IMMEDIATELY and refreshed behind the caller.
//
// Across generations it never serves stale: dimension ids are rowids that
// renumber on republish, so a stale tree would hand out ids naming other rows —
// exactly what ERROR_STALE_CATALOG exists to prevent.
type VocabularyCache struct {
	staleFor time.Duration

	mu      sync.Mutex
	entries map[VocabOptions]*vocabEntry
	// epoch orders completed computations. data_version is connection-local and
	// NOT a cross-connection sequence, so it must never be used for ordering; a
	// monotonic counter taken when a computation STARTS is.
	epoch atomic.Uint64

	sf singleflight.Group

	computations atomic.Int64
}

type vocabEntry struct {
	rev      Revision
	val      *Vocabulary
	storedAt time.Time
	// epoch orders COMPLETED computations so a slow one cannot overwrite a value
	// produced by a later one. It must be our own monotonic counter:
	// data_version is connection-local and not a cross-connection sequence.
	epoch uint64
}

// NewVocabularyCache builds a cache whose same-generation entries may be served
// up to staleFor old while a refresh runs. staleFor == 0 disables stale serving.
func NewVocabularyCache(staleFor time.Duration) *VocabularyCache {
	return &VocabularyCache{staleFor: staleFor, entries: map[VocabOptions]*vocabEntry{}}
}

// Computations reports how many times the aggregation actually ran (cache
// misses + background refreshes). Test- and metrics-facing.
func (c *VocabularyCache) Computations() int64 {
	if c == nil {
		return 0
	}
	return c.computations.Load()
}

// Get returns the vocabulary for `opts` and the generation its dimension ids
// belong to. The returned *Vocabulary is SHARED and must be treated as
// immutable — callers marshal it, they must not mutate it.
func (c *VocabularyCache) Get(ctx context.Context, s *Store, opts VocabOptions) (*Vocabulary, []byte, error) {
	lease := s.Acquire()
	defer lease.Release()
	if lease.DB() == nil {
		return nil, nil, ErrNoCatalog
	}
	if c == nil {
		// Uncached path, for callers that build a bare handler (tests). Living
		// HERE rather than at the call site keeps ONE implementation of "fetch the
		// vocabulary and the generation its ids belong to".
		v, err := GetVocabularyDB(ctx, lease.DB(), opts)
		if err != nil {
			return nil, nil, err
		}
		return v, lease.Generation(), nil
	}
	rev, err := ReadRevision(ctx, lease)
	if err != nil {
		return nil, nil, err
	}
	gen := lease.Generation()

	// compute takes its OWN lease per invocation. It must NOT capture the lease
	// above: the stale-serve path fires this on a background goroutine that
	// outlives Get's `defer lease.Release()`, and once the last lease on a retired
	// snapshot drops, its handle is closed — the refresh would then spend the full
	// aggregation only to end in "sql: database is closed", leaving the entry
	// stale so the next request repeats it. Each invocation is still PINNED to one
	// snapshot for its whole duration, which is what the revision claim needs.
	v, err := c.get(ctx, rev, opts, func(cctx context.Context) (*Vocabulary, error) {
		l := s.Acquire()
		defer l.Release()
		if l.DB() == nil {
			return nil, ErrNoCatalog
		}
		return GetVocabularyDB(cctx, l.DB(), opts)
	})
	if err != nil {
		return nil, nil, err
	}
	return v, gen, nil
}

// get is the pure cache logic, separated from lease handling so it can be tested
// against a synthetic compute function.
func (c *VocabularyCache) get(
	ctx context.Context, rev Revision, opts VocabOptions,
	compute func(context.Context) (*Vocabulary, error),
) (*Vocabulary, error) {
	c.mu.Lock()
	e := c.entries[opts]
	if e != nil && e.rev == rev {
		v := e.val
		c.mu.Unlock()
		return v, nil
	}
	// Same generation, newer content, still inside the stale window: hand back
	// what we have and refresh out of band. This is the case the cache exists
	// for — a browse after an idle period must not wait.
	if e != nil && c.staleFor > 0 && e.rev.Generation == rev.Generation &&
		time.Since(e.storedAt) < c.staleFor {
		v := e.val
		c.mu.Unlock()
		// Refresh behind the caller. No dedup flag needed: computeAndStore goes
		// through singleflight on the same (rev, opts) key, so concurrent stale
		// hits join one computation. Detached context — the requester already has
		// its answer and the refresh must not die with that request.
		go func() { _, _ = c.computeAndStore(context.Background(), rev, opts, compute) }()
		return v, nil
	}
	c.mu.Unlock()
	return c.computeAndStore(ctx, rev, opts, compute)
}

func (c *VocabularyCache) computeAndStore(
	ctx context.Context, rev Revision, opts VocabOptions,
	compute func(context.Context) (*Vocabulary, error),
) (*Vocabulary, error) {
	key := cacheKey(rev, opts)
	epoch := c.epoch.Add(1)
	v, err, _ := c.sf.Do(key, func() (any, error) {
		c.computations.Add(1)
		val, cerr := compute(ctx)
		if cerr != nil {
			return nil, cerr
		}
		c.store(rev, opts, val, epoch)
		return val, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Vocabulary), nil
}

func (c *VocabularyCache) store(rev Revision, opts VocabOptions, val *Vocabulary, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A slow computation must never overwrite a value produced by a LATER one.
	// Ordering is by our own monotonic epoch, never by data_version (which is
	// connection-local and not a sequence).
	if e := c.entries[opts]; e != nil && e.epoch > epoch {
		return
	}
	c.entries[opts] = &vocabEntry{rev: rev, val: val, storedAt: time.Now(), epoch: epoch}
}

func cacheKey(rev Revision, opts VocabOptions) string {
	// Deliberately keyed on (generation, options) WITHOUT data_version. The
	// builder commits continuously, so two clients arriving either side of one
	// commit would otherwise get different keys and BOTH run the full aggregation,
	// serialized on the single pinned connection — k clients, k x 2.44 s — which
	// is precisely the stampede singleflight is here to stop. Generation stays in
	// the key because ids renumber across it and those results must never merge.
	// The winner stores under the revision it actually read; losers accept a
	// value that may be one commit older, which the stale window already allows.
	return fmt.Sprintf("%s|%t%t%t", rev.Generation,
		opts.OmitSources, opts.OmitTagFacets, opts.OmitSiteRobotCounts)
}
