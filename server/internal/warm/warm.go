// Package warm implements the A+ background chunk-index warmer
// (catalog-migration plan §3.2). It pre-fills the shared ChunkIndexCache by reading
// the catalog (read-only) and loading each file's chunk index with bounded
// concurrency, so a download's plan phase is an in-memory hit rather than a WAN
// summary read.
//
// It is READ-ONLY with respect to the catalog (the Python builder is the sole
// writer; the Go catalog writer + in-process indexer were deleted in the M6
// cutover, so this warmer is the chunk-index cache's only warm source). A
// per-file load failure is counted and skipped — one poison file never aborts
// the sweep.
package warm

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/storage"
)

// Warmer pre-fills the chunk-index cache from the catalog.
type Warmer struct {
	Store       *catalog.Store
	Codec       format.Codec
	Blob        storage.BlobStore
	Cache       *format.ChunkIndexCache
	Concurrency int // bounded WAN concurrency; default 4
	// Budget bounds the sweep to roughly one cache-worth of NEWLY-warmed bytes
	// (set to the cache's byte cap). Past it, warming more only evicts what was
	// just warmed — a net-zero WAN cost — so the sweep stops and lets cold misses
	// on the request path fill the hot set. 0 = warm the whole catalog.
	Budget  int64
	Log     *slog.Logger
	Metrics *metrics.Metrics // optional
}

// Run warms every cataloged file's chunk index once. It blocks until the sweep
// finishes or ctx is cancelled (graceful shutdown). Safe to call in a background
// goroutine; it never writes the catalog. Returns an error only if the initial
// catalog list fails — per-file failures are counted, not propagated.
func (w *Warmer) Run(ctx context.Context) error {
	if w.Cache == nil {
		return nil // no cache => nothing to warm
	}
	log := w.Log
	if log == nil {
		log = slog.Default()
	}
	entries, err := catalog.WarmEntries(ctx, w.Store)
	if err != nil {
		return fmt.Errorf("warm: list catalog: %w", err)
	}
	conc := w.Concurrency
	if conc <= 0 {
		conc = 4
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(conc)
	var warmed, skipped, errored atomic.Int64
	var warmedBytes atomic.Int64 // cumulative bytes of NEWLY-warmed indexes this sweep
	scheduled := 0
	for _, e := range entries {
		e := e
		if gctx.Err() != nil {
			break // parent cancelled (shutdown) — stop scheduling
		}
		// Bounded warm: once we've warmed ~one cache-worth of bytes, warming more
		// only evicts what we just warmed — a net-zero WAN cost. Budget on CUMULATIVE
		// warmed bytes (not the cache's current size, which eviction keeps just under
		// the cap and would never trip a "full" check). g.SetLimit paces this loop, so
		// the check runs between (throttled) schedules and overshoots by at most `conc`.
		if w.Budget > 0 && warmedBytes.Load() >= w.Budget {
			break
		}
		scheduled++
		g.Go(func() error {
			// The codec parses UNTRUSTED bucket bytes; a panic on one pathological
			// file must count as a per-file error, not escape the errgroup goroutine
			// (which would kill the whole server — Run is launched as a bare
			// goroutine in main).
			defer func() {
				if r := recover(); r != nil {
					errored.Add(1)
					w.inc(func(m *metrics.Metrics) { m.ChunkIndexWarmErrors.Inc() })
					log.Warn("warm: chunk-index load panicked", "key", e.Key, "panic", r)
				}
			}()
			if gctx.Err() != nil {
				return nil
			}
			// Already cached (e.g. an earlier sweep, or an in-flight session's own
			// cold-miss load) — skip the WAN read.
			if _, ok := w.Cache.Get(e.Key, e.ETag, 0); ok {
				skipped.Add(1)
				w.inc(func(m *metrics.Metrics) { m.ChunkIndexWarmSkipped.Inc() })
				return nil
			}
			// ETag-trust contract (mirrors the session path's cachedChunkIndex,
			// pre-dating this warmer): the load is by key and cached under the
			// CATALOG etag without re-verifying storage's observed etag. The catalog
			// etag is the trusted change-detect token, kept fresh by the Python
			// builder's rescan; a stale row is a transient window (until the next
			// rescan) that re-cataloging closes by changing the etag (= a new cache
			// key). Verifying per-load here would diverge the warmer from the
			// session path and need a Codec signature change — out of scope.
			idx, err := w.Codec.ChunkIndex(gctx, w.Blob, e.Key, 0)
			if err != nil {
				// A poison file must NOT abort the sweep (return nil) — count + log.
				errored.Add(1)
				w.inc(func(m *metrics.Metrics) { m.ChunkIndexWarmErrors.Inc() })
				log.Warn("warm: chunk-index load failed", "key", e.Key, "err", err)
				return nil
			}
			w.Cache.Put(e.Key, e.ETag, idx)
			warmedBytes.Add(int64(idx.ApproxBytes()))
			warmed.Add(1)
			w.inc(func(m *metrics.Metrics) { m.ChunkIndexWarmedTotal.Inc() })
			return nil
		})
	}
	_ = g.Wait() // per-file funcs never return non-nil; Wait surfaces only ctx errors we ignore

	unwarmed := len(entries) - scheduled // not warmed: the byte budget was reached first
	log.Info("warm: chunk-index warm complete",
		"files", len(entries), "warmed", warmed.Load(), "skipped", skipped.Load(),
		"errors", errored.Load(), "unwarmed_over_budget", unwarmed,
		"warmed_bytes", warmedBytes.Load())
	return nil
}

// inc applies fn to the metrics set if it is non-nil.
func (w *Warmer) inc(fn func(*metrics.Metrics)) {
	if w.Metrics != nil {
		fn(w.Metrics)
	}
}
