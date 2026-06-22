// Package warm implements the A+ background chunk-index warmer
// (catalog-migration plan §3.2). It pre-fills the shared ChunkIndexCache by reading
// the catalog (read-only) and loading each file's chunk index with bounded
// concurrency, so a download's plan phase is an in-memory hit rather than a WAN
// summary read.
//
// It is READ-ONLY with respect to the catalog (the Python builder is the sole
// writer post-cutover; pre-cutover the Go indexer already pre-warms during its
// scan, so the warmer mostly finds the cache hot and skips). A per-file load
// failure is counted and skipped — one poison file never aborts the sweep.
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
	Log         *slog.Logger
	Metrics     *metrics.Metrics // optional
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
	for _, e := range entries {
		e := e
		if gctx.Err() != nil {
			break // parent cancelled (shutdown) — stop scheduling
		}
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil
			}
			// Already cached (e.g. the indexer warmed it) — skip the WAN read.
			if _, ok := w.Cache.Get(e.Key, e.ETag, 0); ok {
				skipped.Add(1)
				w.inc(func(m *metrics.Metrics) { m.ChunkIndexWarmSkipped.Inc() })
				return nil
			}
			// ETag-trust contract (mirrors the session path's cachedChunkIndex,
			// pre-dating this warmer): the load is by key and cached under the
			// CATALOG etag without re-verifying storage's observed etag. The catalog
			// etag is the trusted change-detect token, kept fresh by the indexer; a
			// stale row is a transient window (until the next poll) that re-indexing
			// closes by changing the etag (= a new cache key). Verifying per-load
			// here would diverge the warmer from the session path and need a Codec
			// signature change — out of scope. The warmer runs after the warm-start
			// scan, so the catalog is fresh when it warms.
			idx, err := w.Codec.ChunkIndex(gctx, w.Blob, e.Key, 0)
			if err != nil {
				// A poison file must NOT abort the sweep (return nil) — count + log.
				errored.Add(1)
				w.inc(func(m *metrics.Metrics) { m.ChunkIndexWarmErrors.Inc() })
				log.Warn("warm: chunk-index load failed", "key", e.Key, "err", err)
				return nil
			}
			w.Cache.Put(e.Key, e.ETag, idx)
			warmed.Add(1)
			w.inc(func(m *metrics.Metrics) { m.ChunkIndexWarmedTotal.Inc() })
			return nil
		})
	}
	_ = g.Wait() // per-file funcs never return non-nil; Wait surfaces only ctx errors we ignore

	log.Info("warm: chunk-index warm complete",
		"files", len(entries), "warmed", warmed.Load(), "skipped", skipped.Load(), "errors", errored.Load())
	return nil
}

// inc applies fn to the metrics set if it is non-nil.
func (w *Warmer) inc(fn func(*metrics.Metrics)) {
	if w.Metrics != nil {
		fn(w.Metrics)
	}
}
