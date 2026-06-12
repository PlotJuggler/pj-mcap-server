package indexer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"pj-cloud/server/internal/metrics"
)

// errIndexerPanic is recorded as the last-run error when an indexer iteration
// panics and is recovered (the stack is in the slog line; the panic counter
// moved). It surfaces on /dashboard/indexer.
var errIndexerPanic = errors.New("indexer: iteration panicked (recovered; see logs)")

// Loop runs Scanner.RunOnce on a fixed interval. The first call is synchronous
// (warm-start) so the server has a populated catalog before accepting traffic.
// On a warm DB (rows already present for the corpus) the warm-start emits
// unchanged==N, new==0, reindexed==0 — the smoke step-g persistence contract.
type Loop struct {
	Scanner     *Scanner
	Interval    time.Duration
	StartupScan bool
	Log         *slog.Logger
	// WarmStartTimeout bounds the synchronous warm-start scan only (the cold
	// startup full extract). Zero => no separate bound (uses the Start ctx).
	WarmStartTimeout time.Duration

	// Metrics, if set, records indexer run/failure/files-indexed counters and
	// guards each iteration against a panic (spec §8.1). Optional (nil in tests).
	Metrics *metrics.Metrics

	mu        sync.Mutex
	lastRun   time.Time
	lastErr   error
	lastStats RunStats

	RunCountForTest int64 // exported for tests
}

func (l *Loop) log() *slog.Logger {
	if l.Log != nil {
		return l.Log
	}
	return slog.Default()
}

// Start runs the warm-start synchronously (if configured) and then spawns the
// background ticker goroutine. Returns when warm-start completes. The background
// ticker is driven by ctx (the long-lived server context), NOT the warm-start's
// bounded context — so the ticker keeps polling after the warm-start window.
func (l *Loop) Start(ctx context.Context) error {
	if l.StartupScan {
		warmCtx := ctx
		var cancel context.CancelFunc
		if l.WarmStartTimeout > 0 {
			warmCtx, cancel = context.WithTimeout(ctx, l.WarmStartTimeout)
		}
		err := l.runAndRecord(warmCtx)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			return err
		}
	}
	go l.ticker(ctx)
	return nil
}

func (l *Loop) ticker(ctx context.Context) {
	t := time.NewTicker(l.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = l.runAndRecord(ctx)
		}
	}
}

func (l *Loop) runAndRecord(ctx context.Context) error {
	atomic.AddInt64(&l.RunCountForTest, 1)

	// Per-iteration panic recovery (spec §8.1): a panic in the scan (e.g. a
	// malformed object the codec choked on) is recovered, logged + counted, and
	// turned into a normal "this cycle failed" — the ticker keeps polling.
	var (
		stats    RunStats
		err      error
		panicked bool
	)
	panicked = metrics.Guard(l.Metrics, l.log(), "indexer", func() {
		stats, err = l.Scanner.RunOnce(ctx)
	})
	if panicked {
		err = errIndexerPanic
	}

	l.mu.Lock()
	l.lastRun = time.Now()
	l.lastErr = err
	l.lastStats = stats
	l.mu.Unlock()

	if l.Metrics != nil {
		l.Metrics.IndexerRunsTotal.Inc()
		if err != nil {
			l.Metrics.IndexerFailuresTotal.Inc()
		}
		if n := stats.NewFiles + stats.Reindexed; n > 0 {
			l.Metrics.IndexerFilesIndexed.Add(float64(n))
		}
	}

	if err != nil {
		l.log().Warn("indexer: run failed", "err", err)
	} else {
		l.log().Info("indexer: run complete",
			"scanned", stats.Scanned, "new", stats.NewFiles,
			"reindexed", stats.Reindexed, "unchanged", stats.Unchanged,
			"failed", stats.Failed, "duration_ms", stats.Duration.Milliseconds())
	}
	return err
}

// Status snapshots the last-run info (for a future dashboard).
func (l *Loop) Status() (lastRun time.Time, lastErr error, stats RunStats) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastRun, l.lastErr, l.lastStats
}
