package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// defaultBackoffs is the Plan A Task 14a retry schedule: exponential, capped at
// five retries (so a transient failure is tried at most six times). Both the S3
// and GCS arms share it through retryWith so there is ONE backoff policy.
var defaultBackoffs = []time.Duration{
	50 * time.Millisecond,
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
}

// retryWith runs fn with the default exponential backoff, distinguishing
// transient from permanent failures via classify. It is the shared retry the
// storage GetRange/Head/List bodies wrap their raw SDK calls in (Plan A Task 14b
// "Refactor the S3 retry to share the backoff"):
//
//   - classify maps fn's RAW return error onto an ErrTransient/ErrPermanent
//     sentinel (s3.go's classify, gcsreader.go's classifyGCS). retryWith only
//     inspects whether classify(err) Is-permanent — it does not itself wrap.
//   - a permanent error short-circuits immediately, returning fn's ORIGINAL
//     error (callers still get the un-rewrapped classify()'d message at the
//     call site).
//   - ctx cancellation during a backoff sleep returns ctx.Err() promptly.
//   - exhausting the schedule returns the LAST error wrapped as
//     "after N retries: <last>" (the sentinel survives via %w).
func retryWith(ctx context.Context, fn func(ctx context.Context) error, classify func(error) error) error {
	return retryWithSchedule(ctx, defaultBackoffs, fn, classify)
}

// retryWithSchedule is retryWith with an injectable backoff schedule (tests pass
// a near-zero one so they never sleep the real 50..800ms). Production callers go
// through retryWith (the default schedule).
func retryWithSchedule(ctx context.Context, backoffs []time.Duration, fn func(ctx context.Context) error, classify func(error) error) error {
	var last error
	for attempt := 0; attempt <= len(backoffs); attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		last = err
		if errors.Is(classify(err), ErrPermanent) {
			return err
		}
		if attempt == len(backoffs) {
			break
		}
		select {
		case <-time.After(backoffs[attempt]):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("after %d retries: %w", len(backoffs), last)
}
