package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
)

// fastBackoffs replaces the production [50,100,200,400,800]ms schedule with a
// near-zero one so the retry tests never sleep the real durations (the schedule
// LENGTH — 5 — is preserved so "exhausted" still means six attempts). The
// per-step delay is the only thing shortened; ordering/semantics are unchanged.
var fastBackoffs = []time.Duration{
	time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond,
}

// stubClassify maps a sentinel directly through (the test injects ErrTransient /
// ErrPermanent already). retryWith only cares whether classify(err) Is-permanent.
func stubClassify(err error) error {
	if err == nil {
		return nil
	}
	return err
}

// TestRetryWith_TransientThenSucceeds: a couple of transient failures then a
// success → retryWith returns nil, having called fn the right number of times.
func TestRetryWith_TransientThenSucceeds(t *testing.T) {
	calls := 0
	err := retryWithSchedule(context.Background(), fastBackoffs, func(context.Context) error {
		calls++
		if calls < 3 {
			return fmt.Errorf("blip %d: %w", calls, ErrTransient)
		}
		return nil
	}, stubClassify)
	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls (2 transient + 1 ok), got %d", calls)
	}
}

// TestRetryWith_PermanentReturnsImmediately: a permanent error short-circuits on
// the FIRST attempt — fn is called exactly once and the ORIGINAL error is
// returned (not the "after N retries" wrap).
func TestRetryWith_PermanentReturnsImmediately(t *testing.T) {
	calls := 0
	sentinel := fmt.Errorf("404 no such key: %w", ErrPermanent)
	err := retryWithSchedule(context.Background(), fastBackoffs, func(context.Context) error {
		calls++
		return sentinel
	}, stubClassify)
	if calls != 1 {
		t.Fatalf("permanent error must short-circuit after 1 call, got %d", calls)
	}
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected the permanent error back, got %v", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("permanent path must return the ORIGINAL error unwrapped, got %v", err)
	}
}

// TestRetryWith_ExhaustedWrapsLast: all attempts transient → after the schedule
// is exhausted retryWith returns an "after N retries" wrap of the LAST error.
func TestRetryWith_ExhaustedWrapsLast(t *testing.T) {
	calls := 0
	err := retryWithSchedule(context.Background(), fastBackoffs, func(context.Context) error {
		calls++
		return fmt.Errorf("attempt %d: %w", calls, ErrTransient)
	}, stubClassify)
	// 1 initial + len(schedule) retries.
	wantCalls := 1 + len(fastBackoffs)
	if calls != wantCalls {
		t.Fatalf("expected %d calls (1 + %d retries), got %d", wantCalls, len(fastBackoffs), calls)
	}
	if err == nil {
		t.Fatal("exhausted retries must return an error")
	}
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("exhausted error must wrap the last transient error, got %v", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("after %d retries", len(fastBackoffs))) {
		t.Fatalf("exhausted error must mention the retry count, got %q", err.Error())
	}
}

// TestRetryWith_CtxCancelMidBackoff: cancelling the context while retryWith is
// sleeping between attempts returns ctx.Err() promptly. We use a LONG schedule
// so the cancel lands during the first sleep deterministically (no flake).
func TestRetryWith_CtxCancelMidBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	longBackoffs := []time.Duration{10 * time.Second}
	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- retryWithSchedule(ctx, longBackoffs, func(context.Context) error {
			calls++
			return fmt.Errorf("blip: %w", ErrTransient)
		}, stubClassify)
	}()
	// Let fn fail once and enter the (10s) backoff, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retryWith did not return promptly after ctx cancel during backoff")
	}
	if calls != 1 {
		t.Fatalf("ctx cancel during the first backoff means exactly 1 fn call, got %d", calls)
	}
}

// TestRetryWith_DefaultScheduleEntry exercises the exported retryWith (the
// default-schedule wrapper the production GetRange/Head/List use) so the wiring
// — not just the schedule core — is covered. A single permanent error returns
// immediately, proving retryWith threads classify through.
func TestRetryWith_DefaultScheduleEntry(t *testing.T) {
	calls := 0
	err := retryWith(context.Background(), func(context.Context) error {
		calls++
		return fmt.Errorf("denied: %w", ErrPermanent)
	}, stubClassify)
	if calls != 1 || !errors.Is(err, ErrPermanent) {
		t.Fatalf("retryWith default schedule: calls=%d err=%v", calls, err)
	}
}

// apiErr is a minimal stand-in for an S3/smithy APIError carrying an error code,
// mirroring gcsreader_test.go's googleAPIErr. classify (non-test code) reaches it
// via errors.As on smithy.APIError, so it never names this test-only type.
type apiErr struct {
	code string
	msg  string
}

func (e *apiErr) Error() string                 { return e.code + ": " + e.msg }
func (e *apiErr) ErrorCode() string             { return e.code }
func (e *apiErr) ErrorMessage() string          { return e.msg }
func (e *apiErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// TestS3_ClassifyErrors mirrors gcsreader_test.go's TestGCS_ClassifyErrors for
// the S3 arm: the permanent codes (NoSuchKey/NotFound/NoSuchBucket/AccessDenied/
// Forbidden/InvalidAccessKeyId*) map to ErrPermanent; everything else (throttle,
// 5xx-class, network) defaults to ErrTransient.
func TestS3_ClassifyErrors(t *testing.T) {
	if classify(nil) != nil {
		t.Error("nil must classify to nil")
	}
	permanentCodes := []string{
		"NoSuchKey", "NotFound", "NoSuchBucket", "AccessDenied",
		"Forbidden", "InvalidAccessKeyId", "InvalidAccessKeyIdExtra",
	}
	for _, code := range permanentCodes {
		if !errors.Is(classify(&apiErr{code: code, msg: "x"}), ErrPermanent) {
			t.Errorf("code %q should be permanent", code)
		}
	}
	transientCodes := []string{"SlowDown", "InternalError", "ServiceUnavailable", "RequestTimeout"}
	for _, code := range transientCodes {
		if !errors.Is(classify(&apiErr{code: code, msg: "x"}), ErrTransient) {
			t.Errorf("code %q should be transient", code)
		}
	}
	if !errors.Is(classify(errors.New("dial tcp: connection refused")), ErrTransient) {
		t.Error("an unclassified network error should default to transient")
	}
}
