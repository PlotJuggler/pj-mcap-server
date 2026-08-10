package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestDisambiguate404 pins the §7.1 upgrade rule: a 404-shaped Head error
// carries ErrNotFound ONLY when the bucket is confirmed to exist (probe
// returns nil); anything ambiguous stays as-is (fail-closed to the
// outage-shaped code), the probe runs only when it can change the outcome,
// runs under a live context despite a cancelled caller (error-path
// classification must not be aborted by a bailing caller), and ErrPermanent
// is always preserved for existing retry logic.
func TestDisambiguate404(t *testing.T) {
	ctx := context.Background()
	base := fmt.Errorf("%w: head \"k\": NotFound", ErrPermanent)
	exists := func(context.Context) error { return nil }
	missing := func(context.Context) error { return errors.New("no bucket") }

	probed := false
	got := disambiguate404(ctx, base, true, func(pc context.Context) error {
		probed = true
		if pc.Err() != nil {
			t.Error("probe context must be live")
		}
		return nil
	})
	if !probed {
		t.Error("bucket probe should run for a 404-shaped error")
	}
	if !errors.Is(got, ErrNotFound) || !errors.Is(got, ErrPermanent) {
		t.Errorf("bucket exists: want ErrNotFound + ErrPermanent, got %v", got)
	}

	// A cancelled caller must not veto the probe (WithoutCancel).
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	got = disambiguate404(cancelled, base, true, func(pc context.Context) error {
		return pc.Err() // nil iff detached from the cancelled parent
	})
	if !errors.Is(got, ErrNotFound) {
		t.Error("cancelled caller: probe must run detached and still upgrade")
	}

	got = disambiguate404(ctx, base, true, missing)
	if errors.Is(got, ErrNotFound) {
		t.Error("bucket missing/ambiguous: must NOT carry ErrNotFound")
	}
	if !errors.Is(got, ErrPermanent) {
		t.Error("bucket missing: ErrPermanent must be preserved")
	}

	probed = false
	got = disambiguate404(ctx, base, false, func(context.Context) error {
		probed = true
		return nil
	})
	if probed {
		t.Error("non-404 error: the probe must not run")
	}
	if errors.Is(got, ErrNotFound) {
		t.Error("non-404 error: must NOT carry ErrNotFound")
	}

	already := fmt.Errorf("%w: %w: NoSuchKey", ErrPermanent, ErrNotFound)
	probed = false
	got = disambiguate404(ctx, already, true, func(context.Context) error {
		probed = true
		return nil
	})
	if probed {
		t.Error("already-precise error: the probe must not run")
	}
	if !errors.Is(got, ErrNotFound) {
		t.Error("already-precise error: ErrNotFound must be preserved")
	}

	if disambiguate404(ctx, nil, true, exists) != nil {
		t.Error("nil error must stay nil")
	}
}
