package storage

import (
	"errors"
	"fmt"
	"testing"
)

// TestDisambiguate404 pins the §7.1 upgrade rule: a 404-shaped Head error
// carries ErrNotFound ONLY when the bucket is confirmed to exist; anything
// ambiguous stays as-is (fail-closed to the outage-shaped code), the probe
// runs only when it can change the outcome, and ErrPermanent is always
// preserved for existing retry logic.
func TestDisambiguate404(t *testing.T) {
	base := fmt.Errorf("%w: head \"k\": NotFound", ErrPermanent)

	probed := false
	got := disambiguate404(base, true, func() bool { probed = true; return true })
	if !probed {
		t.Error("bucket probe should run for a 404-shaped error")
	}
	if !errors.Is(got, ErrNotFound) || !errors.Is(got, ErrPermanent) {
		t.Errorf("bucket exists: want ErrNotFound + ErrPermanent, got %v", got)
	}

	got = disambiguate404(base, true, func() bool { return false })
	if errors.Is(got, ErrNotFound) {
		t.Error("bucket missing/ambiguous: must NOT carry ErrNotFound")
	}
	if !errors.Is(got, ErrPermanent) {
		t.Error("bucket missing: ErrPermanent must be preserved")
	}

	probed = false
	got = disambiguate404(base, false, func() bool { probed = true; return true })
	if probed {
		t.Error("non-404 error: the probe must not run")
	}
	if errors.Is(got, ErrNotFound) {
		t.Error("non-404 error: must NOT carry ErrNotFound")
	}

	already := fmt.Errorf("%w: %w: NoSuchKey", ErrPermanent, ErrNotFound)
	probed = false
	got = disambiguate404(already, true, func() bool { probed = true; return true })
	if probed {
		t.Error("already-precise error: the probe must not run")
	}
	if !errors.Is(got, ErrNotFound) {
		t.Error("already-precise error: ErrNotFound must be preserved")
	}

	if disambiguate404(nil, true, func() bool { return true }) != nil {
		t.Error("nil error must stay nil")
	}
}
