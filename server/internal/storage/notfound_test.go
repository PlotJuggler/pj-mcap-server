package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// TestHeadErrorFlowPreserves404Shape pins the property the 2026-08-10
// merge-gate review found broken: the error a store Head hands to
// disambiguate404 has passed through classify, and classify MUST preserve the
// typed SDK error / sentinel chain (a %v cause-wrap silently made the §7.1
// upgrade unreachable). These reproduce the stores' exact wrap order.
func TestHeadErrorFlowPreserves404Shape(t *testing.T) {
	// s3Store.Head's inner fn shape:
	s3err := classify(fmt.Errorf("head %q: %w", "k", &types.NotFound{}))
	if !isHead404(s3err) {
		t.Error("classified S3 HEAD 404 must still be recognizable by isHead404")
	}
	if !errors.Is(s3err, ErrPermanent) {
		t.Error("classified S3 HEAD 404 must be permanent")
	}
	// gcsStore.Head's inner fn shape:
	gcsErr := classifyGCS(fmt.Errorf("gcs attrs %q: %w", "k", gcs.ErrObjectNotExist))
	if !errors.Is(gcsErr, gcs.ErrObjectNotExist) {
		t.Error("classified GCS object-404 must still carry ErrObjectNotExist")
	}
	// End-to-end through the upgrade: bucket exists => ErrNotFound attaches.
	up := disambiguate404(context.Background(), s3err, isHead404(s3err),
		func(context.Context) error { return nil })
	if !errors.Is(up, ErrNotFound) {
		t.Error("classified S3 HEAD 404 + live bucket must upgrade to ErrNotFound")
	}
}
