package ws

import (
	"errors"
	"fmt"
	"testing"

	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// §7.1 of the 2026-07-30 event-discovery design: during the deletion-staleness
// window a catalog row can name a vanished object. Plan-build must surface
// that as ERROR_NOT_FOUND ("recording was deleted — refresh the list"), never
// as a generic bucket outage — and ONLY that case: auth-shaped permanents and
// transients stay ERROR_S3_UNAVAILABLE (outage-shaped).
func TestPlanBuildErrorCode(t *testing.T) {
	gone := fmt.Errorf("load chunk index for file 7: %w",
		fmt.Errorf("%w: %w: NoSuchKey", storage.ErrPermanent, storage.ErrNotFound))
	if got := planBuildErrorCode(gone); got != pb.ErrorCode_ERROR_NOT_FOUND {
		t.Errorf("vanished object: got %v, want ERROR_NOT_FOUND", got)
	}
	outage := fmt.Errorf("load chunk index for file 7: %w",
		fmt.Errorf("%w: connection refused", storage.ErrTransient))
	if got := planBuildErrorCode(outage); got != pb.ErrorCode_ERROR_S3_UNAVAILABLE {
		t.Errorf("outage: got %v, want ERROR_S3_UNAVAILABLE", got)
	}
	denied := fmt.Errorf("%w: AccessDenied", storage.ErrPermanent)
	if got := planBuildErrorCode(denied); got != pb.ErrorCode_ERROR_S3_UNAVAILABLE {
		t.Errorf("auth failure: got %v, want ERROR_S3_UNAVAILABLE", got)
	}
	if !errors.Is(gone, storage.ErrPermanent) {
		t.Error("dual-wrap must preserve ErrPermanent for existing retry logic")
	}
}
