package ws

import (
	"context"
	"path/filepath"
	"testing"

	"pj-cloud/server/internal/catalog"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// TestHello_TagEditSupported_WritableStore is a regression pin: the legacy
// writable catalog (catalog.Open) still advertises tag_edit_supported=true —
// UpdateTags actually works there, so the capability must not be gated off.
func TestHello_TagEditSupported_WritableStore(t *testing.T) {
	store, err := catalog.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	resp := helloRoundTrip(t, newWSTestServer(t, store), "")
	hr := resp.GetHelloResponse()
	if hr == nil {
		t.Fatalf("expected HelloResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	if !hr.GetCapabilities().GetTagEditSupported() {
		t.Error("writable catalog must advertise tag_edit_supported=true")
	}
}

// TestHello_TagEditSupported_ReadOnlyStore proves Hello no longer hardcodes
// tag_edit_supported=true: over a catalog opened via OpenReadOnly (the
// external-builder auryn migration mode, where Write always fails with
// catalog.ErrReadOnly) it must advertise the capability as off, so clients
// never offer an "Edit Tags" UI that is guaranteed to fail at runtime.
func TestHello_TagEditSupported_ReadOnlyStore(t *testing.T) {
	store := openAurynReadStore(t)

	resp := helloRoundTrip(t, newWSTestServer(t, store), "")
	hr := resp.GetHelloResponse()
	if hr == nil {
		t.Fatalf("expected HelloResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	if hr.GetCapabilities().GetTagEditSupported() {
		t.Error("read-only catalog must advertise tag_edit_supported=false")
	}
}

// TestUpdateTags_ReadOnlyStore_WireError proves that even if a client ignores
// (or predates) the tag_edit_supported=false advertisement and sends
// UpdateTags anyway, the server maps catalog.ErrReadOnly to a clear
// operator-facing wire Error instead of a generic "UpdateTags failed" — the
// exact message (not just a substring) must name the read-only condition so
// client-side logs/UI are actionable. Asserting the literal (rather than just
// "contains read-only") also pins the mapping to the dedicated ErrReadOnly
// branch: catalog.ErrReadOnly.Error() itself contains "read-only", so a
// substring check alone would still pass via the generic fallback's details
// even if the dedicated branch were deleted.
func TestUpdateTags_ReadOnlyStore_WireError(t *testing.T) {
	store := openAurynReadStore(t) // seeds files with ids 1 and 2
	c := dialClient(t, newWSTestServer(t, store))
	c.hello()

	c.send(&pb.ClientMessage{
		RequestId: 2,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId:  1,
			SetTags: []*pb.Tag{{Key: "verified", Value: "yes"}},
		}},
	})
	resp := c.recv()
	e := resp.GetError()
	if e == nil {
		t.Fatalf("expected an Error frame, got %T", resp.GetPayload())
	}
	if e.GetCode() != pb.ErrorCode_ERROR_INVALID_REQUEST {
		t.Errorf("Error.Code = %v, want ERROR_INVALID_REQUEST", e.GetCode())
	}
	if e.GetMessage() != readOnlyTagEditMessage {
		t.Errorf("Error.Message = %q, want %q", e.GetMessage(), readOnlyTagEditMessage)
	}
}

// TestUpdateTags_ReadOnlyStore_EmptyMutation_WireError: an UpdateTags with
// NEITHER set_tags NOR unset_keys must still be rejected on a read-only
// store. Without a fast-path check, CatalogHandler.UpdateTags
// verifies the file, loops zero times over both empty slices, performs no
// writes, reads EffectiveTags, and returns success — so a client could "edit
// tags" as a no-op even though tag_edit_supported=false, which is an
// inconsistent (and confusing) capability story.
func TestUpdateTags_ReadOnlyStore_EmptyMutation_WireError(t *testing.T) {
	store := openAurynReadStore(t) // seeds files with ids 1 and 2
	c := dialClient(t, newWSTestServer(t, store))
	c.hello()

	c.send(&pb.ClientMessage{
		RequestId: 2,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId: 1,
			// No SetTags, no UnsetKeys: an empty mutation.
		}},
	})
	resp := c.recv()
	e := resp.GetError()
	if e == nil {
		t.Fatalf("expected an Error frame, got %T (an empty-mutation UpdateTags must not succeed on a read-only store)", resp.GetPayload())
	}
	if e.GetCode() != pb.ErrorCode_ERROR_INVALID_REQUEST {
		t.Errorf("Error.Code = %v, want ERROR_INVALID_REQUEST", e.GetCode())
	}
	if e.GetMessage() != readOnlyTagEditMessage {
		t.Errorf("Error.Message = %q, want %q", e.GetMessage(), readOnlyTagEditMessage)
	}
}
