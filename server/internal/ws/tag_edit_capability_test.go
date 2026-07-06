package ws

import (
	"testing"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// TestHello_TagEditSupported_ReadOnlyStore is a regression pin (catalog-
// migration §2.6, replacing the old writable-store pin which no longer
// applies now that the Go catalog writer is gone): the catalog is always
// read-only, so with no tag-IPC forwarder configured, tag_edit_supported must
// be false — clients must never offer an "Edit Tags" UI that is guaranteed to
// fail at runtime.
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
// UpdateTags anyway (with no tag-IPC forwarder configured), the server
// rejects it with a clear operator-facing wire Error instead of a generic
// "UpdateTags failed" — the exact message (not just a substring) must name
// the read-only condition so client-side logs/UI are actionable.
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
// NEITHER set_tags NOR unset_keys must still be rejected when no tag-IPC
// forwarder is configured — handleUpdateTags rejects on tagIPC==nil alone, so
// an empty mutation can never "succeed" as a no-op even though
// tag_edit_supported=false, which would otherwise be an inconsistent (and
// confusing) capability story.
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
