package wire_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	hello := &pb.ClientMessage{
		RequestId: 7,
		Payload: &pb.ClientMessage_Hello{
			Hello: &pb.Hello{
				ProtocolVersion: 1,
				AuthToken:       "test-token",
			},
		},
	}

	encoded, err := proto.Marshal(hello)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded pb.ClientMessage
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.GetRequestId() != 7 {
		t.Errorf("request_id: got %d want 7", decoded.GetRequestId())
	}
	if decoded.GetHello().GetProtocolVersion() != 1 {
		t.Errorf("protocol_version: got %d want 1", decoded.GetHello().GetProtocolVersion())
	}
	if decoded.GetHello().GetAuthToken() != "test-token" {
		t.Errorf("auth_token: got %q want %q", decoded.GetHello().GetAuthToken(), "test-token")
	}
}

func TestOpenSessionOneofDiscrimination(t *testing.T) {
	resume := &pb.OpenSessionRequest{
		Mode: &pb.OpenSessionRequest_Resume{
			Resume: &pb.OpenResume{SubscriptionId: 42, ResumeAfterSeq: 17},
		},
	}
	encoded, err := proto.Marshal(resume)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded pb.OpenSessionRequest
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.GetResume() == nil {
		t.Fatal("expected resume mode")
	}
	if decoded.GetFresh() != nil {
		t.Fatal("expected fresh mode to be nil")
	}
}
