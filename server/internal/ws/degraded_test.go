package ws

// Degraded-start behavior at the WS layer: the server is up before the Python
// builder has published its first catalog (catalog.NewAwaiting store). The
// Hello handshake must still succeed (capabilities fall back to the derived-key
// floor), and every catalog-dependent RPC must fail fast with the retryable
// ERROR_CATALOG_UNAVAILABLE — never a panic, never a hung request.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	"pj-cloud/server/internal/catalog"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

func newAwaitingWSServer(t *testing.T) string {
	t.Helper()
	st, err := catalog.NewAwaiting(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("NewAwaiting: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return newWSTestServer(t, st)
}

// dialDegraded dials, completes the Hello handshake (asserting it SUCCEEDS),
// and returns the live conn for follow-up RPCs.
func dialDegraded(t *testing.T, url string) (*websocket.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	conn.SetReadLimit(1 << 20)

	hello := &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 2}},
	}
	writeMsg(t, ctx, conn, hello)
	resp := readMsg(t, ctx, conn)
	if resp.GetError() != nil {
		t.Fatalf("Hello on a degraded server must succeed, got Error: %v", resp.GetError())
	}
	if resp.GetHelloResponse() == nil {
		t.Fatalf("expected HelloResponse, got %T", resp.GetPayload())
	}
	return conn, ctx
}

func writeMsg(t *testing.T, ctx context.Context, conn *websocket.Conn, m *pb.ClientMessage) {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readMsg(t *testing.T, ctx context.Context, conn *websocket.Conn) *pb.ServerMessage {
	t.Helper()
	_, raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var resp pb.ServerMessage
	if err := proto.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &resp
}

func assertCatalogUnavailable(t *testing.T, resp *pb.ServerMessage, rpc string) {
	t.Helper()
	e := resp.GetError()
	if e == nil {
		t.Fatalf("%s on degraded server: expected Error frame, got %T", rpc, resp.GetPayload())
	}
	if e.GetCode() != pb.ErrorCode_ERROR_CATALOG_UNAVAILABLE {
		t.Fatalf("%s on degraded server: code = %v (%q), want ERROR_CATALOG_UNAVAILABLE",
			rpc, e.GetCode(), e.GetMessage())
	}
}

func TestDegraded_HelloSucceedsCatalogRPCsUnavailable(t *testing.T) {
	url := newAwaitingWSServer(t)
	conn, ctx := dialDegraded(t, url)

	writeMsg(t, ctx, conn, &pb.ClientMessage{
		RequestId: 2,
		Payload:   &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{}},
	})
	assertCatalogUnavailable(t, readMsg(t, ctx, conn), "ListFiles")

	writeMsg(t, ctx, conn, &pb.ClientMessage{
		RequestId: 3,
		Payload:   &pb.ClientMessage_GetVocabulary{GetVocabulary: &pb.GetVocabularyRequest{}},
	})
	assertCatalogUnavailable(t, readMsg(t, ctx, conn), "GetVocabulary")

	key := "customer=x/f.mcap"
	writeMsg(t, ctx, conn, &pb.ClientMessage{
		RequestId: 4,
		Payload:   &pb.ClientMessage_GetFile{GetFile: &pb.GetFileRequest{S3Key: &key}},
	})
	assertCatalogUnavailable(t, readMsg(t, ctx, conn), "GetFile")

	writeMsg(t, ctx, conn, &pb.ClientMessage{
		RequestId: 5,
		Payload: &pb.ClientMessage_OpenSession{OpenSession: &pb.OpenSessionRequest{
			Mode: &pb.OpenSessionRequest_Fresh{Fresh: &pb.OpenFresh{S3Keys: []string{key}}},
		}},
	})
	assertCatalogUnavailable(t, readMsg(t, ctx, conn), "OpenSession")

	writeMsg(t, ctx, conn, &pb.ClientMessage{
		RequestId: 6,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			S3Key: &key,
		}},
	})
	assertCatalogUnavailable(t, readMsg(t, ctx, conn), "UpdateTags")
}
