package ws

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// newAuthTestServer stands up a catalog-only WS handler with the given shared
// token ("" => dev-anonymous) over an httptest server. Auth happens at Hello
// before any catalog/session work, so the fixture catalog's actual content is
// irrelevant here — openAurynReadStore (vocabulary_test.go) is reused as a
// convenient, already-valid auryn store.
func newAuthTestServer(t *testing.T, token string) string {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cat := openAurynReadStore(t)

	h := NewHandler(cat, token, log)
	mux := http.NewServeMux()
	mux.Handle("/api/ws", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws"
}

// helloRoundTrip dials, sends a Hello with authToken, and returns the server's
// first frame.
func helloRoundTrip(t *testing.T, url, authToken string) *pb.ServerMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)

	hello := &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 2, AuthToken: authToken}},
	}
	data, err := proto.Marshal(hello)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	_, raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp pb.ServerMessage
	if err := proto.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &resp
}

// TestHello_WrongTokenRejected: a server configured with a shared token rejects a
// Hello carrying the wrong token with Error{ERROR_AUTH_FAILED}.
func TestHello_WrongTokenRejected(t *testing.T) {
	url := newAuthTestServer(t, "correct-secret")
	resp := helloRoundTrip(t, url, "wrong-secret")
	e := resp.GetError()
	if e == nil {
		t.Fatalf("expected an Error frame, got %T", resp.GetPayload())
	}
	if e.GetCode() != pb.ErrorCode_ERROR_AUTH_FAILED {
		t.Fatalf("expected ERROR_AUTH_FAILED, got %v (%q)", e.GetCode(), e.GetMessage())
	}
	if e.GetMessage() != "invalid bearer token" {
		t.Errorf("auth error message: got %q want %q", e.GetMessage(), "invalid bearer token")
	}
}

// TestHello_EmptyTokenAlsoRejectedWhenConfigured: with a token configured, an
// EMPTY presented token must NOT be treated as dev-anonymous — it is just a
// wrong credential and is rejected (the constant-time compare fails).
func TestHello_EmptyTokenAlsoRejectedWhenConfigured(t *testing.T) {
	url := newAuthTestServer(t, "correct-secret")
	resp := helloRoundTrip(t, url, "")
	if resp.GetError() == nil || resp.GetError().GetCode() != pb.ErrorCode_ERROR_AUTH_FAILED {
		t.Fatalf("empty token against a configured server must be rejected, got %T (%v)", resp.GetPayload(), resp.GetError())
	}
}

// TestHello_CorrectTokenAccepted: the matching token yields a HelloResponse.
func TestHello_CorrectTokenAccepted(t *testing.T) {
	url := newAuthTestServer(t, "correct-secret")
	resp := helloRoundTrip(t, url, "correct-secret")
	if resp.GetHelloResponse() == nil {
		t.Fatalf("expected HelloResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
}

// expectServerClose asserts the next read observes a SERVER-initiated close (or
// EOF) rather than idling until the local budget expires: a local
// context.DeadlineExceeded means the server left the connection open.
func expectServerClose(t *testing.T, conn *websocket.Conn, budget time.Duration, what string) {
	t.Helper()
	rctx, rcancel := context.WithTimeout(context.Background(), budget)
	defer rcancel()
	_, _, err := conn.Read(rctx)
	if err == nil {
		t.Fatalf("%s: expected the server to close the connection, got a frame", what)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%s: connection still open (local read timeout, no server close)", what)
	}
}

// TestHello_WrongTokenClosesConnection: after the AUTH_FAILED error frame the
// server must CLOSE the socket — a peer with a bad token must not be able to
// retry Hello forever (or hold the connection) inside one connection.
func TestHello_WrongTokenClosesConnection(t *testing.T) {
	url := newAuthTestServer(t, "correct-secret")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)

	hello := &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 2, AuthToken: "wrong-secret"}},
	}
	data, _ := proto.Marshal(hello)
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	_, raw, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read error frame: %v", err)
	}
	var resp pb.ServerMessage
	if err := proto.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.GetError().GetCode() != pb.ErrorCode_ERROR_AUTH_FAILED {
		t.Fatalf("expected ERROR_AUTH_FAILED first, got %T (%v)", resp.GetPayload(), resp.GetError())
	}
	expectServerClose(t, conn, 3*time.Second, "post-auth-failure")
}

// TestHello_DeadlineClosesUnauthenticated: a connection that never sends a
// successful Hello is closed at the Hello deadline instead of lingering until
// the keepalive reaper notices.
func TestHello_DeadlineClosesUnauthenticated(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cat := openAurynReadStore(t)
	h := NewHandler(cat, "correct-secret", log)
	h.SetHelloDeadline(200 * time.Millisecond)
	mux := http.NewServeMux()
	mux.Handle("/api/ws", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	// Send nothing: the server must close at the deadline.
	expectServerClose(t, conn, 3*time.Second, "hello-deadline")
}

// TestHello_DevAnonymousAcceptsAll: a server with NO token (dev-anonymous) accepts
// a Hello regardless of the presented token — including the empty one the smoke
// harness's clients send. This is the behavior the regression gate depends on.
func TestHello_DevAnonymousAcceptsAll(t *testing.T) {
	url := newAuthTestServer(t, "")
	for _, tok := range []string{"", "anything", "ignored"} {
		resp := helloRoundTrip(t, url, tok)
		if resp.GetHelloResponse() == nil {
			t.Fatalf("dev-anonymous must accept token %q, got %T (err=%v)", tok, resp.GetPayload(), resp.GetError())
		}
	}
}
