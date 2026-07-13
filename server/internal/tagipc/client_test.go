package tagipc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// newUnixTestServer binds a REAL unix-socket listener (net.Listen("unix", …))
// and fronts it with httptest so the test exercises the exact same HTTP/1.x
// framing the Python endpoint speaks, not an in-process fake.
func newUnixTestServer(t *testing.T, handler http.HandlerFunc) (srv *httptest.Server, sockPath string) {
	t.Helper()
	sockPath = filepath.Join(t.TempDir(), "tagipc.sock")
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix %q: %v", sockPath, err)
	}
	srv = httptest.NewUnstartedServer(handler)
	srv.Listener = l
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, sockPath
}

func TestUpdateTags_200_RequestShapeAndResponse(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody []byte
	_, sock := newUnixTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tags":[{"key":"mission","value":"inv","is_override":true}]}`))
	})

	c := NewClient(sock)
	key := "customer=a/customer_site=s/robot=r/source=x/date=2026-01-01/f.mcap"
	tags, err := c.UpdateTags(context.Background(), key,
		map[string]string{"mission": "inv"}, []string{"stale"})
	if err != nil {
		t.Fatalf("UpdateTags: %v", err)
	}

	if gotMethod != http.MethodPost || gotPath != "/update_tags" {
		t.Fatalf("request line = %s %s, want POST /update_tags", gotMethod, gotPath)
	}

	// Assert the REQUEST body shape matches CATALOG_CONTRACT.md §10 exactly:
	// {"key", "set_tags", "unset_keys"}.
	var reqBody struct {
		Key       string            `json:"key"`
		SetTags   map[string]string `json:"set_tags"`
		UnsetKeys []string          `json:"unset_keys"`
	}
	if err := json.Unmarshal(gotBody, &reqBody); err != nil {
		t.Fatalf("decode request body: %v (body=%s)", err, gotBody)
	}
	if reqBody.Key != key {
		t.Errorf("request key = %q, want %q", reqBody.Key, key)
	}
	if len(reqBody.SetTags) != 1 || reqBody.SetTags["mission"] != "inv" {
		t.Errorf("request set_tags = %v, want {mission: inv}", reqBody.SetTags)
	}
	if len(reqBody.UnsetKeys) != 1 || reqBody.UnsetKeys[0] != "stale" {
		t.Errorf("request unset_keys = %v, want [stale]", reqBody.UnsetKeys)
	}

	if len(tags) != 1 || tags[0] != (Tag{Key: "mission", Value: "inv", IsOverride: true}) {
		t.Errorf("tags = %+v, want [{mission inv true}]", tags)
	}
}

func TestUpdateTags_404_ErrNotFound(t *testing.T) {
	_, sock := newUnixTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"unknown key"}`))
	})
	c := NewClient(sock)
	_, err := c.UpdateTags(context.Background(), "some/key.mcap", nil, nil)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateTags err = %v, want ErrNotFound", err)
	}
}

func TestUpdateTags_503_ErrBusy(t *testing.T) {
	_, sock := newUnixTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	})
	c := NewClient(sock)
	_, err := c.UpdateTags(context.Background(), "some/key.mcap", nil, nil)
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("UpdateTags err = %v, want ErrBusy", err)
	}
}

func TestUpdateTags_NoSocket_ConnectionError(t *testing.T) {
	c := NewClient(filepath.Join(t.TempDir(), "nonexistent.sock"))
	_, err := c.UpdateTags(context.Background(), "some/key.mcap", nil, nil)
	if err == nil {
		t.Fatal("UpdateTags with no listener: expected an error, got nil")
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrBusy) {
		t.Errorf("UpdateTags err = %v, want a generic connection error (not ErrNotFound/ErrBusy)", err)
	}
}

// TestUpdateTags_200_TruncatedBody_ProtocolError is the S3 regression: a 200
// body longer than maxResponseBytes must be treated as a protocol failure,
// never parsed (a body truncated exactly at the limit could still coincide
// with syntactically-valid-looking JSON and silently drop trailing tags).
func TestUpdateTags_200_TruncatedBody_ProtocolError(t *testing.T) {
	pad := make([]byte, maxResponseBytes+1024)
	for i := range pad {
		pad[i] = ' '
	}
	var body []byte
	body = append(body, []byte(`{"tags":[`)...)
	body = append(body, pad...)
	body = append(body, []byte(`]}`)...)

	_, sock := newUnixTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	c := NewClient(sock)
	tags, err := c.UpdateTags(context.Background(), "some/key.mcap", nil, nil)
	if err == nil {
		t.Fatalf("expected a protocol-failure error for a body exceeding %d bytes, got tags=%+v, nil error", maxResponseBytes, tags)
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrBusy) {
		t.Errorf("UpdateTags err = %v, want a generic protocol error (not ErrNotFound/ErrBusy)", err)
	}
}

// TestUpdateTags_200_BodyReadError_ProtocolFailure is the S3 read-error
// regression: the server promises (via Content-Length) more bytes than it
// actually sends before closing the connection, so io.ReadAll surfaces a read
// error mid-body — this must be returned as a protocol failure, never fed
// (partially) into json.Unmarshal.
func TestUpdateTags_200_BodyReadError_ProtocolFailure(t *testing.T) {
	_, sock := newUnixTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000") // promises far more than is actually written
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tags":`)) // connection then closes, well short of 1000 bytes
	})
	c := NewClient(sock)
	tags, err := c.UpdateTags(context.Background(), "some/key.mcap", nil, nil)
	if err == nil {
		t.Fatalf("expected a body-read protocol error, got tags=%+v, nil error", tags)
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrBusy) {
		t.Errorf("UpdateTags err = %v, want a generic protocol error (not ErrNotFound/ErrBusy)", err)
	}
}

func TestUpdateTags_400_GenericError(t *testing.T) {
	_, sock := newUnixTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"malformed"}`))
	})
	c := NewClient(sock)
	_, err := c.UpdateTags(context.Background(), "some/key.mcap", nil, nil)
	if err == nil {
		t.Fatal("expected an error for 400")
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrBusy) {
		t.Errorf("UpdateTags err = %v, want a generic error (not ErrNotFound/ErrBusy)", err)
	}
}
