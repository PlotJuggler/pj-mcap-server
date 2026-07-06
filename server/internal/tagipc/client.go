// Package tagipc is the Go forwarder side of D2 (the tag-edit write path,
// CATALOG_CONTRACT.md §10). The Go server is read-only in external-builder
// mode; the Python mcap_catalog builder is the catalog's sole writer and
// exposes a UNIX-socket HTTP endpoint for tag edits. Client is a thin
// transport over that endpoint — no business logic, no cloud SDKs, stdlib
// net/http only.
package tagipc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// ErrNotFound is returned when the addressed object key parses but names no
// cataloged file (HTTP 404 — CATALOG_CONTRACT.md §10, a lookup-only miss).
var ErrNotFound = errors.New("tagipc: no file with that key")

// ErrBusy is returned when the builder could not confirm the edit applied
// within its own deadline (HTTP 503). Not a guarantee the edit will never
// land — only that this request could not confirm it in time; the caller
// should retry.
var ErrBusy = errors.New("tagipc: tag edit service busy")

// Tag mirrors the wire Tag shape, decoded from the endpoint's JSON response.
type Tag struct {
	Key        string
	Value      string
	IsOverride bool
}

// requestTimeout bounds the total round trip (dial + write + read). The
// Python side's own IPC-thread wait is deadline_seconds+2s (default 5s+2s=7s,
// CATALOG_CONTRACT.md §10 "Deadline semantics"); 8s leaves that a margin
// before the Go side gives up and maps the failure to "service unavailable".
const requestTimeout = 8 * time.Second

// maxResponseBytes bounds how much of an error/success body this client will
// read into memory (mirrors the endpoint's own MAX_BODY_BYTES discipline).
const maxResponseBytes = 64 << 10

// Client forwards tag edits to the Python catalog builder's tag-edit IPC
// endpoint over a UNIX domain socket. One Client per configured socket path;
// safe for concurrent use (http.Client is).
type Client struct {
	httpClient *http.Client
}

// NewClient builds a Client that dials socketPath for every request.
// socketPath must already be a listening UNIX socket (the Python builder's
// tag_ipc server); this package never creates, binds, or health-checks it —
// a dial failure just surfaces as a generic error from UpdateTags.
func NewClient(socketPath string) *Client {
	dialer := &net.Dialer{}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	return &Client{httpClient: &http.Client{Transport: transport, Timeout: requestTimeout}}
}

// updateTagsRequest is the wire request body (CATALOG_CONTRACT.md §10):
// {"key", "set_tags", "unset_keys"}, forwarded VERBATIM from the caller — this
// client has no set/unset business logic of its own.
type updateTagsRequest struct {
	Key       string            `json:"key"`
	SetTags   map[string]string `json:"set_tags,omitempty"`
	UnsetKeys []string          `json:"unset_keys,omitempty"`
}

type tagWire struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	IsOverride bool   `json:"is_override"`
}

type updateTagsResponse struct {
	Tags []tagWire `json:"tags"`
}

// UpdateTags POSTs /update_tags over the UNIX socket and returns the file's
// full tags_effective after the edit (CATALOG_CONTRACT.md §10). set/unset are
// forwarded verbatim: Python's update_tags implements the same mask-on-unset
// semantics the legacy Go writer had.
//
// Errors: ErrNotFound (404), ErrBusy (503); any other non-200 status or
// transport failure (dial refused, timeout, malformed response, …) is
// returned as a generic error carrying the HTTP status (if any) and a body
// snippet — callers distinguish only ErrNotFound/ErrBusy and treat everything
// else the same way (CATALOG_CONTRACT.md §10's "the caller should retry").
func (c *Client) UpdateTags(ctx context.Context, key string, set map[string]string, unset []string) ([]Tag, error) {
	body, err := json.Marshal(updateTagsRequest{Key: key, SetTags: set, UnsetKeys: unset})
	if err != nil {
		return nil, fmt.Errorf("tagipc: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix/update_tags", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("tagipc: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tagipc: request failed: %w", err)
	}
	defer resp.Body.Close()

	// S3: read one byte past maxResponseBytes so a body that is truncated
	// EXACTLY at the limit is distinguishable from one that legitimately ends
	// there, and check the read error — a body read failure (connection reset
	// mid-response, etc.) must be a protocol failure, never a JSON parse of a
	// partial/absent body.
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if readErr != nil {
		return nil, fmt.Errorf("tagipc: read response body: %w", readErr)
	}
	truncated := len(respBody) > maxResponseBytes
	if truncated {
		respBody = respBody[:maxResponseBytes]
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if truncated {
			// A truncated 200 body could still parse as syntactically valid JSON
			// (e.g. cut cleanly after a complete array element) while silently
			// dropping trailing tags — a protocol failure, not a partial success.
			return nil, fmt.Errorf("tagipc: 200 response body exceeds %d bytes (truncated, not parsed): %s",
				maxResponseBytes, snippet(respBody))
		}
		var wire updateTagsResponse
		if err := json.Unmarshal(respBody, &wire); err != nil {
			return nil, fmt.Errorf("tagipc: decode 200 response: %w (body: %s)", err, snippet(respBody))
		}
		out := make([]Tag, 0, len(wire.Tags))
		for _, t := range wire.Tags {
			out = append(out, Tag{Key: t.Key, Value: t.Value, IsOverride: t.IsOverride})
		}
		return out, nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	case http.StatusServiceUnavailable:
		return nil, ErrBusy
	default:
		return nil, fmt.Errorf("tagipc: unexpected status %d: %s", resp.StatusCode, snippet(respBody))
	}
}

func snippet(b []byte) string {
	const max = 256
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}
