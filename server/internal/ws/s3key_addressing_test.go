package ws

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/tagipc"
	pb "pj-cloud/server/internal/wire/pj_cloud"

	"google.golang.org/protobuf/proto"
)

// s3key_addressing_test.go covers the wire s3_key addressing added to
// GetFileRequest / UpdateTagsRequest: post-M6, file ids RENUMBER on every
// external-builder rebuild (the Store hot-swaps generations via
// ReopenIfSwapped), so a wire file_id resolved from an earlier ListFiles is a
// generation-scoped handle that can silently name a DIFFERENT file minutes
// later. s3_key (= FileSummary.s3_key) is the stable identity; when present
// it WINS and file_id is ignored. Fixtures reused from tag_edit_ipc_test.go:
// tfGenerationA has key K (tfSharedDims) at file_id=1; tfGenerationB has K at
// file_id=2 with an override tag, while file_id=1 is a DIFFERENT file Y
// ("other-b.mcap") — the exact renumber hazard the key closes.

// swapToGenerationB atomically publishes generation B over the served catalog
// path (the builder's temp+rename idiom) and forces the store's reopen — the
// same effect as the production 30s freshness tick observing the (dev,inode)
// change.
func swapToGenerationB(t *testing.T, store *catalog.Store, path string, buildGenB func(t *testing.T, path string)) {
	t.Helper()
	genB := filepath.Join(filepath.Dir(path), "genB-swap.db")
	buildGenB(t, genB)
	if err := os.Rename(genB, path); err != nil {
		t.Fatalf("swap-in generation B: %v", err)
	}
	swapped, err := store.ReopenIfSwapped(context.Background())
	if err != nil {
		t.Fatalf("ReopenIfSwapped: %v", err)
	}
	if !swapped {
		t.Fatal("ReopenIfSwapped = false, want true (generation B was renamed into place)")
	}
}

// getFile sends one GetFile request and returns the raw server message.
func (c *wsClient) getFile(reqID uint64, req *pb.GetFileRequest) *pb.ServerMessage {
	c.t.Helper()
	c.send(&pb.ClientMessage{RequestId: reqID, Payload: &pb.ClientMessage_GetFile{GetFile: req}})
	return c.recv()
}

// TestGetFile_KeyAddressing_SurvivesRebuildRenumber is THE review-required
// case, GetFile half: after a rebuild renumbers key K from file_id=1 to
// file_id=2 (and re-issues id 1 to a DIFFERENT file Y), a key-addressed
// GetFile still returns K's file — while the very same request id-addressed
// returns Y. The second assertion deliberately documents the stale-id hazard
// the key closes: it is the CURRENT (hazardous) legacy behavior, not a bug in
// this change.
func TestGetFile_KeyAddressing_SurvivesRebuildRenumber(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	tfGenerationA(t, path) // key K = file_id 1

	store, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	url := newWSTestServer(t, store)
	c := dialClient(t, url)
	c.hello()

	keyK := tfSharedDims.key()

	// Generation A sanity: id 1 IS key K (this is where a client would have
	// resolved its id from — its "last ListFiles").
	if got := c.getFile(2, &pb.GetFileRequest{FileId: 1}).GetGetFile(); got == nil ||
		got.GetSummary().GetS3Key() != keyK {
		t.Fatalf("generation A file_id=1 = %v, want key %q", got, keyK)
	}

	// The rebuild lands: K renumbers 1 -> 2, id 1 now denotes file Y.
	swapToGenerationB(t, store, path, tfGenerationB)

	// (i) Key-addressed (with the stale id still attached, as a real client
	// would send it): must return K's file — the key wins, the id is ignored.
	resp := c.getFile(3, &pb.GetFileRequest{FileId: 1, S3Key: proto.String(keyK)})
	gf := resp.GetGetFile()
	if gf == nil {
		t.Fatalf("key-addressed GetFile: expected GetFileResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	if got := gf.GetSummary().GetS3Key(); got != keyK {
		t.Errorf("key-addressed GetFile s3_key = %q, want %q (key addressing must survive the renumber)", got, keyK)
	}
	if got := gf.GetSummary().GetId(); got != 2 {
		t.Errorf("key-addressed GetFile id = %d, want 2 (K's CURRENT generation-B id)", got)
	}
	if tags := gf.GetSummary().GetTags(); len(tags) != 1 ||
		tags[0].GetKey() != "mission" || tags[0].GetValue() != "B-shared-override" || !tags[0].GetIsOverride() {
		t.Errorf("key-addressed GetFile tags = %+v, want [{mission B-shared-override override=true}] (generation B's data)", tags)
	}

	// (i, hazard documentation) The same request id-addressed: returns Y — the
	// WRONG file from the client's point of view. This is exactly the silent
	// misaddressing the s3_key field exists to prevent.
	resp = c.getFile(4, &pb.GetFileRequest{FileId: 1})
	gf = resp.GetGetFile()
	if gf == nil {
		t.Fatalf("id-addressed GetFile: expected GetFileResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	wantY := tfDims{Customer: "beta", Site: "s2", Robot: "r2", Source: "synthetic", Date: "2026-02-02", Filename: "other-b.mcap"}.key()
	if got := gf.GetSummary().GetS3Key(); got != wantY {
		t.Errorf("id-addressed GetFile after the rebuild s3_key = %q, want %q (file Y — documenting the stale-id hazard)", got, wantY)
	}
}

// TestUpdateTags_KeyAddressing_SurvivesRebuildRenumber is THE review-required
// case, UpdateTags half: after the renumber, a key-addressed UpdateTags (with
// the stale id 1 still attached, now denoting file Y) must forward K — the
// client's key, verbatim — to the tag IPC, NOT Y's key (which an id->key
// phase-1 resolve would have produced). The response must be the key-based
// re-read from the current generation.
func TestUpdateTags_KeyAddressing_SurvivesRebuildRenumber(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	tfGenerationA(t, path)

	store, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// The rebuild lands BEFORE the request: id 1 now denotes Y; K is id 2.
	swapToGenerationB(t, store, path, tfGenerationB)

	keyK := tfSharedDims.key()
	var gotBody []byte
	sock := newFakeTagIPCServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tags":[{"key":"mission","value":"IPC-SENTINEL","is_override":true}]}`))
	})
	url := newWSTestServerWithTagIPC(t, store, tagipc.NewClient(sock))

	c := dialClient(t, url)
	c.hello()
	c.send(&pb.ClientMessage{
		RequestId: 2,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId:  1, // STALE: now denotes file Y — must be ignored
			S3Key:   proto.String(keyK),
			SetTags: []*pb.Tag{{Key: "mission", Value: "updated"}},
		}},
	})
	resp := c.recv()
	ut := resp.GetUpdateTags()
	if ut == nil {
		t.Fatalf("expected UpdateTagsResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}

	// The IPC must have been addressed with K (the client's key, verbatim) —
	// an id->key resolve of the stale id 1 would have produced Y's key.
	var reqBody struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(gotBody, &reqBody); err != nil {
		t.Fatalf("decode forwarded request: %v (body=%s)", err, gotBody)
	}
	if reqBody.Key != keyK {
		t.Errorf("forwarded key = %q, want %q (X's key; forwarding Y's key means the stale file_id was consulted)",
			reqBody.Key, keyK)
	}

	// Response = the key-based re-read from the CURRENT generation (never the
	// IPC sentinel, never Y's tags).
	if tags := ut.GetEffectiveTags(); len(tags) != 1 ||
		tags[0].GetKey() != "mission" || tags[0].GetValue() != "B-shared-override" || !tags[0].GetIsOverride() {
		t.Errorf("effective tags = %+v, want [{mission B-shared-override override=true}]", ut.GetEffectiveTags())
	}
}

// TestGetFile_EmptyS3Key_InvalidRequest: a PRESENT but empty s3_key is an
// explicit ERROR_INVALID_REQUEST with the exact pinned message — never a
// silent fallback to file_id (the FileId here is valid; falling back would
// "succeed").
func TestGetFile_EmptyS3Key_InvalidRequest(t *testing.T) {
	url := newWSTestServer(t, openAurynReadStore(t))
	c := dialClient(t, url)
	c.hello()

	resp := c.getFile(2, &pb.GetFileRequest{FileId: 1, S3Key: proto.String("")})
	e := resp.GetError()
	if e == nil {
		t.Fatalf("expected an Error frame, got %T (empty-present s3_key must NEVER fall back to file_id)", resp.GetPayload())
	}
	if e.GetCode() != pb.ErrorCode_ERROR_INVALID_REQUEST {
		t.Errorf("Error.Code = %v, want ERROR_INVALID_REQUEST", e.GetCode())
	}
	if e.GetMessage() != "s3_key must be non-empty" {
		t.Errorf("Error.Message = %q, want %q", e.GetMessage(), "s3_key must be non-empty")
	}
}

// TestUpdateTags_EmptyS3Key_InvalidRequest: same contract for UpdateTags; the
// tag IPC must never be contacted.
func TestUpdateTags_EmptyS3Key_InvalidRequest(t *testing.T) {
	store := openAurynReadStore(t)
	sock := newFakeTagIPCServer(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("tag IPC must not be called for an empty-present s3_key")
		w.WriteHeader(http.StatusInternalServerError)
	})
	url := newWSTestServerWithTagIPC(t, store, tagipc.NewClient(sock))

	c := dialClient(t, url)
	c.hello()
	c.send(&pb.ClientMessage{
		RequestId: 2,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId:  1,
			S3Key:   proto.String(""),
			SetTags: []*pb.Tag{{Key: "mission", Value: "updated"}},
		}},
	})
	resp := c.recv()
	e := resp.GetError()
	if e == nil {
		t.Fatalf("expected an Error frame, got %T (empty-present s3_key must NEVER fall back to file_id)", resp.GetPayload())
	}
	if e.GetCode() != pb.ErrorCode_ERROR_INVALID_REQUEST {
		t.Errorf("Error.Code = %v, want ERROR_INVALID_REQUEST", e.GetCode())
	}
	if e.GetMessage() != "s3_key must be non-empty" {
		t.Errorf("Error.Message = %q, want %q", e.GetMessage(), "s3_key must be non-empty")
	}
}

// TestGetFile_UnknownKey_NotFound: a key-addressed GetFile for a key the
// catalog does not contain (whether a well-formed Hive key naming nothing, or
// a malformed non-Hive key) is ERROR_NOT_FOUND with the exact pinned message.
func TestGetFile_UnknownKey_NotFound(t *testing.T) {
	url := newWSTestServer(t, openAurynReadStore(t))
	c := dialClient(t, url)
	c.hello()

	cases := []struct {
		name string
		key  string
	}{
		{"well-formed unknown", "customer=alpha/customer_site=s1/robot=r1/source=ros-bags/date=2026-06-01/nope.mcap"},
		{"malformed non-hive", "flat-name.mcap"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := c.getFile(uint64(10+i), &pb.GetFileRequest{S3Key: proto.String(tc.key)})
			e := resp.GetError()
			if e == nil {
				t.Fatalf("expected an Error frame, got %T", resp.GetPayload())
			}
			if e.GetCode() != pb.ErrorCode_ERROR_NOT_FOUND {
				t.Errorf("Error.Code = %v, want ERROR_NOT_FOUND", e.GetCode())
			}
			want := `no file with s3_key "` + tc.key + `"`
			if e.GetMessage() != want {
				t.Errorf("Error.Message = %q, want %q", e.GetMessage(), want)
			}
		})
	}
}

// TestGetFile_KeyPrecedence_BogusIDIgnored: with BOTH a bogus file_id and a
// valid s3_key set, the key wins and the request succeeds — the id is never
// consulted (an id lookup of 9999 would have been ERROR_NOT_FOUND).
func TestGetFile_KeyPrecedence_BogusIDIgnored(t *testing.T) {
	url := newWSTestServer(t, openAurynReadStore(t))
	c := dialClient(t, url)
	c.hello()

	const keyF1 = "customer=alpha/customer_site=s1/robot=r1/source=ros-bags/date=2026-06-01/f1.mcap"
	resp := c.getFile(2, &pb.GetFileRequest{FileId: 9999, S3Key: proto.String(keyF1)})
	gf := resp.GetGetFile()
	if gf == nil {
		t.Fatalf("expected GetFileResponse (key must win over the bogus id), got %T (err=%v)",
			resp.GetPayload(), resp.GetError())
	}
	if got := gf.GetSummary().GetS3Key(); got != keyF1 {
		t.Errorf("s3_key = %q, want %q", got, keyF1)
	}
	if got := gf.GetSummary().GetId(); got != 1 {
		t.Errorf("id = %d, want 1 (the key's current id)", got)
	}
}

// TestUpdateTags_KeyPrecedence_BogusIDIgnored: same precedence contract for
// UpdateTags — a bogus file_id alongside a valid s3_key must not fail (the
// legacy id path's phase-1 resolve of 9999 would have been ERROR_NOT_FOUND),
// and the IPC receives the client's key verbatim.
func TestUpdateTags_KeyPrecedence_BogusIDIgnored(t *testing.T) {
	store := openAurynReadStore(t) // file 1 = keyF1, embedded mission=inv

	const keyF1 = "customer=alpha/customer_site=s1/robot=r1/source=ros-bags/date=2026-06-01/f1.mcap"
	var gotBody []byte
	sock := newFakeTagIPCServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tags":[{"key":"bogus","value":"WRONG-FROM-IPC","is_override":true}]}`))
	})
	url := newWSTestServerWithTagIPC(t, store, tagipc.NewClient(sock))

	c := dialClient(t, url)
	c.hello()
	c.send(&pb.ClientMessage{
		RequestId: 2,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId:  9999, // bogus — must be ignored, not resolved
			S3Key:   proto.String(keyF1),
			SetTags: []*pb.Tag{{Key: "mission", Value: "updated"}},
		}},
	})
	resp := c.recv()
	ut := resp.GetUpdateTags()
	if ut == nil {
		t.Fatalf("expected UpdateTagsResponse (key must win over the bogus id), got %T (err=%v)",
			resp.GetPayload(), resp.GetError())
	}

	var reqBody struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(gotBody, &reqBody); err != nil {
		t.Fatalf("decode forwarded request: %v (body=%s)", err, gotBody)
	}
	if reqBody.Key != keyF1 {
		t.Errorf("forwarded key = %q, want %q (the client's key, verbatim)", reqBody.Key, keyF1)
	}

	// Response = the fixture DB's real tags_effective (the A1 re-read), not
	// the IPC body's sentinel.
	if tags := ut.GetEffectiveTags(); len(tags) != 1 ||
		tags[0].GetKey() != "mission" || tags[0].GetValue() != "inv" || tags[0].GetIsOverride() {
		t.Errorf("effective tags = %+v, want [{mission inv override=false}] (the local re-read)", ut.GetEffectiveTags())
	}
}
