package ws

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/tagipc"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// newWSTestServerWithTagIPC is newWSTestServer (caps_test.go) plus a D2
// tag-IPC client wired in via Handler.SetTagIPC.
func newWSTestServerWithTagIPC(t *testing.T, store *catalog.Store, client *tagipc.Client) string {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(store, "", log)
	h.SetTagIPC(client)
	mux := http.NewServeMux()
	mux.Handle("/api/ws", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws"
}

// newFakeTagIPCServer binds a REAL unix-socket HTTP server standing in for
// the Python builder's tag_ipc endpoint, and returns the client-facing socket
// path plus the last decoded request body (populated after each round trip).
func newFakeTagIPCServer(t *testing.T, handler http.HandlerFunc) (sockPath string) {
	t.Helper()
	sockPath = filepath.Join(t.TempDir(), "tagipc.sock")
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix %q: %v", sockPath, err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.Listener = l
	srv.Start()
	t.Cleanup(srv.Close)
	return sockPath
}

// TestUpdateTags_ReadOnlyWithIPC_RoundTrip is test (a): a read-only store with
// a tag-IPC forwarder configured advertises tag_edit_supported=true, and an
// UpdateTags round-trips through the fake socket. The fake server deliberately
// returns tags DIFFERENT from the fixture DB's real tags_effective, so a
// response that echoed them (rather than the fixture's actual data) would
// prove the handler is trusting the IPC response body instead of doing the
// mandatory post-edit re-read (finding A1) from its own catalog.
func TestUpdateTags_ReadOnlyWithIPC_RoundTrip(t *testing.T) {
	store := openAurynReadStore(t) // file 1 = alpha/s1/r1/ros-bags/2026-06-01/f1.mcap, embedded mission=inv

	const wantKey = "customer=alpha/customer_site=s1/robot=r1/source=ros-bags/date=2026-06-01/f1.mcap"
	var gotBody []byte
	sock := newFakeTagIPCServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Deliberately WRONG tags: if the Go handler's response ever echoes
		// these instead of re-reading the fixture DB, this test must fail.
		_, _ = w.Write([]byte(`{"tags":[{"key":"bogus","value":"WRONG-FROM-IPC","is_override":true}]}`))
	})

	url := newWSTestServerWithTagIPC(t, store, tagipc.NewClient(sock))

	// Hello must advertise tag_edit_supported=true now that IPC is configured.
	hr := helloRoundTrip(t, url, "").GetHelloResponse()
	if hr == nil {
		t.Fatal("expected HelloResponse")
	}
	if !hr.GetCapabilities().GetTagEditSupported() {
		t.Error("read-only store WITH tag-IPC configured must advertise tag_edit_supported=true")
	}

	c := dialClient(t, url)
	c.hello()
	c.send(&pb.ClientMessage{
		RequestId: 2,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId:    1,
			SetTags:   []*pb.Tag{{Key: "mission", Value: "updated"}},
			UnsetKeys: []string{"stale"},
		}},
	})
	resp := c.recv()
	ut := resp.GetUpdateTags()
	if ut == nil {
		t.Fatalf("expected UpdateTagsResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}

	// The forwarded request must carry the rebuilt key (not the raw file_id) and
	// the set/unset verbatim.
	var reqBody struct {
		Key       string            `json:"key"`
		SetTags   map[string]string `json:"set_tags"`
		UnsetKeys []string          `json:"unset_keys"`
	}
	if err := json.Unmarshal(gotBody, &reqBody); err != nil {
		t.Fatalf("decode forwarded request: %v (body=%s)", err, gotBody)
	}
	if reqBody.Key != wantKey {
		t.Errorf("forwarded key = %q, want %q", reqBody.Key, wantKey)
	}
	if reqBody.SetTags["mission"] != "updated" {
		t.Errorf("forwarded set_tags = %v", reqBody.SetTags)
	}
	if len(reqBody.UnsetKeys) != 1 || reqBody.UnsetKeys[0] != "stale" {
		t.Errorf("forwarded unset_keys = %v", reqBody.UnsetKeys)
	}

	// The RESPONSE must be the fixture DB's real tags_effective (mission=inv,
	// embedded, is_override=false) — NOT the fake server's "bogus"/"WRONG-FROM-IPC".
	if len(ut.GetEffectiveTags()) != 1 {
		t.Fatalf("effective tags = %+v, want exactly [{mission inv false}]", ut.GetEffectiveTags())
	}
	got := ut.GetEffectiveTags()[0]
	if got.GetKey() != "mission" || got.GetValue() != "inv" || got.GetIsOverride() {
		t.Errorf("effective tags = %+v, want {mission inv false} (the fixture DB's real tags, "+
			"proving the response is a re-read, not an echo of the IPC body)", got)
	}
}

// TestUpdateTags_ReadOnlyIPCDown_WireError is test (c): the socket path is
// configured but nothing is listening (and, separately, the path doesn't
// exist at all) — both must map to the tagIPCUnavailableMessage wire error,
// never a crash or a silently-successful response.
func TestUpdateTags_ReadOnlyIPCDown_WireError(t *testing.T) {
	store := openAurynReadStore(t)

	cases := []struct {
		name string
		sock string
	}{
		{"path absent", filepath.Join(t.TempDir(), "does-not-exist.sock")},
		{"nothing listening", func() string {
			// Bind and immediately close: the path exists on disk but nothing
			// is accepting connections on it.
			p := filepath.Join(t.TempDir(), "closed.sock")
			l, err := net.Listen("unix", p)
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			_ = l.Close()
			return p
		}()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := newWSTestServerWithTagIPC(t, store, tagipc.NewClient(tc.sock))
			c := dialClient(t, url)
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
			if !strings.Contains(e.GetMessage(), "unavailable") {
				t.Errorf("Error.Message = %q, want it to contain %q", e.GetMessage(), "unavailable")
			}
		})
	}
}

// --- S2: mid-request generation swap ---------------------------------------
//
// TestUpdateTags_ReadOnlyWithIPC_RoundTrip above never swaps generations
// mid-request, so it cannot tell a correct key-based, reopen-before-re-read
// handler apart from a (wrong) handler that re-reads by the ORIGINAL
// req.file_id against the SAME (never reopened) Store handle — both would
// happen to return the fixture DB's one real file's tags. The tests below
// force an actual generation swap between the IPC call and the re-read, the
// way a real Python-builder rebuild landing mid-request would, and pin the
// two-part B1 fix: (a) the post-edit re-read must find the NEW generation
// (ReopenIfSwapped before the re-read), and (b) it must re-derive the file by
// KEY, never by the wire file_id (finding A1), because ids are not stable
// across a rebuild.

// tfDims is one Hive-key partition tuple for the tagforward test fixtures.
type tfDims struct {
	Customer, Site, Robot, Source, Date, Filename string
}

func (d tfDims) key() string {
	return fmt.Sprintf("customer=%s/customer_site=%s/robot=%s/source=%s/date=%s/%s",
		d.Customer, d.Site, d.Robot, d.Source, d.Date, d.Filename)
}

// tfFile is one file row for buildTagForwardDB: dims, id, and a single
// "mission" tag (embedded or an override) — enough to distinguish which
// generation/file a query answered from.
type tfFile struct {
	ID         uint64
	Dims       tfDims
	TagValue   string
	IsOverride bool
}

// buildTagForwardDB writes a fresh auryn-schema (v3) DB at path containing
// exactly the given files, each with its OWN customer/site/robot/source
// dimension rows (ids assigned by slice position) — so two DBs built by this
// helper can each be considered a distinct, self-contained "generation" for
// the S2 mid-request-swap tests. Schema mirrors openAurynReadStore
// (vocabulary_test.go); only the parts EffectiveTagsByKey / ObjectKeyForFile
// touch are populated.
func buildTagForwardDB(t *testing.T, path string, files []tfFile) {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()

	ddl := []string{
		fmt.Sprintf(`CREATE TABLE schema_version (id INTEGER PRIMARY KEY CHECK (id=1), version INTEGER NOT NULL);
			INSERT INTO schema_version(id,version) VALUES (1,%d)`, catalog.SchemaVersion),
		`CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE sites (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE robots (id INTEGER PRIMARY KEY, site_id INTEGER NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE sources (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE topic_names (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE schemas (id INTEGER PRIMARY KEY, name TEXT NOT NULL, encoding TEXT NOT NULL)`,
		`CREATE TABLE topic_sets (id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE topic_set_members (set_id INTEGER NOT NULL, topic_id INTEGER NOT NULL, schema_id INTEGER NOT NULL, PRIMARY KEY(set_id,topic_id)) WITHOUT ROWID`,
		`CREATE TABLE files (id INTEGER PRIMARY KEY, filename TEXT NOT NULL, etag TEXT NOT NULL, size_bytes INTEGER NOT NULL,
			last_modified_ns INTEGER NOT NULL DEFAULT 0, cataloged_at_ns INTEGER NOT NULL DEFAULT 0,
			customer_id INTEGER NOT NULL, site_id INTEGER NOT NULL, robot_id INTEGER NOT NULL, source_id INTEGER NOT NULL,
			date TEXT NOT NULL, start_time_ns INTEGER NOT NULL, end_time_ns INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL DEFAULT 0, topic_set_id INTEGER NOT NULL, topic_counts BLOB NOT NULL,
			has_error INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE tags_embedded (file_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID`,
		`CREATE TABLE tags_override (file_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT, updated_at INTEGER NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID`,
		`CREATE VIEW tags_effective AS
			SELECT file_id,key,value,1 AS is_override FROM tags_override WHERE value IS NOT NULL
			UNION ALL
			SELECT e.file_id,e.key,e.value,0 FROM tags_embedded e
			LEFT JOIN tags_override o ON (o.file_id=e.file_id AND o.key=e.key) WHERE o.file_id IS NULL`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/a')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
	}
	for _, s := range ddl {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ddl %q: %v", s, err)
		}
	}

	for i, f := range files {
		dimID := i + 1
		stmts := []string{
			fmt.Sprintf(`INSERT INTO customers(id,name) VALUES (%d,'%s')`, dimID, f.Dims.Customer),
			fmt.Sprintf(`INSERT INTO sites(id,customer_id,name) VALUES (%d,%d,'%s')`, dimID, dimID, f.Dims.Site),
			fmt.Sprintf(`INSERT INTO robots(id,site_id,name) VALUES (%d,%d,'%s')`, dimID, dimID, f.Dims.Robot),
			fmt.Sprintf(`INSERT INTO sources(id,name) VALUES (%d,'%s')`, dimID, f.Dims.Source),
			fmt.Sprintf(`INSERT INTO files (id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
				VALUES (%d,'%s','e',1,%d,%d,%d,%d,'%s',1,2,1,1,X'01')`,
				f.ID, f.Dims.Filename, dimID, dimID, dimID, dimID, f.Dims.Date),
		}
		if f.IsOverride {
			stmts = append(stmts, fmt.Sprintf(`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (%d,'mission','%s',1)`, f.ID, f.TagValue))
		} else {
			stmts = append(stmts, fmt.Sprintf(`INSERT INTO tags_embedded(file_id,key,value) VALUES (%d,'mission','%s')`, f.ID, f.TagValue))
		}
		for _, s := range stmts {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("file %d ddl %q: %v", f.ID, s, err)
			}
		}
	}
}

// tfSharedDims is the Hive key K that migrates between file ids across the
// two generations below: file_id=1 in generation A, file_id=2 in generation B
// (a DIFFERENT rowid — the exact scenario finding A1 exists to survive).
var tfSharedDims = tfDims{Customer: "alpha", Site: "s1", Robot: "r1", Source: "ros-bags", Date: "2026-06-01", Filename: "shared.mcap"}

// tfGenerationA: key K is file_id=1 (tags_effective = T_A, embedded, not an
// override); file_id=2 is some OTHER, unrelated file.
func tfGenerationA(t *testing.T, path string) {
	buildTagForwardDB(t, path, []tfFile{
		{ID: 1, Dims: tfSharedDims, TagValue: "A-shared", IsOverride: false},
		{ID: 2, Dims: tfDims{Customer: "zzz", Site: "zs", Robot: "zr", Source: "zsrc", Date: "2026-01-01", Filename: "other-a.mcap"}, TagValue: "A-other", IsOverride: false},
	})
}

// tfGenerationB: key K is now file_id=2 (a DIFFERENT rowid than generation
// A!) with a DIFFERENT tags_override set (T_B); file_id=1 is a DIFFERENT file
// entirely (a different key, its own distinct tags) — an id-based re-read
// using the original request's file_id=1 would land HERE, on the wrong file.
func tfGenerationB(t *testing.T, path string) {
	buildTagForwardDB(t, path, []tfFile{
		{ID: 1, Dims: tfDims{Customer: "beta", Site: "s2", Robot: "r2", Source: "synthetic", Date: "2026-02-02", Filename: "other-b.mcap"}, TagValue: "B-other-file", IsOverride: false},
		{ID: 2, Dims: tfSharedDims, TagValue: "B-shared-override", IsOverride: true},
	})
}

// tfGenerationB_NoKeyMatch: a valid generation-B-shaped DB, but key K does not
// exist in it at all (the companion S2 case: the swap lands, but the new
// generation doesn't have the key any more — e.g. the file was deleted/moved).
func tfGenerationB_NoKeyMatch(t *testing.T, path string) {
	buildTagForwardDB(t, path, []tfFile{
		{ID: 1, Dims: tfDims{Customer: "beta", Site: "s2", Robot: "r2", Source: "synthetic", Date: "2026-02-02", Filename: "other-b.mcap"}, TagValue: "B-other-file", IsOverride: false},
	})
}

// tagForwardSwapServer builds a fake tag-IPC socket whose handler, on receipt
// of the POST, atomically replaces the served catalog file (path) with
// buildGenB(genBPath) BEFORE responding 200 with the sentinel tags T_IPC
// ("mission"="IPC-SENTINEL", is_override=true) — simulating a Python-builder
// rebuild racing in between the write and this Go server's own re-read.
func tagForwardSwapServer(t *testing.T, path string, buildGenB func(t *testing.T, path string)) (sock string) {
	t.Helper()
	genBPath := filepath.Join(filepath.Dir(path), "genB.db")
	buildGenB(t, genBPath)
	return newFakeTagIPCServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if err := os.Rename(genBPath, path); err != nil {
			t.Fatalf("swap-in generation B: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tags":[{"key":"mission","value":"IPC-SENTINEL","is_override":true}]}`))
	})
}

const tfWantIPCSentinelKey, tfWantIPCSentinelValue = "mission", "IPC-SENTINEL"

// TestUpdateTags_ReadOnlyWithIPC_MidRequestGenerationSwap is the S2 rebuild:
// the fake IPC server swaps the served catalog file from generation A to
// generation B (where key K now lives at a DIFFERENT file_id, with different
// tags) between the write and the Go handler's own post-edit re-read. The
// response must be T_B, proving BOTH: (1) the re-read reopens onto the NEW
// generation (the B1 fix — without it, the old *sql.DB keeps serving
// generation A's T_A forever, per TestReadOnly_PoolPinning_SurvivesFileReplace's
// pinned-connection contract), and (2) the re-read resolves by KEY, not by the
// stale wire file_id (finding A1 — an id-based re-read would land on
// generation B's file_id=1, "B-other-file", a totally different file).
func TestUpdateTags_ReadOnlyWithIPC_MidRequestGenerationSwap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	tfGenerationA(t, path)

	store, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	sock := tagForwardSwapServer(t, path, tfGenerationB)
	url := newWSTestServerWithTagIPC(t, store, tagipc.NewClient(sock))

	c := dialClient(t, url)
	c.hello()
	c.send(&pb.ClientMessage{
		RequestId: 2,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId:  1, // resolves to key K in generation A (phase 1, before the swap)
			SetTags: []*pb.Tag{{Key: "mission", Value: "updated"}},
		}},
	})
	resp := c.recv()
	ut := resp.GetUpdateTags()
	if ut == nil {
		t.Fatalf("expected UpdateTagsResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	if len(ut.GetEffectiveTags()) != 1 {
		t.Fatalf("effective tags = %+v, want exactly 1 (T_B)", ut.GetEffectiveTags())
	}
	got := ut.GetEffectiveTags()[0]
	if got.GetKey() != "mission" || got.GetValue() != "B-shared-override" || !got.GetIsOverride() {
		t.Errorf("effective tags = %+v, want {mission B-shared-override override=true} (T_B, generation B's "+
			"key-based re-read) — got T_A (%q) => reopen-before-re-read is missing/broken; got generation B's "+
			"file_id=1 tags (%q) => the re-read used the stale wire file_id instead of the key",
			got, "A-shared", "B-other-file")
	}
}

// TestUpdateTags_ReadOnlyWithIPC_MidRequestSwap_KeyGoneFallsBackToIPCTags is
// the S2 companion case: the swap lands, but the NEW generation does not
// contain key K at all (EffectiveTagsByKey returns ErrFileNotFound even after
// a correct reopen). The B1-fix-part-2 fallback must kick in: the response is
// the tag-IPC endpoint's OWN response tags (T_IPC) — the edit DID apply
// (phase 2 returned 200), so this must NOT be ERROR_INTERNAL.
func TestUpdateTags_ReadOnlyWithIPC_MidRequestSwap_KeyGoneFallsBackToIPCTags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	tfGenerationA(t, path)

	store, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	sock := tagForwardSwapServer(t, path, tfGenerationB_NoKeyMatch)
	url := newWSTestServerWithTagIPC(t, store, tagipc.NewClient(sock))

	c := dialClient(t, url)
	c.hello()
	c.send(&pb.ClientMessage{
		RequestId: 2,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId:  1,
			SetTags: []*pb.Tag{{Key: "mission", Value: "updated"}},
		}},
	})
	resp := c.recv()
	ut := resp.GetUpdateTags()
	if ut == nil {
		t.Fatalf("expected an UpdateTagsResponse falling back to the IPC tags (NOT an error), got %T (err=%v)",
			resp.GetPayload(), resp.GetError())
	}
	if len(ut.GetEffectiveTags()) != 1 {
		t.Fatalf("effective tags = %+v, want exactly 1 (T_IPC)", ut.GetEffectiveTags())
	}
	got := ut.GetEffectiveTags()[0]
	if got.GetKey() != tfWantIPCSentinelKey || got.GetValue() != tfWantIPCSentinelValue || !got.GetIsOverride() {
		t.Errorf("effective tags = %+v, want {%s %s override=true} (T_IPC, the IPC response's own tags, "+
			"since the key vanished from the new generation)", got, tfWantIPCSentinelKey, tfWantIPCSentinelValue)
	}
}
