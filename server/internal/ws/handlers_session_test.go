package ws

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/session"
	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

const (
	zegTestKey       = "nissan_zala_50_zeg_1_0.mcap"
	zegRelTestPath   = "../format/testdata/nissan_zala_50_zeg_1_0.mcap"
	zegTotalMessages = 33670
	zegImuTopic      = "/nissan/gps/duro/imu"
	zegImuMessages   = 14904
	zegSpeedTopic    = "/nissan/vehicle_speed"
	zegSpeedMessages = 4513
)

// memBlobStore is a hermetic in-memory storage.BlobStore (List + Head + GetRange)
// over a fixed key->bytes map. It backs both the catalog scan and the producer's
// ranged chunk reads with no network / Minio.
type memBlobStore struct{ data map[string][]byte }

func (m memBlobStore) GetRange(_ context.Context, key string, off, length int64) ([]byte, error) {
	d, ok := m.data[key]
	if !ok {
		return nil, storage.ErrPermanent
	}
	if off >= int64(len(d)) {
		return nil, nil
	}
	end := off + length
	if length <= 0 || end > int64(len(d)) {
		end = int64(len(d))
	}
	out := make([]byte, end-off)
	copy(out, d[off:end])
	return out, nil
}

func (m memBlobStore) Head(_ context.Context, key string) (storage.ObjectInfo, error) {
	d, ok := m.data[key]
	if !ok {
		return storage.ObjectInfo{}, storage.ErrPermanent
	}
	return storage.ObjectInfo{Key: key, ETag: "mem-" + key, Size: int64(len(d))}, nil
}

func (m memBlobStore) List(_ context.Context, _, _ string) ([]storage.ObjectInfo, string, error) {
	out := make([]storage.ObjectInfo, 0, len(m.data))
	for k, d := range m.data {
		out = append(out, storage.ObjectInfo{Key: k, ETag: "mem-" + k, Size: int64(len(d))})
	}
	return out, "", nil
}

// testServer is a fully-wired in-process WS server (SQLite catalog + session)
// over a memBlobStore loaded from the real testdata MCAP. Closing it tears the
// httptest server down.
type testServer struct {
	httptest *httptest.Server
	url      string
	cat      *catalog.Store
	reg      *session.Registry
	store    storage.BlobStore
	idxCache *format.ChunkIndexCache
}

func newTestServer(t *testing.T, files map[string][]byte, cfg config.SessionConfig) *testServer {
	t.Helper()
	return newTestServerWithBlob(t, memBlobStore{data: files}, cfg)
}

// newTestServerWithBlob is newTestServer with the storage.BlobStore injected, so
// tests can wrap it (e.g. the countingBlobStore for the estimate-5% gate). The
// store must serve the same MCAP corpus the catalog fixture expects — see
// buildAurynCatalog (auryn_fixture_test.go), which extracts every ".mcap" key
// already present in blob and catalogs it for real (no in-process indexer
// exists anymore to do this as a background scan).
func newTestServerWithBlob(t *testing.T, blob storage.BlobStore, cfg config.SessionConfig) *testServer {
	t.Helper()
	codec, err := format.NewCodec("mcap")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	idxCache := format.NewChunkIndexCache(1024)
	cat, hiveBlob := buildAurynCatalog(t, context.Background(), blob, codec, idxCache)

	reg := session.NewRegistry(session.RegistryOpts{
		MaxConcurrent:         cfg.MaxConcurrent,
		RetainAfterDisconnect: cfg.RetainAfterDisconnect,
	})
	deps := &SessionDeps{Store: cat, Codec: codec, Blob: hiveBlob, Registry: reg, Cfg: cfg, Log: log, IdxCache: idxCache}
	h := NewHandlerWithSession(cat, "", log, deps)

	mux := http.NewServeMux()
	mux.Handle("/api/ws", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &testServer{
		httptest: srv,
		url:      "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws",
		cat:      cat,
		reg:      reg,
		store:    hiveBlob,
		idxCache: idxCache,
	}
}

func loadZegFile(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(zegRelTestPath))
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	return raw
}

// wsClient is a minimal protobuf WS client for the component tests.
type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
	dec  *zstd.Decoder
}

func dialClient(t *testing.T, url string) *wsClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.SetReadLimit(32 << 20)
	dec, _ := zstd.NewReader(nil)
	c := &wsClient{t: t, conn: conn, dec: dec}
	t.Cleanup(func() {
		_ = conn.Close(websocket.StatusNormalClosure, "done")
		dec.Close()
	})
	return c
}

func (c *wsClient) send(msg *pb.ClientMessage) {
	c.t.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		c.t.Fatalf("marshal client msg: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		c.t.Fatalf("ws write: %v", err)
	}
}

func (c *wsClient) recv() *pb.ServerMessage {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, data, err := c.conn.Read(ctx)
	if err != nil {
		c.t.Fatalf("ws read: %v", err)
	}
	var msg pb.ServerMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		c.t.Fatalf("unmarshal server msg: %v", err)
	}
	return &msg
}

func (c *wsClient) hello() {
	c.t.Helper()
	c.send(&pb.ClientMessage{RequestId: 1, Payload: &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 2}}})
	resp := c.recv()
	if resp.GetHelloResponse() == nil {
		c.t.Fatalf("expected HelloResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
}

// fileID looks up a file by its LOGICAL (flat) key — the same key the caller
// used to seed its memBlobStore/genFile map. Every test server's catalog is
// auryn (buildAurynCatalog), which always reports the REBUILT Hive s3_key
// (hiveKeyFor(key)), never the bare flat name — that translation happens here,
// once, so call sites can keep using the flat key unchanged.
func (c *wsClient) fileID(t *testing.T, key string) uint64 {
	t.Helper()
	c.send(&pb.ClientMessage{RequestId: 2, Payload: &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{Limit: 1000}}})
	resp := c.recv()
	lf := resp.GetListFiles()
	if lf == nil {
		t.Fatalf("expected ListFilesResponse, got %T", resp.GetPayload())
	}
	want := hiveKeyFor(key)
	for _, f := range lf.GetFiles() {
		if f.GetS3Key() == want {
			return f.GetId()
		}
	}
	t.Fatalf("file %q (hive key %q) not in listing", key, want)
	return 0
}

// fileKeys maps flat fixture keys to the rebuilt Hive s3_keys the catalog
// serves — the durable identity OpenFresh is addressed by (v2). Call sites that
// used to resolve a wire file_id (fileID) now pass keys straight through.
func fileKeys(flat ...string) []string {
	out := make([]string, len(flat))
	for i, k := range flat {
		out[i] = hiveKeyFor(k)
	}
	return out
}

// collectResult tallies a streamed session: total messages, per-topic counts,
// max seq seen, gap detection, the terminal Eos, and any Error.
type collectResult struct {
	total    int
	perTopic map[string]int
	maxSeq   uint64
	seqGaps  bool
	eos      *pb.Eos
	errFrame *pb.Error
	openResp *pb.OpenSessionResponse
}

// decodeBatch appends a batch's messages into res, mapping topic ids to names.
func (c *wsClient) decodeBatch(res *collectResult, b *pb.MessageBatch, topicName map[uint32]string) {
	c.t.Helper()
	var msgs []*pb.Message
	switch b.GetBodyEncoding() {
	case pb.BodyEncoding_BODY_ENCODING_ZSTD:
		raw, err := c.dec.DecodeAll(b.GetBody(), nil)
		if err != nil {
			c.t.Fatalf("batch %d body must decompress standalone: %v", b.GetSeq(), err)
		}
		if uint64(len(raw)) != b.GetBodyUncompressedSize() {
			c.t.Errorf("batch %d body_uncompressed_size: got %d want %d", b.GetSeq(), b.GetBodyUncompressedSize(), len(raw))
		}
		var body pb.MessageBatchBody
		if err := proto.Unmarshal(raw, &body); err != nil {
			c.t.Fatalf("batch %d unmarshal body: %v", b.GetSeq(), err)
		}
		msgs = body.GetMessages()
	case pb.BodyEncoding_BODY_ENCODING_NONE:
		msgs = b.GetMessages()
	default:
		c.t.Fatalf("batch %d unknown body_encoding %v (client must reject)", b.GetSeq(), b.GetBodyEncoding())
	}
	if res.maxSeq != 0 && b.GetSeq() != res.maxSeq+1 {
		res.seqGaps = true
	}
	if b.GetSeq() > res.maxSeq {
		res.maxSeq = b.GetSeq()
	}
	for _, m := range msgs {
		res.perTopic[topicName[m.GetTopicId()]]++
		res.total++
	}
}

// streamToEnd drives a session that has already had OpenSessionResponse received
// (passed in), reading frames until a terminal Eos. It acks every ackEvery
// batches.
func (c *wsClient) streamToEnd(res *collectResult, subID uint64, ackEvery int) {
	c.t.Helper()
	topicName := map[uint32]string{}
	for _, b := range res.openResp.GetTopicIdMap() {
		topicName[b.GetTopicId()] = b.GetTopicName()
	}
	batchesSinceAck := 0
	for {
		msg := c.recv()
		switch {
		case msg.GetBatch() != nil:
			c.decodeBatch(res, msg.GetBatch(), topicName)
			batchesSinceAck++
			if ackEvery > 0 && batchesSinceAck >= ackEvery {
				c.send(&pb.ClientMessage{Payload: &pb.ClientMessage_Ack{Ack: &pb.SessionAck{
					SubscriptionId: subID, ThroughSeq: res.maxSeq,
				}}})
				batchesSinceAck = 0
			}
		case msg.GetProgress() != nil:
			// observed; nothing to assert here beyond it not crashing
		case msg.GetEos() != nil:
			res.eos = msg.GetEos()
			return
		case msg.GetError() != nil:
			res.errFrame = msg.GetError()
			return
		}
	}
}

func defaultTestSessionCfg() config.SessionConfig {
	c := config.DefaultSession()
	return c
}

func TestSession_FullLifecycle_AllTopics(t *testing.T) {
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	c.send(&pb.ClientMessage{RequestId: 10, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys(zegTestKey)},
		}},
	}})
	open := c.recv()
	or := open.GetOpenSession()
	if or == nil {
		t.Fatalf("expected OpenSessionResponse, got %T (err=%v)", open.GetPayload(), open.GetError())
	}
	if open.GetRequestId() != 10 {
		t.Errorf("request_id echo: got %d want 10", open.GetRequestId())
	}
	if or.GetApproximateMessages() != zegTotalMessages {
		t.Errorf("approximate_messages: got %d want %d", or.GetApproximateMessages(), zegTotalMessages)
	}
	if len(or.GetTopicIdMap()) != 6 {
		t.Errorf("topic_id_map size: got %d want 6", len(or.GetTopicIdMap()))
	}

	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	c.streamToEnd(res, or.GetSubscriptionId(), 64)

	if res.errFrame != nil {
		t.Fatalf("unexpected Error: %v", res.errFrame)
	}
	if res.seqGaps {
		t.Error("seq gaps / non-monotonic")
	}
	if res.total != zegTotalMessages {
		t.Errorf("total messages: got %d want %d", res.total, zegTotalMessages)
	}
	if res.perTopic[zegImuTopic] != zegImuMessages {
		t.Errorf("imu: got %d want %d", res.perTopic[zegImuTopic], zegImuMessages)
	}
	if res.eos.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("eos reason: got %v want COMPLETE", res.eos.GetReason())
	}
	if res.eos.GetTotalMessagesSent() != zegTotalMessages {
		t.Errorf("eos total_messages_sent: got %d want %d", res.eos.GetTotalMessagesSent(), zegTotalMessages)
	}
}

func TestSession_TopicSubset(t *testing.T) {
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	c.send(&pb.ClientMessage{RequestId: 11, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys(zegTestKey), TopicNames: []string{zegSpeedTopic}},
		}},
	}})
	or := c.recv().GetOpenSession()
	if or == nil {
		t.Fatal("expected OpenSessionResponse")
	}
	if len(or.GetTopicIdMap()) != 1 {
		t.Errorf("topic_id_map should hold only the requested topic, got %d", len(or.GetTopicIdMap()))
	}
	if or.GetApproximateMessages() != zegSpeedMessages {
		t.Errorf("approximate_messages: got %d want %d", or.GetApproximateMessages(), zegSpeedMessages)
	}

	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	c.streamToEnd(res, or.GetSubscriptionId(), 64)

	if res.total != zegSpeedMessages {
		t.Errorf("subset total: got %d want %d", res.total, zegSpeedMessages)
	}
	for topic := range res.perTopic {
		if topic != zegSpeedTopic {
			t.Errorf("subset leaked topic %q", topic)
		}
	}
	if res.eos.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("eos reason: got %v want COMPLETE", res.eos.GetReason())
	}
}

func TestSession_EmptyPlan(t *testing.T) {
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	// A topic that exists in no file -> silently dropped -> empty plan.
	c.send(&pb.ClientMessage{RequestId: 12, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys(zegTestKey), TopicNames: []string{"/does/not/exist"}},
		}},
	}})
	or := c.recv().GetOpenSession()
	if or == nil {
		t.Fatal("expected OpenSessionResponse for empty plan (success, not error)")
	}
	if or.GetEstimatedChunkBytes() != 0 || or.GetApproximateMessages() != 0 {
		t.Errorf("empty plan estimates: got bytes=%d msgs=%d want 0/0", or.GetEstimatedChunkBytes(), or.GetApproximateMessages())
	}
	if len(or.GetTopicIdMap()) != 0 || len(or.GetSchemas()) != 0 {
		t.Errorf("empty plan should have empty maps, got topics=%d schemas=%d", len(or.GetTopicIdMap()), len(or.GetSchemas()))
	}

	eos := c.recv().GetEos()
	if eos == nil {
		t.Fatal("expected Eos immediately after empty-plan OpenSessionResponse")
	}
	if eos.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("empty plan eos reason: got %v want COMPLETE", eos.GetReason())
	}
	if eos.GetTotalMessagesSent() != 0 {
		t.Errorf("empty plan total_messages_sent: got %d want 0", eos.GetTotalMessagesSent())
	}
}

func TestSession_ResourceLimit(t *testing.T) {
	// MaxConcurrent=1: the first session occupies the slot; a second OpenFresh
	// (with tiny retain caps so the first session does not complete instantly)
	// must get RESOURCE_LIMIT.
	cfg := defaultTestSessionCfg()
	cfg.MaxConcurrent = 1
	cfg.RetainMaxSeqs = 2
	cfg.MaxBatchBytes = 16 << 10
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, cfg)
	c := dialClient(t, ts.url)
	c.hello()

	c.send(&pb.ClientMessage{RequestId: 50, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys(zegTestKey)},
		}},
	}})
	if c.recv().GetOpenSession() == nil {
		t.Fatal("first OpenSession should succeed")
	}
	// Do not drain: the first session stays active (producer parks at the cap).
	c.send(&pb.ClientMessage{RequestId: 51, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys(zegTestKey), TopicNames: []string{zegSpeedTopic}},
		}},
	}})
	// The second response may be preceded by in-flight batches/progress from the
	// first session on the same connection; scan for the Error with request_id 51.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		msg := c.recv()
		if e := msg.GetError(); e != nil && msg.GetRequestId() == 51 {
			if e.GetCode() != pb.ErrorCode_ERROR_RESOURCE_LIMIT {
				t.Fatalf("second OpenSession: got %v want RESOURCE_LIMIT", e.GetCode())
			}
			return
		}
	}
	t.Fatal("did not receive RESOURCE_LIMIT for the second OpenSession")
}

func TestSession_UnknownFileID(t *testing.T) {
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	c.send(&pb.ClientMessage{RequestId: 13, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: []string{"customer=x/customer_site=y/robot=z/source=s/date=2026-01-01/nope.mcap"}},
		}},
	}})
	e := c.recv().GetError()
	if e == nil || e.GetCode() != pb.ErrorCode_ERROR_NOT_FOUND {
		t.Fatalf("expected NOT_FOUND, got %v", e)
	}
}

func TestSession_Cancel(t *testing.T) {
	// Tiny retain caps so the producer parks quickly; we cancel mid-stream.
	cfg := defaultTestSessionCfg()
	cfg.RetainMaxSeqs = 2
	cfg.MaxBatchBytes = 16 << 10 // smaller batches -> many seqs
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, cfg)
	c := dialClient(t, ts.url)
	c.hello()

	c.send(&pb.ClientMessage{RequestId: 14, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys(zegTestKey)},
		}},
	}})
	or := c.recv().GetOpenSession()
	if or == nil {
		t.Fatal("expected OpenSessionResponse")
	}
	subID := or.GetSubscriptionId()

	// Read a couple of frames, then cancel.
	_ = c.recv() // at least one batch/progress
	c.send(&pb.ClientMessage{Payload: &pb.ClientMessage_Cancel{Cancel: &pb.CancelSession{SubscriptionId: subID}}})

	// Drain until we see Eos{CANCELLED} (there may be in-flight batches first).
	deadline := time.Now().Add(15 * time.Second)
	var cancelled bool
	for time.Now().Before(deadline) {
		msg := c.recv()
		if e := msg.GetEos(); e != nil {
			if e.GetReason() == pb.EosReason_EOS_REASON_CANCELLED {
				cancelled = true
			}
			break
		}
	}
	if !cancelled {
		t.Fatal("expected Eos{CANCELLED} after CancelSession")
	}
	// Session must be deregistered.
	if _, ok := ts.reg.Lookup(subID); ok {
		t.Error("session still registered after cancel")
	}
}
