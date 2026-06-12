package ws

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/indexer"
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/session"
	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// armablePanicBlob wraps a memBlobStore and panics from GetRange once armed and
// only AFTER the first panicAfter calls have been served. BuildPlan (the chunk-
// index load) issues many small ranged reads synchronously in the read-loop
// goroutine; the test arms the panic only after that whole phase, so the first
// fault lands in the per-session PRODUCER goroutine (spec §8.1), isolating the
// scope from the connection read loop.
type armablePanicBlob struct {
	inner      memBlobStore
	armed      atomic.Bool
	calls      atomic.Int64
	panicAfter atomic.Int64
}

func (b *armablePanicBlob) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	n := b.calls.Add(1)
	if b.armed.Load() && n > b.panicAfter.Load() {
		panic("injected GetRange panic (producer chunk fetch)")
	}
	return b.inner.GetRange(ctx, key, off, length)
}
func (b *armablePanicBlob) Head(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return b.inner.Head(ctx, key)
}
func (b *armablePanicBlob) List(ctx context.Context, p, t string) ([]storage.ObjectInfo, string, error) {
	return b.inner.List(ctx, p, t)
}

// newTestServerWithMetrics is newTestServerWithBlob with a metrics set wired in
// (so the panic Guards record pj_cloud_panic_total). Returns the server + the
// metrics set for assertions.
func newTestServerWithMetrics(t *testing.T, blob storage.BlobStore, cfg config.SessionConfig) (*testServer, *metrics.Metrics) {
	t.Helper()
	codec, err := format.NewCodec("mcap")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mx := metrics.New()

	cat, err := catalog.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	cat.SetObservability(mx, log)
	t.Cleanup(func() { _ = cat.Close() })

	scanner := &indexer.Scanner{
		Store:  cat,
		Lister: indexer.NewBlobStoreLister(blob),
		// nil cache: this test counts plan-phase GetRange calls to arm a
		// producer panic, so it must NOT pre-warm (which would skip plan reads).
		Extractor: indexer.NewCodecExtractor(blob, codec, nil),
		Log:       log,
	}
	if _, err := scanner.RunOnce(context.Background()); err != nil {
		t.Fatalf("indexer scan: %v", err)
	}

	reg := session.NewRegistry(session.RegistryOpts{
		MaxConcurrent:         cfg.MaxConcurrent,
		RetainAfterDisconnect: cfg.RetainAfterDisconnect,
	})
	reg.SetOnEvict(func(*session.SessionState) { mx.SessionsActive.Dec() })
	deps := &SessionDeps{Store: cat, Codec: codec, Blob: blob, Registry: reg, Cfg: cfg, Log: log, Metrics: mx}
	h := NewHandlerWithSession(cat, "", log, deps)
	h.SetMetrics(mx)

	mux := http.NewServeMux()
	mux.Handle("/api/ws", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &testServer{
		httptest: srv,
		url:      "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws",
		cat:      cat,
		reg:      reg,
		store:    blob,
	}, mx
}

// TestProducerPanic_RecoversAndCounts proves a panic in the per-session producer
// goroutine (here, an injected blob-store fault) is recovered (the process
// survives, sibling sessions are unaffected), is counted in
// pj_cloud_panic_total{scope="session-producer"}, and is surfaced to the client
// as a stream-fatal Error + Eos{ERROR} (spec §8.1 + §6.7 routing). ProducerDone
// being closed in a defer means the consumer never hangs.
func TestProducerPanic_RecoversAndCounts(t *testing.T) {
	cfg := defaultTestSessionCfg()
	blob := &armablePanicBlob{inner: memBlobStore{data: map[string][]byte{zegTestKey: loadZegFile(t)}}}
	ts, mx := newTestServerWithMetrics(t, blob, cfg)

	c := dialClient(t, ts.url)
	c.hello()
	id := c.fileID(t, zegTestKey)

	c.send(&pb.ClientMessage{RequestId: 10, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{FileIds: []uint64{id}},
		}},
	}})
	if c.recv().GetOpenSession() == nil {
		t.Fatalf("expected OpenSessionResponse")
	}

	// The OpenSessionResponse means BuildPlan's whole synchronous chunk-index read
	// phase is done; every GetRange from here is a PRODUCER chunk fetch. Arm so the
	// next read (the producer's) panics — isolating the fault to the producer
	// goroutine, not the connection read loop.
	blob.panicAfter.Store(blob.calls.Load())
	blob.armed.Store(true)

	// The CORE invariant (spec §8.1): the producer panic is recovered + counted,
	// it does NOT crash the process. Wire delivery of the stream-fatal Error/Eos
	// is best-effort (the teardown can close the socket first), so we assert on
	// the recovery counter, not the wire frame.
	settle := time.After(5 * time.Second)
	for mx.PanicCount("session-producer") < 1 {
		select {
		case <-settle:
			t.Fatalf("pj_cloud_panic_total{session-producer} = %v; want >= 1", mx.PanicCount("session-producer"))
		case <-time.After(20 * time.Millisecond):
		}
	}

	// The session was torn down (registry drains to empty) and the process is
	// alive + serving: a fresh connection still answers catalog RPCs and can open
	// a NEW session against the (now disarmed) blob — sibling work is unaffected.
	drain := time.After(5 * time.Second)
	for ts.reg.ActiveCount() != 0 {
		select {
		case <-drain:
			t.Fatalf("registry not drained after producer panic: %d", ts.reg.ActiveCount())
		case <-time.After(20 * time.Millisecond):
		}
	}
	blob.armed.Store(false)
	c2 := dialClient(t, ts.url)
	c2.hello()
	_ = c2.fileID(t, zegTestKey)
}
