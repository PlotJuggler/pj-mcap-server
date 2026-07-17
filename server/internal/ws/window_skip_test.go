package ws

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"pj-cloud/server/internal/genmcap"
	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// perKeyCountingBlob counts GetRange calls per object key. Used to prove that
// OpenSession with a time window only touches the files that intersect it.
type perKeyCountingBlob struct {
	inner storage.BlobStore
	mu    sync.Mutex
	gets  map[string]int
}

func newPerKeyCountingBlob(inner storage.BlobStore) *perKeyCountingBlob {
	return &perKeyCountingBlob{inner: inner, gets: map[string]int{}}
}

func (b *perKeyCountingBlob) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	b.mu.Lock()
	b.gets[key]++
	b.mu.Unlock()
	return b.inner.GetRange(ctx, key, off, length)
}
func (b *perKeyCountingBlob) Head(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return b.inner.Head(ctx, key)
}
func (b *perKeyCountingBlob) List(ctx context.Context, p, t string) ([]storage.ObjectInfo, string, error) {
	return b.inner.List(ctx, p, t)
}
func (b *perKeyCountingBlob) reset() {
	b.mu.Lock()
	b.gets = map[string]int{}
	b.mu.Unlock()
}
func (b *perKeyCountingBlob) count(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets[key]
}

// genFile builds one time-disjoint synthetic MCAP starting at startSec seconds.
func genFile(t *testing.T, key string, startSec int64) []byte {
	t.Helper()
	const sec = int64(1_000_000_000)
	var buf bytes.Buffer
	spec := genmcap.FileSpec{
		Key:     key,
		StartNs: startSec * sec,
		StepNs:  sec / 10, // 10 Hz
		Topics: []genmcap.TopicSpec{
			{Topic: "/a", SchemaName: "S", SchemaEnc: "ros2msg", MessageCount: 100},
			{Topic: "/b", SchemaName: "S", SchemaEnc: "ros2msg", MessageCount: 100},
		},
	}
	if err := genmcap.Write(&buf, spec); err != nil {
		t.Fatalf("genmcap.Write %s: %v", key, err)
	}
	return buf.Bytes()
}

// The test fixture (buildAurynCatalog, standing in for the external Python
// builder's own scan) pre-warms the chunk-index cache when it builds the
// catalog, so OpenSession's PLAN phase must do ZERO storage reads for an
// already-indexed file. Proven by requesting a topic that doesn't exist: the
// plan loads the (cached) index, finds nothing, and streams no bodies — so
// the file is never read from storage at all. Without the pre-warm the plan
// would read the summary first.
func TestOpenSession_PrewarmedPlanReadsNoStorage(t *testing.T) {
	files := map[string][]byte{"only.mcap": genFile(t, "only.mcap", 5000)}
	counter := newPerKeyCountingBlob(memBlobStore{data: files})
	ts := newTestServerWithBlob(t, counter, defaultTestSessionCfg()) // fixture build pre-warms the cache
	c := dialClient(t, ts.url)
	c.hello()

	counter.reset() // ignore the fixture-build scan reads

	// A topic that doesn't exist -> empty plan -> no chunk bodies. The only
	// possible storage read would be the chunk-index summary, which the
	// pre-warm already cached.
	c.send(&pb.ClientMessage{RequestId: 40, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys("only.mcap"), TopicNames: []string{"/does-not-exist"}},
		}},
	}})
	open := c.recv()
	or := open.GetOpenSession()
	if or == nil {
		t.Fatalf("OpenSession: got %T (err=%v)", open.GetPayload(), open.GetError())
	}
	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	c.streamToEnd(res, or.GetSubscriptionId(), 4)

	if got := counter.count("only.mcap"); got != 0 {
		t.Errorf("plan phase read storage %d times for an indexed file, want 0 (cache not pre-warmed)", got)
	}
}

// A windowed OpenSession over a stitched set must NOT read the chunk index of
// files whose time range lies entirely outside the window — they contribute no
// chunks, so paying their (WAN, bandwidth-bound) summary read is pure waste.
// Three time-disjoint files; a window inside the MIDDLE file must touch only it.
func TestOpenSession_WindowSkipsNonOverlappingFiles(t *testing.T) {
	const sec = int64(1_000_000_000)
	files := map[string][]byte{
		"a.mcap": genFile(t, "a.mcap", 1000), // [1000s, ~1010s]
		"b.mcap": genFile(t, "b.mcap", 2000), // [2000s, ~2010s]
		"c.mcap": genFile(t, "c.mcap", 3000), // [3000s, ~3010s]
	}
	counter := newPerKeyCountingBlob(memBlobStore{data: files})
	ts := newTestServerWithBlob(t, counter, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	counter.reset() // ignore the fixture-build scan + ListFiles reads

	// Window strictly inside file b ([2000s, 2010s]); a/c don't intersect it.
	c.send(&pb.ClientMessage{RequestId: 30, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{
				S3Keys:    fileKeys("a.mcap", "b.mcap", "c.mcap"),
				TimeRange: &pb.TimeRange{StartNs: 2002 * sec, EndNs: 2008 * sec},
			},
		}},
	}})
	open := c.recv()
	or := open.GetOpenSession()
	if or == nil {
		t.Fatalf("OpenSession: got %T (err=%v)", open.GetPayload(), open.GetError())
	}
	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	c.streamToEnd(res, or.GetSubscriptionId(), 4)
	if res.eos == nil || res.errFrame != nil {
		t.Fatalf("must stream to Eos (eos=%v err=%v)", res.eos != nil, res.errFrame)
	}

	// The out-of-window files must have been read ZERO times (chunk-index load
	// skipped + no chunk bodies); the in-window file must have been read.
	if got := counter.count("a.mcap"); got != 0 {
		t.Errorf("file a (outside window) was read %d times, want 0 (chunk-index load not skipped)", got)
	}
	if got := counter.count("c.mcap"); got != 0 {
		t.Errorf("file c (outside window) was read %d times, want 0 (chunk-index load not skipped)", got)
	}
	if got := counter.count("b.mcap"); got == 0 {
		t.Error("file b (inside window) was never read — the window dropped the data too")
	}
	if res.total == 0 {
		t.Error("no messages streamed from the in-window file")
	}
}
