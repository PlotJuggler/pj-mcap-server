package ws

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// gateBlob wraps memBlobStore with two armable behaviors:
//   - slow: every GetRange sleeps `delay` (a WAN-shaped plan-build)
//   - broken: every storage call fails (the bucket is unreachable)
//
// Both are armed AFTER the fixture-build scan so cataloging stays instant.
type gateBlob struct {
	inner  memBlobStore
	delay  time.Duration
	slow   atomic.Bool
	broken atomic.Bool
}

func (b *gateBlob) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	if b.broken.Load() {
		return nil, fmt.Errorf("gateBlob: storage unreachable")
	}
	if b.slow.Load() {
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.inner.GetRange(ctx, key, off, length)
}

func (b *gateBlob) Head(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if b.broken.Load() {
		return storage.ObjectInfo{}, fmt.Errorf("gateBlob: storage unreachable")
	}
	return b.inner.Head(ctx, key)
}

func (b *gateBlob) List(ctx context.Context, p, tok string) ([]storage.ObjectInfo, string, error) {
	if b.broken.Load() {
		return nil, "", fmt.Errorf("gateBlob: storage unreachable")
	}
	return b.inner.List(ctx, p, tok)
}

// OpenSession plan-building must not monopolize the connection's read loop: a
// catalog RPC sent right after a (storage-slow) OpenSession must be answered
// BEFORE the open completes. Inline dispatch fails this (the ListFiles reply
// only ever arrived after the whole plan-build).
func TestOpenSession_DoesNotBlockCatalogRPCsOnTheSameConnection(t *testing.T) {
	blob := &gateBlob{inner: memBlobStore{data: map[string][]byte{zegTestKey: loadZegFile(t)}}, delay: 400 * time.Millisecond}
	ts := newTestServerWithBlob(t, blob, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()
	id := c.fileID(t, zegTestKey)

	ts.idxCache.Clear()   // force a COLD plan (the fixture-build scan pre-warmed it)
	blob.slow.Store(true) // WAN-shaped from here on

	c.send(&pb.ClientMessage{RequestId: 10, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{FileIds: []uint64{id}},
		}},
	}})
	c.send(&pb.ClientMessage{RequestId: 11, Payload: &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{}}})

	// The catalog reply must come first: the open is still grinding storage.
	first := c.recv()
	if first.GetRequestId() != 11 || first.GetListFiles() == nil {
		t.Fatalf("first reply: want ListFilesResponse(req 11) while OpenSession grinds, got req=%d %T (err=%v)",
			first.GetRequestId(), first.GetPayload(), first.GetError())
	}

	// The open must still complete normally afterwards.
	for {
		msg := c.recv()
		if msg.GetRequestId() == 10 {
			if msg.GetOpenSession() == nil {
				t.Fatalf("OpenSession reply: got %T (err=%v)", msg.GetPayload(), msg.GetError())
			}
			break
		}
	}
}

// Repeat opens of the same (key, etag) must serve the chunk index from the
// SessionDeps cache: with storage made completely unreachable after the first
// open, a second OpenSession must STILL answer with a valid plan (only the
// producer's chunk fetches may fail afterwards). Without the cache the second
// open dies in plan-build with "load chunk index".
func TestOpenSession_SecondOpenServesChunkIndexFromCache(t *testing.T) {
	blob := &gateBlob{inner: memBlobStore{data: map[string][]byte{zegTestKey: loadZegFile(t)}}}
	ts := newTestServerWithBlob(t, blob, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()
	id := c.fileID(t, zegTestKey)

	c.send(&pb.ClientMessage{RequestId: 20, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{FileIds: []uint64{id}},
		}},
	}})
	open := c.recv()
	or := open.GetOpenSession()
	if or == nil {
		t.Fatalf("first open: expected OpenSessionResponse, got %T (err=%v)", open.GetPayload(), open.GetError())
	}
	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	c.streamToEnd(res, or.GetSubscriptionId(), 4)
	if res.eos == nil || res.errFrame != nil {
		t.Fatalf("first open must stream to Eos (eos=%v err=%v)", res.eos != nil, res.errFrame)
	}

	blob.broken.Store(true) // bucket gone: only the cache can answer now

	c.send(&pb.ClientMessage{RequestId: 21, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{FileIds: []uint64{id}},
		}},
	}})
	for {
		msg := c.recv()
		if msg.GetRequestId() != 21 {
			continue
		}
		or := msg.GetOpenSession()
		if or == nil {
			t.Fatalf("second open must be served from the chunk-index cache, got %T (err=%v)",
				msg.GetPayload(), msg.GetError())
		}
		if len(or.GetTopicIdMap()) == 0 {
			t.Fatal("cached plan lost its topic bindings")
		}
		return
	}
}
