package ws

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// countingBlobStore wraps a storage.BlobStore and tallies the bytes returned by
// GetRange. It is the instrumented store for the estimated_chunk_bytes-within-5%
// gate (Slice 10 TASK 1): it counts ALL ranged reads (both the chunk-index
// summary loads done during OpenFresh AND the producer's stream chunk fetches),
// so the test snapshots the counter at OpenSessionResponse (after the index load)
// and reads it again at Eos — the delta is the producer's session fetch bytes,
// the actual budget the pre-flight estimate is compared against.
type countingBlobStore struct {
	inner    storage.BlobStore
	getBytes atomic.Int64
	getCalls atomic.Int64
}

func newCountingBlobStore(inner storage.BlobStore) *countingBlobStore {
	return &countingBlobStore{inner: inner}
}

func (c *countingBlobStore) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	b, err := c.inner.GetRange(ctx, key, off, length)
	c.getCalls.Add(1)
	c.getBytes.Add(int64(len(b)))
	return b, err
}

func (c *countingBlobStore) Head(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return c.inner.Head(ctx, key)
}

func (c *countingBlobStore) List(ctx context.Context, prefix, token string) ([]storage.ObjectInfo, string, error) {
	return c.inner.List(ctx, prefix, token)
}

func (c *countingBlobStore) bytes() int64 { return c.getBytes.Load() }

// newCountingTestServer wraps the blob store in a countingBlobStore AND wires a
// fresh metrics set, returning all three. The countingBlobStore is retained as a
// documented cross-check of total GetRange traffic; the AUTHORITATIVE per-session
// stream-fetch budget the gate asserts against is the metrics FetchedBytesTotal
// counter (the producer's own FetchedBytes(), the exact ground truth the slog
// "session: complete" line + the /metrics counter report).
//
// FetchedBytesTotal counts ONLY producer stream chunk fetches (it is fed by
// Producer.FetchedBytes() after Run returns) — NOT the synchronous chunk-index
// summary reads OpenFresh issues. So on a fresh server it equals exactly this
// session's stream fetch budget with no startup race: the producer's
// FetchedBytesTotal.Add runs before close(ProducerDone), and the consumer only
// sends the terminal Eos after ProducerDone — so by the time the client observes
// Eos the counter is final and complete. This sidesteps the snapshot-after-
// response race a mid-stream countingBlobStore delta would suffer (the producer
// can fetch chunks before the client receives OpenSessionResponse).
func newCountingTestServer(t *testing.T, files map[string][]byte, cfg config.SessionConfig) (*testServer, *metrics.Metrics) {
	t.Helper()
	counting := newCountingBlobStore(memBlobStore{data: files})
	ts, mx := newTestServerWithMetrics(t, counting, cfg)
	return ts, mx
}

// estimateLeg drives one OpenFresh to Eos and returns the OpenSessionResponse's
// estimated_chunk_bytes and the producer's actual session stream-fetch bytes
// (the metrics FetchedBytesTotal, which on a fresh-per-leg server is exactly this
// session's producer GetRange budget — see newCountingTestServer).
func estimateLeg(t *testing.T, ts *testServer, mx *metrics.Metrics, fresh *pb.OpenFresh) (estimated uint64, fetched int64, msgs int) {
	t.Helper()
	c := dialClient(t, ts.url)
	c.hello()

	c.send(&pb.ClientMessage{RequestId: 99, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{Fresh: fresh}},
	}})
	or := c.recv().GetOpenSession()
	if or == nil {
		t.Fatal("expected OpenSessionResponse")
	}

	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	c.streamToEnd(res, or.GetSubscriptionId(), 64)
	if res.errFrame != nil {
		t.Fatalf("unexpected Error: %v", res.errFrame)
	}
	if res.eos == nil || res.eos.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
		t.Fatalf("expected Eos{COMPLETE}, got %+v", res.eos)
	}
	// FetchedBytesTotal is final by the time Eos is observed (producer Add runs
	// before close(ProducerDone); consumer Eos follows ProducerDone).
	fetched = int64(metrics.CounterValue(mx.FetchedBytesTotal))
	return or.GetEstimatedChunkBytes(), fetched, res.total
}

// assertWithin5Pct asserts |estimated - fetched| / fetched <= 0.05 (the spec
// pre-flight accuracy contract). An empty plan (fetched==0) requires estimated==0.
func assertWithin5Pct(t *testing.T, leg string, estimated uint64, fetched int64) {
	t.Helper()
	if fetched == 0 {
		if estimated != 0 {
			t.Errorf("%s: empty plan must estimate 0 bytes, got %d", leg, estimated)
		}
		return
	}
	diff := int64(estimated) - fetched
	if diff < 0 {
		diff = -diff
	}
	pct := 100.0 * float64(diff) / float64(fetched)
	t.Logf("%s: estimated=%d fetched=%d delta=%.3f%%", leg, estimated, fetched, pct)
	if pct > 5.0 {
		t.Errorf("%s: estimated_chunk_bytes %d vs actual fetched %d => %.3f%% > 5%%",
			leg, estimated, fetched, pct)
	}
}

// TestEstimateWithin5Pct_SingleFile proves the OpenSessionResponse.estimated_chunk_bytes
// is within 5% of the bytes the producer actually Range-GET from the blob store,
// across the full, topic-subset, and time-window session shapes on zeg_1. This is
// the wire-field-free assertion of the spec's pre-flight accuracy contract: the
// estimate is the sum of intersecting chunk lengths; the producer fetches exactly
// those chunks, so they must match closely (equal by construction today — the gate
// catches any future drift such as over-read padding or a planning/fetch mismatch).
//
// MEASUREMENT NOTE: each leg uses its OWN fresh server + metrics set, and reads
// the producer's stream-fetch budget from the metrics FetchedBytesTotal counter
// (the exact ground truth the slog "session: complete" line + /metrics report),
// final by the time Eos is observed. This avoids a mid-stream snapshot race: the
// producer goroutine is spawned right after OpenSessionResponse is queued and can
// fetch chunks before the client receives that response, so a "delta of GetRange
// bytes between response-receipt and Eos" would undercount whenever the producer
// races ahead. FetchedBytesTotal is per-server (fresh per leg) and excludes the
// synchronous chunk-index summary reads, so it is exactly this session's stream
// fetch budget.
func TestEstimateWithin5Pct_SingleFile(t *testing.T) {
	zeg := loadZegFile(t)
	freshLeg := func() (*testServer, *metrics.Metrics, uint64) {
		ts, mx := newCountingTestServer(t, map[string][]byte{zegTestKey: zeg}, defaultTestSessionCfg())
		c := dialClient(t, ts.url)
		c.hello()
		return ts, mx, c.fileID(t, zegTestKey)
	}

	t.Run("full", func(t *testing.T) {
		ts, mx, id := freshLeg()
		est, fetched, msgs := estimateLeg(t, ts, mx, &pb.OpenFresh{FileIds: []uint64{id}})
		if msgs != zegTotalMessages {
			t.Errorf("full: got %d msgs want %d", msgs, zegTotalMessages)
		}
		assertWithin5Pct(t, "full", est, fetched)
	})

	t.Run("topic-subset", func(t *testing.T) {
		ts, mx, id := freshLeg()
		est, fetched, msgs := estimateLeg(t, ts, mx,
			&pb.OpenFresh{FileIds: []uint64{id}, TopicNames: []string{zegSpeedTopic}})
		if msgs != zegSpeedMessages {
			t.Errorf("subset: got %d msgs want %d", msgs, zegSpeedMessages)
		}
		assertWithin5Pct(t, "topic-subset", est, fetched)
	})

	t.Run("time-window", func(t *testing.T) {
		ts, mx, id := freshLeg()
		// The middle ~30% of zeg_1 (the smoke f4 window). Crossing chunk boundaries
		// means the estimate (whole-chunk lengths) over-counts vs. messages, but the
		// producer still FETCHES the whole intersecting chunks — so estimated_chunk_bytes
		// and fetched bytes track each other (both are whole-chunk sums).
		est, fetched, _ := estimateLeg(t, ts, mx, &pb.OpenFresh{
			FileIds:   []uint64{id},
			TimeRange: &pb.TimeRange{StartNs: 1696577469299761084, EndNs: 1696577514415840735},
		})
		if fetched == 0 {
			t.Fatal("time-window: fetched 0 bytes (window selected nothing?)")
		}
		assertWithin5Pct(t, "time-window", est, fetched)
	})
}

// TestEstimateWithin5Pct_Stitched runs the gate over a 2-file stitched session
// (zeg_2 + zeg_3, time-disjoint, the smoke f5 pair). It uses the local on-disk
// originals; if they are absent (a checkout without the dataset) the stitched leg
// skips — the single-file legs above remain the hermetic core of the gate.
func TestEstimateWithin5Pct_Stitched(t *testing.T) {
	const dir = "/home/gn/ws/jkk_dataset02"
	keyA, keyB := "nissan_zala_50_zeg_2_0.mcap", "nissan_zala_50_zeg_3_0.mcap"
	rawA, errA := os.ReadFile(filepath.Join(dir, keyA))
	rawB, errB := os.ReadFile(filepath.Join(dir, keyB))
	if errA != nil || errB != nil {
		t.Skipf("stitch originals not present (%v / %v) — single-file legs cover the gate", errA, errB)
	}
	ts, mx := newCountingTestServer(t, map[string][]byte{keyA: rawA, keyB: rawB}, defaultTestSessionCfg())

	c := dialClient(t, ts.url)
	c.hello()
	idA := c.fileID(t, keyA)
	idB := c.fileID(t, keyB)

	est, fetched, msgs := estimateLeg(t, ts, mx, &pb.OpenFresh{FileIds: []uint64{idA, idB}})
	if msgs == 0 {
		t.Fatal("stitched: streamed 0 messages")
	}
	t.Logf("stitched: %d messages across 2 files", msgs)
	assertWithin5Pct(t, "stitched", est, fetched)
}
