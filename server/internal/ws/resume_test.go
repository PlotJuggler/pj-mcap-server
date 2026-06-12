package ws

import (
	"testing"
	"time"

	"nhooyr.io/websocket"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// TestSession_ResumeNoGapsNoDupes drops the WS mid-stream, reconnects, and
// resumes with OpenResume{subscription_id, resume_after_seq}. The combined
// stream across both attachments must reach COMPLETE with the exact total and
// no seq gaps or duplicate seqs (spec §6.5/§6.6).
func TestSession_ResumeNoGapsNoDupes(t *testing.T) {
	// Small batches + a generous retain window so the producer keeps running
	// while we are disconnected and nothing is evicted before we reconnect.
	cfg := defaultTestSessionCfg()
	cfg.MaxBatchBytes = 16 << 10
	cfg.RetainAfterDisconnect = 30 * time.Second
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, cfg)

	c1 := dialClient(t, ts.url)
	c1.hello()
	id := c1.fileID(t, zegTestKey)

	c1.send(&pb.ClientMessage{RequestId: 20, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{FileIds: []uint64{id}},
		}},
	}})
	or := c1.recv().GetOpenSession()
	if or == nil {
		t.Fatal("expected OpenSessionResponse")
	}
	subID := or.GetSubscriptionId()
	topicName := map[uint32]string{}
	for _, b := range or.GetTopicIdMap() {
		topicName[b.GetTopicId()] = b.GetTopicName()
	}

	// Phase 1: read a handful of batches, acking the first few so they are
	// pruned (resume must NOT re-deliver acked batches), then drop the socket.
	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	seen := map[uint64]int{}
	var ackedThrough uint64
	batchesRead := 0
	for batchesRead < 8 {
		msg := c1.recv()
		if b := msg.GetBatch(); b != nil {
			seen[b.GetSeq()]++
			c1.decodeBatch(res, b, topicName)
			batchesRead++
			// Ack through the 3rd batch's seq only (prune up to there).
			if batchesRead == 3 {
				ackedThrough = res.maxSeq
				c1.send(&pb.ClientMessage{Payload: &pb.ClientMessage_Ack{Ack: &pb.SessionAck{
					SubscriptionId: subID, ThroughSeq: ackedThrough,
				}}})
			}
		}
	}
	resumeAfter := res.maxSeq // last seq durably received on attachment 1

	// Hard-drop the WS (abnormal closure) -> server detaches the consumer; the
	// producer keeps running into the retain buffer; eviction timer armed.
	_ = c1.conn.Close(websocket.StatusGoingAway, "simulated drop")

	// Give the server a moment to observe the disconnect + detach.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := ts.reg.Lookup(subID); ok {
			break // still alive (retained) — good
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := ts.reg.Lookup(subID); !ok {
		t.Fatal("session evicted before resume could attach")
	}

	// Phase 2: reconnect, Hello, OpenResume after resumeAfter.
	c2 := dialClient(t, ts.url)
	c2.hello()
	c2.send(&pb.ClientMessage{RequestId: 21, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Resume{
			Resume: &pb.OpenResume{SubscriptionId: subID, ResumeAfterSeq: resumeAfter},
		}},
	}})
	or2 := c2.recv().GetOpenSession()
	if or2 == nil {
		t.Fatalf("expected OpenSessionResponse on resume, got error")
	}
	if or2.GetSubscriptionId() != subID {
		t.Errorf("resume subscription_id: got %d want %d", or2.GetSubscriptionId(), subID)
	}
	if len(or2.GetTopicIdMap()) != len(or.GetTopicIdMap()) {
		t.Errorf("resume topic_id_map size: got %d want %d", len(or2.GetTopicIdMap()), len(or.GetTopicIdMap()))
	}

	// Drain the remainder, acking as we go so the producer keeps draining the
	// retain buffer (default caps would otherwise park it). Batches must start
	// strictly after resumeAfter.
	batchesSinceAck := 0
	for {
		msg := c2.recv()
		switch {
		case msg.GetBatch() != nil:
			b := msg.GetBatch()
			if b.GetSeq() <= resumeAfter {
				t.Errorf("resume re-delivered already-received seq %d (resume_after=%d)", b.GetSeq(), resumeAfter)
			}
			if seen[b.GetSeq()] > 0 {
				t.Errorf("resume duplicated seq %d", b.GetSeq())
			}
			seen[b.GetSeq()]++
			c2.decodeBatch(res, b, topicName)
			batchesSinceAck++
			if batchesSinceAck >= 16 {
				c2.send(&pb.ClientMessage{Payload: &pb.ClientMessage_Ack{Ack: &pb.SessionAck{
					SubscriptionId: subID, ThroughSeq: res.maxSeq,
				}}})
				batchesSinceAck = 0
			}
		case msg.GetEos() != nil:
			res.eos = msg.GetEos()
		case msg.GetError() != nil:
			t.Fatalf("unexpected Error on resume: %v", msg.GetError())
		}
		if res.eos != nil {
			break
		}
	}

	// No gaps: every seq from (ackedThrough+1) up to maxSeq present exactly once.
	for s := ackedThrough + 1; s <= res.maxSeq; s++ {
		if seen[s] != 1 {
			t.Errorf("seq %d delivered %d times (want exactly 1)", s, seen[s])
		}
	}
	if res.eos.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("resume eos reason: got %v want COMPLETE", res.eos.GetReason())
	}
	if res.total != zegTotalMessages {
		t.Errorf("combined total across resume: got %d want %d", res.total, zegTotalMessages)
	}
	// The terminal Eos.total_messages_sent MUST account for the messages delivered
	// on attachment 1 too — the post-resume consumer carries the prior counter
	// forward (Slice 10 flagged defect 1: spawnConsumer used to zero it, so the
	// resumed Eos undercounted by the attachment-1 deliveries).
	if res.eos.GetTotalMessagesSent() != zegTotalMessages {
		t.Errorf("resume eos total_messages_sent: got %d want %d (post-resume undercount?)",
			res.eos.GetTotalMessagesSent(), zegTotalMessages)
	}
	if res.eos.GetTotalBytesSent() == 0 {
		t.Errorf("resume eos total_bytes_sent: got 0, want the full combined bytes")
	}
}

// TestSession_AckPruneUnderTinyCaps drives a session with a tiny retain seq cap.
// Without acks the producer would park at the cap; steady acking keeps it
// draining to COMPLETE with the full message count and the retain buffer stays
// bounded.
func TestSession_AckPruneUnderTinyCaps(t *testing.T) {
	cfg := defaultTestSessionCfg()
	cfg.RetainMaxSeqs = 3
	cfg.MaxBatchBytes = 16 << 10 // many small batches -> exercises the cap
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, cfg)

	c := dialClient(t, ts.url)
	c.hello()
	id := c.fileID(t, zegTestKey)

	c.send(&pb.ClientMessage{RequestId: 30, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{FileIds: []uint64{id}, TopicNames: []string{zegSpeedTopic}},
		}},
	}})
	or := c.recv().GetOpenSession()
	if or == nil {
		t.Fatal("expected OpenSessionResponse")
	}
	subID := or.GetSubscriptionId()
	topicName := map[uint32]string{}
	for _, b := range or.GetTopicIdMap() {
		topicName[b.GetTopicId()] = b.GetTopicName()
	}

	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	// Ack after EVERY batch so the tiny cap never permanently blocks the producer.
	for {
		msg := c.recv()
		switch {
		case msg.GetBatch() != nil:
			c.decodeBatch(res, msg.GetBatch(), topicName)
			c.send(&pb.ClientMessage{Payload: &pb.ClientMessage_Ack{Ack: &pb.SessionAck{
				SubscriptionId: subID, ThroughSeq: res.maxSeq,
			}}})
		case msg.GetEos() != nil:
			res.eos = msg.GetEos()
		case msg.GetError() != nil:
			t.Fatalf("unexpected Error: %v", msg.GetError())
		}
		if res.eos != nil {
			break
		}
	}

	if res.seqGaps {
		t.Error("seq gaps under ack-prune")
	}
	if res.total != zegSpeedMessages {
		t.Errorf("ack-prune total: got %d want %d", res.total, zegSpeedMessages)
	}
	if res.eos.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("eos reason: got %v want COMPLETE", res.eos.GetReason())
	}

	// After COMPLETE + final ack, the session should be deregistered.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := ts.reg.Lookup(subID); !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := ts.reg.Lookup(subID); ok {
		t.Error("session still registered after COMPLETE")
	}
}

// TestSession_OverlapRejected builds two catalog files whose time ranges overlap
// and asserts OpenFresh over both returns INVALID_REQUEST.
//
// We synthesize the overlap by reusing the single testdata file twice via the
// catalog (same time bounds => overlap). Because the in-memory catalog keys on
// object name, we register the same bytes under two keys.
func TestSession_OverlapRejected(t *testing.T) {
	raw := loadZegFile(t)
	files := map[string][]byte{
		"a_" + zegTestKey: raw,
		"b_" + zegTestKey: raw, // identical time range => pairwise overlap
	}
	ts := newTestServer(t, files, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	idA := c.fileID(t, "a_"+zegTestKey)
	idB := c.fileID(t, "b_"+zegTestKey)

	c.send(&pb.ClientMessage{RequestId: 40, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{FileIds: []uint64{idA, idB}},
		}},
	}})
	e := c.recv().GetError()
	if e == nil || e.GetCode() != pb.ErrorCode_ERROR_INVALID_REQUEST {
		t.Fatalf("expected INVALID_REQUEST for overlapping files, got %v", e)
	}
}
