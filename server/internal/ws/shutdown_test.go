package ws

import (
	"context"
	"testing"
	"time"

	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/session"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// TestGracefulShutdown_MidStream models the SIGTERM path (spec §8.4 / main's
// shutdown goroutine): a session is opened and streaming, then Registry.CancelAll
// is invoked (the exact call main makes) followed by the catalog store close
// (WAL checkpoint). The assertions: CancelAll + Close both complete inside a
// bounded window (no producer/consumer goroutine wedges the shutdown), no panic
// is recorded, and the registry ends empty.
func TestGracefulShutdown_MidStream(t *testing.T) {
	// Tiny retain caps so the producer parks mid-plan (truly mid-stream) once the
	// consumer stops draining — the worst case for a clean teardown.
	cfg := defaultTestSessionCfg()
	cfg.RetainMaxSeqs = 2
	cfg.RetainMaxBytes = 4096
	cfg.MaxBatchBytes = 1024
	cfg.RetainAfterDisconnect = 60 * time.Second // long, so only CancelAll evicts

	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, cfg)

	c := dialClient(t, ts.url)
	c.hello()
	id := c.fileID(t, zegTestKey)

	c.send(&pb.ClientMessage{RequestId: 10, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{FileIds: []uint64{id}},
		}},
	}})
	or := c.recv().GetOpenSession()
	if or == nil {
		t.Fatalf("expected OpenSessionResponse")
	}

	// Receive at least one MessageBatch so the session is genuinely mid-stream.
	deadline := time.After(15 * time.Second)
	gotBatch := false
	for !gotBatch {
		select {
		case <-deadline:
			t.Fatalf("no MessageBatch before shutdown")
		default:
		}
		if c.recv().GetBatch() != nil {
			gotBatch = true
		}
	}
	if ts.reg.ActiveCount() != 1 {
		t.Fatalf("active sessions before shutdown = %d; want 1", ts.reg.ActiveCount())
	}

	// SHUTDOWN: the exact call main's signal goroutine makes. Must return well
	// within the bounded window even with a producer parked on the retain caps.
	done := make(chan struct{})
	go func() {
		ts.reg.CancelAll()
		_ = ts.cat.Close() // WAL checkpoint
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("graceful shutdown did not complete within 10s (a goroutine wedged it)")
	}

	if n := ts.reg.ActiveCount(); n != 0 {
		t.Errorf("registry not empty after CancelAll: %d", n)
	}
	// A second Close is idempotent (the deferred cleanup will Close again) — assert
	// it does not error or panic, matching the catalog store's shutdown contract.
	if err := ts.cat.Close(); err != nil {
		t.Errorf("second catalog Close returned error: %v", err)
	}
}

// TestGracefulShutdown_NoActiveSessions: CancelAll on an empty registry is a
// no-op and the store closes cleanly (the common quiescent-shutdown case).
func TestGracefulShutdown_NoActiveSessions(t *testing.T) {
	cfg := defaultTestSessionCfg()
	ts := newTestServer(t, map[string][]byte{zegTestKey: loadZegFile(t)}, cfg)

	done := make(chan struct{})
	go func() {
		ts.reg.CancelAll()
		_ = ts.cat.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("quiescent shutdown did not complete within 5s")
	}
}

// TestSessionsActiveGauge_RoundTrip proves the SessionsActive gauge wiring: a
// fresh open increments it (via the handler) and Registry.Cancel via the onEvict
// callback decrements it — the exact wiring main installs. Drives the registry
// directly (no WS) to keep it hermetic + fast.
func TestSessionsActiveGauge_RoundTrip(t *testing.T) {
	mx := metrics.New()
	reg := session.NewRegistry(session.RegistryOpts{MaxConcurrent: 4})
	reg.SetOnEvict(func(*session.SessionState) { mx.SessionsActive.Dec() })

	// Simulate the handler's increment-on-register.
	st := &session.SessionState{Retain: session.NewRetainBuffer(session.RetainOpts{MaxSeqs: 1, MaxBytes: 1024})}
	if _, err := reg.Register(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	mx.SessionsActive.Inc()

	if g := metrics.GaugeValue(mx.SessionsActive); g != 1 {
		t.Fatalf("sessions_active after register = %v; want 1", g)
	}

	reg.Cancel(st.ID) // fires onEvict -> Dec
	if g := metrics.GaugeValue(mx.SessionsActive); g != 0 {
		t.Fatalf("sessions_active after cancel = %v; want 0", g)
	}
}
