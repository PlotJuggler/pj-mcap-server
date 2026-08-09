package ws

import (
	"context"
	"testing"
	"time"
)

// Hello must NEVER wait for SQLite.
//
// This is the user-visible invariant behind the 2026-08-09 outage: handleHello
// used to derive BackendCapabilities with two live catalog queries, and the
// read-only store pins exactly ONE physical connection (catalog/readonly.go's
// C1 pin). So while a slow catalog RPC held that connection, a brand-new
// client's WebSocket upgrade succeeded, its Hello was received and
// authenticated, and then the HelloResponse starved — the client gave up with
// "no response to handshake". Measured against production: GetVocabulary at
// 40.7s on an 8.78M-file catalog, with concurrent connects failing for its
// whole duration and recovering ~15s after it finished.
//
// The unit tests in catalog/caps_snapshot_test.go prove the CapsSnapshot type
// behaves (memory-only reads, last-good on refresh failure). They deliberately
// do NOT prove this: a refactor could reintroduce a direct catalog call into
// handleHello and leave every one of those tests green. This test fails if that
// happens, because it holds the sole connection hostage for longer than the
// deadline it gives the handshake.
func TestHello_DoesNotWaitForTheCatalogConnection(t *testing.T) {
	store := openAurynReadStore(t)
	url := newWSTestServer(t, store)

	// Occupy the single pinned connection with a long-running query. BEGIN
	// IMMEDIATE is not needed — a plain long read on the one pooled connection is
	// enough, because database/sql hands it to exactly one caller at a time.
	held := make(chan struct{})
	release := make(chan struct{})
	go func() {
		lease := store.Acquire()
		defer lease.Release()
		db := lease.DB()
		if db == nil {
			close(held)
			return
		}
		conn, err := db.Conn(context.Background())
		if err != nil {
			close(held)
			return
		}
		defer conn.Close()
		close(held) // the connection is ours from here
		<-release
	}()
	<-held
	defer close(release)

	// With the connection held, a fresh client must still complete its handshake
	// well inside the budget. Any catalog I/O on this path would block until the
	// goroutine above releases, i.e. forever from this test's perspective.
	done := make(chan struct{})
	go func() {
		defer close(done)
		c := dialClient(t, url)
		c.hello()
	}()

	select {
	case <-done:
		// Handshake completed while the catalog connection was occupied.
	case <-time.After(5 * time.Second):
		t.Fatal("Hello did not complete while the sole catalog connection was held — " +
			"the handshake path is touching SQLite again (see catalog.CapsSnapshot)")
	}
}
