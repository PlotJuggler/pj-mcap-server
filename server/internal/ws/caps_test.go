package ws

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"pj-cloud/server/internal/catalog"
)

// newWSTestServer stands up a dev-anonymous WS handler over a caller-supplied,
// already-opened *catalog.Store (writable or read-only) and returns the ws://
// URL. Shared by every test file that needs a specific store shape (freshly
// seeded, auryn read-only, …) fronted by the real handler/mux/httptest
// scaffolding, so that scaffolding is written exactly once.
func newWSTestServer(t *testing.T, store *catalog.Store) string {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(store, "", log)
	mux := http.NewServeMux()
	mux.Handle("/api/ws", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws"
}

// newCapsTestServer stands up a dev-anonymous WS handler over a catalog the
// caller seeds via the returned *catalog.Store. This lets the BackendCapabilities
// derivation be exercised against a KNOWN set of object keys / tags, unlike the
// empty-catalog auth tests.
func newCapsTestServer(t *testing.T) (string, *catalog.Store) {
	t.Helper()
	cat, err := catalog.Open(context.Background(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return newWSTestServer(t, cat), cat
}

func capsSeedFile(t *testing.T, s *catalog.Store, key string) uint64 {
	t.Helper()
	ctx := context.Background()
	var id int64
	err := s.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(
			`INSERT INTO files (s3_key, s3_etag, s3_last_modified, size_bytes,
			    indexed_at, start_time_ns, end_time_ns, chunk_count, message_count, has_message_index)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			key, "etag", 1, 1, 1, 0, 1, 1, 1, 1,
		)
		if err != nil {
			return err
		}
		id, err = res.LastInsertId()
		return err
	})
	if err != nil {
		t.Fatalf("capsSeedFile(%q): %v", key, err)
	}
	return uint64(id)
}

// TestHello_DerivesCapsFromCatalog_FlatCorpus proves the Hello handler no longer
// hardcodes BackendCapabilities: for the FLAT Dexory nissan corpus (no '/' in any
// object key, no embedded tags) it advertises supports_file_hierarchy=false and a
// metadata_key_vocabulary == the constant DERIVED keys (the stable floor). This is
// the ground truth the C++ B3 live test asserts against on the seeded smoke bucket.
func TestHello_DerivesCapsFromCatalog_FlatCorpus(t *testing.T) {
	url, cat := newCapsTestServer(t)
	capsSeedFile(t, cat, "nissan_zala_50_zeg_1_0.mcap")
	capsSeedFile(t, cat, "nissan_zala_50_zeg_2_0.mcap")

	resp := helloRoundTrip(t, url, "")
	hr := resp.GetHelloResponse()
	if hr == nil {
		t.Fatalf("expected HelloResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	backend := hr.GetCapabilities() // sanity: top-level caps still present
	if backend == nil {
		t.Fatal("HelloResponse.capabilities missing")
	}
	b := hr.GetBackend()
	if b == nil {
		t.Fatal("HelloResponse.backend (BackendCapabilities) missing")
	}
	if b.GetSupportsFileHierarchy() {
		t.Error("flat-key corpus must advertise supports_file_hierarchy=false")
	}
	want := catalog.DerivedMetadataKeys()
	if !reflect.DeepEqual(b.GetMetadataKeyVocabulary(), want) {
		t.Errorf("metadata_key_vocabulary: got %v want %v (derived keys on a tag-free corpus)",
			b.GetMetadataKeyVocabulary(), want)
	}
}

// TestHello_DerivesCapsFromCatalog_HierarchyAndTags proves the derivation flips
// to hierarchy=true when ANY object key bears a '/', and that distinct override
// tag keys join the vocabulary (derived ∪ tags, sorted, deduped).
func TestHello_DerivesCapsFromCatalog_HierarchyAndTags(t *testing.T) {
	url, cat := newCapsTestServer(t)
	capsSeedFile(t, cat, "robot-7/2026-06-05/run.mcap") // '/'-bearing => hierarchy
	id := capsSeedFile(t, cat, "robot-7/2026-06-06/run.mcap")
	if err := catalog.SetOverride(context.Background(), cat, id, "site", "wh-3"); err != nil {
		t.Fatal(err)
	}

	resp := helloRoundTrip(t, url, "")
	hr := resp.GetHelloResponse()
	if hr == nil {
		t.Fatalf("expected HelloResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	b := hr.GetBackend()
	if b == nil {
		t.Fatal("HelloResponse.backend missing")
	}
	if !b.GetSupportsFileHierarchy() {
		t.Error("a '/'-bearing object key must advertise supports_file_hierarchy=true")
	}
	vocab := b.GetMetadataKeyVocabulary()
	hasSite := false
	for _, k := range vocab {
		if k == "site" {
			hasSite = true
		}
	}
	if !hasSite {
		t.Errorf("vocabulary %v must include the distinct override tag key \"site\"", vocab)
	}
	// Must still contain the derived floor.
	for _, dk := range catalog.DerivedMetadataKeys() {
		found := false
		for _, k := range vocab {
			if k == dk {
				found = true
			}
		}
		if !found {
			t.Errorf("vocabulary %v dropped derived key %q", vocab, dk)
		}
	}
}
