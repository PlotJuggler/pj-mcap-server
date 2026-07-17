package warm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/storage"

	_ "modernc.org/sqlite"
)

// warmFixtureKey is the rebuilt auryn s3_key for fixture file i (every warm_test
// fixture shares one dimension tuple; only the filename varies) — the Warmer
// pulls WarmEntries from the catalog and asks the codec for THIS key, so tests
// must assert against it rather than the bare "k<i>" name the legacy Go-writer
// fixture used directly as its (non-Hive) s3_key.
func warmFixtureKey(i int) string {
	return fmt.Sprintf("customer=t/customer_site=t/robot=t/source=t/date=2026-01-01/k%d.mcap", i)
}

// fakeCodec counts ChunkIndex calls per key and can fail or panic on selected keys.
type fakeCodec struct {
	mu        sync.Mutex
	calls     map[string]int
	failKeys  map[string]bool
	panicKeys map[string]bool
}

func newFakeCodec(fail ...string) *fakeCodec {
	fc := &fakeCodec{calls: map[string]int{}, failKeys: map[string]bool{}, panicKeys: map[string]bool{}}
	for _, k := range fail {
		fc.failKeys[k] = true
	}
	return fc
}
func (f *fakeCodec) callsFor(key string) int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls[key] }

func (f *fakeCodec) Extract(context.Context, storage.BlobStore, string) (format.FileSummary, error) {
	return format.FileSummary{}, nil
}
func (f *fakeCodec) ChunkIndex(_ context.Context, _ storage.BlobStore, key string, fileID uint64) (format.FileChunkIndex, error) {
	f.mu.Lock()
	f.calls[key]++
	fail := f.failKeys[key]
	panicky := f.panicKeys[key]
	f.mu.Unlock()
	if panicky {
		panic("codec bug: malformed MCAP")
	}
	if fail {
		return format.FileChunkIndex{}, errors.New("boom")
	}
	return format.FileChunkIndex{FileID: fileID}, nil
}
func (f *fakeCodec) ExtractAndIndex(context.Context, storage.BlobStore, string, uint64) (format.FileSummary, format.FileChunkIndex, error) {
	return format.FileSummary{}, format.FileChunkIndex{}, nil
}

// seedStore creates an auryn-schema (v3) catalog store with n files (filenames
// k0.mcap..k{n-1}.mcap under one shared dimension tuple; every file's etag is
// "e"). warmFixtureKey(i) computes the rebuilt s3_key WarmEntries/the Warmer
// will actually use for file i.
func seedStore(t *testing.T, n int) *catalog.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "c.db")
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
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
		`INSERT INTO customers(id,name) VALUES (1,'t')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'t')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'t')`,
		`INSERT INTO sources(id,name) VALUES (1,'t')`,
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
	for i := 0; i < n; i++ {
		if _, err := db.Exec(`INSERT INTO files
			(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (?,?,?,1,1,1,1,1,'2026-01-01',1,2,1,1,X'01')`,
			i+1, fmt.Sprintf("k%d.mcap", i), "e"); err != nil {
			t.Fatalf("insert file %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close writer handle: %v", err)
	}

	st, err := catalog.OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestWarmer_WarmsAllThenSkips(t *testing.T) {
	store := seedStore(t, 5)
	codec := newFakeCodec()
	cache := format.NewChunkIndexCache(64)
	w := &Warmer{Store: store, Codec: codec, Blob: nil, Cache: cache, Concurrency: 3, Metrics: metrics.New()}

	// First sweep: warm all 5.
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cache.Len() != 5 {
		t.Fatalf("cache len = %d, want 5", cache.Len())
	}
	for i := 0; i < 5; i++ {
		if c := codec.callsFor(warmFixtureKey(i)); c != 1 {
			t.Fatalf("k%d codec calls = %d, want 1", i, c)
		}
	}

	// Second sweep: all cached => zero new codec calls (skipped).
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	for i := 0; i < 5; i++ {
		if c := codec.callsFor(warmFixtureKey(i)); c != 1 {
			t.Fatalf("k%d codec calls after re-warm = %d, want still 1 (skipped)", i, c)
		}
	}
}

func TestWarmer_StopsAtBudget(t *testing.T) {
	store := seedStore(t, 5)
	codec := newFakeCodec()
	// Each fake index is ~ApproxBytes() (a base ~512 with no schemas). A budget of
	// ~2 entries must stop the sweep well before all 5 — the warmer must NOT keep
	// fetching files that would only evict what it just warmed. Concurrency 1 keeps
	// the overshoot minimal.
	oneEntry := int64((format.FileChunkIndex{}).ApproxBytes())
	cache := format.NewChunkIndexCacheSized(0, 100*oneEntry) // roomy cache; budget does the bounding
	w := &Warmer{
		Store: store, Codec: codec, Cache: cache, Concurrency: 1,
		Budget: 2 * oneEntry, Metrics: metrics.New(),
	}

	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	total := 0
	for i := 0; i < 5; i++ {
		total += codec.callsFor(warmFixtureKey(i))
	}
	if total >= 5 || total < 2 {
		t.Fatalf("warmed %d files, want a bounded 2-3 (stopped at budget, not all 5)", total)
	}
}

func TestWarmer_NoBudgetWarmsAll(t *testing.T) {
	store := seedStore(t, 4)
	codec := newFakeCodec()
	cache := format.NewChunkIndexCache(64)
	w := &Warmer{Store: store, Codec: codec, Cache: cache, Concurrency: 2, Metrics: metrics.New()}
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cache.Len() != 4 {
		t.Fatalf("cache len=%d, want 4 (Budget=0 warms everything)", cache.Len())
	}
}

func TestWarmer_PoisonFileDoesNotAbort(t *testing.T) {
	store := seedStore(t, 4)
	codec := newFakeCodec(warmFixtureKey(2)) // k2 always fails
	cache := format.NewChunkIndexCache(64)
	w := &Warmer{Store: store, Codec: codec, Blob: nil, Cache: cache, Concurrency: 2}

	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The 3 healthy files warmed despite k2 failing — the sweep did not abort.
	if cache.Len() != 3 {
		t.Fatalf("cache len = %d, want 3 (k2 failed, others warmed)", cache.Len())
	}
	if _, ok := cache.Get(warmFixtureKey(2), "e", 0); ok {
		t.Fatal("k2 should NOT be cached (it failed)")
	}
	for _, i := range []int{0, 1, 3} {
		if _, ok := cache.Get(warmFixtureKey(i), "e", 0); !ok {
			t.Fatalf("k%d should be cached", i)
		}
	}
}

// A codec PANIC on one pathological file must behave exactly like a per-file
// error: counted, skipped, sweep continues. Without in-closure recovery the
// panic escapes the errgroup goroutine and kills the whole server process
// (main.go runs the warmer as a bare goroutine).
func TestWarmer_PanicFileDoesNotCrash(t *testing.T) {
	store := seedStore(t, 4)
	codec := newFakeCodec()
	codec.panicKeys[warmFixtureKey(1)] = true // k1 panics the codec
	cache := format.NewChunkIndexCache(64)
	w := &Warmer{Store: store, Codec: codec, Blob: nil, Cache: cache, Concurrency: 2, Metrics: metrics.New()}

	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cache.Len() != 3 {
		t.Fatalf("cache len = %d, want 3 (k1 panicked, others warmed)", cache.Len())
	}
	if _, ok := cache.Get(warmFixtureKey(1), "e", 0); ok {
		t.Fatal("k1 should NOT be cached (its load panicked)")
	}
	for _, i := range []int{0, 2, 3} {
		if _, ok := cache.Get(warmFixtureKey(i), "e", 0); !ok {
			t.Fatalf("k%d should be cached", i)
		}
	}
}

func TestWarmer_NilCacheNoop(t *testing.T) {
	w := &Warmer{Store: seedStore(t, 2), Codec: newFakeCodec(), Cache: nil}
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run with nil cache: %v", err)
	}
}

func TestWarmer_CancelledContext(t *testing.T) {
	store := seedStore(t, 3)
	codec := newFakeCodec()
	cache := format.NewChunkIndexCache(64)
	w := &Warmer{Store: store, Codec: codec, Cache: cache, Concurrency: 2}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	// Must return promptly without panicking. A cancelled ctx may fail the catalog
	// list (propagated as context.Canceled) or, if the list slipped through, warm
	// nothing — both are acceptable; a panic or hang is not.
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(cancelled): unexpected err %v", err)
	}
}
