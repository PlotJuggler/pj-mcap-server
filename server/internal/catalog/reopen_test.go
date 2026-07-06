package catalog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// buildNamedAurynDB writes a one-file v3 auryn DB at path whose rebuilt s3_key
// embeds genName — distinct DBs built with distinct names let a test assert
// exactly which generation a query is being served from.
//
// SELF-IDENTIFYING (S2): genName is stamped into THREE independently-queried
// places — the customer dimension (read by the summary query), an embedded
// "gen" tag (read by the separate tags_effective query), and the topic name
// (read by the separate topic_set query). A B1 regression (a logical read
// re-fetching Store.DB() between phases instead of pinning once) can then
// surface as these three signals DISAGREEING within a single response, which
// checkGenerationConsistency below asserts never happens.
func buildNamedAurynDB(t *testing.T, path, genName string) {
	t.Helper()
	db := openAurynTestDB(t, path)
	defer db.Close()
	filename := genName + ".mcap"
	topicName := "/topic/" + genName
	ddl := []string{
		fmt.Sprintf(`INSERT INTO customers(id,name) VALUES (1,'%s')`, genName),
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'site')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'r1')`,
		`INSERT INTO sources(id,name) VALUES (1,'src')`,
		fmt.Sprintf(`INSERT INTO topic_names(id,name) VALUES (1,'%s')`, topicName),
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp1')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
		fmt.Sprintf(`INSERT INTO tags_embedded(file_id,key,value) VALUES (1,'gen','%s')`, genName),
	}
	for _, s := range ddl {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ddl %q: %v", s, err)
		}
	}
	insertFile := fmt.Sprintf(`INSERT INTO files
		(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
		VALUES (1,'%s','etag1',999,1,1,1,1,'2026-06-01',1000,2000,1,1,?)`, filename)
	if _, err := db.Exec(insertFile, encodeCounts(1)); err != nil {
		t.Fatalf("insert file: %v", err)
	}
}

// genFromS3Key extracts the customer segment (which buildNamedAurynDB also uses
// as the generation name) from a rebuilt s3_key "customer=<gen>/...". Returns
// ok=false on a malformed key instead of failing the test, so it is safe to call
// from a non-test goroutine (race-test workers).
func genFromS3Key(key string) (gen string, ok bool) {
	const prefix = "customer="
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	rest := key[len(prefix):]
	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		return "", false
	}
	return rest[:idx], true
}

// tagValue looks up a key in an []EffectiveTag slice.
func tagValue(tags []EffectiveTag, key string) (string, bool) {
	for _, t := range tags {
		if t.Key == key {
			return t.Value, true
		}
	}
	return "", false
}

// servedCustomer runs a read through the public reader path (FilterFiles) and
// returns the rebuilt s3_key's customer segment — a signature of which
// generation the Store is currently serving.
func servedCustomer(t *testing.T, st *Store) string {
	t.Helper()
	files, _, err := FilterFiles(context.Background(), st, FilterArgs{})
	if err != nil {
		t.Fatalf("FilterFiles: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("FilterFiles = %d files, want 1", len(files))
	}
	gen, ok := genFromS3Key(files[0].S3Key)
	if !ok {
		t.Fatalf("s3_key %q not parseable", files[0].S3Key)
	}
	return gen
}

// checkGenerationConsistency runs BOTH FilterFiles (summary+tags, a B1-pinned
// read) and GetFileDetail (summary+topics+tags, also B1-pinned) and
// cross-checks that every independently-sourced generation signal within EACH
// call agrees: the customer segment of s3_key, the embedded "gen" tag, and (for
// GetFileDetail) the topic name's generation suffix. A B1 regression — a
// logical read that re-fetches Store.DB() per phase instead of pinning once —
// would let a mid-swap read observe an old generation's summary paired with a
// new generation's tags/topics; that surfaces here as a MISMATCH, never merely
// as a query error, so it is a distinct signal from the transient
// closed-connection errors the caller separately tolerates.
//
// Returns a non-empty mismatch description on inconsistency, or an error for a
// (tolerated) transient failure. Deliberately takes no *testing.T so it is safe
// to call from race-test worker goroutines (t.Fatal is not goroutine-safe).
func checkGenerationConsistency(ctx context.Context, st *Store) (mismatch string, err error) {
	files, _, ffErr := FilterFiles(ctx, st, FilterArgs{})
	if ffErr != nil {
		return "", ffErr
	}
	if len(files) != 1 {
		return fmt.Sprintf("FilterFiles returned %d files, want 1", len(files)), nil
	}
	f := files[0]
	genKey, okKey := genFromS3Key(f.S3Key)
	genTag, okTag := tagValue(f.Tags, "gen")
	if !okKey || !okTag || genKey != genTag {
		return fmt.Sprintf("FilterFiles: s3_key gen=%q(ok=%v) vs tag gen=%q(ok=%v) file=%d",
			genKey, okKey, genTag, okTag, f.ID), nil
	}

	rec, topics, tags, gfErr := GetFileDetail(ctx, st, f.ID)
	if gfErr != nil {
		return "", gfErr
	}
	genKey2, okKey2 := genFromS3Key(rec.S3Key)
	genTag2, okTag2 := tagValue(tags, "gen")
	if len(topics) != 1 {
		return fmt.Sprintf("GetFileDetail: %d topics, want 1 (file=%d)", len(topics), rec.ID), nil
	}
	const topicPrefix = "/topic/"
	if !strings.HasPrefix(topics[0].Name, topicPrefix) {
		return fmt.Sprintf("GetFileDetail: unexpected topic name %q (file=%d)", topics[0].Name, rec.ID), nil
	}
	genTopic := strings.TrimPrefix(topics[0].Name, topicPrefix)
	if !okKey2 || !okTag2 || genKey2 != genTag2 || genKey2 != genTopic {
		return fmt.Sprintf("GetFileDetail: s3_key gen=%q(ok=%v) tag gen=%q(ok=%v) topic gen=%q file=%d",
			genKey2, okKey2, genTag2, okTag2, genTopic, rec.ID), nil
	}
	return "", nil
}

// isClosedConnErr reports whether err is database/sql's "the *sql.DB is
// closed" sentinel-class error (returned by every operation issued directly
// against a Close()d *sql.DB, driver-independent).
func isClosedConnErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "database is closed")
}

// replaceFile atomically swaps newPath onto path (os.Rename within the same
// tmpdir, so same filesystem — an atomic rename, mirroring the builder's
// os.replace onto the served path).
func replaceFile(t *testing.T, path, newPath string) {
	t.Helper()
	if err := os.Rename(newPath, path); err != nil {
		t.Fatalf("rename %s -> %s: %v", newPath, path, err)
	}
}

// TestReadOnly_PoolPinning_SurvivesFileReplace is the C1 regression test.
//
// database/sql's default pool (unlimited open conns) may lazily open a NEW
// physical connection whenever an existing one is busy — and a fresh
// connection re-opens the DSN path from scratch, on-disk, with ZERO schema/
// identity verification. If SetMaxOpenConns(1) (et al.) were removed from
// openVerifiedOnce, this test forces exactly that: it checks out the Store's
// one physical connection and holds it, replaces the served file, then issues
// a concurrent query under a short deadline.
//   - Pinned (correct): MaxOpenConns=1 means the pool has nowhere to get a
//     second connection from — the query BLOCKS waiting for the held one and
//     the short context deadline expires. Once released, the connection is
//     still the ORIGINAL physical connection (same open fd), so it still reads
//     "alpha" even though "bravo" now lives at that path on disk.
//   - Unpinned (the bug this guards against): the pool opens a second, fresh
//     connection to serve the concurrent query immediately — which resolves
//     the DSN path fresh from disk and returns "bravo" with NO schema check, NO
//     identity check, and NO ReopenIfSwapped call. This test would then either
//     see the query succeed within the deadline (not time out) or, if it
//     raced past the deadline anyway, would still be exposed by the sequential
//     re-check afterward returning "bravo" instead of "alpha".
func TestReadOnly_PoolPinning_SurvivesFileReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	buildNamedAurynDB(t, path, "alpha")

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	if got := servedCustomer(t, st); got != "alpha" {
		t.Fatalf("pre-replace customer = %q, want alpha", got)
	}

	// Check out and hold the sole pinned connection so any concurrent query has
	// no idle connection available.
	held, err := st.DB().Conn(context.Background())
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	if err := held.PingContext(context.Background()); err != nil {
		t.Fatalf("held.Ping: %v", err)
	}

	// Replace the served file with a DIFFERENT valid v3 DB WITHOUT ever calling
	// ReopenIfSwapped.
	bravoPath := filepath.Join(dir, "bravo.db")
	buildNamedAurynDB(t, bravoPath, "bravo")
	replaceFile(t, path, bravoPath)

	shortCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _, ffErr := FilterFiles(shortCtx, st, FilterArgs{})
	if ffErr == nil {
		t.Fatal("a concurrent query completed while the sole pinned connection was checked out: " +
			"the pool opened a SECOND physical connection (the C1 hole — it would have re-read the " +
			"swapped-in file with zero verification). Did someone remove SetMaxOpenConns(1)?")
	}
	if !errors.Is(ffErr, context.DeadlineExceeded) {
		t.Fatalf("concurrent query error = %v, want context.DeadlineExceeded (blocked waiting on the pinned connection)", ffErr)
	}

	// Release the held connection and query again (several times, to defeat any
	// idle-connection-reuse luck): the pool must still be the ORIGINAL physical
	// connection, so content is still "alpha" even though "bravo" now lives at
	// that path on disk — nobody re-opened it.
	if err := held.Close(); err != nil {
		t.Fatalf("held.Close: %v", err)
	}
	for i := 0; i < 5; i++ {
		if got := servedCustomer(t, st); got != "alpha" {
			t.Fatalf("query %d after unreopened replace = %q, want alpha (pool must stay pinned to the original file)", i, got)
		}
	}
}

// TestReopenIfSwapped_HappySwap covers the (true, nil) path: after a file
// replace, ReopenIfSwapped detects the new inode, swaps the handle, and queries
// serve the new generation; a second call is a no-op.
func TestReopenIfSwapped_HappySwap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	buildNamedAurynDB(t, path, "alpha")

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	bravoPath := filepath.Join(dir, "bravo.db")
	buildNamedAurynDB(t, bravoPath, "bravo")
	replaceFile(t, path, bravoPath)

	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil || !swapped {
		t.Fatalf("ReopenIfSwapped = (%v, %v), want (true, nil)", swapped, err)
	}
	if got := servedCustomer(t, st); got != "bravo" {
		t.Fatalf("post-swap customer = %q, want bravo", got)
	}

	swapped, err = st.ReopenIfSwapped(context.Background())
	if err != nil || swapped {
		t.Fatalf("second ReopenIfSwapped = (%v, %v), want (false, nil)", swapped, err)
	}
}

// TestReopenIfSwapped_FailClosed covers the (false, err) path: the file changed
// identity but the new content fails verification (wrong schema_version). The
// OLD handle must keep serving; a later valid replace then swaps normally.
func TestReopenIfSwapped_FailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	buildNamedAurynDB(t, path, "alpha")

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	badPath := filepath.Join(dir, "bad.db")
	writeStampedDB(t, badPath, SchemaVersion+1)
	replaceFile(t, path, badPath)

	swapped, err := st.ReopenIfSwapped(context.Background())
	if err == nil || swapped {
		t.Fatalf("ReopenIfSwapped over a bad DB = (%v, %v), want (false, err)", swapped, err)
	}
	var sve *SchemaVersionError
	if !errors.As(err, &sve) {
		t.Fatalf("ReopenIfSwapped err = %v, want *SchemaVersionError", err)
	}
	if got := servedCustomer(t, st); got != "alpha" {
		t.Fatalf("post-fail-closed customer = %q, want alpha (old handle must keep serving)", got)
	}

	// A subsequent VALID replace still swaps normally.
	bravoPath := filepath.Join(dir, "bravo.db")
	buildNamedAurynDB(t, bravoPath, "bravo")
	replaceFile(t, path, bravoPath)

	swapped, err = st.ReopenIfSwapped(context.Background())
	if err != nil || !swapped {
		t.Fatalf("ReopenIfSwapped after fixing the file = (%v, %v), want (true, nil)", swapped, err)
	}
	if got := servedCustomer(t, st); got != "bravo" {
		t.Fatalf("post-recovery customer = %q, want bravo", got)
	}
}

// TestReopenIfSwapped_NoopUnchanged covers the (false, nil) no-op path on an
// unchanged file, and proves a legacy writable Store never attempts a swap.
func TestReopenIfSwapped_NoopUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	buildNamedAurynDB(t, path, "alpha")

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil || swapped {
		t.Fatalf("ReopenIfSwapped on unchanged file = (%v, %v), want (false, nil)", swapped, err)
	}

	rw, err := Open(context.Background(), filepath.Join(dir, "legacy.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rw.Close()
	for i := 0; i < 3; i++ {
		swapped, err := rw.ReopenIfSwapped(context.Background())
		if err != nil || swapped {
			t.Fatalf("writable-store ReopenIfSwapped call %d = (%v, %v), want (false, nil) always", i, swapped, err)
		}
	}
}

// TestReopenIfSwapped_Race hammers FilterFiles/GetFileDetail (the B1-pinned
// compound reads) from many goroutines while a single goroutine replaces the
// served file and calls ReopenIfSwapped repeatedly. Must pass under -race.
// Transient closed-connection errors from a just-swapped-out old handle are
// tolerated (counted, not failed) — but every response, at every point in
// time, must be INTERNALLY consistent (checkGenerationConsistency: the
// summary, tags, and topics of one response must all describe the SAME
// generation); a mixed-generation response is never acceptable, regardless of
// whether it also happens to be the "final" generation by the time the test
// ends.
func TestReopenIfSwapped_Race(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	buildNamedAurynDB(t, path, "gen0")

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	const generations = 6
	ctx := context.Background()

	var stop atomic.Bool
	var transientErrs int64
	var wrongContent atomic.Bool
	var wrongContentDetail atomic.Value // string

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				detail, err := checkGenerationConsistency(ctx, st)
				if err != nil {
					atomic.AddInt64(&transientErrs, 1)
					continue
				}
				if detail != "" {
					wrongContent.Store(true)
					wrongContentDetail.Store(detail)
					continue
				}
			}
		}()
	}

	// Writer side: replace the file + reopen, generations 1..N, then signal stop
	// only after the LAST reopen has been observed to succeed.
	for g := 1; g <= generations; g++ {
		name := fmt.Sprintf("gen%d", g)
		next := filepath.Join(dir, name+".db")
		buildNamedAurynDB(t, next, name)
		replaceFile(t, path, next)

		// Retry ReopenIfSwapped until it succeeds (it may transiently race the
		// rename on some filesystems; bounded so the test cannot hang forever).
		var swapped bool
		for attempt := 0; attempt < 50 && !swapped; attempt++ {
			var rerr error
			swapped, rerr = st.ReopenIfSwapped(ctx)
			if rerr != nil {
				time.Sleep(2 * time.Millisecond)
				continue
			}
		}
		if !swapped {
			t.Fatalf("ReopenIfSwapped never swapped to %s", name)
		}
	}
	stop.Store(true)
	wg.Wait()

	if wrongContent.Load() {
		t.Fatalf("a query observed structurally wrong content: %v", wrongContentDetail.Load())
	}
	t.Logf("tolerated %d transient errors across %d generations", atomic.LoadInt64(&transientErrs), generations)

	if got := servedCustomer(t, st); got != fmt.Sprintf("gen%d", generations) {
		t.Fatalf("final customer = %q, want gen%d", got, generations)
	}
}

// TestReopenIfSwapped_CloseUnderInFlightReader (S3) pins the ACCEPTED-TRANSIENT
// contract documented on Store.dbPtr / ReopenIfSwapped: an in-flight reader
// holding the OLD *sql.DB (or a connection checked out from it) across a swap
// must never observe MIXED or wrong-generation data. It may either keep
// serving the old generation (if its physical connection outlives the Close())
// or fail outright with a closed-connection-class error — but it must never
// silently start serving the NEW generation through the OLD handle, and a
// FRESH Store.DB() call after the swap must serve the new generation.
func TestReopenIfSwapped_CloseUnderInFlightReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	buildNamedAurynDB(t, path, "alpha")

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	oldDB := st.DB()
	// Simulate an in-flight reader: check out (and hold) a connection from the
	// OLD handle BEFORE the swap happens underneath it.
	held, err := oldDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	if err := held.PingContext(context.Background()); err != nil {
		t.Fatalf("held.Ping: %v", err)
	}

	bravoPath := filepath.Join(dir, "bravo.db")
	buildNamedAurynDB(t, bravoPath, "bravo")
	replaceFile(t, path, bravoPath)

	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil || !swapped {
		t.Fatalf("ReopenIfSwapped = (%v, %v), want (true, nil)", swapped, err)
	}

	// (a1) The already-checked-out connection survives the old *sql.DB's
	// Close() (database/sql only closes IDLE connections on Close; a connection
	// held via Conn() keeps its physical connection until it is itself closed)
	// — it must keep serving the OLD generation ("alpha"), never "bravo".
	var gotName string
	row := held.QueryRowContext(context.Background(), `SELECT name FROM customers WHERE id = 1`)
	if err := row.Scan(&gotName); err != nil {
		t.Fatalf("in-flight held-connection query after swap failed (want it to keep serving alpha): %v", err)
	}
	if gotName != "alpha" {
		t.Fatalf("in-flight held-connection query after swap = %q, want alpha (must never observe the new generation)", gotName)
	}
	if err := held.Close(); err != nil {
		t.Fatalf("held.Close: %v", err)
	}

	// (a2) The "next" operation against the OLD *sql.DB handle itself (not the
	// already-checked-out connection, which is now returned) must fail closed —
	// Close()d underneath by ReopenIfSwapped — rather than silently opening a
	// FRESH connection that would re-read the since-replaced path with zero
	// verification (the C1 hole TestReadOnly_PoolPinning_SurvivesFileReplace
	// guards against).
	_, nextErr := oldDB.QueryContext(context.Background(), `SELECT 1 FROM customers LIMIT 1`)
	if nextErr == nil {
		t.Fatal("a query against the OLD *sql.DB handle succeeded after ReopenIfSwapped Close()d it — " +
			"it must fail closed, never silently reopen the replaced path")
	}
	if !isClosedConnErr(nextErr) {
		t.Fatalf("old-handle query error after swap = %v, want a closed-connection-class error", nextErr)
	}

	// (b) A FRESH Store.DB() query after the swap must serve the NEW generation.
	if got := servedCustomer(t, st); got != "bravo" {
		t.Fatalf("post-swap servedCustomer = %q, want bravo", got)
	}
}
