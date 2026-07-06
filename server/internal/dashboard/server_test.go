package dashboard

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/session"

	_ "modernc.org/sqlite"
)

// buildDashboardFixtureDB writes a minimal auryn-schema (v3) catalog DB at path
// containing exactly one file — "seed.mcap" under a shared dimension tuple, one
// topic (/imu, sensor_msgs/Imu, 7 msgs), and an override tag verified=yes —
// enough to exercise every dashboard page (overview stats, the files list, and
// the file-detail topics + tag-layer rendering) without the (now-deleted) Go
// catalog writer. It also seeds a build_metadata row (§6.5 catalog-freshness)
// and one catalog_failures row (§4.5 quarantine) so /dashboard/indexer's two
// real panels — the Python builder's freshness snapshot and the quarantined-
// file list — both have something to render, not just the legacy-Go-indexer-
// era empty states.
func buildDashboardFixtureDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer db.Close()
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
		`CREATE TABLE catalog_failures (s3_key TEXT NOT NULL PRIMARY KEY, failed_at_ns INTEGER NOT NULL, error_text TEXT NOT NULL)`,
		`CREATE TABLE build_metadata (id INTEGER PRIMARY KEY CHECK (id=1), build_id INTEGER NOT NULL,
			last_build_ns INTEGER NOT NULL, files_scanned INTEGER NOT NULL, files_failed INTEGER NOT NULL,
			build_outcome TEXT NOT NULL, builder_version TEXT NOT NULL)`,
		`INSERT INTO build_metadata(id,build_id,last_build_ns,files_scanned,files_failed,build_outcome,builder_version)
			VALUES (1,42,1700000000000000000,1,0,'ok','test-builder-9.9.9')`,
		`INSERT INTO catalog_failures(s3_key,failed_at_ns,error_text) VALUES ('bad/key/broken.mcap',1700000000000000000,'quarantine-test-error')`,
		`INSERT INTO customers(id,name) VALUES (1,'t')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'t')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'t')`,
		`INSERT INTO sources(id,name) VALUES (1,'t')`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/imu')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'sensor_msgs/Imu','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp1')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
		// topic_counts = varint(7) = single byte 0x07.
		`INSERT INTO files (id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (1,'seed.mcap','etag1',4096,1,1,1,1,'2026-01-01',1000000000,2000000000,1,1,X'07')`,
		`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (1,'verified','yes',1)`,
	}
	for _, s := range ddl {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("ddl %q: %v", s, err)
		}
	}
}

func setupMux(t *testing.T) (*http.ServeMux, *catalog.Store) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "c.db")
	buildDashboardFixtureDB(t, dbPath)
	store, err := catalog.OpenReadOnly(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	mux := http.NewServeMux()
	if err := Register(mux, Deps{
		Store:         store,
		Sessions:      session.NewRegistry(session.RegistryOpts{MaxConcurrent: 4}),
		StartedAt:     time.Now(),
		ServerVersion: "test",
		Backend:       "s3",
		Bucket:        "recordings",
		BasicAuthUser: "admin",
		BasicAuthPwd:  "pw",
	}); err != nil {
		t.Fatal(err)
	}
	return mux, store
}

func get(t *testing.T, url, user, pass string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	// Don't follow redirects so we can assert the 302 from "/".
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestDashboard_AuthRejectsMissing(t *testing.T) {
	mux, _ := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp := get(t, srv.URL+"/dashboard/", "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestDashboard_AuthRejectsWrongPassword(t *testing.T) {
	mux, _ := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp := get(t, srv.URL+"/dashboard/", "admin", "wrong")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestDashboard_OverviewRenders(t *testing.T) {
	mux, _ := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp := get(t, srv.URL+"/dashboard/", "admin", "pw")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"Overview", "recordings", "Active:"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("overview body missing %q", want)
		}
	}
}

func TestDashboard_AllPagesRender(t *testing.T) {
	mux, _ := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	for _, p := range []string{"/dashboard/files", "/dashboard/sessions", "/dashboard/indexer"} {
		resp := get(t, srv.URL+p, "admin", "pw")
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s = %d; want 200", p, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

// TestDashboard_IndexerPage_ContentAssertions proves /dashboard/indexer
// actually renders the two panels that replaced the (deleted) Go in-process
// indexer's status page: the Python builder's build_metadata freshness
// snapshot (§6.5) and the catalog_failures quarantine list (§4.5) — not just
// a bare 200 (TestDashboard_AllPagesRender only proves the route doesn't
// error). buildDashboardFixtureDB seeds one build_metadata row (build_id=42,
// builder_version="test-builder-9.9.9") and one catalog_failures row
// (bad/key/broken.mcap / "quarantine-test-error").
func TestDashboard_IndexerPage_ContentAssertions(t *testing.T) {
	mux, _ := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := get(t, srv.URL+"/dashboard/indexer", "admin", "pw")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/dashboard/indexer = %d; want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	s := string(body)

	// Catalog-freshness panel (build_metadata).
	for _, want := range []string{"Catalog build", "42", "test-builder-9.9.9", "ok"} {
		if !strings.Contains(s, want) {
			t.Errorf("indexer page missing freshness content %q; body:\n%s", want, s)
		}
	}
	// Quarantine panel (catalog_failures) — the row must render, and the
	// legacy "No recent ... failures" empty-state text must NOT appear since
	// there IS a failure seeded.
	for _, want := range []string{"bad/key/broken.mcap", "quarantine-test-error"} {
		if !strings.Contains(s, want) {
			t.Errorf("indexer page missing quarantine content %q; body:\n%s", want, s)
		}
	}
	if strings.Contains(s, "No recent") {
		t.Errorf("indexer page shows the empty quarantine state despite a seeded failure; body:\n%s", s)
	}
	// The page must never claim it's reporting on an in-process Go indexer.
	if strings.Contains(s, "indexer failures") {
		t.Errorf("indexer page still uses stale \"indexer failures\" wording; body:\n%s", s)
	}
}

func TestDashboard_RootRedirects(t *testing.T) {
	mux, _ := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp := get(t, srv.URL+"/", "", "")
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("/ = %d; want 302", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/dashboard/" {
		t.Errorf("/ redirect Location = %q; want /dashboard/", loc)
	}
}

func TestDashboard_StaticCSS(t *testing.T) {
	mux, _ := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	resp := get(t, srv.URL+"/static/pico.min.css", "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pico.min.css = %d; want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) < 1000 {
		t.Errorf("pico.min.css suspiciously small: %d bytes", len(body))
	}
}

func TestDashboard_FileDetail(t *testing.T) {
	mux, _ := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// setupMux's fixture already seeds exactly one file ("seed.mcap", id 1) with
	// a topic (/imu) and an override tag (verified=yes) — see
	// buildDashboardFixtureDB.
	const id = uint64(1)

	resp := get(t, srv.URL+"/dashboard/files/"+itoa(id), "admin", "pw")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("file detail = %d; want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"seed.mcap", "/imu", "sensor_msgs/Imu", "verified", "override"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("file detail missing %q", want)
		}
	}

	// Unknown id => 404.
	resp = get(t, srv.URL+"/dashboard/files/999999", "admin", "pw")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown file detail = %d; want 404", resp.StatusCode)
	}
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}
