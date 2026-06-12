package dashboard

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/indexer"
	"pj-cloud/server/internal/session"
)

func setupMux(t *testing.T) (*http.ServeMux, *catalog.Store) {
	t.Helper()
	dir := t.TempDir()
	store, err := catalog.Open(context.Background(), dir+"/c.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	mux := http.NewServeMux()
	if err := Register(mux, Deps{
		Store:         store,
		Indexer:       &indexer.Loop{},
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
	mux, store := setupMux(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Seed one file + a topic + an override tag through the catalog writer.
	id, _, err := catalog.UpsertFile(context.Background(), store, catalog.FileRecord{
		S3Key:        "seed.mcap",
		S3ETag:       "etag1",
		SizeBytes:    4096,
		StartTimeNs:  1_000_000_000,
		EndTimeNs:    2_000_000_000,
		MessageCount: 7,
		ChunkCount:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.ReplaceTopicsForFile(context.Background(), store, id, []catalog.TopicRecord{
		{Name: "/imu", SchemaName: "sensor_msgs/Imu", SchemaEncoding: "ros2msg", MessageCount: 7},
	}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetOverride(context.Background(), store, id, "verified", "yes"); err != nil {
		t.Fatal(err)
	}

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
