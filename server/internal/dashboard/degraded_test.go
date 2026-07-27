package dashboard

// A degraded-start store (no catalog published yet) must yield a 503 waiting
// page on every dashboard route — never a template error or a nil-DB panic.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"pj-cloud/server/internal/catalog"
)

func TestDashboard_DegradedStoreServes503WaitingPage(t *testing.T) {
	st, err := catalog.NewAwaiting(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("NewAwaiting: %v", err)
	}
	defer st.Close()

	mux := http.NewServeMux()
	if err := Register(mux, Deps{Store: st, BasicAuthUser: "u", BasicAuthPwd: "p"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	for _, path := range []string{"/dashboard/", "/dashboard/files", "/dashboard/indexer"} {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		req.SetBasicAuth("u", "p")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("GET %s = %d, want 503", path, resp.StatusCode)
		}
		if !strings.Contains(string(body), "waiting for first catalog build") {
			t.Fatalf("GET %s body = %q, want waiting message", path, body)
		}
	}
}
