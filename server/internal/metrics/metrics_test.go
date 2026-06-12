package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestGuard_RecoversAndCounts proves a panic inside a guarded scope does NOT
// propagate (the process survives), is recorded in pj_cloud_panic_total{scope},
// and reported via the bool return so the caller can close only that scope.
func TestGuard_RecoversAndCounts(t *testing.T) {
	m := New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	got := Guard(m, log, "test-scope", func() {
		panic("boom")
	})
	if !got {
		t.Fatalf("Guard returned false on a panicking fn; want true (recovered)")
	}
	if c := m.PanicCount("test-scope"); c != 1 {
		t.Fatalf("panic counter = %v; want 1", c)
	}

	// A second panic in the same scope increments again; a different scope is
	// tracked independently.
	_ = Guard(m, log, "test-scope", func() { panic("again") })
	_ = Guard(m, log, "other-scope", func() { panic(errors.New("e")) })
	if c := m.PanicCount("test-scope"); c != 2 {
		t.Fatalf("panic counter after 2 = %v; want 2", c)
	}
	if c := m.PanicCount("other-scope"); c != 1 {
		t.Fatalf("other-scope counter = %v; want 1", c)
	}
}

// TestGuard_NoPanicNoCount: a clean fn returns false and leaves the counter at 0.
func TestGuard_NoPanicNoCount(t *testing.T) {
	m := New()
	ran := false
	got := Guard(m, nil, "clean", func() { ran = true })
	if got {
		t.Fatalf("Guard returned true for a clean fn; want false")
	}
	if !ran {
		t.Fatalf("fn did not run")
	}
	if c := m.PanicCount("clean"); c != 0 {
		t.Fatalf("clean counter = %v; want 0", c)
	}
}

// TestGuard_NilMetricsSafe: with a nil *Metrics the wrapper still recovers (no
// crash) — covers tests / catalog-only configs that have no metrics wired.
func TestGuard_NilMetricsSafe(t *testing.T) {
	var m *Metrics
	got := Guard(m, slog.New(slog.NewTextHandler(io.Discard, nil)), "nilm", func() { panic("x") })
	if !got {
		t.Fatalf("Guard with nil metrics returned false; want true")
	}
}

// TestHealthHandler covers the OK and 503 branches.
func TestHealthHandler(t *testing.T) {
	ok := HealthHandler(nil)
	rec := httptest.NewRecorder()
	ok.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("nil-check health = %d; want 200", rec.Code)
	}

	bad := HealthHandler(func(context.Context) error { return errors.New("db down") })
	rec = httptest.NewRecorder()
	bad.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("failing-check health = %d; want 503", rec.Code)
	}
}

// TestMetricsHandler_UnauthByDefault: /metrics with empty creds is open and
// exposes the counter names; with creds it 401s without auth.
func TestMetricsHandler_UnauthByDefault(t *testing.T) {
	m := New()
	m.SessionsTotal.Inc()

	srv := httptest.NewServer(m.Handler("", ""))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unauth /metrics = %d; want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "pj_cloud_sessions_total") {
		t.Fatalf("/metrics body missing pj_cloud_sessions_total")
	}
}

func TestMetricsHandler_AuthRequired(t *testing.T) {
	m := New()
	srv := httptest.NewServer(m.Handler("admin", "pw"))
	defer srv.Close()

	resp, _ := http.Get(srv.URL)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-cred /metrics = %d; want 401", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.SetBasicAuth("admin", "pw")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("good-cred /metrics = %d; want 200", resp.StatusCode)
	}
}
