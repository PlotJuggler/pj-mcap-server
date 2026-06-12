package metrics

import (
	"context"
	"crypto/subtle"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessCheck returns nil when the server is ready to accept traffic
// (typically a catalog DB ping). nil check => always-OK health.
type ReadinessCheck func(ctx context.Context) error

// HealthHandler returns 200 when the readiness check passes, 503 otherwise.
// Always unauthenticated (spec §8.5).
func HealthHandler(check ReadinessCheck) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if check == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := check(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
}

// Handler returns the /metrics handler for this Metrics' registry. When user and
// pass are both non-empty the handler is wrapped in constant-time HTTP Basic auth
// (config metrics.require_auth); otherwise it is unauthenticated by default per
// spec §8.5.
func (m *Metrics) Handler(user, pass string) http.Handler {
	h := promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
	if user == "" || pass == "" {
		return h
	}
	return basicAuth(user, pass, h)
}

// basicAuth wraps next with constant-time-compare HTTP Basic auth.
func basicAuth(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(u), []byte(user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="pj-cloud"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
