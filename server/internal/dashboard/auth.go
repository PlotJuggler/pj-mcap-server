package dashboard

import (
	"crypto/subtle"
	"net/http"
)

// basicAuth wraps next with constant-time-compare HTTP Basic auth. If username
// or password is empty the wrapper returns 503 on every request (dashboard
// "disabled until configured", spec §8.5) — a defense-in-depth backstop; the
// caller already gates registration on DashboardConfig.Active().
func basicAuth(username, password string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if username == "" || password == "" {
			http.Error(w, "dashboard not configured", http.StatusServiceUnavailable)
			return
		}
		u, p, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(u), []byte(username)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(password)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="pj-cloud"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
