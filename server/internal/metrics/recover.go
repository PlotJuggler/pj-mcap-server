package metrics

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Guard runs fn inside a defer-recover wrapper scoped to one unit of work (one
// WS connection, one session producer/consumer, one chunk-index warmer sweep
// — spec §8.1). On a recovered panic it:
//
//	(a) logs the stack with slog.Error (scope + the panic value);
//	(b) increments pj_cloud_panic_total{scope};
//	(c) returns true so the caller can close ONLY the affected scope.
//
// It returns false when fn completed normally. One bad request cannot crash the
// process; one bad session cannot break sibling sessions on the same connection.
//
// m and log may be nil (tests / catalog-only configs): the wrapper still
// recovers and logs to slog.Default(), it simply skips the counter when m is
// nil.
func Guard(m *Metrics, log *slog.Logger, scope string, fn func()) (recovered bool) {
	defer func() {
		if r := recover(); r != nil {
			recovered = true
			if log == nil {
				log = slog.Default()
			}
			log.Error("panic recovered",
				"scope", scope,
				"panic", fmt.Sprint(r),
				"stack", string(debug.Stack()))
			if m != nil {
				m.PanicTotal.WithLabelValues(scope).Inc()
			}
		}
	}()
	fn()
	return false
}

// counterValue reads a Counter's current value (used by PanicCount and tests).
func counterValue(c prometheus.Counter) float64 {
	var dm dto.Metric
	if err := c.Write(&dm); err != nil {
		return 0
	}
	return dm.GetCounter().GetValue()
}

// GaugeValue reads a Gauge's current value (test helper for asserting gauge
// wiring such as SessionsActive).
func GaugeValue(g prometheus.Gauge) float64 {
	var dm dto.Metric
	if err := g.Write(&dm); err != nil {
		return 0
	}
	return dm.GetGauge().GetValue()
}

// CounterValue reads a Counter's current value (exported test helper for
// asserting counter movement such as FetchedBytesTotal / BytesSentTotal).
func CounterValue(c prometheus.Counter) float64 { return counterValue(c) }
