// Package metrics exposes the Prometheus counters/gauges shared across the
// server subsystems plus the panic-recovery wrapper (spec §8.1 "Panic
// recovery": defer-recover at the smallest scope; on recovery log the stack
// with slog.Error and increment pj_cloud_panic_total{scope}).
//
// The collectors live on a process-wide Registry built by New(); the server
// wires the natural increment points (ws connections, sessions, indexer runs,
// catalog writer) by calling the typed helpers. Tests may construct a fresh
// Registry to assert counter movement in isolation.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the set of process-wide collectors, all registered on one
// prometheus.Registry. Construct with New(); pass the value around (it is a
// small struct of pointers — safe to copy).
type Metrics struct {
	reg *prometheus.Registry

	// PanicTotal counts recovered panics by scope (spec §8.1). A non-zero value
	// is a strong signal in /metrics + alerting that something is wrong without
	// the process having died.
	PanicTotal *prometheus.CounterVec

	// SessionsActive is the live registered-session gauge; SessionsTotal counts
	// every session ever opened.
	SessionsActive prometheus.Gauge
	SessionsTotal  prometheus.Counter

	// WSConnectionsActive / WSConnectionsTotal track WebSocket connections.
	WSConnectionsActive prometheus.Gauge
	WSConnectionsTotal  prometheus.Counter

	// Indexer run accounting.
	IndexerRunsTotal     prometheus.Counter
	IndexerFailuresTotal prometheus.Counter
	IndexerFilesIndexed  prometheus.Counter

	// Streaming throughput accounting (server -> client).
	BytesSentTotal    prometheus.Counter
	MessagesSentTotal prometheus.Counter

	// FetchedBytesTotal is the total chunk-record bytes Range-GET from the blob
	// store across all sessions (the producer fetch budget; the ground truth the
	// estimated_chunk_bytes pre-flight is asserted against in the component test).
	FetchedBytesTotal prometheus.Counter
}

// New builds the collector set on a fresh Registry and registers them. The
// returned *Metrics is shared by every subsystem; Registry() feeds /metrics.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		PanicTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "pj_cloud_panic_total", Help: "Recovered panics by scope."},
			[]string{"scope"},
		),
		SessionsActive: prometheus.NewGauge(
			prometheus.GaugeOpts{Name: "pj_cloud_sessions_active", Help: "Currently registered (attached or retained) sessions."},
		),
		SessionsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_sessions_total", Help: "Total sessions opened."},
		),
		WSConnectionsActive: prometheus.NewGauge(
			prometheus.GaugeOpts{Name: "pj_cloud_ws_connections_active", Help: "Currently open WebSocket connections."},
		),
		WSConnectionsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_ws_connections_total", Help: "Total WebSocket connections accepted."},
		),
		IndexerRunsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_indexer_runs_total", Help: "Total indexer poll cycles completed."},
		),
		IndexerFailuresTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_indexer_failures_total", Help: "Indexer cycles that ended with an error."},
		),
		IndexerFilesIndexed: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_indexer_files_indexed_total", Help: "Files newly indexed or reindexed by the indexer."},
		),
		BytesSentTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_bytes_sent_total", Help: "Total session payload bytes sent to clients."},
		),
		MessagesSentTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_messages_sent_total", Help: "Total session messages sent to clients."},
		),
		FetchedBytesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_fetched_bytes_total", Help: "Total chunk-record bytes fetched from the blob store."},
		),
	}
	reg.MustRegister(
		m.PanicTotal,
		m.SessionsActive, m.SessionsTotal,
		m.WSConnectionsActive, m.WSConnectionsTotal,
		m.IndexerRunsTotal, m.IndexerFailuresTotal, m.IndexerFilesIndexed,
		m.BytesSentTotal, m.MessagesSentTotal, m.FetchedBytesTotal,
	)
	return m
}

// Registry exposes the underlying prometheus.Registry for the /metrics handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// PanicCount returns the current recovered-panic count for a scope (test helper).
func (m *Metrics) PanicCount(scope string) float64 {
	if m == nil {
		return 0
	}
	return counterValue(m.PanicTotal.WithLabelValues(scope))
}
