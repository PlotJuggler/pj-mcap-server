// Package metrics exposes the Prometheus counters/gauges shared across the
// server subsystems plus the panic-recovery wrapper (spec §8.1 "Panic
// recovery": defer-recover at the smallest scope; on recovery log the stack
// with slog.Error and increment pj_cloud_panic_total{scope}).
//
// The collectors live on a process-wide Registry built by New(); the server
// wires the natural increment points (ws connections, sessions, streaming
// throughput, the chunk-index warmer, catalog freshness/reopen) by calling the
// typed helpers. Tests may construct a fresh Registry to assert counter
// movement in isolation.
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

	// Streaming throughput accounting (server -> client).
	BytesSentTotal    prometheus.Counter
	MessagesSentTotal prometheus.Counter

	// FetchedBytesTotal is the total chunk-record bytes Range-GET from the blob
	// store across all sessions (the producer fetch budget; the ground truth the
	// estimated_chunk_bytes pre-flight is asserted against in the component test).
	FetchedBytesTotal prometheus.Counter

	// Chunk-index background warmer accounting (catalog-migration §3.2). Warmed =
	// files whose chunk index was loaded into the cache; Skipped = already-cached
	// hits; Errors = per-file load failures (a poison file never aborts the sweep).
	ChunkIndexWarmedTotal prometheus.Counter
	ChunkIndexWarmSkipped prometheus.Counter
	ChunkIndexWarmErrors  prometheus.Counter

	// Catalog freshness (catalog-migration §6.5): mirrors the Python builder's
	// build_metadata so monitoring sees catalog staleness once the writer is
	// out-of-process. All 0 on the legacy in-process path (no build_metadata).
	CatalogBuildID            prometheus.Gauge
	CatalogLastBuildTimestamp prometheus.Gauge // unix seconds of the last build
	CatalogFilesScanned       prometheus.Gauge
	CatalogFilesFailed        prometheus.Gauge

	// Catalog atomic-publish reopen accounting (catalog-migration §6.2a): the
	// reader-side half of the swap protocol (CATALOG_CONTRACT.md §9). Reopens
	// counts successful hot-swaps onto a rebuilt catalog file; ReopenFailures
	// counts a detected identity change whose new file failed verification (the
	// old handle keeps serving — fail-closed — and the next poll tick retries).
	CatalogReopensTotal        prometheus.Counter
	CatalogReopenFailuresTotal prometheus.Counter

	// Tag-IPC forwarding (D2, catalog-migration §1.1/CATALOG_CONTRACT.md §10):
	// Forwards counts UpdateTags requests successfully forwarded to (and
	// accepted by) the Python builder's tag-edit socket; Failures counts the
	// forward call itself failing (busy, not-found, connection error, ...).
	// Both stay 0 when tag-IPC forwarding is not configured, or the store is
	// writable (the local write path never forwards).
	TagIPCForwardsTotal prometheus.Counter
	TagIPCFailuresTotal prometheus.Counter

	// Response-compression effectiveness (compressed-envelope path). Input =
	// marshaled ServerMessage bytes that entered the compression path (allowlisted
	// + above threshold); Output = bytes actually emitted for them (compressed
	// frame, or the raw fallback when zstd didn't shrink it). Output/Input is the
	// realized wire savings on compressible RPC responses. Both stay 0 when the
	// feature is off or no client negotiated it.
	WSResponseCompressInputBytes  prometheus.Counter
	WSResponseCompressOutputBytes prometheus.Counter
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
		BytesSentTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_bytes_sent_total", Help: "Total session payload bytes sent to clients."},
		),
		MessagesSentTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_messages_sent_total", Help: "Total session messages sent to clients."},
		),
		FetchedBytesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_fetched_bytes_total", Help: "Total chunk-record bytes fetched from the blob store."},
		),
		ChunkIndexWarmedTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_chunkindex_warmed_total", Help: "Files whose chunk index the background warmer loaded into the cache."},
		),
		ChunkIndexWarmSkipped: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_chunkindex_warm_skipped_total", Help: "Files the warmer skipped because the chunk index was already cached."},
		),
		ChunkIndexWarmErrors: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_chunkindex_warm_errors_total", Help: "Per-file chunk-index warm failures (the sweep continues)."},
		),
		CatalogBuildID: prometheus.NewGauge(
			prometheus.GaugeOpts{Name: "pj_cloud_catalog_build_id", Help: "Monotonic id of the last completed catalog build (Python builder)."},
		),
		CatalogLastBuildTimestamp: prometheus.NewGauge(
			prometheus.GaugeOpts{Name: "pj_cloud_catalog_last_build_timestamp_seconds", Help: "Unix time of the last completed catalog build."},
		),
		CatalogFilesScanned: prometheus.NewGauge(
			prometheus.GaugeOpts{Name: "pj_cloud_catalog_files_scanned", Help: "Files seen by the last catalog build."},
		),
		CatalogFilesFailed: prometheus.NewGauge(
			prometheus.GaugeOpts{Name: "pj_cloud_catalog_files_failed", Help: "Files quarantined by the last catalog build."},
		),
		CatalogReopensTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_catalog_reopens_total", Help: "Successful reader-side catalog reopens after an atomic-publish rebuild swapped the served file."},
		),
		CatalogReopenFailuresTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_catalog_reopen_failures_total", Help: "Detected catalog file swaps whose reopen failed verification (old handle kept serving)."},
		),
		TagIPCForwardsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_tag_ipc_forwards_total", Help: "UpdateTags requests successfully forwarded to the Python catalog builder's tag-edit IPC endpoint."},
		),
		TagIPCFailuresTotal: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_tag_ipc_failures_total", Help: "Tag-edit IPC forward attempts that failed (busy, not-found, connection error, ...)."},
		),
		WSResponseCompressInputBytes: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_ws_response_compress_input_bytes_total", Help: "Marshaled ServerMessage bytes that entered the response-compression path (allowlisted + above threshold)."},
		),
		WSResponseCompressOutputBytes: prometheus.NewCounter(
			prometheus.CounterOpts{Name: "pj_cloud_ws_response_compress_output_bytes_total", Help: "Bytes emitted for compression-path responses (compressed frame or raw fallback); Output/Input is the realized savings."},
		),
	}
	reg.MustRegister(
		m.PanicTotal,
		m.SessionsActive, m.SessionsTotal,
		m.WSConnectionsActive, m.WSConnectionsTotal,
		m.BytesSentTotal, m.MessagesSentTotal, m.FetchedBytesTotal,
		m.ChunkIndexWarmedTotal, m.ChunkIndexWarmSkipped, m.ChunkIndexWarmErrors,
		m.CatalogBuildID, m.CatalogLastBuildTimestamp, m.CatalogFilesScanned, m.CatalogFilesFailed,
		m.CatalogReopensTotal, m.CatalogReopenFailuresTotal,
		m.TagIPCForwardsTotal, m.TagIPCFailuresTotal,
		m.WSResponseCompressInputBytes, m.WSResponseCompressOutputBytes,
	)
	return m
}

// SetCatalogFreshness mirrors a build_metadata snapshot onto the pj_cloud_catalog_*
// gauges (catalog-migration §6.5). Nil-safe; a not-present snapshot leaves them 0.
func (m *Metrics) SetCatalogFreshness(buildID, lastBuildNs, scanned, failed int64) {
	if m == nil {
		return
	}
	m.CatalogBuildID.Set(float64(buildID))
	m.CatalogLastBuildTimestamp.Set(float64(lastBuildNs) / 1e9)
	m.CatalogFilesScanned.Set(float64(scanned))
	m.CatalogFilesFailed.Set(float64(failed))
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
