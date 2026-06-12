// Command pj-cloud-server is the PJ Cloud Connector server: it indexes MCAP
// recordings in an S3/Minio bucket into a SQLite-WAL catalog and serves the
// Hello / ListFiles / GetFile / UpdateTags catalog RPCs plus the session
// streaming path over one WebSocket.
//
// Layout follows Plan A (cmd/pj-cloud-server + internal/{config,storage,format,
// catalog,indexer,session,ws,wire}).
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/dashboard"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/indexer"
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/session"
	"pj-cloud/server/internal/storage"
	"pj-cloud/server/internal/ws"
)

func main() {
	var (
		configPath   = flag.String("config", "", "path to YAML config (optional; defaults match infra/minio)")
		listen       = flag.String("listen", "", "listen address (overrides config; default :8080)")
		logLevel     = flag.String("log-level", "info", "log level: debug|info|warn|error")
		dbPath       = flag.String("db", "", "SQLite catalog DB path (overrides config / PJ_CLOUD_DB; default /tmp/pj-cloud-catalog.db)")
		pollInterval = flag.Duration("poll-interval", 0, "indexer poll interval (overrides config; e.g. 3s — used by the smoke harness)")
		tlsCert      = flag.String("tls-cert", "", "TLS cert path (overrides config / PJ_CLOUD_TLS_CERT; set with -tls-key to serve TLS)")
		tlsKey       = flag.String("tls-key", "", "TLS key path (overrides config / PJ_CLOUD_TLS_KEY)")
	)
	flag.Parse()

	log := newLogger(*logLevel)

	cfg := config.Default()
	if *configPath != "" {
		loaded, err := config.Load(*configPath)
		if err != nil {
			log.Error("config load failed", "err", err)
			os.Exit(1)
		}
		cfg = loaded
	}
	if *listen != "" {
		cfg.Server.Listen = *listen
	}
	// DB path: -db flag wins, else PJ_CLOUD_DB env, else config/default.
	if *dbPath != "" {
		cfg.Catalog.DBPath = *dbPath
	} else if env := os.Getenv("PJ_CLOUD_DB"); env != "" {
		cfg.Catalog.DBPath = env
	}
	if *pollInterval > 0 {
		cfg.Indexer.PollInterval = *pollInterval
	}
	// TLS: -tls-cert/-tls-key flags win, else PJ_CLOUD_TLS_* env, else config.
	if *tlsCert != "" {
		cfg.Server.TLS.Cert = *tlsCert
	} else if env := os.Getenv("PJ_CLOUD_TLS_CERT"); env != "" {
		cfg.Server.TLS.Cert = env
	}
	if *tlsKey != "" {
		cfg.Server.TLS.Key = *tlsKey
	} else if env := os.Getenv("PJ_CLOUD_TLS_KEY"); env != "" {
		cfg.Server.TLS.Key = env
	}
	// Env override for the bearer token always wins (deploy convention).
	if tok := os.Getenv("PJ_CLOUD_TOKEN"); tok != "" {
		cfg.Auth.BearerToken = tok
	}

	if cfg.Auth.BearerToken == "" {
		log.Warn("PJ_CLOUD_TOKEN not set: running in DEV ANONYMOUS mode (any token accepted)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Storage seam: dispatch on the StorageConfig tagged union (S3/Minio for
	// Dexory, GCS for Asensus). New is the single selection point; nothing outside
	// internal/storage picks a backend or imports a cloud SDK.
	bs, err := storage.New(ctx, cfg.Storage)
	if err != nil {
		log.Error("storage init failed", "err", err)
		os.Exit(1)
	}
	// Backend-neutral identity for the startup log, dashboard, and base-prefix scoping.
	backendName, bucketName, storagePrefix, endpointName := storageIdentity(cfg.Storage)
	log.Info("storage configured", "backend", backendName, "bucket", bucketName,
		"prefix", storagePrefix, "endpoint", endpointName)

	codec, err := format.NewCodec(cfg.Format)
	if err != nil {
		log.Error("format codec init failed", "err", err)
		os.Exit(1)
	}

	// SQLite-WAL catalog store (Plan A Tasks 7-12). File ids are stable rowids
	// that survive restart for unchanged keys.
	store, err := catalog.Open(ctx, cfg.Catalog.DBPath)
	if err != nil {
		log.Error("catalog: open SQLite store failed", "db", cfg.Catalog.DBPath, "err", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()
	log.Info("catalog: opened SQLite store", "db", cfg.Catalog.DBPath)

	// Indexer: reconcile the bucket into the store. Warm-start (synchronous) serves
	// existing rows with no re-extract for unchanged (etag,size,last_modified);
	// then a background ticker polls. The change-detect re-extract preserves tag
	// overrides; failures land in indexer_failures.
	// The BlobStore already scopes every List to the configured base prefix
	// (storage owns it; List returns full keys, GetRange/Head take full keys), so
	// the scanner passes an EMPTY sub-prefix. Passing storagePrefix here too would
	// double it (prefix+prefix) and match nothing — invisible to the empty-prefix
	// smoke/matrix path, fatal the moment a real base prefix is configured.
	// Shared chunk-index cache: the indexer pre-warms it from its scan (which
	// already reads each file's summary), so a download's plan phase is a pure
	// in-memory hit instead of re-reading every file's summary over WAN.
	idxCache := format.NewChunkIndexCache(4096)
	scanner := &indexer.Scanner{
		Store:     store,
		Lister:    indexer.NewBlobStoreLister(bs),
		Extractor: indexer.NewCodecExtractor(bs, codec, idxCache),
		Prefix:    "",
		Log:       log,
	}
	loop := &indexer.Loop{
		Scanner:          scanner,
		Interval:         cfg.Indexer.PollInterval,
		StartupScan:      cfg.Indexer.StartupScan,
		Log:              log,
		WarmStartTimeout: cfg.Indexer.WarmStartTimeout,
	}

	// Observability: metrics collectors + panic-recovery wiring (spec §8.1/§8.5).
	// The metric set is process-wide; subsystems guard their goroutines through it.
	// WIRE IT BEFORE loop.Start: the synchronous warm-start scan increments the
	// indexer counters, so the metric set + store observability must be attached
	// first or the startup files-indexed/run counts are lost.
	mx := metrics.New()
	store.SetObservability(mx, log)
	loop.Metrics = mx

	log.Info("indexer: starting (warm-start + poll)", "backend", backendName, "bucket", bucketName,
		"endpoint", endpointName, "poll_interval", cfg.Indexer.PollInterval)
	// Pass the long-lived ctx: the warm-start scan is internally bounded by
	// WarmStartTimeout, but the background ticker must outlive that window and
	// poll until shutdown.
	if startErr := loop.Start(ctx); startErr != nil {
		log.Error("indexer: warm-start failed", "err", startErr)
		os.Exit(1)
	}

	// Session/streaming subsystem: registry (concurrency cap + retain-after-
	// disconnect eviction) wired to the same store/codec/storage. Cancel/evict
	// discards the retain buffer and unblocks any parked producer.
	registry := session.NewRegistry(session.RegistryOpts{
		MaxConcurrent:         cfg.Session.MaxConcurrent,
		RetainAfterDisconnect: cfg.Session.RetainAfterDisconnect,
	})
	sessDeps := &ws.SessionDeps{
		Store:    store,
		Codec:    codec,
		Blob:     bs,
		Registry: registry,
		Cfg:      cfg.Session,
		Log:      log,
		Metrics:  mx,
		IdxCache: idxCache, // pre-warmed by the indexer scan above
	}
	// Decrement the active-sessions gauge whenever a session leaves the registry
	// (client Cancel or retain-window eviction — both go through Registry.Cancel,
	// which invokes onEvict exactly once per session).
	registry.SetOnEvict(func(*session.SessionState) { mx.SessionsActive.Dec() })

	// WS handler on /api/ws, now with streaming + metrics wired.
	handler := ws.NewHandlerWithSession(store, cfg.Auth.BearerToken, log, sessDeps)
	handler.SetMetrics(mx)
	mux := http.NewServeMux()
	mux.Handle("/api/ws", handler)

	// /health: always unauthenticated; readiness = catalog DB reachable (spec §8.5).
	mux.Handle("/health", metrics.HealthHandler(func(hctx context.Context) error {
		return store.DB().PingContext(hctx)
	}))

	// /metrics: enabled by default; auth optional (metrics.require_auth gates it
	// behind the dashboard Basic credentials, spec §8.6).
	if cfg.Metrics.Enabled {
		mUser, mPass := "", ""
		if cfg.Metrics.RequireAuth {
			mUser, mPass = cfg.Dashboard.BasicAuth.Username, cfg.Dashboard.BasicAuth.Password
		}
		mux.Handle("/metrics", mx.Handler(mUser, mPass))
		log.Info("metrics enabled", "path", "/metrics", "require_auth", cfg.Metrics.RequireAuth)
	}

	// Dashboard: registered only when enabled AND fully credentialed (spec §8.5
	// "disabled until configured"). It owns "/" (redirect to /dashboard/) +
	// "/static/"; otherwise the bare-"/" WS fallback is kept for robustness.
	if cfg.Dashboard.Active() {
		if derr := dashboard.Register(mux, dashboard.Deps{
			Store:         store,
			Indexer:       loop,
			Sessions:      registry,
			StartedAt:     time.Now(),
			ServerVersion: ws.ServerVersion(),
			Backend:       backendName,
			Bucket:        bucketName,
			BasicAuthUser: cfg.Dashboard.BasicAuth.Username,
			BasicAuthPwd:  cfg.Dashboard.BasicAuth.Password,
		}); derr != nil {
			log.Error("dashboard register failed", "err", derr)
			os.Exit(1)
		}
		log.Info("dashboard enabled", "path", "/dashboard/")
	} else {
		mux.Handle("/", handler) // bare-/ WS fallback when no dashboard
	}

	srv := &http.Server{
		Addr:        cfg.Server.Listen,
		Handler:     mux,
		ReadTimeout: 0, // WS connections are long-lived
	}

	// Graceful shutdown (spec §8.4): on SIGINT/SIGTERM stop accepting, cancel
	// active session producers/consumers (clients get a clean close / Eos),
	// checkpoint the catalog WAL via store.Close, and exit within a bounded
	// window. http.Server.Shutdown drains in-flight HTTP; CancelAll tears down
	// the streaming subsystem (which http.Server does NOT track).
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		log.Info("shutting down (graceful)")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		registry.CancelAll() // stop producers/consumers; discard retain buffers
		_ = srv.Shutdown(shutdownCtx)
	}()

	scheme := "http"
	if cfg.Server.TLS.Enabled() {
		scheme = "https"
	}
	log.Info("pj-cloud-server listening", "addr", cfg.Server.Listen, "scheme", scheme, "ws_path", "/api/ws")

	var serveErr error
	if cfg.Server.TLS.Enabled() {
		serveErr = srv.ListenAndServeTLS(cfg.Server.TLS.Cert, cfg.Server.TLS.Key)
	} else {
		serveErr = srv.ListenAndServe()
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		log.Error("server error", "err", serveErr)
		os.Exit(1)
	}
	// Wait for the graceful-shutdown goroutine to finish (store close + drain)
	// before returning, so deferred store.Close runs after CancelAll.
	<-shutdownDone
}

// storageIdentity returns the backend-neutral (backend, bucket, prefix, endpoint)
// for logging / the scanner prefix / the dashboard, without main.go reaching into
// a backend-specific config arm. config.Validate guarantees exactly one arm.
func storageIdentity(c config.StorageConfig) (backend, bucket, prefix, endpoint string) {
	switch {
	case c.GCS != nil:
		return "gcs", c.GCS.Bucket, c.GCS.Prefix, ""
	case c.S3 != nil:
		return "s3", c.S3.Bucket, c.S3.Prefix, c.S3.Endpoint
	default:
		return "none", "", "", ""
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}
