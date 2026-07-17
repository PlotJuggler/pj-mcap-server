// Command pj-cloud-server is the PJ Cloud Connector server: it serves the
// Hello / ListFiles / GetFile / GetVocabulary / UpdateTags catalog RPCs plus
// the session streaming path over one WebSocket, reading a SQLite catalog the
// Python mcap_catalog builder writes (the Go catalog writer + in-process
// indexer were retired in the M6 catalog-migration cutover — see
// docs/CATALOG_CONTRACT.md).
//
// Layout follows Plan A (cmd/pj-cloud-server + internal/{config,storage,format,
// catalog,session,ws,wire}).
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
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/session"
	"pj-cloud/server/internal/storage"
	"pj-cloud/server/internal/tagipc"
	"pj-cloud/server/internal/warm"
	"pj-cloud/server/internal/ws"
)

func main() {
	var (
		configPath = flag.String("config", "", "path to YAML config (optional; defaults match infra/minio)")
		listen     = flag.String("listen", "", "listen address (overrides config; default :8080)")
		logLevel   = flag.String("log-level", "info", "log level: debug|info|warn|error")
		dbPath     = flag.String("db", "", "SQLite catalog DB path (overrides config / PJ_CLOUD_DB; default /tmp/pj-cloud-catalog.db)")
		// externalBld / poll-interval are DEPRECATED NO-OPS (catalog-migration §2.6):
		// external-builder read-only mode is now the server's ONLY mode (the Go
		// catalog writer + in-process indexer are deleted), so there is nothing left
		// to opt into and no indexer poll loop left to tune. Both flags are kept,
		// accepted, and ignored (with a startup warning) purely so an existing
		// launch script or config.yaml that still sets them does not fail to start.
		externalBld  = flag.Bool("external-builder", false, "DEPRECATED no-op: external-builder read-only mode is now the only mode (overrides config / PJ_CLOUD_EXTERNAL_BUILDER)")
		tagIPCSocket = flag.String("tag-ipc-socket", "", "D2: UNIX socket of the Python catalog builder's tag-edit IPC endpoint (overrides config / PJ_CLOUD_TAG_IPC_SOCKET; empty disables tag-edit forwarding)")
		pollInterval = flag.Duration("poll-interval", 0, "DEPRECATED no-op: the in-process indexer this tuned no longer exists")
		tlsCert      = flag.String("tls-cert", "", "TLS cert path (overrides config / PJ_CLOUD_TLS_CERT; set with -tls-key to serve TLS)")
		tlsKey       = flag.String("tls-key", "", "TLS key path (overrides config / PJ_CLOUD_TLS_KEY)")
		allowAnon    = flag.Bool("allow-anonymous", false, "start with NO client authentication (accept every client). REQUIRED to run without a bearer token; also via PJ_CLOUD_ALLOW_ANONYMOUS=1")
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
	// external-builder is now the only mode; the flag/env/config field are
	// deprecated no-ops (see the flag doc above) — warn once if anything asked
	// for the old opt-in, but do not act on it.
	if *externalBld || envTruthy(os.Getenv("PJ_CLOUD_EXTERNAL_BUILDER")) || cfg.Catalog.ExternalBuilder {
		log.Warn("external-builder is now the only mode; flag is deprecated")
	}
	if *pollInterval != 0 {
		log.Warn("-poll-interval is deprecated and ignored; the in-process indexer it tuned no longer exists")
	}
	if cfg.DeprecatedIndexerInFile {
		log.Warn("config: the indexer.* block is a deprecated no-op (the in-process indexer was deleted in the M6 cutover; " +
			"the Python mcap_catalog builder owns the catalog) — remove it from the config file")
	}
	// D2 tag-edit IPC socket: -tag-ipc-socket flag wins, else PJ_CLOUD_TAG_IPC_SOCKET
	// env, else config/default (empty => forwarding stays off).
	if *tagIPCSocket != "" {
		cfg.Catalog.TagIPCSocket = *tagIPCSocket
	} else if env := os.Getenv("PJ_CLOUD_TAG_IPC_SOCKET"); env != "" {
		cfg.Catalog.TagIPCSocket = env
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

	// Re-validate AFTER every flag/env override has been applied: config.Load
	// validated the file, but a flag/env override (e.g. -tls-cert with no
	// -tls-key) can re-break an invariant — a half-configured TLS pair must be a
	// startup error, never a silent plaintext fallback.
	if err := cfg.Validate(); err != nil {
		log.Error("config invalid after flag/env overrides", "err", err)
		os.Exit(1)
	}

	// Fail CLOSED on a missing bearer token: without one the server accepts EVERY
	// client, so refuse to start unless anonymous mode is explicitly opted into
	// (flag or env). This turns "forgot PJ_CLOUD_TOKEN" from a silently wide-open
	// deploy into a hard startup error. The env parse is STRICT ("1"/"true"):
	// PJ_CLOUD_ALLOW_ANONYMOUS=0 must not fail open.
	if cfg.Auth.BearerToken == "" {
		if !*allowAnon && !envTruthy(os.Getenv("PJ_CLOUD_ALLOW_ANONYMOUS")) {
			log.Error("refusing to start: no bearer token configured (set PJ_CLOUD_TOKEN or auth.bearer_token). " +
				"To run with NO authentication on purpose, pass -allow-anonymous or set PJ_CLOUD_ALLOW_ANONYMOUS=1")
			os.Exit(1)
		}
		log.Warn("authentication DISABLED (anonymous mode): every client is accepted — never expose this port publicly")
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

	// SQLite-WAL catalog store: opened READ-ONLY (the Python mcap_catalog
	// builder is the sole writer — the Go catalog writer + in-process indexer
	// were deleted in the M6 catalog-migration cutover, §2.6). File ids are
	// stable rowids that survive a builder rescan for unchanged keys.
	mx := metrics.New()
	store, err := catalog.OpenReadOnly(ctx, cfg.Catalog.DBPath)
	if err != nil {
		log.Error("catalog: open SQLite store (read-only) failed — has the Python builder run?",
			"db", cfg.Catalog.DBPath, "err", err)
		os.Exit(1)
	}
	defer func() { _ = store.Close() }()
	log.Info("catalog: opened SQLite store READ-ONLY (external builder)", "db", cfg.Catalog.DBPath)
	log.Info("indexer: DISABLED (external-builder read-only mode); Python builder owns the catalog")

	// Shared chunk-index cache: starts empty (there is no in-process scan to
	// pre-warm it anymore); the A+ background warmer below fills it from the
	// catalog, and any cache miss on the request path falls back to a direct
	// codec read (see SessionDeps.cachedChunkIndex). BYTE-bounded (not entry
	// count): a cached index is dominated by its per-topic schema text, so a large
	// real catalog would pin many GB under an entry cap. The warmer stops at this
	// budget (see warm.Warmer).
	cacheBytes := cfg.Catalog.ChunkIndexCacheBytes
	if cacheBytes <= 0 {
		cacheBytes = 512 << 20
	}
	idxCache := format.NewChunkIndexCacheSized(0, cacheBytes)
	log.Info("chunk-index cache configured", "max_bytes", cacheBytes)

	// A+ background chunk-index warmer (catalog-migration §3.2): pre-fill the shared
	// cache from the catalog so a download's plan phase is an in-memory hit —
	// the cache's ONLY warm source now that the Go indexer is gone. Background —
	// never blocks serving.
	warmer := &warm.Warmer{
		Store: store, Codec: codec, Blob: bs, Cache: idxCache,
		Concurrency: 4, Budget: cacheBytes, Log: log, Metrics: mx,
	}
	go func() {
		if werr := warmer.Run(ctx); werr != nil {
			log.Warn("chunk-index warmer: list failed", "err", werr)
		}
	}()

	// Catalog-freshness updater (§6.5): mirror the Python builder's build_metadata
	// onto the pj_cloud_catalog_* gauges so monitoring sees staleness once the
	// writer is out-of-process.
	//
	// Same tick also drives the READER side of atomic-publish + reopen-on-swap
	// (§6.2a; CATALOG_CONTRACT.md §9): ReopenIfSwapped is a no-op unless the
	// Python builder's rebuild replaced the served DB file (a new inode) since
	// the last check.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		refresh := func() {
			swapped, rerr := store.ReopenIfSwapped(ctx)
			if rerr != nil {
				mx.CatalogReopenFailuresTotal.Inc()
				log.Warn("catalog reopen-on-swap failed; still serving the previous generation", "err", rerr)
			}
			// Counter fires immediately on a successful swap, independent of
			// whether the freshness read below succeeds (S1 fix — it must not be
			// possible for a real swap to go uncounted just because GetBuildInfo
			// happened to error on that same tick).
			if swapped {
				mx.CatalogReopensTotal.Inc()
			}

			bi, err := catalog.GetBuildInfo(ctx, store)
			if err != nil {
				log.Warn("catalog freshness read failed", "err", err)
				if swapped {
					// Still log the swap itself, just without the new generation's
					// build_id (GetBuildInfo is what would have supplied it).
					log.Info("catalog file replaced; reopened")
				}
				return
			}
			if bi.Present {
				mx.SetCatalogFreshness(bi.BuildID, bi.LastBuildNs, bi.FilesScanned, bi.FilesFailed)
			}
			// Logged after GetBuildInfo (not right after the swap) so the message
			// carries the new generation's build_id — the swap itself doesn't know
			// it, only that the file changed.
			if swapped {
				log.Info("catalog file replaced; reopened", "build_id", bi.BuildID)
			}
		}
		refresh()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()

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
		IdxCache: idxCache, // filled by the background warmer + on-demand cache misses
	}
	// Decrement the active-sessions gauge whenever a session leaves the registry
	// (client Cancel or retain-window eviction — both go through Registry.Cancel,
	// which invokes onEvict exactly once per session).
	registry.SetOnEvict(func(*session.SessionState) { mx.SessionsActive.Dec() })

	// WS handler on /api/ws, now with streaming + metrics wired.
	handler := ws.NewHandlerWithSession(store, cfg.Auth.BearerToken, log, sessDeps)
	handler.SetMetrics(mx)
	// Compressed-envelope path for bulky catalog RPC responses (opt-in per client
	// via Hello). Transport-level; applies even to catalog-only connections.
	if err := handler.SetResponseCompression(cfg.Server.ResponseCompression); err != nil {
		log.Error("ws: response compression init failed", "err", err)
		os.Exit(1)
	}
	if cfg.Server.ResponseCompression.Enabled {
		log.Info("ws: response compression enabled",
			"level", cfg.Server.ResponseCompression.Level,
			"threshold_bytes", cfg.Server.ResponseCompression.ThresholdBytes)
	}
	// D2 tag-edit IPC forwarder (CATALOG_CONTRACT.md §10): the only way
	// UpdateTags can succeed now that the catalog is always read-only; a
	// server with no socket configured rejects every UpdateTags outright.
	if cfg.Catalog.TagIPCSocket != "" {
		handler.SetTagIPC(tagipc.NewClient(cfg.Catalog.TagIPCSocket))
		log.Info("catalog: tag-edit IPC forwarding enabled", "socket", cfg.Catalog.TagIPCSocket)
	}
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
	// close the read-only catalog handle via store.Close (mode=ro cannot
	// checkpoint the WAL — that's the Python builder's job, not ours), and
	// exit within a bounded window. http.Server.Shutdown drains in-flight
	// HTTP; CancelAll tears down the streaming subsystem (which http.Server
	// does NOT track).
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

// envTruthy is the strict boolean-env parse ("1"/"true" exactly) shared by every
// PJ_CLOUD_* opt-in. Anything else — including "0", "false", or padded values —
// is false, so a fail-closed feature can never be switched off by a value that
// was meant to disable it.
func envTruthy(v string) bool {
	return v == "1" || v == "true"
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
