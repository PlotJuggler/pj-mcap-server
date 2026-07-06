// Package config defines the on-disk YAML config shape and loaders for the
// catalog-browse server slice. It expands ${ENV_VAR} references at parse time.
//
// SCOPE NOTE: this is a SLICE of Plan A's full config (2026-05-28-pj-cloud-server-v1.md
// Task 19). It keeps the exact field names Plan A pins for the parts this slice
// uses (Server.Listen, Auth.BearerToken, Storage.S3 as a tagged union, Indexer
// poll/scan) so the full config drops in mechanically later. Catalog.DBPath now
// lands (the SQLite catalog slice); Dashboard/Metrics are intentionally omitted
// until those subsystems land.
package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level server configuration.
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Auth      AuthConfig      `yaml:"auth"`
	Storage   StorageConfig   `yaml:"storage"`
	Format    string          `yaml:"format"` // recording format; v1 only "mcap" (default)
	Catalog   CatalogConfig   `yaml:"catalog"`
	Indexer   IndexerConfig   `yaml:"indexer"`
	Session   SessionConfig   `yaml:"session"`
	Dashboard DashboardConfig `yaml:"dashboard"`
	Metrics   MetricsConfig   `yaml:"metrics"`
}

// CatalogConfig holds the SQLite-WAL catalog settings (Plan A Task 19).
type CatalogConfig struct {
	// DBPath is the SQLite database file. Interactive default
	// /tmp/pj-cloud-catalog.db; smoke + tests use their own temp paths. The -db
	// flag / PJ_CLOUD_DB env override it (main.go).
	DBPath string `yaml:"db_path"`

	// ExternalBuilder is a DEPRECATED NO-OP (catalog-migration §2.6): the server
	// now ALWAYS opens the catalog READ-ONLY (the Python mcap_catalog builder is
	// the sole writer; the Go catalog writer + in-process indexer were deleted),
	// so there is no other mode left to select. Kept only so an existing
	// config.yaml that still sets it does not fail to start — main.go logs a
	// deprecation warning and otherwise ignores it. The -external-builder flag /
	// PJ_CLOUD_EXTERNAL_BUILDER env are the same deprecated no-op.
	ExternalBuilder bool `yaml:"external_builder"`

	// TagIPCSocket is the UNIX socket path of the Python catalog builder's
	// tag-edit IPC endpoint (D2, CATALOG_CONTRACT.md §10). Default "" = off: the
	// (always read-only) catalog then rejects UpdateTags outright. When set, the
	// WS UpdateTags handler forwards set/unset edits over this socket instead.
	// The -tag-ipc-socket flag / PJ_CLOUD_TAG_IPC_SOCKET env override it (main.go).
	TagIPCSocket string `yaml:"tag_ipc_socket"`
}

type ServerConfig struct {
	Listen string    `yaml:"listen"` // e.g. ":8080"
	TLS    TLSConfig `yaml:"tls"`
}

// TLSConfig holds the optional server TLS cert/key (spec §8.6). When BOTH Cert
// and Key are set the listener serves HTTPS (wss:// + https dashboard);
// otherwise plaintext. A self-signed dev pair is produced by
// scripts/gen-dev-cert.sh.
type TLSConfig struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

// Enabled reports whether both cert and key are configured.
func (t TLSConfig) Enabled() bool { return t.Cert != "" && t.Key != "" }

// DashboardConfig is the operator dashboard block (spec §8.5/§8.6). The
// dashboard is DISABLED until Enabled is true AND BasicAuth is fully configured
// (a non-empty username+password). /health and /metrics are independent of this.
type DashboardConfig struct {
	Enabled   bool            `yaml:"enabled"`
	BasicAuth BasicAuthConfig `yaml:"basic_auth"`
}

// Active reports whether the dashboard should be registered: enabled AND both
// basic-auth credentials present (spec §8.5 "dashboard disabled until configured").
func (d DashboardConfig) Active() bool {
	return d.Enabled && d.BasicAuth.Username != "" && d.BasicAuth.Password != ""
}

// BasicAuthConfig is an HTTP Basic credential pair (env-expanded).
type BasicAuthConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// MetricsConfig governs the /metrics endpoint (spec §8.6). RequireAuth gates it
// behind the dashboard's Basic credentials; default false => unauthenticated.
type MetricsConfig struct {
	Enabled     bool `yaml:"enabled"`
	RequireAuth bool `yaml:"require_auth"`
}

type AuthConfig struct {
	// BearerToken is the single shared bearer token (env-expanded). When empty
	// the server runs in DEV ANONYMOUS mode (logs a warning, accepts any token).
	BearerToken string `yaml:"bearer_token"`
}

// StorageConfig is a TAGGED UNION: exactly one of {S3, GCS} is non-nil. This is
// the StorageCredentials responsibility boundary (Plan A unified-plan seam 2) —
// only storage.New(cfg) ever reads what is inside. Validate() enforces the
// exactly-one-of invariant. Default() seeds the S3/Minio (Dexory) arm; an
// Asensus GCS config must explicitly null S3 (storage: { s3: null, gcs: {...} }).
type StorageConfig struct {
	S3  *S3Config  `yaml:"s3"`
	GCS *GCSConfig `yaml:"gcs"`
}

type S3Config struct {
	Bucket    string `yaml:"bucket"`
	Region    string `yaml:"region"`
	Prefix    string `yaml:"prefix"`     // optional key prefix
	Endpoint  string `yaml:"endpoint"`   // optional, for Minio
	AccessKey string `yaml:"access_key"` // optional; static creds (Minio dev)
	SecretKey string `yaml:"secret_key"` // optional; static creds (Minio dev)
}

// GCSConfig is the Google Cloud Storage arm of the storage union (Plan A Task
// 14b, Asensus M1b). Credentials baseline is ADC / Workload Identity (the
// attached service account — no key on disk); CredentialsFile is DEV ONLY. There
// is intentionally NO Endpoint field — the emulator endpoint is auto-selected by
// the SDK from the STORAGE_EMULATOR_HOST env var, not config.
type GCSConfig struct {
	Bucket          string `yaml:"bucket"`
	Prefix          string `yaml:"prefix"`           // optional key prefix
	CredentialsFile string `yaml:"credentials_file"` // optional, DEV ONLY (ADC is the baseline)
}

// IndexerConfig is a DEPRECATED NO-OP left over from the in-process Go
// indexer, which was deleted in the M6 catalog-migration cutover (§2.6) along
// with the Go catalog writer. Kept only so an existing config.yaml / the
// `-poll-interval` flag (main.go) does not fail to start; nothing reads these
// fields anymore.
type IndexerConfig struct {
	PollInterval time.Duration `yaml:"poll_interval"`
	StartupScan  bool          `yaml:"startup_scan"`
	// WarmStartTimeout bounds the synchronous startup scan (cold full extract).
	// The 2m default fits localhost Minio; a real WAN bucket at ~10s/file needs
	// a deliberately larger budget (warm-start failure is fatal by design).
	WarmStartTimeout time.Duration `yaml:"warm_start_timeout"`
}

// SessionConfig is the streaming subsystem's tuning block (design spec §8.6).
// All fields are env-overridable through ${ENV} expansion at parse time and via
// the PJ_CLOUD_* env overrides applied after Load (see Default). Defaults match
// the spec defaults exactly so the server streams sensibly out of the box.
type SessionConfig struct {
	// MaxConcurrent caps simultaneously-active sessions; over it OpenSession
	// returns Error{RESOURCE_LIMIT} (spec 8.6 default 16).
	MaxConcurrent int `yaml:"max_concurrent"`
	// RetainAfterDisconnect is how long a detached session's producer + retain
	// buffer stay alive awaiting OpenResume before eviction (default 60s).
	RetainAfterDisconnect time.Duration `yaml:"retain_after_disconnect"`
	// RetainMaxSeqs / RetainMaxBytes bound the per-session retain buffer; the
	// first cap hit backpressures the producer (defaults 256 / 64 MiB).
	RetainMaxSeqs  int   `yaml:"retain_max_seqs"`
	RetainMaxBytes int64 `yaml:"retain_max_bytes"`
	// MaxBatchBytes / MaxBatchAge are the batch-flush thresholds (512 KiB / 50ms).
	MaxBatchBytes int           `yaml:"max_batch_bytes"`
	MaxBatchAge   time.Duration `yaml:"max_batch_age"`
	// MaxMessageBytes is the hard per-message cap; over it a message is dropped
	// and counted in Progress.dropped_messages (default 16 MiB).
	MaxMessageBytes int `yaml:"max_message_bytes"`
	// CompressThresholdBytes governs per-message RAW/ZSTD on the NONE singleton
	// fallback path only (default 4 KiB).
	CompressThresholdBytes int `yaml:"compress_threshold_bytes"`
	// BodyZstdLevel is the one-shot ZSTD level for the default batch-body frame
	// (default 3).
	BodyZstdLevel int `yaml:"body_zstd_level"`
}

var envRefRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func expandEnv(b []byte) []byte {
	return envRefRe.ReplaceAllFunc(b, func(m []byte) []byte {
		name := envRefRe.FindSubmatch(m)[1]
		return []byte(os.Getenv(string(name)))
	})
}

// Default returns a Config pre-populated to match infra/minio (bucket
// "recordings", endpoint http://localhost:9000, admin/password123) so the
// server runs out-of-the-box against the dev stack with no config file.
func Default() Config {
	return Config{
		Server: ServerConfig{Listen: ":8080"},
		Auth:   AuthConfig{BearerToken: os.Getenv("PJ_CLOUD_TOKEN")},
		Format: "mcap",
		Storage: StorageConfig{
			S3: &S3Config{
				Bucket:    "recordings",
				Region:    "us-east-1",
				Endpoint:  "http://localhost:9000",
				AccessKey: "admin",
				SecretKey: "password123",
			},
		},
		Catalog: CatalogConfig{DBPath: "/tmp/pj-cloud-catalog.db"},
		Indexer: IndexerConfig{
			PollInterval:     30 * time.Second,
			StartupScan:      true,
			WarmStartTimeout: 2 * time.Minute,
		},
		Session: DefaultSession(),
		// Dashboard is OFF by default (no credentials) per spec §8.5; metrics +
		// /health are always-on, /metrics unauthenticated by default.
		Dashboard: DashboardConfig{Enabled: false},
		Metrics:   MetricsConfig{Enabled: true, RequireAuth: false},
	}
}

// DefaultSession returns the spec §8.6 session defaults.
func DefaultSession() SessionConfig {
	return SessionConfig{
		MaxConcurrent:          16,
		RetainAfterDisconnect:  60 * time.Second,
		RetainMaxSeqs:          256,
		RetainMaxBytes:         64 << 20,  // 64 MiB
		MaxBatchBytes:          512 << 10, // 512 KiB
		MaxBatchAge:            50 * time.Millisecond,
		MaxMessageBytes:        16 << 20, // 16 MiB
		CompressThresholdBytes: 4096,     // 4 KiB
		BodyZstdLevel:          3,
	}
}

// withSessionDefaults fills any zero-valued session field with its spec default,
// so a YAML file that sets only a subset (or omits the block entirely) still gets
// sensible values. Called by Load after unmarshal.
func (s *SessionConfig) withSessionDefaults() {
	d := DefaultSession()
	if s.MaxConcurrent <= 0 {
		s.MaxConcurrent = d.MaxConcurrent
	}
	if s.RetainAfterDisconnect <= 0 {
		s.RetainAfterDisconnect = d.RetainAfterDisconnect
	}
	if s.RetainMaxSeqs <= 0 {
		s.RetainMaxSeqs = d.RetainMaxSeqs
	}
	if s.RetainMaxBytes <= 0 {
		s.RetainMaxBytes = d.RetainMaxBytes
	}
	if s.MaxBatchBytes <= 0 {
		s.MaxBatchBytes = d.MaxBatchBytes
	}
	if s.MaxBatchAge <= 0 {
		s.MaxBatchAge = d.MaxBatchAge
	}
	if s.MaxMessageBytes <= 0 {
		s.MaxMessageBytes = d.MaxMessageBytes
	}
	if s.CompressThresholdBytes <= 0 {
		s.CompressThresholdBytes = d.CompressThresholdBytes
	}
	if s.BodyZstdLevel <= 0 {
		s.BodyZstdLevel = d.BodyZstdLevel
	}
}

// Load reads a YAML config file, expands ${ENV} refs, and overlays it on top of
// Default() so any field the file omits keeps its dev-friendly default.
func Load(path string) (Config, error) {
	cfg := Default()
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(expandEnv(raw), &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	// A YAML file may set only a subset of the session block (or omit it). Fill
	// the gaps with spec defaults so streaming always has valid tuning.
	cfg.Session.withSessionDefaults()
	if cfg.Catalog.DBPath == "" {
		cfg.Catalog.DBPath = Default().Catalog.DBPath
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks the invariants this slice depends on.
func (c Config) Validate() error {
	if c.Server.Listen == "" {
		return fmt.Errorf("server.listen must not be empty")
	}
	// Storage is a tagged union: EXACTLY ONE of {s3, gcs}. Default() seeds the S3
	// arm; an Asensus deploy nulls it and sets gcs. Neither / both is a deploy
	// mistake, not a silent pick.
	switch {
	case c.Storage.S3 != nil && c.Storage.GCS != nil:
		return fmt.Errorf("storage: set exactly one of storage.s3 or storage.gcs, not both")
	case c.Storage.S3 == nil && c.Storage.GCS == nil:
		return fmt.Errorf("storage: set exactly one of storage.s3 or storage.gcs")
	case c.Storage.S3 != nil:
		if c.Storage.S3.Bucket == "" {
			return fmt.Errorf("storage.s3.bucket must not be empty")
		}
	case c.Storage.GCS != nil:
		if c.Storage.GCS.Bucket == "" {
			return fmt.Errorf("storage.gcs.bucket must not be empty")
		}
	}
	if c.Format != "" && c.Format != "mcap" {
		return fmt.Errorf("format %q unsupported (only \"mcap\" in v1)", c.Format)
	}
	if c.Catalog.DBPath == "" {
		return fmt.Errorf("catalog.db_path must not be empty")
	}
	// TLS is all-or-nothing: a half-configured pair is a deploy mistake, not a
	// silent plaintext fallback.
	if (c.Server.TLS.Cert == "") != (c.Server.TLS.Key == "") {
		return fmt.Errorf("server.tls.cert and server.tls.key must be set together (or both empty for plaintext)")
	}
	// NOTE: dashboard.enabled=true with an empty username/password is NOT an
	// error — the example config wires password: ${PJ_CLOUD_DASHBOARD_PASSWORD}
	// and an unset env var must DISABLE the dashboard (spec §8.5 "empty disables
	// dashboard"), not fail startup. The gate is DashboardConfig.Active(): the
	// dashboard registers only when enabled AND both credentials are present.
	return nil
}
