package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestDefaults: the new blocks default to dashboard-off, metrics-on, plaintext.
func TestDefaults(t *testing.T) {
	c := Default()
	if c.Server.TLS.Enabled() {
		t.Errorf("default TLS should be disabled")
	}
	if c.Dashboard.Active() {
		t.Errorf("default dashboard should be inactive")
	}
	if !c.Metrics.Enabled {
		t.Errorf("default metrics should be enabled")
	}
	if c.Metrics.RequireAuth {
		t.Errorf("default metrics should be unauthenticated")
	}
	if c.Catalog.ExternalBuilder {
		t.Errorf("default external_builder should be false (legacy in-process indexer)")
	}
}

// TestLoad_ExternalBuilder: the auryn cutover flag round-trips through YAML.
func TestLoad_ExternalBuilder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("catalog:\n  db_path: /tmp/x.db\n  external_builder: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !c.Catalog.ExternalBuilder {
		t.Fatalf("external_builder should be true from YAML")
	}
	if c.Catalog.DBPath != "/tmp/x.db" {
		t.Fatalf("db_path = %q, want /tmp/x.db", c.Catalog.DBPath)
	}
}

// TestDefaults_TagIPCSocket: off by default (mirrors ExternalBuilder).
func TestDefaults_TagIPCSocket(t *testing.T) {
	if Default().Catalog.TagIPCSocket != "" {
		t.Errorf("default tag_ipc_socket should be empty (forwarding off)")
	}
}

// TestLoad_TagIPCSocket: the D2 tag-edit IPC socket path round-trips through
// YAML, mirroring TestLoad_ExternalBuilder.
func TestLoad_TagIPCSocket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	body := "catalog:\n  db_path: /tmp/x.db\n  external_builder: true\n  tag_ipc_socket: /tmp/tag-ipc.sock\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Catalog.TagIPCSocket != "/tmp/tag-ipc.sock" {
		t.Fatalf("tag_ipc_socket = %q, want /tmp/tag-ipc.sock", c.Catalog.TagIPCSocket)
	}
}

// TestLoad_TLSDashboardMetrics: the spec §8.6 field names round-trip through YAML.
func TestLoad_TLSDashboardMetrics(t *testing.T) {
	t.Setenv("PJ_CLOUD_DASHBOARD_PASSWORD", "s3cret")
	p := writeTemp(t, `
server:
  listen: ":8443"
  tls:
    cert: /etc/pj-cloud/server.crt
    key:  /etc/pj-cloud/server.key
dashboard:
  enabled: true
  basic_auth:
    username: admin
    password: ${PJ_CLOUD_DASHBOARD_PASSWORD}
metrics:
  enabled: true
  require_auth: true
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Server.TLS.Enabled() {
		t.Errorf("TLS should be enabled")
	}
	if c.Server.TLS.Cert != "/etc/pj-cloud/server.crt" || c.Server.TLS.Key != "/etc/pj-cloud/server.key" {
		t.Errorf("TLS cert/key not parsed: %+v", c.Server.TLS)
	}
	if !c.Dashboard.Active() {
		t.Errorf("dashboard should be active with full creds")
	}
	if c.Dashboard.BasicAuth.Password != "s3cret" {
		t.Errorf("dashboard password env-expansion failed: %q", c.Dashboard.BasicAuth.Password)
	}
	if !c.Metrics.RequireAuth {
		t.Errorf("metrics.require_auth should be true")
	}
}

// TestDashboardInactiveWithoutPassword: enabled but env var unset => Active()
// false (graceful disable), NOT a load error.
func TestDashboardInactiveWithoutPassword(t *testing.T) {
	t.Setenv("PJ_CLOUD_DASHBOARD_PASSWORD", "")
	p := writeTemp(t, `
dashboard:
  enabled: true
  basic_auth:
    username: admin
    password: ${PJ_CLOUD_DASHBOARD_PASSWORD}
`)
	c, err := Load(p)
	if err != nil {
		t.Fatalf("load should succeed even with empty password: %v", err)
	}
	if c.Dashboard.Active() {
		t.Errorf("dashboard should be inactive when password is empty")
	}
}

// TestValidate_TLSHalfConfigured: cert without key (or vice versa) is rejected.
func TestValidate_TLSHalfConfigured(t *testing.T) {
	p := writeTemp(t, `
server:
  listen: ":8443"
  tls:
    cert: /only/cert.pem
`)
	if _, err := Load(p); err == nil {
		t.Fatalf("expected error for half-configured TLS")
	}
}

// An indexer: block in a config file is a deprecated no-op (the in-process Go
// indexer was deleted in the M6 cutover). Load must FLAG it so main can warn —
// an operator setting indexer.startup_scan today gets silence otherwise.
func TestLoad_DeprecatedIndexerBlockFlagged(t *testing.T) {
	with := writeTemp(t, `
indexer:
  startup_scan: true
`)
	cfg, err := Load(with)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.DeprecatedIndexerInFile {
		t.Error("config with an indexer: block must set DeprecatedIndexerInFile")
	}

	without := writeTemp(t, `
server:
  listen: ":8080"
`)
	cfg, err = Load(without)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DeprecatedIndexerInFile {
		t.Error("config without an indexer: block must NOT set DeprecatedIndexerInFile")
	}
}

// metrics.require_auth borrows the dashboard Basic credentials; asking for auth
// with no credentials configured must be a startup error, not a silently
// UNAUTHENTICATED /metrics handler (metrics.Handler("","") skips the wrap).
func TestValidate_MetricsRequireAuthNeedsCredentials(t *testing.T) {
	p := writeTemp(t, `
metrics:
  require_auth: true
`)
	if _, err := Load(p); err == nil {
		t.Fatalf("expected error for metrics.require_auth without dashboard basic_auth credentials")
	}

	ok := writeTemp(t, `
metrics:
  require_auth: true
dashboard:
  basic_auth:
    username: admin
    password: s3cret
`)
	if _, err := Load(ok); err != nil {
		t.Fatalf("require_auth with full credentials should validate, got: %v", err)
	}
}

// TestLoad_GCSOnly: a storage.gcs-only config (the GCS-use-case M1b leg) loads and
// validates when the S3 arm is explicitly cleared. Default() seeds S3, so a GCS
// config must null it (storage: { s3: null, gcs: {...} }) to satisfy the
// exactly-one-of union. credentials_file is env-expanded (dev-only path).
func TestLoad_GCSOnly(t *testing.T) {
	t.Setenv("PJ_CLOUD_GCS_KEY", "/var/run/secrets/gcs-sa.json")
	p := writeTemp(t, `
storage:
  s3: null
  gcs:
    bucket: gcs-recordings
    prefix: site-a/
    credentials_file: ${PJ_CLOUD_GCS_KEY}
`)
	c, err := Load(p)
	if err != nil {
		t.Fatalf("gcs-only config should load: %v", err)
	}
	if c.Storage.S3 != nil {
		t.Errorf("S3 arm should be nil for a gcs-only config, got %+v", c.Storage.S3)
	}
	if c.Storage.GCS == nil {
		t.Fatal("GCS arm must be set")
	}
	if c.Storage.GCS.Bucket != "gcs-recordings" {
		t.Errorf("gcs bucket: %q", c.Storage.GCS.Bucket)
	}
	if c.Storage.GCS.Prefix != "site-a/" {
		t.Errorf("gcs prefix: %q", c.Storage.GCS.Prefix)
	}
	if c.Storage.GCS.CredentialsFile != "/var/run/secrets/gcs-sa.json" {
		t.Errorf("gcs credentials_file env-expansion failed: %q", c.Storage.GCS.CredentialsFile)
	}
}

// TestValidate_StorageExactlyOneOf: the tagged union accepts exactly one arm.
// BOTH set => error; NEITHER set => error; the error names both options.
func TestValidate_StorageExactlyOneOf(t *testing.T) {
	t.Run("both", func(t *testing.T) {
		c := Default() // S3 is non-nil
		c.Storage.GCS = &GCSConfig{Bucket: "b"}
		if err := c.Validate(); err == nil {
			t.Fatalf("both S3 and GCS set should be an error")
		}
	})
	t.Run("neither", func(t *testing.T) {
		c := Default()
		c.Storage.S3 = nil
		c.Storage.GCS = nil
		err := c.Validate()
		if err == nil {
			t.Fatalf("neither S3 nor GCS set should be an error")
		}
	})
	t.Run("gcs-only-valid", func(t *testing.T) {
		c := Default()
		c.Storage.S3 = nil
		c.Storage.GCS = &GCSConfig{Bucket: "b"}
		if err := c.Validate(); err != nil {
			t.Fatalf("gcs-only with a bucket should validate: %v", err)
		}
	})
	t.Run("gcs-no-bucket", func(t *testing.T) {
		c := Default()
		c.Storage.S3 = nil
		c.Storage.GCS = &GCSConfig{} // bucket empty
		if err := c.Validate(); err == nil {
			t.Fatalf("gcs arm without a bucket should be an error")
		}
	})
}
