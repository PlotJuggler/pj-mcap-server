package catalog

// Reader for the Python builder's status sidecar (<db>.status.json —
// CATALOG_CONTRACT.md §12). The builder publishes it atomically (tmp+rename)
// next to the served DB; the server reads it BEST-EFFORT to explain what the
// first catalog build is doing while /health reports degraded. Absence or
// malformation is never an error surfaced to clients — the caller just omits
// the detail.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// StatusSidecarSuffix is appended to the served DB path to locate the sidecar
// (mirrors the Python builder's status.STATUS_SUFFIX — CATALOG_CONTRACT.md §12).
const StatusSidecarSuffix = ".status.json"

// maxSidecarBytes bounds the sidecar read: the document is a few hundred bytes;
// anything larger is malformed and refused rather than slurped.
const maxSidecarBytes = 64 << 10

// BuilderStatus is the subset of the sidecar document the server consumes.
// Unknown fields are ignored (the builder may add fields freely).
type BuilderStatus struct {
	Version       int     `json:"version"`
	Phase         string  `json:"phase"` // listing | extracting | idle | error
	ListedTotal   int64   `json:"listed_total"`
	ExtractTotal  int64   `json:"extract_total"`
	ExtractDone   int64   `json:"extract_done"`
	Cataloged     int64   `json:"cataloged"`
	Skipped       int64   `json:"skipped"`
	Failed        int64   `json:"failed"`
	LastError     string  `json:"last_error"`
	UpdatedAtUnix float64 `json:"updated_at_unix"`
	PID           int     `json:"pid"`
}

// ReadBuilderStatus reads and parses the sidecar at path.
func ReadBuilderStatus(path string) (*BuilderStatus, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxSidecarBytes {
		return nil, fmt.Errorf("builder status sidecar %q: %d bytes exceeds cap", path, len(raw))
	}
	var bs BuilderStatus
	if err := json.Unmarshal(raw, &bs); err != nil {
		return nil, fmt.Errorf("builder status sidecar %q: %w", path, err)
	}
	return &bs, nil
}

// DegradedMessage is the human-readable "why is this server not ready yet"
// line: the fixed waiting message, enriched best-effort with the builder's
// sidecar progress. Shared by /health and the dashboard's degraded page.
func DegradedMessage(sidecarPath string) string {
	msg := "waiting for first catalog build"
	if bs, err := ReadBuilderStatus(sidecarPath); err == nil && bs.Phase != "" {
		switch {
		case bs.Phase == "error" && bs.LastError != "":
			msg += fmt.Sprintf(" (builder error: %.200s)", bs.LastError)
		case bs.ExtractTotal > 0:
			msg += fmt.Sprintf(" (builder: %s %d/%d)", bs.Phase, bs.ExtractDone, bs.ExtractTotal)
		default:
			msg += fmt.Sprintf(" (builder: %s)", bs.Phase)
		}
	}
	return msg
}

// ReadinessCheck composes the /health readiness function: while the store is
// degraded (no first catalog yet) it fails with DegradedMessage; once ready it
// pings the catalog handle as before.
func ReadinessCheck(s *Store, sidecarPath string) func(context.Context) error {
	return func(ctx context.Context) error {
		if !s.Ready() {
			return errors.New(DegradedMessage(sidecarPath))
		}
		db := s.DB()
		if db == nil {
			return errors.New("catalog store closed")
		}
		return db.PingContext(ctx)
	}
}
