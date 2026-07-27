package catalog

// Tests for the builder-status sidecar reader (<db>.status.json — the Python
// builder's progress/liveness signal, CATALOG_CONTRACT.md §12) and for the
// composed /health readiness check that reports it while the server runs
// degraded ("waiting for first catalog build").

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadBuilderStatus_ParsesSidecar(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "catalog.db.status.json")
	doc := `{"version":1,"phase":"extracting","listed_total":39279,"extract_total":31000,` +
		`"extract_done":1234,"failed":3,"updated_at_unix":1785200000.5,"pid":7}`
	if err := os.WriteFile(p, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	bs, err := ReadBuilderStatus(p)
	if err != nil {
		t.Fatalf("ReadBuilderStatus: %v", err)
	}
	if bs.Phase != "extracting" || bs.ExtractDone != 1234 || bs.ExtractTotal != 31000 {
		t.Fatalf("unexpected status: %+v", bs)
	}
}

func TestReadBuilderStatus_MissingOrMalformed(t *testing.T) {
	dir := t.TempDir()
	if _, err := ReadBuilderStatus(filepath.Join(dir, "nope.json")); err == nil {
		t.Fatal("missing sidecar: want error")
	}
	p := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(p, []byte("{torn"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBuilderStatus(p); err == nil {
		t.Fatal("malformed sidecar: want error")
	}
}

func TestReadinessCheck_DegradedReportsWaitingWithBuilderDetail(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "catalog.db")
	st := newAwaitingT(t, dbPath)
	defer st.Close()

	check := ReadinessCheck(st, dbPath+".status.json")

	// No sidecar yet: plain waiting message.
	err := check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "waiting for first catalog build") {
		t.Fatalf("degraded check without sidecar = %v, want waiting message", err)
	}

	// Sidecar present: the builder's progress is surfaced in the message.
	doc := `{"version":1,"phase":"extracting","extract_total":100,"extract_done":42,"updated_at_unix":1}`
	if err := os.WriteFile(dbPath+".status.json", []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	err = check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "waiting for first catalog build") {
		t.Fatalf("degraded check = %v, want waiting message", err)
	}
	if !strings.Contains(err.Error(), "extracting") || !strings.Contains(err.Error(), "42/100") {
		t.Fatalf("degraded check = %v, want builder detail 'extracting ... 42/100'", err)
	}
}

func TestReadinessCheck_ReadyPingsTheCatalog(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "catalog.db")
	buildNamedAurynDB(t, dbPath, "ok")
	st, err := OpenReadOnly(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	check := ReadinessCheck(st, dbPath+".status.json")
	if err := check(context.Background()); err != nil {
		t.Fatalf("ready check = %v, want nil", err)
	}
}
