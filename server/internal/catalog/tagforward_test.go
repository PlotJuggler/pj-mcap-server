package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// buildMinimalAurynDB (auryn_read_test.go) seeds one file: id=1,
// customer=dexory/site=london/robot=r1/source=ros-bags/date=2026-06-01/
// x.mcap, with embedded tags site=london,masked=x, an override quality=good
// and a NULL-mask override on "masked" — so tags_effective = {quality:good
// (override), site:london (embedded)}, "masked" hidden.

func openMinimalAurynStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "auryn.db")
	buildMinimalAurynDB(t, path)
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

const minimalAurynKey = "customer=dexory/customer_site=london/robot=r1/source=ros-bags/date=2026-06-01/x.mcap"

func TestObjectKeyForFile(t *testing.T) {
	st := openMinimalAurynStore(t)

	key, err := ObjectKeyForFile(context.Background(), st.DB(), 1)
	if err != nil {
		t.Fatalf("ObjectKeyForFile(1): %v", err)
	}
	if key != minimalAurynKey {
		t.Fatalf("ObjectKeyForFile(1) = %q, want %q", key, minimalAurynKey)
	}

	if _, err := ObjectKeyForFile(context.Background(), st.DB(), 9999); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("ObjectKeyForFile(9999) = %v, want ErrFileNotFound", err)
	}
}

func TestEffectiveTagsByKey(t *testing.T) {
	st := openMinimalAurynStore(t)

	tags, err := EffectiveTagsByKey(context.Background(), st, minimalAurynKey)
	if err != nil {
		t.Fatalf("EffectiveTagsByKey: %v", err)
	}
	byKey := map[string]EffectiveTag{}
	for _, tg := range tags {
		byKey[tg.Key] = tg
	}
	if got := byKey["quality"]; got.Value != "good" || !got.IsOverride {
		t.Errorf("quality = %+v, want {good, override=true}", got)
	}
	if got := byKey["site"]; got.Value != "london" || got.IsOverride {
		t.Errorf("site = %+v, want {london, override=false}", got)
	}
	if _, masked := byKey["masked"]; masked {
		t.Errorf("tags = %+v, 'masked' should be hidden by its NULL override", tags)
	}
}

func TestEffectiveTagsByKey_UnknownFile(t *testing.T) {
	st := openMinimalAurynStore(t)

	// Well-formed key, but a customer/site/robot/source combination that
	// doesn't exist: a lookup-only miss, must not fabricate anything.
	unknown := "customer=nope/customer_site=x/robot=y/source=z/date=2026-01-01/f.mcap"
	if _, err := EffectiveTagsByKey(context.Background(), st, unknown); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("EffectiveTagsByKey(unknown dims) = %v, want ErrFileNotFound", err)
	}
}

func TestEffectiveTagsByKey_MalformedKey(t *testing.T) {
	st := openMinimalAurynStore(t)

	if _, err := EffectiveTagsByKey(context.Background(), st, "flat_name.mcap"); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("EffectiveTagsByKey(malformed key) = %v, want ErrFileNotFound", err)
	}
}

// TestFileIDForKey pins the exported key->id resolver (wire s3_key
// addressing): a known key resolves to its CURRENT id; an unknown (but
// well-formed) key and a malformed non-Hive key are both lookup-only misses
// (ErrFileNotFound — never fabricating dimension rows).
func TestFileIDForKey(t *testing.T) {
	st := openMinimalAurynStore(t)

	id, err := FileIDForKey(context.Background(), st.DB(), minimalAurynKey)
	if err != nil {
		t.Fatalf("FileIDForKey(%q): %v", minimalAurynKey, err)
	}
	if id != 1 {
		t.Fatalf("FileIDForKey(%q) = %d, want 1", minimalAurynKey, id)
	}

	unknown := "customer=nope/customer_site=x/robot=y/source=z/date=2026-01-01/f.mcap"
	if _, err := FileIDForKey(context.Background(), st.DB(), unknown); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("FileIDForKey(unknown dims) = %v, want ErrFileNotFound", err)
	}
	if _, err := FileIDForKey(context.Background(), st.DB(), "flat_name.mcap"); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("FileIDForKey(malformed key) = %v, want ErrFileNotFound", err)
	}
}
