package catalog

import (
	"strings"
	"testing"
)

// TestParseHiveKey_RoundTrip proves parseHiveKey is the exact inverse of
// rebuildHiveKey: rebuild(parse(k)) == k for every well-formed k.
func TestParseHiveKey_RoundTrip(t *testing.T) {
	cases := []hiveDims{
		{Customer: "globex", Site: "london", Robot: "r1", Source: "ros-bags", Date: "2026-06-01", Filename: "x.mcap"},
		{Customer: "a", Site: "b", Robot: "c", Source: "d", Date: "2026-01-01", Filename: "f.mcap"},
		{Customer: "cust with spaces", Site: "s", Robot: "r", Source: "src-2", Date: "2026-12-31", Filename: "a.b.c.mcap"},
	}
	for _, d := range cases {
		key := rebuildHiveKey(d.Customer, d.Site, d.Robot, d.Source, d.Date, d.Filename)
		got, ok := parseHiveKey(key)
		if !ok {
			t.Fatalf("parseHiveKey(%q) ok=false, want true", key)
		}
		if got != d {
			t.Fatalf("parseHiveKey(%q) = %+v, want %+v", key, got, d)
		}
		if rebuild := rebuildHiveKey(got.Customer, got.Site, got.Robot, got.Source, got.Date, got.Filename); rebuild != key {
			t.Fatalf("rebuildHiveKey(parseHiveKey(%q)) = %q, want %q", key, rebuild, key)
		}
	}
}

// TestParseHiveKey_LeadingSlashes mirrors the Python side's key.lstrip("/")
// and pins the N1 invariant precisely: rebuildHiveKey(parseHiveKey(k)) ==
// strings.TrimLeft(k, "/"), NOT "== k" — a key with leading slashes round-trips
// to its slash-stripped form, never to itself verbatim.
func TestParseHiveKey_LeadingSlashes(t *testing.T) {
	key := "//customer=a/customer_site=b/robot=c/source=d/date=2026-01-01/f.mcap"
	got, ok := parseHiveKey(key)
	if !ok {
		t.Fatal("parseHiveKey with leading slashes: ok=false, want true")
	}
	want := hiveDims{Customer: "a", Site: "b", Robot: "c", Source: "d", Date: "2026-01-01", Filename: "f.mcap"}
	if got != want {
		t.Fatalf("parseHiveKey = %+v, want %+v", got, want)
	}
	if rebuild := rebuildHiveKey(got.Customer, got.Site, got.Robot, got.Source, got.Date, got.Filename); rebuild != strings.TrimLeft(key, "/") {
		t.Fatalf("rebuildHiveKey(parseHiveKey(%q)) = %q, want %q (TrimLeft(key, \"/\"), NOT key itself)",
			key, rebuild, strings.TrimLeft(key, "/"))
	}
}

// TestParseHiveKey_Malformed: anything not matching the exact template is
// rejected, never partially parsed.
func TestParseHiveKey_Malformed(t *testing.T) {
	bad := []string{
		"",
		"flat_name.mcap",
		"customer=a/robot=c/source=d/date=2026-01-01/f.mcap",                   // missing customer_site
		"customer=a/customer_site=b/robot=c/source=d/date=2026-01-01/f.bag",    // not .mcap
		"customer=a/customer_site=b/robot=c/source=d/date=2026-01-01/",         // no filename
		"customer=a/customer_site=b/robot=c/source=d/date=2026-01-01/x/f.mcap", // extra segment
		"customer=a/customer_site=b/robot=c/source=d/f.mcap",                   // missing date
	}
	for _, key := range bad {
		if got, ok := parseHiveKey(key); ok {
			t.Errorf("parseHiveKey(%q) = %+v, ok=true, want ok=false", key, got)
		}
	}
}
