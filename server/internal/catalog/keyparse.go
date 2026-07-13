package catalog

import (
	"regexp"
	"strings"
)

// keyparse.go is the Go mirror of the Python builder's
// keyparse.parse_hive_key (mcap_catalog/mcap_catalog_builder/keyparse.py).
// Its invariant is rebuildHiveKey(parseHiveKey(k)) == strings.TrimLeft(k, "/")
// — an inverse of rebuildHiveKey up to stripped leading slashes, matching the
// Python side's key.lstrip("/") (see parseHiveKey's doc comment below). It
// exists for the tag-IPC forwarder (D2, tagforward.go): the tag-edit IPC
// endpoint addresses files by object key, not file_id (CATALOG_CONTRACT.md
// §10 "Key-based addressing"), so the forwarder must turn a wire file_id into
// a key (rebuildHiveKey, already landed) and — after the edit — a key back
// into dimension NAMES to re-resolve the CURRENT file_id (this file).
var hiveKeyRe = regexp.MustCompile(
	`^customer=([^/]+)/customer_site=([^/]+)/robot=([^/]+)/source=([^/]+)/date=([^/]+)/([^/]+\.mcap)$`)

// hiveDims is one Hive key's parsed dimension NAMES (not catalog row ids —
// the caller resolves those itself, e.g. by joining on these names).
type hiveDims struct {
	Customer, Site, Robot, Source, Date, Filename string
}

// parseHiveKey parses key into its dimension names. The invariant is
// rebuildHiveKey(parseHiveKey(k)) == strings.TrimLeft(k, "/") for every k this
// accepts — NOT a plain "== k": key's leading slashes (if any) are stripped
// first, mirroring the Python side's `key.lstrip("/")` (ALL leading slashes,
// not just one), so a key that had leading slashes round-trips to its
// slash-stripped form, not to itself verbatim. ok is false if key does not
// match the template exactly (a flat name, a missing partition, a
// non-".mcap" filename, an extra path segment, …).
func parseHiveKey(key string) (hiveDims, bool) {
	m := hiveKeyRe.FindStringSubmatch(strings.TrimLeft(key, "/"))
	if m == nil {
		return hiveDims{}, false
	}
	return hiveDims{
		Customer: m[1],
		Site:     m[2],
		Robot:    m[3],
		Source:   m[4],
		Date:     m[5],
		Filename: m[6],
	}, true
}
