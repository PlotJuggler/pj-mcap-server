package catalog

import (
	"context"
	"database/sql"
	"fmt"
)

// vocabulary.go builds the filter VOCABULARY served by the GetVocabulary RPC
// (catalog-vocabulary-rpc.md): the strict customer->site->robot tree, the flat
// source dimension, and the flat tag facets. Auryn-schema only — the dimension
// tables exist solely there. On the legacy Go-schema store (s.readOnly == false)
// Vocabulary returns an empty result (the live path has no dimensions; no client
// calls this pre-cutover).

// TagFacetCap is the per-key distinct-value cap (V4): a tag key with more than this
// many distinct values is NOT emitted as a facet (it would be a useless combobox +
// bloat the frame); it stays filterable via the free-text/Lua path.
const TagFacetCap = 50

// VocabRobot / VocabSite / VocabCustomer are the strict hierarchy nodes; VocabSource
// is the flat dimension; VocabFacet/VocabFacetValue are the flat tag facets. Each
// carries a file_count for the UX (0 if unavailable).
type VocabRobot struct {
	ID        uint64
	Name      string
	FileCount uint64
}
type VocabSite struct {
	ID        uint64
	Name      string
	FileCount uint64
	Robots    []VocabRobot
}
type VocabCustomer struct {
	ID        uint64
	Name      string
	FileCount uint64
	Sites     []VocabSite
}
type VocabSource struct {
	ID        uint64
	Name      string
	FileCount uint64
}
type VocabFacetValue struct {
	Value     string
	FileCount uint64
}
type VocabFacet struct {
	Key    string
	Values []VocabFacetValue
}

// Vocabulary is the whole filter vocabulary: the hierarchy, the flat source combo,
// and the tag facets.
type Vocabulary struct {
	Customers []VocabCustomer
	Sources   []VocabSource
	Tags      []VocabFacet
}

// GetVocabulary assembles the vocabulary from the auryn dimension tables + the tag
// facet query. Returns an empty (non-nil) Vocabulary on a legacy Go-schema store.
//
// Pins db := s.DB() ONCE and threads it through every phase below (B1 —
// catalog-migration §6.2a review): the vocabulary is built from ~8 separate
// queries (per-dimension counts, the robot/site/customer tree, sources, tag
// facets) that must all describe the SAME generation — a swap landing mid-build
// could otherwise pair one generation's file counts with another generation's
// dimension rows.
func GetVocabulary(ctx context.Context, s *Store) (*Vocabulary, error) {
	if !s.readOnly {
		// The live Go-schema path has no dimension tables. Return empty rather than
		// erroring so the RPC is always answerable (no client uses it pre-cutover).
		return &Vocabulary{}, nil
	}
	db := s.DB()

	custCount, err := groupCount(ctx, db, "customer_id")
	if err != nil {
		return nil, err
	}
	siteCount, err := groupCount(ctx, db, "site_id")
	if err != nil {
		return nil, err
	}
	robotCount, err := groupCount(ctx, db, "robot_id")
	if err != nil {
		return nil, err
	}
	srcCount, err := groupCount(ctx, db, "source_id")
	if err != nil {
		return nil, err
	}

	// robots grouped by site_id, sorted by name. The EXISTS gate prunes ORPHAN
	// dimension rows (the auryn builder leaves lookup rows behind on delete/rename
	// by design), so the vocabulary never shows a ghost node with file_count=0.
	robotsBySite := map[uint64][]VocabRobot{}
	if err := queryRows(ctx, db,
		`SELECT id, site_id, name FROM robots r WHERE EXISTS (SELECT 1 FROM files WHERE robot_id = r.id) ORDER BY name`,
		func(scan func(...any) error) error {
			var id, siteID uint64
			var name string
			if err := scan(&id, &siteID, &name); err != nil {
				return err
			}
			robotsBySite[siteID] = append(robotsBySite[siteID],
				VocabRobot{ID: id, Name: name, FileCount: robotCount[id]})
			return nil
		}); err != nil {
		return nil, err
	}

	// sites grouped by customer_id, each with its robots, sorted by name.
	sitesByCustomer := map[uint64][]VocabSite{}
	if err := queryRows(ctx, db,
		`SELECT id, customer_id, name FROM sites s WHERE EXISTS (SELECT 1 FROM files WHERE site_id = s.id) ORDER BY name`,
		func(scan func(...any) error) error {
			var id, custID uint64
			var name string
			if err := scan(&id, &custID, &name); err != nil {
				return err
			}
			sitesByCustomer[custID] = append(sitesByCustomer[custID],
				VocabSite{ID: id, Name: name, FileCount: siteCount[id], Robots: robotsBySite[id]})
			return nil
		}); err != nil {
		return nil, err
	}

	var vocab Vocabulary
	if err := queryRows(ctx, db,
		`SELECT id, name FROM customers c WHERE EXISTS (SELECT 1 FROM files WHERE customer_id = c.id) ORDER BY name`,
		func(scan func(...any) error) error {
			var id uint64
			var name string
			if err := scan(&id, &name); err != nil {
				return err
			}
			vocab.Customers = append(vocab.Customers,
				VocabCustomer{ID: id, Name: name, FileCount: custCount[id], Sites: sitesByCustomer[id]})
			return nil
		}); err != nil {
		return nil, err
	}

	if err := queryRows(ctx, db,
		`SELECT id, name FROM sources src WHERE EXISTS (SELECT 1 FROM files WHERE source_id = src.id) ORDER BY name`,
		func(scan func(...any) error) error {
			var id uint64
			var name string
			if err := scan(&id, &name); err != nil {
				return err
			}
			vocab.Sources = append(vocab.Sources,
				VocabSource{ID: id, Name: name, FileCount: srcCount[id]})
			return nil
		}); err != nil {
		return nil, err
	}

	facets, err := tagFacets(ctx, db)
	if err != nil {
		return nil, err
	}
	vocab.Tags = facets
	return &vocab, nil
}

// groupCount returns file counts grouped by a dimension FK column on files.
// Takes an already-pinned db (B1) — see GetVocabulary.
func groupCount(ctx context.Context, db *sql.DB, col string) (map[uint64]uint64, error) {
	out := map[uint64]uint64{}
	// col is an internal constant (never user input) — safe to interpolate.
	q := fmt.Sprintf(`SELECT %s, COUNT(*) FROM files GROUP BY %s`, col, col)
	if err := queryRows(ctx, db, q, func(scan func(...any) error) error {
		var id, n uint64
		if err := scan(&id, &n); err != nil {
			return err
		}
		out[id] = n
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// tagFacets groups tags_effective into per-key facets, dropping keys whose distinct
// value count exceeds TagFacetCap (V4). Keys + values are returned sorted. Takes
// an already-pinned db (B1) — see GetVocabulary.
func tagFacets(ctx context.Context, db *sql.DB) ([]VocabFacet, error) {
	type kv struct {
		value string
		count uint64
	}
	byKey := map[string][]kv{}
	var keyOrder []string
	if err := queryRows(ctx, db,
		`SELECT key, value, COUNT(*) FROM tags_effective GROUP BY key, value ORDER BY key, value`,
		func(scan func(...any) error) error {
			var key, value string
			var n uint64
			if err := scan(&key, &value, &n); err != nil {
				return err
			}
			if _, seen := byKey[key]; !seen {
				keyOrder = append(keyOrder, key)
			}
			byKey[key] = append(byKey[key], kv{value: value, count: n})
			return nil
		}); err != nil {
		return nil, err
	}
	var out []VocabFacet
	for _, key := range keyOrder {
		vals := byKey[key]
		if len(vals) > TagFacetCap { // high-cardinality => not a facet (free-text/Lua)
			continue
		}
		facet := VocabFacet{Key: key}
		for _, v := range vals {
			facet.Values = append(facet.Values, VocabFacetValue{Value: v.value, FileCount: v.count})
		}
		out = append(out, facet)
	}
	return out, nil
}

// queryRows runs q against an already-pinned db and invokes fn for each row; fn
// receives a scan closure bound to the current row. Centralizes the
// rows.Close/Err boilerplate.
func queryRows(ctx context.Context, db *sql.DB, q string, fn func(scan func(...any) error) error) error {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(rows.Scan); err != nil {
			return err
		}
	}
	return rows.Err()
}
