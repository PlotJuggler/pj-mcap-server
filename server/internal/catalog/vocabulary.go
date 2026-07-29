package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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
// facet query. Leases ONE snapshot for every phase (B1): the vocabulary is
// built from ~8 separate queries (per-dimension counts, the robot/site/customer
// tree, sources, tag facets) that must all describe the SAME generation.
func GetVocabulary(ctx context.Context, s *Store) (*Vocabulary, error) {
	lease := s.Acquire()
	defer lease.Release()
	return GetVocabularyDB(ctx, lease.DB())
}

// GetVocabularyDB is GetVocabulary against an already-leased snapshot handle:
// the ws layer pins ONE Snapshot so the vocabulary rows and the generation
// token stamped on the response are guaranteed to describe the same generation
// (the dimension ids are only meaningful together with that token).
func GetVocabularyDB(ctx context.Context, db *sql.DB) (*Vocabulary, error) {
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

// tagFacets computes the tags_effective facet counts, dropping keys whose distinct
// value count exceeds TagFacetCap (V4). Keys + values are returned sorted. Takes
// an already-pinned db (B1) — see GetVocabulary.
//
// It deliberately does NOT `GROUP BY` the tags_effective VIEW: the view's
// UNION ALL + anti-join forces a temp-b-tree aggregation over every tag row
// (measured: 991 ms at 1M files / 2M tags). The equivalent decomposition below
// is index-ordered or O(overrides) per leg (measured: 113 ms total):
//
//	effective(key,value) = embedded_total(key,value)         -- idx_tags_embedded_kv order
//	                     − embedded rows MASKED by an override row of the same
//	                       (file_id,key) — non-NULL replaces, NULL hides
//	                       (driven from tags_override: O(overrides) PK probes)
//	                     + non-NULL overrides                 -- idx_tags_override_kv order
//
// The three legs run inside ONE read transaction so a concurrent tag edit
// (the builder's IPC commits between queries otherwise) cannot skew the merge.
func tagFacets(ctx context.Context, db *sql.DB) ([]VocabFacet, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }() // read-only: always rollback

	counts := map[string]map[string]int64{} // key -> value -> effective count
	add := func(q string, sign int64) error {
		return queryRows(ctx, tx, q, func(scan func(...any) error) error {
			var key, value string
			var n int64
			if err := scan(&key, &value, &n); err != nil {
				return err
			}
			if counts[key] == nil {
				counts[key] = map[string]int64{}
			}
			counts[key][value] += sign * n
			return nil
		})
	}
	if err := add(`SELECT key, value, COUNT(*) FROM tags_embedded GROUP BY key, value`, +1); err != nil {
		return nil, err
	}
	// CROSS JOIN pins tags_override as the driving side: O(overrides) primary-key
	// probes into tags_embedded, never a full embedded scan.
	if err := add(`SELECT e.key, e.value, COUNT(*) FROM tags_override o CROSS JOIN tags_embedded e
		ON e.file_id = o.file_id AND e.key = o.key GROUP BY e.key, e.value`, -1); err != nil {
		return nil, err
	}
	// Drop fully-masked groups BEFORE overlaying override values: an override
	// (key,value) that also survives in embedded form must SUM (the view's
	// GROUP BY collapses both branches into one group).
	for key, vals := range counts {
		for value, n := range vals {
			if n <= 0 {
				delete(vals, value)
			}
		}
		if len(vals) == 0 {
			delete(counts, key)
		}
	}
	if err := add(`SELECT key, value, COUNT(*) FROM tags_override WHERE value IS NOT NULL GROUP BY key, value`, +1); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out []VocabFacet
	for _, key := range keys {
		vals := counts[key]
		if len(vals) > TagFacetCap { // high-cardinality => not a facet (free-text/Lua)
			continue
		}
		values := make([]string, 0, len(vals))
		for value := range vals {
			values = append(values, value)
		}
		sort.Strings(values)
		facet := VocabFacet{Key: key}
		for _, value := range values {
			facet.Values = append(facet.Values,
				VocabFacetValue{Value: value, FileCount: uint64(vals[value])})
		}
		out = append(out, facet)
	}
	return out, nil
}

// querier is the read surface shared by *sql.DB and *sql.Tx, so queryRows can
// run both standalone queries and the transaction-grouped facet legs.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// queryRows runs q against an already-pinned db/tx and invokes fn for each row;
// fn receives a scan closure bound to the current row. Centralizes the
// rows.Close/Err boilerplate.
func queryRows(ctx context.Context, db querier, q string, fn func(scan func(...any) error) error) error {
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
