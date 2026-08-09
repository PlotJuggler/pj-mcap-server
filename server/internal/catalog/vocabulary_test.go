package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

// buildVocabDB builds a richer auryn DB exercising the hierarchy + facets:
//
//	alpha ─ s1 ─ r1, r2      (files: r1 x2, r2 x1)
//	      └ s2 ─ r3          (file:  r3 x1)
//	beta  ─ s3 ─ r4          (file:  r4 x1, source=synthetic)
//
// sources: ros-bags (4 files), synthetic (1 file). One embedded tag facet
// (mission ∈ {inv, audit}) and a high-cardinality key to exercise the cap.
func buildVocabDB(t *testing.T, path string, highCard int) {
	t.Helper()
	db := openAurynTestDB(t, path)
	defer db.Close()
	stmts := []string{
		`INSERT INTO customers(id,name) VALUES (1,'alpha'),(2,'beta')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'s1'),(2,1,'s2'),(3,2,'s3')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'r1'),(2,1,'r2'),(3,2,'r3'),(4,3,'r4')`,
		`INSERT INTO sources(id,name) VALUES (1,'ros-bags'),(2,'synthetic')`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/a')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp1')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}
	// files: (id, cust, site, robot, source). topic_counts=[1] for all.
	type frow struct{ id, cust, site, robot, source int }
	files := []frow{
		{1, 1, 1, 1, 1}, {2, 1, 1, 1, 1}, // alpha/s1/r1 x2
		{3, 1, 1, 2, 1}, // alpha/s1/r2
		{4, 1, 2, 3, 1}, // alpha/s2/r3
		{5, 2, 3, 4, 2}, // beta/s3/r4 (synthetic)
	}
	for _, f := range files {
		if _, err := db.Exec(`INSERT INTO files
			(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			f.id, fmt.Sprintf("f%d.mcap", f.id), "e", 1, f.cust, f.site, f.robot, f.source,
			"2026-06-01", 1000, 2000, 1, 1, encodeCounts(1)); err != nil {
			t.Fatalf("insert file %d: %v", f.id, err)
		}
	}
	// A bounded tag facet on files 1,2 (embedded) + 3 (override).
	if _, err := db.Exec(`INSERT INTO tags_embedded(file_id,key,value) VALUES (1,'mission','inv'),(2,'mission','inv')`); err != nil {
		t.Fatalf("tags_embedded: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (3,'mission','audit',1)`); err != nil {
		t.Fatalf("tags_override: %v", err)
	}
	// High-cardinality key: PK(file_id,key) caps one run_id per file, so > cap
	// distinct values needs > cap files. Add `highCard` extra files (ids 100+),
	// each with a unique run_id, to exceed TagFacetCap.
	for i := 0; i < highCard; i++ {
		fid := 100 + i
		if _, err := db.Exec(`INSERT INTO files
			(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			fid, fmt.Sprintf("hc%d.mcap", fid), "e", 1, 1, 1, 1, 1,
			"2026-06-01", 1000, 2000, 1, 1, encodeCounts(1)); err != nil {
			t.Fatalf("high-card file %d: %v", fid, err)
		}
		if _, err := db.Exec(
			`INSERT INTO tags_embedded(file_id,key,value) VALUES (?, 'run_id', ?)`,
			fid, fmt.Sprintf("uuid-%d", i)); err != nil {
			t.Fatalf("high-card tag: %v", err)
		}
	}
}

func TestGetVocabulary_Hermetic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vocab.db")
	buildVocabDB(t, path, 0) // no high-card key
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	v, err := GetVocabulary(ctx, st, VocabOptions{})
	if err != nil {
		t.Fatalf("GetVocabulary: %v", err)
	}

	// The tree: 2 customers, alpha has 2 sites, s1 has 2 robots, name-sorted.
	if len(v.Customers) != 2 || v.Customers[0].Name != "alpha" || v.Customers[1].Name != "beta" {
		t.Fatalf("customers = %+v, want [alpha beta]", v.Customers)
	}
	alpha := v.Customers[0]
	if alpha.FileCount != 4 {
		t.Fatalf("alpha file_count = %d, want 4", alpha.FileCount)
	}
	if len(alpha.Sites) != 2 || alpha.Sites[0].Name != "s1" || alpha.Sites[1].Name != "s2" {
		t.Fatalf("alpha sites = %+v, want [s1 s2]", alpha.Sites)
	}
	s1 := alpha.Sites[0]
	if s1.FileCount != 3 || len(s1.Robots) != 2 {
		t.Fatalf("s1 file_count=%d robots=%d, want 3 / 2", s1.FileCount, len(s1.Robots))
	}
	if s1.Robots[0].Name != "r1" || s1.Robots[0].FileCount != 2 || s1.Robots[1].FileCount != 1 {
		t.Fatalf("s1 robots = %+v, want r1 x2, r2 x1", s1.Robots)
	}
	beta := v.Customers[1]
	if len(beta.Sites) != 1 || beta.Sites[0].Robots[0].Name != "r4" {
		t.Fatalf("beta = %+v, want s3/r4", beta)
	}

	// Flat sources with counts.
	if len(v.Sources) != 2 {
		t.Fatalf("sources = %+v, want 2", v.Sources)
	}
	srcCount := map[string]uint64{}
	for _, s := range v.Sources {
		srcCount[s.Name] = s.FileCount
	}
	if srcCount["ros-bags"] != 4 || srcCount["synthetic"] != 1 {
		t.Fatalf("source counts = %v, want ros-bags:4 synthetic:1", srcCount)
	}

	// Tag facet: mission ∈ {audit x1, inv x2}, override+embedded merged.
	var mission *VocabFacet
	for i := range v.Tags {
		if v.Tags[i].Key == "mission" {
			mission = &v.Tags[i]
		}
	}
	if mission == nil {
		t.Fatalf("no 'mission' facet in %+v", v.Tags)
	}
	mc := map[string]uint64{}
	for _, vv := range mission.Values {
		mc[vv.Value] = vv.FileCount
	}
	if mc["inv"] != 2 || mc["audit"] != 1 {
		t.Fatalf("mission facet = %v, want inv:2 audit:1", mc)
	}
}

func TestGetVocabulary_TagFacetCap(t *testing.T) {
	// Pin the EXACT boundary: a key with exactly TagFacetCap distinct values is
	// KEPT; TagFacetCap+1 is DROPPED. (Guards a `>=` vs `>` off-by-one — Codex M3.)
	for _, tc := range []struct {
		name     string
		distinct int
		wantKept bool
	}{
		{"exactly cap is kept", TagFacetCap, true},
		{"cap+1 is dropped", TagFacetCap + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "vocab.db")
			buildVocabDB(t, path, tc.distinct)
			st, err := OpenReadOnly(context.Background(), path)
			if err != nil {
				t.Fatalf("OpenReadOnly: %v", err)
			}
			defer st.Close()
			v, err := GetVocabulary(context.Background(), st, VocabOptions{})
			if err != nil {
				t.Fatalf("GetVocabulary: %v", err)
			}
			var runID *VocabFacet
			for i := range v.Tags {
				if v.Tags[i].Key == "run_id" {
					runID = &v.Tags[i]
				}
			}
			if tc.wantKept {
				if runID == nil {
					t.Fatalf("run_id (%d distinct) should be KEPT (<= cap %d), but it was dropped",
						tc.distinct, TagFacetCap)
				}
				if len(runID.Values) != tc.distinct {
					t.Fatalf("run_id kept with %d values, want %d", len(runID.Values), tc.distinct)
				}
			} else if runID != nil {
				t.Fatalf("run_id (%d distinct) should be DROPPED (> cap %d), got %d values",
					tc.distinct, TagFacetCap, len(runID.Values))
			}
		})
	}
}

func TestGetVocabulary_PrunesOrphans(t *testing.T) {
	// The auryn builder leaves orphan lookup rows on delete/rename (no GC). A
	// dimension row with NO referencing file must NOT appear in the vocabulary
	// (Claude M3 review) — otherwise the cascade shows stale ghost nodes.
	path := filepath.Join(t.TempDir(), "vocab.db")
	db := openAurynTestDB(t, path)
	orphans := []string{
		`INSERT INTO customers(id,name) VALUES (1,'alpha'),(9,'ORPHAN_CUST')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'s1'),(9,1,'ORPHAN_SITE')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'r1'),(9,1,'ORPHAN_ROBOT')`,
		`INSERT INTO sources(id,name) VALUES (1,'ros-bags'),(9,'ORPHAN_SRC')`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/a')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
		// Exactly ONE file, referencing only the non-orphan dimension ids.
		`INSERT INTO files (id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (1,'f.mcap','e',1,1,1,1,1,'2026-06-01',1,2,1,1,X'01')`,
	}
	for _, s := range orphans {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}
	db.Close()

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()
	v, err := GetVocabulary(context.Background(), st, VocabOptions{})
	if err != nil {
		t.Fatalf("GetVocabulary: %v", err)
	}
	// Only the file-referenced dimensions survive.
	if len(v.Customers) != 1 || v.Customers[0].Name != "alpha" {
		t.Fatalf("customers = %+v, want only [alpha] (orphan pruned)", v.Customers)
	}
	if len(v.Customers[0].Sites) != 1 || v.Customers[0].Sites[0].Name != "s1" {
		t.Fatalf("sites = %+v, want only [s1]", v.Customers[0].Sites)
	}
	if len(v.Customers[0].Sites[0].Robots) != 1 || v.Customers[0].Sites[0].Robots[0].Name != "r1" {
		t.Fatalf("robots = %+v, want only [r1]", v.Customers[0].Sites[0].Robots)
	}
	if len(v.Sources) != 1 || v.Sources[0].Name != "ros-bags" {
		t.Fatalf("sources = %+v, want only [ros-bags]", v.Sources)
	}
}

func TestAurynFilterFiles_DimensionPredicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vocab.db")
	buildVocabDB(t, path, 0)
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()
	ctx := context.Background()
	u := func(v uint64) *uint64 { return &v }

	// customer=alpha (id 1) => 4 files.
	got, _, err := FilterFiles(ctx, st, FilterArgs{CustomerID: u(1)})
	if err != nil || len(got) != 4 {
		t.Fatalf("CustomerID=1 => %d files (err %v), want 4", len(got), err)
	}
	// site=s1 (id 1) => 3 files.
	got, _, _ = FilterFiles(ctx, st, FilterArgs{SiteID: u(1)})
	if len(got) != 3 {
		t.Fatalf("SiteID=1 => %d files, want 3", len(got))
	}
	// robot=r1 (id 1) => 2 files.
	got, _, _ = FilterFiles(ctx, st, FilterArgs{RobotID: u(1)})
	if len(got) != 2 {
		t.Fatalf("RobotID=1 => %d files, want 2", len(got))
	}
	// source=synthetic (id 2) => 1 file.
	got, _, _ = FilterFiles(ctx, st, FilterArgs{SourceID: u(2)})
	if len(got) != 1 {
		t.Fatalf("SourceID=2 => %d files, want 1", len(got))
	}
	// combined customer=alpha AND source=synthetic => 0 (synthetic is beta's).
	got, _, _ = FilterFiles(ctx, st, FilterArgs{CustomerID: u(1), SourceID: u(2)})
	if len(got) != 0 {
		t.Fatalf("CustomerID=1 + SourceID=2 => %d files, want 0", len(got))
	}
}

// TestTagFacets_EquivalentToEffectiveView pins the 3-query facet decomposition
// to the tags_effective VIEW's own GROUP BY (the previous implementation and
// the semantic ground truth), across every merge edge: an override REPLACES its
// embedded value, a NULL override MASKS it (a fully-masked value must vanish),
// an override value that coexists with surviving embedded rows SUMS into one
// group, and an override-only key (no embedded row) still appears.
func TestTagFacets_EquivalentToEffectiveView(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cat.db")
	buildVocabDB(t, path, 0)
	db, err := sql.Open("sqlite",
		fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	seeds := []string{
		`INSERT INTO tags_embedded(file_id,key,value) VALUES (1,'quality','good'),(2,'quality','good'),(4,'quality','bad')`,
		// f1: override REPLACES good -> bad; bad then SUMS with f4's embedded bad.
		`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (1,'quality','bad',1)`,
		// f4: NULL override MASKS its only 'session' tag -> s9 must vanish entirely.
		`INSERT INTO tags_embedded(file_id,key,value) VALUES (4,'session','s9')`,
		`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (4,'session',NULL,1)`,
		// f5: override-only key, no embedded row at all.
		`INSERT INTO tags_override(file_id,key,value,updated_at) VALUES (5,'reviewed','yes',1)`,
	}
	for _, s := range seeds {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}

	got, err := tagFacets(context.Background(), db)
	if err != nil {
		t.Fatalf("tagFacets: %v", err)
	}

	// Ground truth: the view's GROUP BY, shaped exactly like the pre-rewrite code.
	var want []VocabFacet
	byKey := map[string][]VocabFacetValue{}
	var keyOrder []string
	if err := queryRows(context.Background(), db,
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
			byKey[key] = append(byKey[key], VocabFacetValue{Value: value, FileCount: n})
			return nil
		}); err != nil {
		t.Fatalf("view ground truth: %v", err)
	}
	for _, key := range keyOrder {
		if len(byKey[key]) > TagFacetCap {
			continue
		}
		want = append(want, VocabFacet{Key: key, Values: byKey[key]})
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("facet decomposition diverged from tags_effective view:\n got: %+v\nwant: %+v", got, want)
	}
}

// TestGetVocabularyDB_SingleSnapshotUnderConcurrentWriter pins the documented
// same-snapshot invariant (Codex review of PR #12): the store lease pins the
// served FILE's generation, not a WAL read snapshot — the builder commits per
// file, so unless GetVocabularyDB wraps its ~8 queries in ONE read
// transaction they can straddle commits and return a customer count that
// disagrees with the sum of its sites' counts (or tag facets newer than both).
// Seed: every file carries exactly one 'quality' tag, so on EVERY read three
// cross-checks must hold: sum(site counts) == customer count, sum(robot
// counts) == site count, and sum(quality facet counts) == total files.
func TestGetVocabularyDB_SingleSnapshotUnderConcurrentWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cat.db")
	w := openAurynTestDB(t, path)
	defer w.Close()
	seeds := []string{
		`INSERT INTO customers(id,name) VALUES (1,'alpha')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'s1')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'r1'),(2,1,'r2')`,
		`INSERT INTO sources(id,name) VALUES (1,'ros-bags')`,
		`INSERT INTO topic_names(id,name) VALUES (1,'/a')`,
		`INSERT INTO schemas(id,name,encoding) VALUES (1,'S','ros2msg')`,
		`INSERT INTO topic_sets(id,fingerprint) VALUES (1,'fp1')`,
		`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (1,1,1)`,
	}
	for _, s := range seeds {
		if _, err := w.Exec(s); err != nil {
			t.Fatalf("seed %q: %v", s, err)
		}
	}
	insertFile := func(id int) error {
		tx, err := w.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO files
			(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (?,?,?,?,1,1,?,1,'2026-06-01',1000,2000,1,1,?)`,
			id, fmt.Sprintf("f%d.mcap", id), "e", 1, id%2+1, encodeCounts(1)); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO tags_embedded(file_id,key,value) VALUES (?,'quality',?)`,
			id, fmt.Sprintf("q%d", id%3)); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	if err := insertFile(1); err != nil { // never-empty catalog for OpenReadOnly
		t.Fatalf("first insert: %v", err)
	}

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	done := make(chan error, 1)
	go func() {
		for i := 2; i <= 300; i++ {
			if err := insertFile(i); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	check := func(v *Vocabulary) error {
		var total uint64
		for _, c := range v.Customers {
			var siteSum uint64
			for _, s := range c.Sites {
				var robotSum uint64
				for _, r := range s.Robots {
					robotSum += r.FileCount
				}
				if robotSum != s.FileCount {
					return fmt.Errorf("site %s: robots sum %d != site count %d", s.Name, robotSum, s.FileCount)
				}
				siteSum += s.FileCount
			}
			if siteSum != c.FileCount {
				return fmt.Errorf("customer %s: sites sum %d != customer count %d", c.Name, siteSum, c.FileCount)
			}
			total += c.FileCount
		}
		var facetTotal uint64
		for _, f := range v.Tags {
			if f.Key != "quality" {
				continue
			}
			for _, val := range f.Values {
				facetTotal += val.FileCount
			}
		}
		if facetTotal != total {
			return fmt.Errorf("quality facet total %d != file total %d (facets from a different snapshot)", facetTotal, total)
		}
		return nil
	}

	writerDone := false
	for !writerDone {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("writer: %v", err)
			}
			writerDone = true
		default:
			v, err := GetVocabulary(context.Background(), st, VocabOptions{})
			if err != nil {
				t.Fatalf("GetVocabulary: %v", err)
			}
			if err := check(v); err != nil {
				t.Fatalf("cross-snapshot inconsistency: %v", err)
			}
		}
	}
	// One final read with the writer quiescent must also be consistent.
	v, err := GetVocabulary(context.Background(), st, VocabOptions{})
	if err != nil {
		t.Fatalf("final GetVocabulary: %v", err)
	}
	if err := check(v); err != nil {
		t.Fatalf("final: %v", err)
	}
}

// TestGetVocabulary_LeanOmitsUnreadSections pins the opt-in scoping contract.
//
// The full assembly is four whole-table GROUP BY scans over `files` plus a
// tag-facet scan — 2.44 s at 8.78M rows, on the browse path's critical section.
// The shipped C++ picker (wire_mapping.cpp) maps ONLY the customer->site->robot
// tree and CUSTOMER file counts, so it asks for the rest to be skipped. Two
// things must hold or that is unsafe: the TREE must be byte-for-byte what the
// full response would carry, and the omitted sections must actually be absent
// (not silently still computed).
func TestGetVocabulary_LeanOmitsUnreadSections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vocab-lean.db")
	buildVocabDB(t, path, 0)
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	full, err := GetVocabulary(ctx, st, VocabOptions{})
	if err != nil {
		t.Fatalf("full: %v", err)
	}
	lean, err := GetVocabulary(ctx, st, VocabOptions{
		OmitSources: true, OmitTagFacets: true, OmitSiteRobotCounts: true,
	})
	if err != nil {
		t.Fatalf("lean: %v", err)
	}

	// The tree is what the picker renders: identical shape, names and ids.
	if len(lean.Customers) != len(full.Customers) {
		t.Fatalf("lean dropped customers: %d vs %d", len(lean.Customers), len(full.Customers))
	}
	for i := range full.Customers {
		fc, lc := full.Customers[i], lean.Customers[i]
		if lc.ID != fc.ID || lc.Name != fc.Name {
			t.Fatalf("customer %d differs: %v vs %v", i, lc, fc)
		}
		// Customer counts drive the picker's summary hint — never omitted.
		if lc.FileCount != fc.FileCount {
			t.Fatalf("lean must keep customer counts: %d vs %d", lc.FileCount, fc.FileCount)
		}
		if len(lc.Sites) != len(fc.Sites) {
			t.Fatalf("customer %q: lean dropped sites: %d vs %d", fc.Name, len(lc.Sites), len(fc.Sites))
		}
		for j := range fc.Sites {
			if lc.Sites[j].ID != fc.Sites[j].ID || lc.Sites[j].Name != fc.Sites[j].Name {
				t.Fatalf("site %d differs under %q", j, fc.Name)
			}
			if len(lc.Sites[j].Robots) != len(fc.Sites[j].Robots) {
				t.Fatalf("site %q: lean dropped robots: %d vs %d",
					fc.Sites[j].Name, len(lc.Sites[j].Robots), len(fc.Sites[j].Robots))
			}
			for k := range fc.Sites[j].Robots {
				if lc.Sites[j].Robots[k].ID != fc.Sites[j].Robots[k].ID ||
					lc.Sites[j].Robots[k].Name != fc.Sites[j].Robots[k].Name {
					t.Fatalf("robot %d differs under site %q", k, fc.Sites[j].Name)
				}
			}
		}
	}

	// REVIEW DEFECT (both reviewers): OmitSiteRobotCounts was never asserted, so
	// ignoring it entirely — silently restoring two of the four whole-table scans
	// this option exists to remove — would have passed. Assert the counts are
	// actually zero, and require the FULL response to carry non-zero ones first so
	// the assertion cannot be vacuous.
	sawNonZeroLeafCount := false
	for i := range full.Customers {
		for j := range full.Customers[i].Sites {
			if full.Customers[i].Sites[j].FileCount > 0 {
				sawNonZeroLeafCount = true
			}
			if lean.Customers[i].Sites[j].FileCount != 0 {
				t.Fatalf("OmitSiteRobotCounts ignored: site %q still reports %d files",
					lean.Customers[i].Sites[j].Name, lean.Customers[i].Sites[j].FileCount)
			}
			for k := range full.Customers[i].Sites[j].Robots {
				if lean.Customers[i].Sites[j].Robots[k].FileCount != 0 {
					t.Fatalf("OmitSiteRobotCounts ignored: robot %q still reports %d files",
						lean.Customers[i].Sites[j].Robots[k].Name,
						lean.Customers[i].Sites[j].Robots[k].FileCount)
				}
			}
		}
	}
	if !sawNonZeroLeafCount {
		t.Fatal("fixture has no non-zero site counts: the OmitSiteRobotCounts assertion would be vacuous")
	}

	// The omitted sections must be gone. If the fixture has none of a section to
	// begin with, the assertion would be vacuous — require the FULL response to
	// carry it first, so this test can actually fail.
	if len(full.Sources) == 0 {
		t.Fatal("fixture carries no sources: the omit_sources assertion would be vacuous")
	}
	if len(lean.Sources) != 0 {
		t.Fatalf("OmitSources ignored: got %d sources", len(lean.Sources))
	}
	if len(full.Tags) == 0 {
		t.Fatal("fixture carries no tag facets: the omit_tag_facets assertion would be vacuous")
	}
	if len(lean.Tags) != 0 {
		t.Fatalf("OmitTagFacets ignored: got %d facets", len(lean.Tags))
	}
}
