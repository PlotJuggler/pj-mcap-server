package ws

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/storage"

	_ "modernc.org/sqlite"
)

// auryn_fixture_test.go replaces the (retired) catalog.Open + indexer.Scanner
// pattern the session-path component tests used to build a live catalog: with
// the Go catalog writer + in-process indexer deleted (catalog-migration §2.6),
// tests build a real auryn-schema SQLite catalog DIRECTLY (raw SQL — the same
// approach internal/catalog's own hermetic auryn fixtures use), from a REAL
// codec.Extract/ExtractAndIndex read against the test's storage.BlobStore —
// exactly the extraction the production external-builder pipeline performs,
// just run once at fixture-build time instead of by a background scanner.
//
// hiveKeyAdapter is the seam that lets every EXISTING test file's memBlobStore
// (and its wrappers — countingBlobStore, gateBlob, armablePanicBlob,
// perKeyCountingBlob — all keyed with plain flat names like "only.mcap") keep
// working unchanged: the auryn schema always rebuilds a Hive-partitioned
// object key (customer=.../.../filename), so the key production code fetches
// by is NEVER the bare flat name a test constructs its blob with. The adapter
// sits between the catalog (which only ever speaks Hive keys) and the test's
// blob (which only ever speaks flat keys), translating one way on GetRange/
// Head and the other way on List — so a test's own key-space (used by its
// counting/gating wrappers and by wsClient.fileID, see below) is completely
// unaffected by the Hive rewrite.

// hivePrefix is the single shared dimension tuple every ws-package fixture
// catalogs its files under; only the filename varies per file. Tests do not
// need dimension variety (that is catalog/vocabulary_test.go's job) — they
// only need one real, stable Hive prefix around whatever flat name they
// already use as a logical file identifier.
const hivePrefix = "customer=t/customer_site=t/robot=t/source=t/date=2026-01-01/"

// hiveKeyFor rebuilds the auryn object key for a flat test filename. filename
// must be a bare name with no '/' (every literal key these tests use already
// is) so the round trip through flatKeyFromHive is lossless.
func hiveKeyFor(filename string) string { return hivePrefix + filename }

// flatKeyFromHive is hiveKeyFor's inverse.
func flatKeyFromHive(key string) string { return strings.TrimPrefix(key, hivePrefix) }

// hiveKeyAdapter wraps a flat-keyed storage.BlobStore so it can serve a
// Hive-keyed catalog transparently — see the file doc above.
type hiveKeyAdapter struct{ inner storage.BlobStore }

func (h *hiveKeyAdapter) GetRange(ctx context.Context, key string, off, length int64) ([]byte, error) {
	return h.inner.GetRange(ctx, flatKeyFromHive(key), off, length)
}

func (h *hiveKeyAdapter) Head(ctx context.Context, key string) (storage.ObjectInfo, error) {
	info, err := h.inner.Head(ctx, flatKeyFromHive(key))
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	info.Key = key
	return info, nil
}

func (h *hiveKeyAdapter) List(ctx context.Context, prefix, token string) ([]storage.ObjectInfo, string, error) {
	objs, next, err := h.inner.List(ctx, prefix, token)
	if err != nil {
		return nil, "", err
	}
	out := make([]storage.ObjectInfo, len(objs))
	for i, o := range objs {
		o.Key = hiveKeyFor(o.Key)
		out[i] = o
	}
	return out, next, nil
}

// buildAurynCatalog lists every ".mcap" key currently in blob (flat-keyed, as
// every test constructs it), extracts each one's REAL FileSummary via codec
// (through the hiveKeyAdapter, so this is byte-for-byte what the production
// external-builder pipeline would read), and writes a fresh auryn-schema (v3)
// SQLite catalog DB containing exactly those files under the shared
// hivePrefix dimension tuple. When idxCache is non-nil, each file's chunk
// index is ALSO built (via ExtractAndIndex) and pre-warmed into it — the
// pre-cutover in-process indexer used to do this from the same scan, and
// several tests assert zero further storage reads for an already-"indexed"
// file, so the fixture builder must reproduce that side effect explicitly now
// that no in-process scanner exists.
//
// Returns the opened read-only *catalog.Store and the storage.BlobStore
// production code (SessionDeps.Blob) must use — the hiveKeyAdapter wrapping
// the caller's blob.
func buildAurynCatalog(t *testing.T, ctx context.Context, blob storage.BlobStore, codec format.Codec, idxCache *format.ChunkIndexCache) (*catalog.Store, storage.BlobStore) {
	t.Helper()
	hiveBlob := &hiveKeyAdapter{inner: blob}

	objs, _, err := hiveBlob.List(ctx, "", "")
	if err != nil {
		t.Fatalf("buildAurynCatalog: list: %v", err)
	}
	var keys []string
	for _, o := range objs {
		if strings.HasSuffix(o.Key, ".mcap") {
			keys = append(keys, o.Key)
		}
	}
	sort.Strings(keys) // deterministic file-id assignment

	path := filepath.Join(t.TempDir(), "catalog.db")
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)", path))
	if err != nil {
		t.Fatalf("buildAurynCatalog: open %s: %v", path, err)
	}
	ddl := []string{
		fmt.Sprintf(`CREATE TABLE schema_version (id INTEGER PRIMARY KEY CHECK (id=1), version INTEGER NOT NULL);
			INSERT INTO schema_version(id,version) VALUES (1,%d)`, catalog.SchemaVersion),
		`CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE sites (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE robots (id INTEGER PRIMARY KEY, site_id INTEGER NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE sources (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE topic_names (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE schemas (id INTEGER PRIMARY KEY, name TEXT NOT NULL, encoding TEXT NOT NULL)`,
		`CREATE TABLE topic_sets (id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE topic_set_members (set_id INTEGER NOT NULL, topic_id INTEGER NOT NULL, schema_id INTEGER NOT NULL, PRIMARY KEY(set_id,topic_id)) WITHOUT ROWID`,
		`CREATE TABLE files (id INTEGER PRIMARY KEY, filename TEXT NOT NULL, etag TEXT NOT NULL, size_bytes INTEGER NOT NULL,
			last_modified_ns INTEGER NOT NULL DEFAULT 0, cataloged_at_ns INTEGER NOT NULL DEFAULT 0,
			customer_id INTEGER NOT NULL, site_id INTEGER NOT NULL, robot_id INTEGER NOT NULL, source_id INTEGER NOT NULL,
			date TEXT NOT NULL, start_time_ns INTEGER NOT NULL, end_time_ns INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL DEFAULT 0, topic_set_id INTEGER NOT NULL, topic_counts BLOB NOT NULL,
			has_error INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE tags_embedded (file_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID`,
		`CREATE TABLE tags_override (file_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT, updated_at INTEGER NOT NULL, PRIMARY KEY(file_id,key)) WITHOUT ROWID`,
		`CREATE VIEW tags_effective AS
			SELECT file_id,key,value,1 AS is_override FROM tags_override WHERE value IS NOT NULL
			UNION ALL
			SELECT e.file_id,e.key,e.value,0 FROM tags_embedded e
			LEFT JOIN tags_override o ON (o.file_id=e.file_id AND o.key=e.key) WHERE o.file_id IS NULL`,
		`INSERT INTO customers(id,name) VALUES (1,'t')`,
		`INSERT INTO sites(id,customer_id,name) VALUES (1,1,'t')`,
		`INSERT INTO robots(id,site_id,name) VALUES (1,1,'t')`,
		`INSERT INTO sources(id,name) VALUES (1,'t')`,
	}
	for _, s := range ddl {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("buildAurynCatalog: ddl %q: %v", s, err)
		}
	}

	topicID := map[string]int64{}
	schemaKeyID := map[[2]string]int64{}
	nextTopicID := int64(1)
	nextSchemaID := int64(1)
	topicSetID := map[string]int64{}
	nextSetID := int64(1)

	for i, key := range keys {
		var (
			summary format.FileSummary
			extErr  error
		)
		if idxCache != nil {
			var idx format.FileChunkIndex
			summary, idx, extErr = codec.ExtractAndIndex(ctx, hiveBlob, key, 0)
			if extErr == nil {
				idxCache.Put(key, summary.ETag, idx)
			}
		} else {
			summary, extErr = codec.Extract(ctx, hiveBlob, key)
		}
		if extErr != nil {
			t.Fatalf("buildAurynCatalog: extract %q: %v", key, extErr)
		}

		type topicWithID struct {
			id       int64
			schemaID int64
			count    uint64
		}
		var withIDs []topicWithID
		for _, top := range summary.Topics {
			tid, ok := topicID[top.Name]
			if !ok {
				tid = nextTopicID
				nextTopicID++
				topicID[top.Name] = tid
				if _, err := db.Exec(`INSERT INTO topic_names(id,name) VALUES (?,?)`, tid, top.Name); err != nil {
					t.Fatalf("insert topic_name %q: %v", top.Name, err)
				}
			}
			sk := [2]string{top.SchemaName, top.SchemaEncoding}
			sid, ok := schemaKeyID[sk]
			if !ok {
				sid = nextSchemaID
				nextSchemaID++
				schemaKeyID[sk] = sid
				if _, err := db.Exec(`INSERT INTO schemas(id,name,encoding) VALUES (?,?,?)`, sid, top.SchemaName, top.SchemaEncoding); err != nil {
					t.Fatalf("insert schema %q/%q: %v", top.SchemaName, top.SchemaEncoding, err)
				}
			}
			withIDs = append(withIDs, topicWithID{id: tid, schemaID: sid, count: top.MessageCount})
		}
		sort.Slice(withIDs, func(a, b int) bool { return withIDs[a].id < withIDs[b].id })

		// Fingerprint from the sorted (topic_id, schema_id) MEMBER PAIRS — mirrors
		// the real builder's compute_set_fingerprint(members) (builder.py), which
		// hashes sorted (topic_id, schema_id) tuples, not topic ids alone. Using
		// topic ids alone would incorrectly dedup two files that share topic names
		// but differ in schema (encoding/name) onto the same topic_set.
		var idsForFP []string
		for _, w := range withIDs {
			idsForFP = append(idsForFP, fmt.Sprintf("%d:%d", w.id, w.schemaID))
		}
		fp := strings.Join(idsForFP, ",")
		if fp == "" {
			fp = fmt.Sprintf("empty-%d", i) // topic-less file: unique fingerprint per file
		}
		setID, ok := topicSetID[fp]
		if !ok {
			setID = nextSetID
			nextSetID++
			topicSetID[fp] = setID
			if _, err := db.Exec(`INSERT INTO topic_sets(id,fingerprint) VALUES (?,?)`, setID, fp); err != nil {
				t.Fatalf("insert topic_set: %v", err)
			}
			for _, w := range withIDs {
				if _, err := db.Exec(`INSERT INTO topic_set_members(set_id,topic_id,schema_id) VALUES (?,?,?)`,
					setID, w.id, w.schemaID); err != nil {
					t.Fatalf("insert topic_set_member: %v", err)
				}
			}
		}

		counts := make([]byte, 0, len(withIDs))
		for _, w := range withIDs {
			counts = append(counts, encodeVarintByte(w.count)...)
		}

		filename := flatKeyFromHive(key)
		fileID := int64(i + 1)
		if _, err := db.Exec(`INSERT INTO files
			(id,filename,etag,size_bytes,customer_id,site_id,robot_id,source_id,date,start_time_ns,end_time_ns,chunk_count,topic_set_id,topic_counts)
			VALUES (?,?,?,?,1,1,1,1,'2026-01-01',?,?,?,?,?)`,
			fileID, filename, summary.ETag, summary.Size, summary.StartNs, summary.EndNs, summary.ChunkCount, setID, counts); err != nil {
			t.Fatalf("insert file %q: %v", key, err)
		}
		// Deliberately NOT writing summary.Metadata into tags_embedded: the
		// production Python builder's derive_tags() is a stub returning [] today
		// (CATALOG_CONTRACT.md §6), so a REAL catalog never has embedded tags for
		// this corpus — manufacturing them here from the codec's summary would let
		// a test observe a catalog state production cannot currently produce. A
		// test that needs an embedded (or override) tag should seed it explicitly
		// against the DB this function returns (or via tags_override), not rely on
		// this fixture inferring one from codec metadata.
	}
	if err := db.Close(); err != nil {
		t.Fatalf("buildAurynCatalog: close writer handle: %v", err)
	}

	st, err := catalog.OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatalf("buildAurynCatalog: OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, hiveBlob
}

// encodeVarintByte unsigned-LEB128-encodes n (mirrors the Python builder's
// varint.encode_counts_blob / catalog/auryn_read_test.go's encodeVarint).
func encodeVarintByte(n uint64) []byte {
	var out []byte
	for {
		b := byte(n & 0x7f)
		n >>= 7
		if n != 0 {
			out = append(out, b|0x80)
		} else {
			out = append(out, b)
			return out
		}
	}
}
