PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS files (
    id                INTEGER PRIMARY KEY,
    s3_key            TEXT    NOT NULL UNIQUE,
    s3_etag           TEXT    NOT NULL,
    s3_last_modified  INTEGER NOT NULL,
    size_bytes        INTEGER NOT NULL,
    indexed_at        INTEGER NOT NULL,
    start_time_ns     INTEGER NOT NULL,
    end_time_ns       INTEGER NOT NULL,
    chunk_count       INTEGER NOT NULL,
    message_count     INTEGER NOT NULL,
    has_message_index INTEGER NOT NULL,
    mcap_summary      BLOB
);
CREATE INDEX IF NOT EXISTS idx_files_time ON files(start_time_ns, end_time_ns);

CREATE TABLE IF NOT EXISTS topics (
    file_id         INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    name            TEXT    NOT NULL,
    schema_name     TEXT    NOT NULL,
    schema_encoding TEXT    NOT NULL,
    message_count   INTEGER NOT NULL,
    PRIMARY KEY (file_id, name)
);
CREATE INDEX IF NOT EXISTS idx_topics_name ON topics(name);

CREATE TABLE IF NOT EXISTS tags_embedded (
    file_id  INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    key      TEXT    NOT NULL,
    value    TEXT    NOT NULL,
    PRIMARY KEY (file_id, key)
);
CREATE INDEX IF NOT EXISTS idx_tags_embedded_kv ON tags_embedded(key, value);

CREATE TABLE IF NOT EXISTS tags_override (
    file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    key        TEXT    NOT NULL,
    value      TEXT,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (file_id, key)
);
CREATE INDEX IF NOT EXISTS idx_tags_override_kv ON tags_override(key, value);

-- UNION ALL (not UNION): the two SELECTs are disjoint on (file_id, key) by
-- construction — the first yields only override keys, the second only embedded
-- keys with NO override — so dedup is wasted work. Kept identical to the auryn
-- schema's view so the two languages' tags_effective cannot drift.
CREATE VIEW IF NOT EXISTS tags_effective AS
SELECT file_id, key, value, 1 AS is_override
FROM tags_override
WHERE value IS NOT NULL
UNION ALL
SELECT e.file_id, e.key, e.value, 0 AS is_override
FROM tags_embedded e
LEFT JOIN tags_override o ON (o.file_id = e.file_id AND o.key = e.key)
WHERE o.file_id IS NULL;

CREATE TABLE IF NOT EXISTS indexer_failures (
    s3_key      TEXT NOT NULL PRIMARY KEY,
    failed_at   INTEGER NOT NULL,
    error_text  TEXT NOT NULL
);
