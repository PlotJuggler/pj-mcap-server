package catalog

import (
	"context"
	"database/sql"
)

// detail.go implements the compound GetFileDetail read (B1 — catalog-migration
// §6.2a review): the WS GetFile handler used to compose catalog.GetFile +
// catalog.ListTopicsForFile + catalog.EffectiveTags as three SEPARATE Store
// calls, each of which independently calls Store.DB(). Between any two of those
// calls, ReopenIfSwapped could swap the Store onto a new generation — and
// because file ids can renumber across a full auryn rebuild, that window could
// serve an old generation's file summary paired with a NEW generation's topics
// or tags (or vice versa): wrong data, not just a transient error.
//
// GetFileDetail pins db := s.DB() ONCE and threads that single handle through
// all three phases, exactly like the three functions it replaces, so the whole
// response is guaranteed to describe one generation.

// GetFileDetail returns the file summary, its topics, and its effective tags in
// one generation-pinned read. It is the compound entry point the WS GetFile
// handler should use instead of composing GetFile/ListTopicsForFile/
// EffectiveTags itself. Returns ErrFileNotFound (wrapped) when the summary
// phase does.
func GetFileDetail(ctx context.Context, s *Store, fileID uint64) (FileRecord, []TopicRecord, []EffectiveTag, error) {
	return getFileDetailDB(ctx, s.DB(), fileID)
}

// GetFileDetailByKey is GetFileDetail addressed by the STABLE object key
// instead of a generation-scoped file id (wire s3_key addressing): it pins
// db := s.DB() ONCE, resolves key -> CURRENT file id on that handle
// (FileIDForKey), and runs the same three-phase read against the SAME handle
// — so the id resolve and the detail read are guaranteed to answer from one
// generation (a rebuild landing between them cannot pair one generation's id
// with another's rows). Returns ErrFileNotFound when the key doesn't parse
// as a Hive key or names no cataloged file.
func GetFileDetailByKey(ctx context.Context, s *Store, key string) (FileRecord, []TopicRecord, []EffectiveTag, error) {
	db := s.DB()
	id, err := FileIDForKey(ctx, db, key)
	if err != nil {
		return FileRecord{}, nil, nil, err
	}
	return getFileDetailDB(ctx, db, id)
}

// getFileDetailDB is the shared three-phase core: every phase runs against
// the caller's single pinned handle (B1 — see the file header).
func getFileDetailDB(ctx context.Context, db *sql.DB, fileID uint64) (FileRecord, []TopicRecord, []EffectiveTag, error) {
	rec, err := aurynGetFile(ctx, db, fileID)
	if err != nil {
		return FileRecord{}, nil, nil, err
	}

	topics, err := aurynListTopicsForFile(ctx, db, rec.ID)
	if err != nil {
		return FileRecord{}, nil, nil, err
	}

	tags, err := effectiveTagsDB(ctx, db, rec.ID)
	if err != nil {
		return FileRecord{}, nil, nil, err
	}
	return rec, topics, tags, nil
}
