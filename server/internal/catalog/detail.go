package catalog

import "context"

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
// all three phases (routing through the existing readOnly/legacy branch
// internally, exactly like the three functions it replaces), so the whole
// response is guaranteed to describe one generation.

// GetFileDetail returns the file summary, its topics, and its effective tags in
// one generation-pinned read. It is the compound entry point the WS GetFile
// handler should use instead of composing GetFile/ListTopicsForFile/
// EffectiveTags itself. Returns ErrFileNotFound (wrapped) when the summary
// phase does.
func GetFileDetail(ctx context.Context, s *Store, fileID uint64) (FileRecord, []TopicRecord, []EffectiveTag, error) {
	db := s.DB()

	var (
		rec FileRecord
		err error
	)
	if s.readOnly {
		rec, err = aurynGetFile(ctx, db, fileID)
	} else {
		rec, err = getFileLegacy(ctx, db, fileID)
	}
	if err != nil {
		return FileRecord{}, nil, nil, err
	}

	var topics []TopicRecord
	if s.readOnly {
		topics, err = aurynListTopicsForFile(ctx, db, rec.ID)
	} else {
		topics, err = listTopicsLegacy(ctx, db, rec.ID)
	}
	if err != nil {
		return FileRecord{}, nil, nil, err
	}

	tags, err := effectiveTagsDB(ctx, db, rec.ID)
	if err != nil {
		return FileRecord{}, nil, nil, err
	}
	return rec, topics, tags, nil
}
