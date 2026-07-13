package catalog

import "context"

// TopicRecord is one topic row (maps onto wire TopicInfo + format.TopicInfo).
type TopicRecord struct {
	Name           string
	SchemaName     string
	SchemaEncoding string
	MessageCount   uint64
}

// ListTopicsForFile returns all topics for the given file in name-sorted order,
// reconstructed from the auryn topic_set.
//
// This is the public, single-call entry point (fetches Store.DB() itself); a
// caller composing this with a sibling summary/tags query in one logical
// operation (B1 — catalog-migration §6.2a review) must instead pin
// db := s.DB() once and call aurynListTopicsForFile directly against that SAME
// handle. See GetFileDetail.
func ListTopicsForFile(ctx context.Context, s *Store, fileID uint64) ([]TopicRecord, error) {
	return aurynListTopicsForFile(ctx, s.DB(), fileID)
}
