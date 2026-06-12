package catalog

import (
	"context"
	"testing"
)

func TestReplaceEmbeddedTags_Roundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))

	in := []TagKV{{Key: "vehicle", Value: "7"}, {Key: "route", Value: "A1"}}
	if err := ReplaceEmbeddedTagsForFile(ctx, s, fid, in); err != nil {
		t.Fatal(err)
	}
	got, err := EffectiveTags(ctx, s, fid)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d effective tags, want 2", len(got))
	}
	for _, tg := range got {
		if tg.IsOverride {
			t.Errorf("tag %s should be from embedded, got override", tg.Key)
		}
	}
}

func TestReplaceEmbeddedTags_RemovesAbsent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))

	_ = ReplaceEmbeddedTagsForFile(ctx, s, fid,
		[]TagKV{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}})
	_ = ReplaceEmbeddedTagsForFile(ctx, s, fid,
		[]TagKV{{Key: "b", Value: "2-new"}})
	got, _ := EffectiveTags(ctx, s, fid)
	if len(got) != 1 || got[0].Key != "b" || got[0].Value != "2-new" {
		t.Errorf("unexpected tags after reindex: %+v", got)
	}
}

func TestSetOverride_Wins(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))

	_ = ReplaceEmbeddedTagsForFile(ctx, s, fid, []TagKV{{Key: "vehicle", Value: "7"}})
	_ = SetOverride(ctx, s, fid, "vehicle", "7-actually-9")

	got, _ := EffectiveTags(ctx, s, fid)
	if len(got) != 1 {
		t.Fatalf("got %d tags", len(got))
	}
	if got[0].Value != "7-actually-9" || !got[0].IsOverride {
		t.Errorf("override did not win: %+v", got[0])
	}
}

func TestUnsetOverride_RevealsEmbedded(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))

	_ = ReplaceEmbeddedTagsForFile(ctx, s, fid, []TagKV{{Key: "vehicle", Value: "7"}})
	_ = SetOverride(ctx, s, fid, "vehicle", "9")
	_ = UnsetOverride(ctx, s, fid, "vehicle")

	got, _ := EffectiveTags(ctx, s, fid)
	if len(got) != 1 || got[0].Value != "7" || got[0].IsOverride {
		t.Errorf("did not revert to embedded: %+v", got)
	}
}

func TestUnsetOverride_NoEmbedded_Vanishes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))

	_ = SetOverride(ctx, s, fid, "verified", "yes")
	_ = UnsetOverride(ctx, s, fid, "verified")

	got, _ := EffectiveTags(ctx, s, fid)
	if len(got) != 0 {
		t.Errorf("expected tag gone (no embedded to reveal), got %+v", got)
	}
}

func TestMaskEmbedded(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))

	_ = ReplaceEmbeddedTagsForFile(ctx, s, fid, []TagKV{{Key: "v", Value: "1"}})
	_ = MaskEmbedded(ctx, s, fid, "v")

	got, _ := EffectiveTags(ctx, s, fid)
	if len(got) != 0 {
		t.Errorf("expected empty (NULL-masked), got %+v", got)
	}
}

func TestHasEmbeddedTag(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))
	_ = ReplaceEmbeddedTagsForFile(ctx, s, fid, []TagKV{{Key: "robot_id", Value: "r7"}})

	if has, _ := HasEmbeddedTag(ctx, s, fid, "robot_id"); !has {
		t.Error("expected robot_id embedded")
	}
	if has, _ := HasEmbeddedTag(ctx, s, fid, "absent"); has {
		t.Error("did not expect absent embedded")
	}
}

func TestPreserveOverrideAcrossReindex(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "etag-1", 1))

	_ = SetOverride(ctx, s, fid, "verified", "yes")
	// Re-index simulated: upsert with new etag + new embedded tags.
	_, _, _ = UpsertFile(ctx, s, FileRecord{
		S3Key: "k", S3ETag: "etag-2", S3LastModified: 2, SizeBytes: 1,
		StartTimeNs: 1, EndTimeNs: 2,
	})
	_ = ReplaceEmbeddedTagsForFile(ctx, s, fid, []TagKV{{Key: "embedded", Value: "x"}})

	got, _ := EffectiveTags(ctx, s, fid)
	if len(got) != 2 {
		t.Fatalf("want 2 effective tags, got %d: %+v", len(got), got)
	}
	var foundOv, foundEmb bool
	for _, tg := range got {
		if tg.Key == "verified" && tg.IsOverride && tg.Value == "yes" {
			foundOv = true
		}
		if tg.Key == "embedded" && !tg.IsOverride && tg.Value == "x" {
			foundEmb = true
		}
	}
	if !foundOv || !foundEmb {
		t.Errorf("tag layers mixed up: %+v", got)
	}
}
