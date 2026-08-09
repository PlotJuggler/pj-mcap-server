package ws

import (
	"context"
	"testing"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// REVIEW DEFECT (both reviewers): nothing exercised the omit_* flags THROUGH the
// ws handler. The mapping at handlers_catalog.go is three independent
// assignments, so swapping two of them — e.g. OmitSources: req.GetOmitTagFacets()
// — changed no test's outcome. This pins each flag to the section it controls by
// toggling exactly one at a time.
func TestGetVocabulary_ScopingFlagsAreWiredIndividually(t *testing.T) {
	store := openAurynReadStore(t)
	h := &CatalogHandler{Store: store}
	ctx := context.Background()

	full, err := h.GetVocabulary(ctx, &pb.GetVocabularyRequest{})
	if err != nil {
		t.Fatalf("full: %v", err)
	}
	// Non-vacuity: the fixture must actually carry every section, or "absent
	// after omitting" proves nothing.
	if len(full.Sources) == 0 {
		t.Fatal("fixture carries no sources; the omit_sources assertion would be vacuous")
	}
	if len(full.Tags) == 0 {
		t.Fatal("fixture carries no tag facets; the omit_tag_facets assertion would be vacuous")
	}

	t.Run("omit_sources only", func(t *testing.T) {
		got, err := h.GetVocabulary(ctx, &pb.GetVocabularyRequest{OmitSources: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Sources) != 0 {
			t.Fatalf("omit_sources not honoured: %d sources", len(got.Sources))
		}
		if len(got.Tags) == 0 {
			t.Fatal("omit_sources leaked into tag facets — the flags are cross-wired")
		}
	})

	t.Run("omit_tag_facets only", func(t *testing.T) {
		got, err := h.GetVocabulary(ctx, &pb.GetVocabularyRequest{OmitTagFacets: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Tags) != 0 {
			t.Fatalf("omit_tag_facets not honoured: %d facets", len(got.Tags))
		}
		if len(got.Sources) == 0 {
			t.Fatal("omit_tag_facets leaked into sources — the flags are cross-wired")
		}
	})

	t.Run("omit_site_robot_counts only", func(t *testing.T) {
		got, err := h.GetVocabulary(ctx, &pb.GetVocabularyRequest{OmitSiteRobotCounts: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Sources) == 0 || len(got.Tags) == 0 {
			t.Fatal("omit_site_robot_counts leaked into sources/facets — the flags are cross-wired")
		}
		for _, c := range got.Customers {
			// Customer counts are ALWAYS computed (the picker's summary hint).
			if c.FileCount == 0 {
				t.Fatalf("customer %q lost its count; only SITE and ROBOT counts may be omitted", c.Name)
			}
			for _, s := range c.Sites {
				if s.FileCount != 0 {
					t.Fatalf("omit_site_robot_counts not honoured: site %q reports %d", s.Name, s.FileCount)
				}
			}
		}
	})

	// The TREE must be identical no matter what is omitted — it is what the
	// picker renders.
	lean, err := h.GetVocabulary(ctx, &pb.GetVocabularyRequest{
		OmitSources: true, OmitTagFacets: true, OmitSiteRobotCounts: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(lean.Customers) != len(full.Customers) {
		t.Fatalf("lean dropped customers: %d vs %d", len(lean.Customers), len(full.Customers))
	}
	for i := range full.Customers {
		if lean.Customers[i].Id != full.Customers[i].Id || len(lean.Customers[i].Sites) != len(full.Customers[i].Sites) {
			t.Fatalf("lean tree differs from full at customer %d", i)
		}
	}
	if string(lean.CatalogGeneration) != string(full.CatalogGeneration) {
		t.Fatal("generation token must not depend on scoping")
	}
}
