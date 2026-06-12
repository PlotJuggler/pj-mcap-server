package ws

import (
	"testing"

	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/session"
)

// A latched/transient-local seed topic is, by definition, ABSENT from the time
// window — so it never appears in plan.Chunks. The producer still emits its
// pre-window message, but the per-message emit drops any topic missing from the
// session binding (p.TopicID). If planTopicSet omits the seed topics, the
// topic_id_map never carries them and every seeded message is silently dropped
// (estimate says ~1, received 0). The binding must be window ∪ seed topics.
func TestPlanTopicSet_IncludesSeedTopics(t *testing.T) {
	plan := session.Plan{
		Chunks: []format.ChunkRef{
			{ChannelTopics: map[string]struct{}{"/odom": {}, "/scan": {}}},
		},
		SeedTopics: map[string]struct{}{"/map_amcl": {}, "/costmap": {}},
	}
	got := planTopicSet(plan)
	for _, tn := range []string{"/odom", "/scan", "/map_amcl", "/costmap"} {
		if _, ok := got[tn]; !ok {
			t.Errorf("planTopicSet missing %q: a seed topic absent from the binding makes the producer drop its seeded message", tn)
		}
	}
	if len(got) != 4 {
		t.Errorf("planTopicSet = %d topics, want 4 (2 window + 2 seed)", len(got))
	}
}
