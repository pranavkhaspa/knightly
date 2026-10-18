package falkor

import (
	"strings"
	"testing"
	"time"
)

func TestOptionalMillisUsesMillisecondsAndPreservesAbsentValues(t *testing.T) {
	if got := optionalMillis(time.Time{}); got != nil {
		t.Fatalf("optionalMillis(zero) = %#v, want nil", got)
	}
	instant := time.Unix(1, 234000000).UTC()
	if got := optionalMillis(instant); got != int64(1234) {
		t.Fatalf("optionalMillis(%s) = %#v, want 1234", instant, got)
	}
	if got := timeValue(nil); !got.IsZero() {
		t.Fatalf("timeValue(nil) = %s, want zero time", got)
	}
}

func TestGraphWritesUseScopedMillisecondTimestampParameters(t *testing.T) {
	queries := map[string]string{
		"item":           ItemUpsertQuery,
		"source":         SourceUpsertQuery,
		"recommendation": RecommendationUpsertQuery,
		"session":        sessionQuery,
	}
	for name, query := range queries {
		if strings.Contains(query, "timestamp()") {
			t.Errorf("%s query uses implicit timestamp(): %s", name, query)
		}
	}
	for _, fragment := range []string{"tenant_id", "user_id"} {
		if !strings.Contains(ItemUpsertQuery, fragment) || !strings.Contains(RecommendationUpsertQuery, fragment) {
			t.Errorf("graph writes missing scope fragment %q", fragment)
		}
	}
	if !strings.Contains(ItemUpsertQuery, "$published_at") || !strings.Contains(ItemUpsertQuery, "$fetched_at") {
		t.Fatal("item query does not expose optional timestamp parameters")
	}
}

func TestPlannerValidationQueryRequiresMatchingScopedRecommendationAndProject(t *testing.T) {
	for _, fragment := range []string{
		"Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id}",
		"Recommendation {id: $recommendation_id, tenant_id: $tenant_id, user_id: $user_id, project_id: $project_id}",
		"HAS_REQUIREMENT",
	} {
		if !strings.Contains(plannerValidationQuery, fragment) {
			t.Errorf("planner validation query missing %q", fragment)
		}
	}
}
