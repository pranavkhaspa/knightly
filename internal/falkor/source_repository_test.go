package falkor

import (
	"strings"
	"testing"
	"time"
)

type sourceRecord map[string]interface{}

func (r sourceRecord) Get(key string) (interface{}, bool) {
	value, ok := r[key]
	return value, ok
}

func TestSourceRecordMappingKeepsDurableCollectionConfiguration(t *testing.T) {
	source, err := sourceFromRecord(sourceRecord{
		"id":                    "source_feed",
		"tenant_id":             "tenant-a",
		"user_id":               "user-a",
		"name":                  "Example feed",
		"kind":                  "rss",
		"url":                   "https://example.com/feed",
		"enabled":               true,
		"trust_score":           float64(0.8),
		"poll_interval_seconds": int64(900),
		"max_items":             int64(25),
		"technology_hints":      []interface{}{"Go", "FalkorDB"},
		"interest_hints":        []interface{}{"Graph agents"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if source.ID != "source_feed" || source.TenantID != "tenant-a" || source.UserID != "user-a" || source.Kind != "rss" || source.PollInterval != 15*time.Minute || source.MaxItems != 25 {
		t.Fatalf("source = %+v", source)
	}
	if len(source.TechnologyHints) != 2 || source.TechnologyHints[1] != "FalkorDB" || len(source.InterestHints) != 1 {
		t.Fatalf("source hints = %+v", source)
	}
}

func TestSourceRepositoryQueriesKeepSourceStateInFalkorDB(t *testing.T) {
	for name, query := range map[string]string{
		"list":      SourceListQuery,
		"cursor":    SourceCursorQuery + SourceCursorSaveQuery,
		"run begin": SourceRunBeginQuery,
		"run end":   SourceRunCompleteQuery,
	} {
		if !strings.Contains(query, "Source") {
			t.Errorf("%s query does not mention Source state", name)
		}
	}
	for _, fragment := range []string{"HAS_CURSOR", "HAS_RUN", "last_success_at", "poll_interval_seconds", "max_items", "tenant_id", "user_id"} {
		combined := SourceListQuery + SourceCursorQuery + SourceCursorSaveQuery + SourceRunBeginQuery + SourceRunCompleteQuery
		if !strings.Contains(combined, fragment) {
			t.Errorf("source repository queries missing %q", fragment)
		}
	}
}
