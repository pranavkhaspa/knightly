package chat

import "testing"

func TestRouteIntentCoversDeterministicDevOneIntents(t *testing.T) {
	tests := []struct {
		input string
		want  Intent
	}{
		{"what changed while I was away", IntentWhatChanged},
		{"search web for FalkorDB releases", IntentSearchWeb},
		{"search sources for graph context", IntentSearchSources},
		{"read my mail", IntentReadMail},
		{"calendar today", IntentCalendarToday},
		{"remind me tomorrow", IntentCreateReminder},
		{"morning report", IntentMorningReport},
		{"why does this matter", IntentWhyDoesThisMatter},
		{"show my tasks", IntentShowTaskStatus},
		{"build a demo", IntentBuildDemo},
		{"Fix the authentication bug", IntentBuildDemo},
		{"please implement password reset", IntentBuildDemo},
		{"refactor the session handler", IntentBuildDemo},
		{"review diff", IntentReviewDiff},
		{"create a GitHub issue", IntentProposeGitHubAction},
	}
	for _, test := range tests {
		if got := RouteIntent(test.input).Intent; got != test.want {
			t.Errorf("RouteIntent(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}
