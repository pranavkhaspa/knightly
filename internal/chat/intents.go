package chat

import "strings"

type Intent string

const (
	IntentGeneral               Intent = "general"
	IntentWhatChanged           Intent = "what_changed"
	IntentSearchWeb             Intent = "search_web"
	IntentSearchSources         Intent = "search_sources"
	IntentReadMail              Intent = "read_mail"
	IntentCalendarToday         Intent = "calendar_today"
	IntentCreateReminder        Intent = "create_reminder"
	IntentMorningReport         Intent = "morning_report"
	IntentWhyDoesThisMatter     Intent = "why_does_this_matter"
	IntentExplainRecommendation Intent = "explain_recommendation"
	IntentShowTaskStatus        Intent = "show_task_status"
	IntentBuildDemo             Intent = "build_demo"
	IntentReviewDiff            Intent = "review_diff"
	IntentProposeGitHubAction   Intent = "propose_github_action"
)

type RoutedIntent struct {
	Intent Intent `json:"intent"`
	Query  string `json:"query,omitempty"`
}

// RouteIntent intentionally uses deterministic, bounded phrase matching. It
// is safe to use before an optional model provider is configured and keeps
// provider policy out of the transport handler.
func RouteIntent(content string) RoutedIntent {
	trimmed := strings.TrimSpace(content)
	lower := strings.ToLower(trimmed)
	for _, prefix := range []string{"/sources ", "search sources ", "source search "} {
		if strings.HasPrefix(lower, prefix) {
			return RoutedIntent{Intent: IntentSearchSources, Query: strings.TrimSpace(trimmed[len(prefix):])}
		}
	}
	for _, prefix := range []string{"/search ", "search ", "browse ", "web search "} {
		if strings.HasPrefix(lower, prefix) {
			query := strings.TrimSpace(trimmed[len(prefix):])
			return RoutedIntent{Intent: IntentSearchWeb, Query: query}
		}
	}
	switch {
	case strings.HasPrefix(lower, "what changed"), strings.HasPrefix(lower, "what's new"), strings.HasPrefix(lower, "whats new"):
		return RoutedIntent{Intent: IntentWhatChanged, Query: trimmed}
	case strings.Contains(lower, "read my mail"), strings.Contains(lower, "check my email"), strings.HasPrefix(lower, "mail"):
		return RoutedIntent{Intent: IntentReadMail, Query: trimmed}
	case strings.Contains(lower, "calendar today"), strings.Contains(lower, "today's calendar"), strings.HasPrefix(lower, "schedule today"):
		return RoutedIntent{Intent: IntentCalendarToday, Query: trimmed}
	case strings.HasPrefix(lower, "remind me"), strings.HasPrefix(lower, "create reminder"), strings.HasPrefix(lower, "/remind "):
		return RoutedIntent{Intent: IntentCreateReminder, Query: trimmed}
	case strings.Contains(lower, "morning report"), strings.HasPrefix(lower, "briefing"), strings.HasPrefix(lower, "morning"):
		return RoutedIntent{Intent: IntentMorningReport, Query: trimmed}
	case strings.Contains(lower, "why does this matter"), strings.Contains(lower, "why is this relevant"):
		return RoutedIntent{Intent: IntentWhyDoesThisMatter, Query: trimmed}
	case strings.HasPrefix(lower, "explain recommendation"), strings.HasPrefix(lower, "why recommend"):
		return RoutedIntent{Intent: IntentExplainRecommendation, Query: trimmed}
	case strings.Contains(lower, "task status"), strings.Contains(lower, "show my tasks"), strings.HasPrefix(lower, "tasks"):
		return RoutedIntent{Intent: IntentShowTaskStatus, Query: trimmed}
	case strings.HasPrefix(lower, "build "), strings.HasPrefix(lower, "implement "), strings.Contains(lower, "build a demo"), strings.HasPrefix(lower, "/build "):
		return RoutedIntent{Intent: IntentBuildDemo, Query: trimmed}
	case strings.Contains(lower, "review diff"), strings.HasPrefix(lower, "review my changes"), strings.HasPrefix(lower, "diff"):
		return RoutedIntent{Intent: IntentReviewDiff, Query: trimmed}
	case strings.Contains(lower, "github issue"), strings.Contains(lower, "github action"), strings.Contains(lower, "create a pull request"):
		return RoutedIntent{Intent: IntentProposeGitHubAction, Query: trimmed}
	default:
		return RoutedIntent{Intent: IntentGeneral, Query: trimmed}
	}
}
