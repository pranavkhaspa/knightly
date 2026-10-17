package github

import (
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/untrusted"
)

// NormalizeRepository bounds repository metadata.
func NormalizeRepositoryMetadata(value Repository) Repository {
	value.FullName = NormalizeRepository(value.FullName)
	value.Description = boundRun(value.Description, MaxSummary)
	value.DefaultBranch = bound(strings.TrimSpace(value.DefaultBranch), MaxRefLength)
	value.HTMLURL = bound(strings.TrimSpace(value.HTMLURL), MaxURLLength)
	return value
}

// NormalizeIssue bounds and marks an issue as untrusted.
func NormalizeIssue(value Issue) Issue {
	value.Repository = NormalizeRepository(value.Repository)
	value.Title = boundRun(value.Title, MaxTitle)
	value.Body = excerpt(value.Body)
	value.State = bound(strings.ToLower(value.State), 32)
	value.HTMLURL = bound(value.HTMLURL, MaxURLLength)
	value.Author.Login = bound(value.Author.Login, MaxLoginLength)
	value.Labels = normalizeLabels(value.Labels)
	value.Origin = UntrustedOrigin
	return value
}

// NormalizePullRequest bounds and marks a pull request as untrusted.
func NormalizePullRequest(value PullRequest) PullRequest {
	value.Repository = NormalizeRepository(value.Repository)
	value.Title = boundRun(value.Title, MaxTitle)
	value.Body = excerpt(value.Body)
	value.State = bound(strings.ToLower(value.State), 32)
	value.HTMLURL = bound(value.HTMLURL, MaxURLLength)
	value.Author.Login = bound(value.Author.Login, MaxLoginLength)
	value.Head = bound(value.Head, MaxRefLength)
	value.Base = bound(value.Base, MaxRefLength)
	value.Origin = UntrustedOrigin
	return value
}

// NormalizeCheckRun bounds and marks a check run as untrusted.
func NormalizeCheckRun(value CheckRun) CheckRun {
	value.Repository = NormalizeRepository(value.Repository)
	value.Name = boundRun(value.Name, MaxTitle)
	value.Status = bound(strings.ToLower(value.Status), 32)
	value.Conclusion = bound(strings.ToLower(value.Conclusion), 32)
	value.HeadSHA = bound(value.HeadSHA, MaxSHALength)
	value.HTMLURL = bound(value.HTMLURL, MaxURLLength)
	value.Origin = UntrustedOrigin
	return value
}

// NormalizeRelease bounds and marks a release as untrusted.
func NormalizeRelease(value Release) Release {
	value.Repository = NormalizeRepository(value.Repository)
	value.TagName = bound(value.TagName, MaxRefLength)
	value.Name = boundRun(value.Name, MaxTitle)
	value.HTMLURL = bound(value.HTMLURL, MaxURLLength)
	value.Origin = UntrustedOrigin
	return value
}

func normalizeLabels(labels []Label) []Label {
	if len(labels) == 0 {
		return nil
	}
	if len(labels) > MaxLabels {
		labels = labels[:MaxLabels]
	}
	result := make([]Label, 0, len(labels))
	for _, label := range labels {
		name := boundRun(label.Name, MaxLabelLength)
		if name == "" {
			continue
		}
		result = append(result, Label{Name: name})
	}
	return result
}

func excerpt(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	if content, err := untrusted.New(untrusted.GitHub, value); err == nil {
		return content.Bounded()
	}
	return bound(value, MaxBody)
}

// parseTime parses a GitHub RFC3339 timestamp, returning the zero time on error.
func parseTime(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func requireRepository(repository string) error {
	if NormalizeRepository(repository) == "" {
		return fmt.Errorf("%w: repository is required", ErrInvalidObject)
	}
	return nil
}
