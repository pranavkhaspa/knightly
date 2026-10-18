// Package github contains provider-neutral, read-only GitHub types plus an
// approval-gated write executor. Reads are always scoped to an allowlisted
// repository; no write is ever automatic, and force-push and merge are refused.
package github

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/untrusted"
)

const (
	DefaultListLimit  = 30
	MaxListLimit      = 100
	MaxRepositoryName = 140
	MaxTitle          = 512
	MaxBody           = 4096
	MaxSummary        = 2048
	MaxRefLength      = 255
	MaxSHALength      = 64
	MaxURLLength      = 2048
	MaxLoginLength    = 128
	MaxLabelLength    = 128
	MaxLabels         = 20
	MaxIDLength       = 128
	MaxAllowlist      = 200
)

// UntrustedOrigin marks GitHub-derived text as untrusted external input.
const UntrustedOrigin = string(untrusted.GitHub)

var (
	ErrInvalidQuery         = errors.New("invalid GitHub query")
	ErrInvalidObject        = errors.New("invalid GitHub object")
	ErrRepositoryNotAllowed = errors.New("GitHub repository is not allowlisted")
	ErrScopeRequired        = errors.New("trusted GitHub scope is required")
	ErrBlockedOperation     = errors.New("GitHub operation is not permitted")
	ErrProviderRequest      = errors.New("GitHub API request failed")
	ErrProviderUnauthorized = errors.New("GitHub API rejected the token")
)

// Repository is bounded repository metadata.
type Repository struct {
	ID            int64     `json:"id"`
	FullName      string    `json:"full_name"`
	Description   string    `json:"description,omitempty"`
	DefaultBranch string    `json:"default_branch,omitempty"`
	HTMLURL       string    `json:"html_url,omitempty"`
	Private       bool      `json:"private"`
	Archived      bool      `json:"archived"`
	OpenIssues    int       `json:"open_issues"`
	Stars         int       `json:"stars"`
	PushedAt      time.Time `json:"pushed_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

type Actor struct {
	Login string `json:"login"`
}

type Label struct {
	Name string `json:"name"`
}

// Issue is a bounded normalized issue (pull requests are reported separately).
type Issue struct {
	ID         int64     `json:"id"`
	Number     int       `json:"number"`
	Repository string    `json:"repository"`
	Title      string    `json:"title"`
	Body       string    `json:"body_excerpt,omitempty"`
	State      string    `json:"state"`
	HTMLURL    string    `json:"html_url,omitempty"`
	Author     Actor     `json:"author"`
	Labels     []Label   `json:"labels,omitempty"`
	Comments   int       `json:"comments"`
	CreatedAt  time.Time `json:"created_at,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
	Origin     string    `json:"origin"`
}

// PullRequest is a bounded normalized pull request.
type PullRequest struct {
	ID         int64     `json:"id"`
	Number     int       `json:"number"`
	Repository string    `json:"repository"`
	Title      string    `json:"title"`
	Body       string    `json:"body_excerpt,omitempty"`
	State      string    `json:"state"`
	HTMLURL    string    `json:"html_url,omitempty"`
	Author     Actor     `json:"author"`
	Head       string    `json:"head"`
	Base       string    `json:"base"`
	Draft      bool      `json:"draft"`
	Merged     bool      `json:"merged"`
	CreatedAt  time.Time `json:"created_at,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
	Origin     string    `json:"origin"`
}

// CheckRun is a bounded normalized check or status result.
type CheckRun struct {
	ID          int64     `json:"id"`
	Repository  string    `json:"repository"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion,omitempty"`
	HeadSHA     string    `json:"head_sha,omitempty"`
	HTMLURL     string    `json:"html_url,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	Origin      string    `json:"origin"`
}

// Release is a bounded normalized release.
type Release struct {
	ID          int64     `json:"id"`
	Repository  string    `json:"repository"`
	TagName     string    `json:"tag_name"`
	Name        string    `json:"name,omitempty"`
	HTMLURL     string    `json:"html_url,omitempty"`
	Draft       bool      `json:"draft"`
	Prerelease  bool      `json:"prerelease"`
	PublishedAt time.Time `json:"published_at,omitempty"`
	Origin      string    `json:"origin"`
}

// ListQuery is a bounded repository-scoped read request.
type ListQuery struct {
	Repository string
	State      string
	HeadSHA    string
	Limit      int
	Page       int
}

// Reader is the read-only GitHub boundary. Repositories must be allowlisted.
type Reader interface {
	GetRepository(context.Context, identity.Scope, string) (Repository, error)
	ListIssues(context.Context, identity.Scope, ListQuery) ([]Issue, error)
	ListPullRequests(context.Context, identity.Scope, ListQuery) ([]PullRequest, error)
	ListChecks(context.Context, identity.Scope, ListQuery) ([]CheckRun, error)
	ListReleases(context.Context, identity.Scope, ListQuery) ([]Release, error)
}

// Allowlist restricts reads and writes to a fixed set of repositories.
type Allowlist struct {
	repos map[string]struct{}
}

func NewAllowlist(values []string) (*Allowlist, error) {
	if len(values) > MaxAllowlist {
		return nil, fmt.Errorf("%w: allowlist is too large", ErrInvalidQuery)
	}
	repos := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := NormalizeRepository(value)
		if normalized == "" {
			return nil, fmt.Errorf("%w: repository name is required", ErrInvalidQuery)
		}
		if len(normalized) > MaxRepositoryName {
			return nil, fmt.Errorf("%w: repository name is too long", ErrInvalidQuery)
		}
		repos[normalized] = struct{}{}
	}
	return &Allowlist{repos: repos}, nil
}

func (a *Allowlist) Allows(fullName string) bool {
	if a == nil || len(a.repos) == 0 {
		return false
	}
	_, ok := a.repos[NormalizeRepository(fullName)]
	return ok
}

func (a *Allowlist) Empty() bool { return a == nil || len(a.repos) == 0 }

func (a *Allowlist) Repositories() []string {
	if a == nil {
		return nil
	}
	values := make([]string, 0, len(a.repos))
	for value := range a.repos {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

// NormalizeRepository lowercases and trims an "owner/name" reference.
func NormalizeRepository(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// NormalizeListQuery validates and bounds a repository-scoped read request.
func NormalizeListQuery(query ListQuery) (ListQuery, error) {
	query.Repository = NormalizeRepository(query.Repository)
	if query.Repository == "" || len(query.Repository) > MaxRepositoryName || !strings.Contains(query.Repository, "/") {
		return ListQuery{}, fmt.Errorf("%w: repository must be owner/name", ErrInvalidQuery)
	}
	query.State = strings.ToLower(strings.TrimSpace(query.State))
	switch query.State {
	case "", "open", "closed", "all":
	default:
		return ListQuery{}, fmt.Errorf("%w: unknown state %q", ErrInvalidQuery, query.State)
	}
	query.HeadSHA = bound(strings.TrimSpace(query.HeadSHA), MaxSHALength)
	if query.Limit == 0 {
		query.Limit = DefaultListLimit
	}
	if query.Limit < 1 || query.Limit > MaxListLimit {
		return ListQuery{}, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxListLimit)
	}
	if query.Page < 0 || query.Page > 1000 {
		return ListQuery{}, fmt.Errorf("%w: page is out of range", ErrInvalidQuery)
	}
	return query, nil
}

func boundRun(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return string(runes)
}

func bound(value string, limit int) string {
	runes := []rune(value)
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return string(runes)
}
