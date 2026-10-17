package github

import (
	"context"
	"fmt"
	"sort"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// FakeReader is a deterministic in-memory Reader for tests and demos. It
// enforces the allowlist and performs no network access.
type FakeReader struct {
	allowlist *Allowlist
	repos     map[string]Repository
	issues    map[string][]Issue
	pulls     map[string][]PullRequest
	checks    map[string][]CheckRun
	releases  map[string][]Release
}

var _ Reader = (*FakeReader)(nil)

func NewFakeReader(allowlist *Allowlist) *FakeReader {
	return &FakeReader{
		allowlist: allowlist,
		repos:     map[string]Repository{},
		issues:    map[string][]Issue{},
		pulls:     map[string][]PullRequest{},
		checks:    map[string][]CheckRun{},
		releases:  map[string][]Release{},
	}
}

func (f *FakeReader) SetRepository(value Repository) {
	f.repos[NormalizeRepository(value.FullName)] = NormalizeRepositoryMetadata(value)
}

func (f *FakeReader) SetIssues(repository string, values ...Issue) {
	f.issues[NormalizeRepository(repository)] = values
}

func (f *FakeReader) SetPullRequests(repository string, values ...PullRequest) {
	f.pulls[NormalizeRepository(repository)] = values
}

func (f *FakeReader) SetChecks(repository string, values ...CheckRun) {
	f.checks[NormalizeRepository(repository)] = values
}

func (f *FakeReader) SetReleases(repository string, values ...Release) {
	f.releases[NormalizeRepository(repository)] = values
}

func (f *FakeReader) authorize(scope identity.Scope, repository string) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if !f.allowlist.Allows(repository) {
		return fmt.Errorf("%w: %s", ErrRepositoryNotAllowed, NormalizeRepository(repository))
	}
	return nil
}

func (f *FakeReader) GetRepository(ctx context.Context, scope identity.Scope, fullName string) (Repository, error) {
	if err := ctx.Err(); err != nil {
		return Repository{}, err
	}
	if err := f.authorize(scope, fullName); err != nil {
		return Repository{}, err
	}
	value, ok := f.repos[NormalizeRepository(fullName)]
	if !ok {
		return Repository{}, fmt.Errorf("%w: repository not found", ErrProviderRequest)
	}
	return value, nil
}

func (f *FakeReader) ListIssues(ctx context.Context, scope identity.Scope, query ListQuery) ([]Issue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query, err := NormalizeListQuery(query)
	if err != nil {
		return nil, err
	}
	if err := f.authorize(scope, query.Repository); err != nil {
		return nil, err
	}
	values := filterIssues(f.issues[query.Repository], query)
	return paginate(values, query), nil
}

func (f *FakeReader) ListPullRequests(ctx context.Context, scope identity.Scope, query ListQuery) ([]PullRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query, err := NormalizeListQuery(query)
	if err != nil {
		return nil, err
	}
	if err := f.authorize(scope, query.Repository); err != nil {
		return nil, err
	}
	var values []PullRequest
	for _, value := range f.pulls[query.Repository] {
		if query.State != "" && query.State != "all" && value.State != query.State {
			continue
		}
		values = append(values, value)
	}
	return paginate(values, query), nil
}

func (f *FakeReader) ListChecks(ctx context.Context, scope identity.Scope, query ListQuery) ([]CheckRun, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query, err := NormalizeListQuery(query)
	if err != nil {
		return nil, err
	}
	if query.HeadSHA == "" {
		return nil, fmt.Errorf("%w: head SHA is required for checks", ErrInvalidQuery)
	}
	if err := f.authorize(scope, query.Repository); err != nil {
		return nil, err
	}
	var values []CheckRun
	for _, value := range f.checks[query.Repository] {
		if value.HeadSHA == query.HeadSHA {
			values = append(values, value)
		}
	}
	return paginate(values, query), nil
}

func (f *FakeReader) ListReleases(ctx context.Context, scope identity.Scope, query ListQuery) ([]Release, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query, err := NormalizeListQuery(query)
	if err != nil {
		return nil, err
	}
	if err := f.authorize(scope, query.Repository); err != nil {
		return nil, err
	}
	return paginate(f.releases[query.Repository], query), nil
}

func filterIssues(values []Issue, query ListQuery) []Issue {
	result := make([]Issue, 0, len(values))
	for _, value := range values {
		if query.State != "" && query.State != "all" && value.State != query.State {
			continue
		}
		result = append(result, value)
	}
	return result
}

func paginate[T any](values []T, query ListQuery) []T {
	if query.State == "closed" {
		sort.SliceStable(values, func(i, j int) bool { return false })
	}
	start := query.Page * query.Limit
	if start >= len(values) {
		return []T{}
	}
	end := start + query.Limit
	if end > len(values) {
		end = len(values)
	}
	return append([]T(nil), values[start:end]...)
}
