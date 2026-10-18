package github

import (
	"context"
	"errors"
	"testing"

	"github.com/pranavkhaspa/knightly/internal/actions"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

var testScope = identity.Scope{TenantID: "tenant-a", UserID: "user-a"}

func testAllowlist(t *testing.T, values ...string) *Allowlist {
	t.Helper()
	allowlist, err := NewAllowlist(values)
	if err != nil {
		t.Fatalf("NewAllowlist() error = %v", err)
	}
	return allowlist
}

func TestAllowlistNormalizesAndRejectsEmpty(t *testing.T) {
	allowlist := testAllowlist(t, "Owner/Repo")
	if !allowlist.Allows("owner/repo") || !allowlist.Allows(" Owner/Repo ") {
		t.Fatal("allowlist should match case-insensitively and trimmed")
	}
	if allowlist.Allows("owner/other") {
		t.Fatal("allowlist matched an unlisted repository")
	}
	if _, err := NewAllowlist([]string{""}); err == nil {
		t.Fatal("NewAllowlist accepted an empty repository")
	}
}

func TestNormalizeListQueryBoundsAndValidates(t *testing.T) {
	query, err := NormalizeListQuery(ListQuery{Repository: "Owner/Repo"})
	if err != nil {
		t.Fatalf("NormalizeListQuery() error = %v", err)
	}
	if query.Repository != "owner/repo" || query.Limit != DefaultListLimit {
		t.Fatalf("query = %+v", query)
	}
	if _, err := NormalizeListQuery(ListQuery{Repository: "owner"}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("expected ErrInvalidQuery, got %v", err)
	}
	if _, err := NormalizeListQuery(ListQuery{Repository: "owner/repo", Limit: MaxListLimit + 1}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("expected ErrInvalidQuery for large limit, got %v", err)
	}
	if _, err := NormalizeListQuery(ListQuery{Repository: "owner/repo", State: "merged"}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("expected ErrInvalidQuery for unknown state, got %v", err)
	}
}

func TestNormalizeIssueMarksUntrustedOrigin(t *testing.T) {
	value := NormalizeIssue(Issue{Repository: "Owner/Repo", Title: "  Bug  ", Body: "line one\nline two", State: "OPEN"})
	if value.Origin != UntrustedOrigin {
		t.Fatalf("origin = %q, want %q", value.Origin, UntrustedOrigin)
	}
	if value.Repository != "owner/repo" || value.State != "open" {
		t.Fatalf("value = %+v", value)
	}
	if value.Title != "Bug" {
		t.Fatalf("title = %q", value.Title)
	}
}

func TestFakeReaderEnforcesAllowlistAndScope(t *testing.T) {
	reader := NewFakeReader(testAllowlist(t, "owner/repo"))
	reader.SetIssues("owner/repo", NormalizeIssue(Issue{ID: 1, Number: 1, Repository: "owner/repo", Title: "one", State: "open"}))

	values, err := reader.ListIssues(context.Background(), testScope, ListQuery{Repository: "owner/repo"})
	if err != nil {
		t.Fatalf("ListIssues() error = %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("issues = %d, want 1", len(values))
	}
	if _, err := reader.ListIssues(context.Background(), testScope, ListQuery{Repository: "owner/other"}); !errors.Is(err, ErrRepositoryNotAllowed) {
		t.Fatalf("expected ErrRepositoryNotAllowed, got %v", err)
	}
	if _, err := reader.ListIssues(context.Background(), identity.Scope{}, ListQuery{Repository: "owner/repo"}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("expected ErrScopeRequired, got %v", err)
	}
}

func TestExecutorRefusesForcePushAndMerge(t *testing.T) {
	writer := &recordingWriter{}
	executor, err := NewExecutor(writer, testAllowlist(t, "owner/repo"), testScope)
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}
	if !executor.SupportedKind(actions.GitHubPush) || executor.SupportedKind(actions.LocalPlan) {
		t.Fatal("SupportedKind() returned unexpected values")
	}

	_, err = executor.Execute(context.Background(), actions.Action{
		Kind: actions.GitHubPush, Target: "owner/repo", Payload: map[string]string{"branch": "main", "sha": "abc", "force": "true"},
	})
	if !errors.Is(err, ErrBlockedOperation) {
		t.Fatalf("expected ErrBlockedOperation for force push, got %v", err)
	}

	_, err = executor.Execute(context.Background(), actions.Action{
		Kind: actions.GitHubPullRequest, Target: "owner/repo", Payload: map[string]string{"branch": "feature", "merge": "true"},
	})
	if !errors.Is(err, ErrBlockedOperation) {
		t.Fatalf("expected ErrBlockedOperation for merge, got %v", err)
	}
	if writer.calls != 0 {
		t.Fatalf("writer was called %d times for blocked operations", writer.calls)
	}

	_, err = executor.Execute(context.Background(), actions.Action{
		Kind: actions.GitHubIssue, Target: "owner/repo", Payload: map[string]string{"title": "Fix", "body": "details"},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if writer.calls != 1 {
		t.Fatalf("writer calls = %d, want 1", writer.calls)
	}
}

type recordingWriter struct {
	calls int
}

func (w *recordingWriter) CreateIssue(context.Context, identity.Scope, WriteRequest) (WriteResult, error) {
	w.calls++
	return WriteResult{Reference: "owner/repo#1", Summary: "issue created"}, nil
}

func (w *recordingWriter) CreateBranch(context.Context, identity.Scope, WriteRequest) (WriteResult, error) {
	w.calls++
	return WriteResult{}, nil
}

func (w *recordingWriter) CreateCommit(context.Context, identity.Scope, WriteRequest) (WriteResult, error) {
	w.calls++
	return WriteResult{}, nil
}

func (w *recordingWriter) PushBranch(context.Context, identity.Scope, WriteRequest) (WriteResult, error) {
	w.calls++
	return WriteResult{}, nil
}

func (w *recordingWriter) OpenPullRequest(context.Context, identity.Scope, WriteRequest) (WriteResult, error) {
	w.calls++
	return WriteResult{}, nil
}
