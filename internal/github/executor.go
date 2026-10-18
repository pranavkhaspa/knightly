package github

import (
	"context"
	"fmt"
	"strings"

	"github.com/pranavkhaspa/knightly/internal/actions"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

// Executor maps an approved action to a GitHub write. It refuses force-push and
// merge before any request is made, and it only writes to allowlisted
// repositories. It implements the actions.Executor contract so the write runs
// only after the action layer has recorded explicit approval.
type Executor struct {
	writer    Writer
	allowlist *Allowlist
	scope     identity.Scope
}

var _ actions.Executor = (*Executor)(nil)

func NewExecutor(writer Writer, allowlist *Allowlist, scope identity.Scope) (*Executor, error) {
	if writer == nil {
		return nil, fmt.Errorf("%w: writer is required", ErrInvalidObject)
	}
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	return &Executor{writer: writer, allowlist: allowlist, scope: scope}, nil
}

// SupportedKind reports whether the executor handles an action kind.
func (e *Executor) SupportedKind(kind actions.Kind) bool {
	switch kind {
	case actions.GitHubIssue, actions.GitHubBranch, actions.GitHubCommit, actions.GitHubPush, actions.GitHubPullRequest:
		return true
	default:
		return false
	}
}

func (e *Executor) Execute(ctx context.Context, action actions.Action) (actions.ExecutionResult, error) {
	if e == nil || e.writer == nil {
		return actions.ExecutionResult{}, fmt.Errorf("%w: executor is not configured", ErrInvalidObject)
	}
	if err := ctx.Err(); err != nil {
		return actions.ExecutionResult{}, err
	}
	request := WriteRequest{
		Repository: action.Target,
		Title:      action.Payload["title"],
		Body:       action.Payload["body"],
		Branch:     action.Payload["branch"],
		Base:       action.Payload["base"],
		Path:       action.Payload["path"],
		Content:    action.Payload["content"],
		Message:    action.Payload["message"],
		SHA:        action.Payload["sha"],
		Force:      strings.EqualFold(action.Payload["force"], "true"),
		Merge:      strings.EqualFold(action.Payload["merge"], "true"),
	}
	if request.Force {
		return actions.ExecutionResult{}, fmt.Errorf("%w: force push is not permitted", ErrBlockedOperation)
	}
	if request.Merge {
		return actions.ExecutionResult{}, fmt.Errorf("%w: auto-merge is not permitted", ErrBlockedOperation)
	}
	if e.allowlist != nil && !e.allowlist.Allows(request.Repository) {
		return actions.ExecutionResult{}, fmt.Errorf("%w: %s", ErrRepositoryNotAllowed, NormalizeRepository(request.Repository))
	}
	var (
		result WriteResult
		err    error
	)
	switch action.Kind {
	case actions.GitHubIssue:
		result, err = e.writer.CreateIssue(ctx, e.scope, request)
	case actions.GitHubBranch:
		result, err = e.writer.CreateBranch(ctx, e.scope, request)
	case actions.GitHubCommit:
		result, err = e.writer.CreateCommit(ctx, e.scope, request)
	case actions.GitHubPush:
		result, err = e.writer.PushBranch(ctx, e.scope, request)
	case actions.GitHubPullRequest:
		result, err = e.writer.OpenPullRequest(ctx, e.scope, request)
	default:
		return actions.ExecutionResult{}, fmt.Errorf("%w: unsupported action kind %q", ErrBlockedOperation, action.Kind)
	}
	if err != nil {
		return actions.ExecutionResult{}, err
	}
	return actions.ExecutionResult{Reference: result.Reference, Output: result.Summary}, nil
}
