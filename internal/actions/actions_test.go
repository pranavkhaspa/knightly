package actions

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeRepository struct {
	actions     map[string]Action
	createCalls int
	saveCalls   int
	claimCalls  int
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{actions: make(map[string]Action)}
}

func (f *fakeRepository) Get(_ context.Context, _ string, actionID string) (Action, error) {
	action, ok := f.actions[actionID]
	if !ok {
		return Action{}, ErrActionNotFound
	}
	return cloneAction(action), nil
}

func (f *fakeRepository) FindByIdempotencyKey(_ context.Context, tenantID, userID, key string) (Action, error) {
	for _, action := range f.actions {
		if action.TenantID == tenantID && action.UserID == userID && action.IdempotencyKey == key {
			return cloneAction(action), nil
		}
	}
	return Action{}, ErrActionNotFound
}

func (f *fakeRepository) Create(_ context.Context, action Action) (Action, error) {
	f.createCalls++
	for _, existing := range f.actions {
		if existing.TenantID == action.TenantID && existing.UserID == action.UserID && existing.IdempotencyKey == action.IdempotencyKey {
			return cloneAction(existing), nil
		}
	}
	f.actions[action.ID] = cloneAction(action)
	return cloneAction(action), nil
}

func (f *fakeRepository) Approve(_ context.Context, _ string, actionID string, approval Approval) (Action, error) {
	action, ok := f.actions[actionID]
	if !ok {
		return Action{}, ErrActionNotFound
	}
	if action.Status != Proposed && action.Status != Failed {
		return action, ErrActionNotClaimable
	}
	action.Status = Approved
	action.ApprovedBy = approval.ApprovedBy
	action.ApprovalID = approval.ID
	action.ApprovalNote = approval.Note
	action.ApprovedAt = approval.At
	action.Error = ""
	f.actions[actionID] = cloneAction(action)
	return action, nil
}

func (f *fakeRepository) Save(_ context.Context, action Action) error {
	f.saveCalls++
	f.actions[action.ID] = cloneAction(action)
	return nil
}

func (f *fakeRepository) ClaimForExecution(_ context.Context, _ string, actionID string) (Action, error) {
	f.claimCalls++
	action, ok := f.actions[actionID]
	if !ok {
		return Action{}, ErrActionNotFound
	}
	if action.Status != Approved {
		return action, ErrActionNotClaimable
	}
	action.Status = Executing
	f.actions[actionID] = cloneAction(action)
	return action, nil
}

type fakeExecutor struct {
	calls int
}

func (f *fakeExecutor) Execute(_ context.Context, _ Action) (ExecutionResult, error) {
	f.calls++
	return ExecutionResult{Reference: "issue-42", Output: "created"}, nil
}

func baseAction() Action {
	return Action{
		ID:             "action_issue_1",
		TenantID:       "tenant_demo",
		UserID:         "user_demo",
		ProjectID:      "project_nightshift",
		Kind:           GitHubIssue,
		Target:         "public/sandbox",
		Payload:        map[string]string{"title": "Investigate graph memory"},
		IdempotencyKey: "recommendation-1-issue",
	}
}

func TestProposeIsIdempotentAndConflictsOnChangedRequest(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository, true)

	first, err := service.Propose(context.Background(), baseAction())
	if err != nil {
		t.Fatalf("first proposal: %v", err)
	}
	second, err := service.Propose(context.Background(), baseAction())
	if err != nil {
		t.Fatalf("duplicate proposal: %v", err)
	}
	if second.ID != first.ID || repository.createCalls != 2 {
		t.Fatalf("duplicate proposal returned %#v with %d create calls", second, repository.createCalls)
	}

	conflicting := baseAction()
	conflicting.ID = "action_issue_2"
	conflicting.Target = "other/repository"
	_, err = service.Propose(context.Background(), conflicting)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting proposal error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestExecuteRequiresApprovalAndDoesNotRepeatCompletedWrite(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository, true)
	service.now = func() time.Time { return time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC) }
	if _, err := service.Propose(context.Background(), baseAction()); err != nil {
		t.Fatalf("propose: %v", err)
	}
	executor := &fakeExecutor{}
	_, err := service.Execute(context.Background(), "user_demo", "action_issue_1", executor)
	if !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("unapproved execute error = %v, want ErrApprovalRequired", err)
	}
	if executor.calls != 0 {
		t.Fatalf("executor called %d times before approval", executor.calls)
	}

	approved, err := service.Approve(context.Background(), "user_demo", "action_issue_1", Approval{ID: "approval_1", ApprovedBy: "user_demo"})
	if err != nil || approved.Status != Approved {
		t.Fatalf("approve = %#v, err %v", approved, err)
	}
	repeatedApproval, err := service.Approve(context.Background(), "user_demo", "action_issue_1", Approval{ID: "approval_2", ApprovedBy: "user_demo"})
	if err != nil || repeatedApproval.ApprovalID != "approval_1" {
		t.Fatalf("repeated approval = %#v, err %v", repeatedApproval, err)
	}

	completed, err := service.Execute(context.Background(), "user_demo", "action_issue_1", executor)
	if err != nil {
		t.Fatalf("execute approved action: %v", err)
	}
	if completed.Status != Completed || completed.ResultReference != "issue-42" {
		t.Fatalf("completed action = %#v", completed)
	}
	if _, err := service.Execute(context.Background(), "user_demo", "action_issue_1", executor); err != nil {
		t.Fatalf("repeat completed execute: %v", err)
	}
	if executor.calls != 1 || repository.claimCalls != 1 {
		t.Fatalf("executor calls = %d, claim calls = %d; want 1 and 1", executor.calls, repository.claimCalls)
	}
}

func TestExecuteHonorsGlobalDisable(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository, false)
	_, err := service.Execute(context.Background(), "user_demo", "missing", &fakeExecutor{})
	if !errors.Is(err, ErrActionsDisabled) {
		t.Fatalf("disabled execute error = %v, want ErrActionsDisabled", err)
	}
}

func TestFailedActionPersistsForExplicitlyApprovedRecovery(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository, true)
	service.now = func() time.Time { return time.Date(2026, 10, 16, 13, 0, 0, 0, time.UTC) }
	if _, err := service.Propose(context.Background(), baseAction()); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if _, err := service.Approve(context.Background(), "user_demo", "action_issue_1", Approval{ID: "approval_1", ApprovedBy: "user_demo"}); err != nil {
		t.Fatalf("approve: %v", err)
	}

	providerFailure := errors.New("provider connection reset after accepting request")
	_, err := service.Execute(context.Background(), "user_demo", "action_issue_1", failingExecutor{err: providerFailure})
	if !errors.Is(err, providerFailure) {
		t.Fatalf("failed execution error = %v, want provider failure", err)
	}
	failed, err := repository.Get(context.Background(), "user_demo", "action_issue_1")
	if err != nil {
		t.Fatalf("read failed action: %v", err)
	}
	if failed.Status != Failed || failed.Error != providerFailure.Error() {
		t.Fatalf("failed action = %#v, want persisted failure", failed)
	}

	if _, err := service.Execute(context.Background(), "user_demo", "action_issue_1", &fakeExecutor{}); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("retry without approval error = %v, want ErrApprovalRequired", err)
	}
	approved, err := service.Approve(context.Background(), "user_demo", "action_issue_1", Approval{ID: "approval_2", ApprovedBy: "user_demo", Note: "Retry after provider recovery"})
	if err != nil || approved.Status != Approved || approved.ApprovalID != "approval_2" {
		t.Fatalf("reapproval = %#v, err %v", approved, err)
	}

	recovered, err := service.Execute(context.Background(), "user_demo", "action_issue_1", &fakeExecutor{})
	if err != nil {
		t.Fatalf("recovered execution: %v", err)
	}
	if recovered.Status != Completed || recovered.ResultReference != "issue-42" || recovered.Error != "" {
		t.Fatalf("recovered action = %#v", recovered)
	}
}

type failingExecutor struct {
	err error
}

func (e failingExecutor) Execute(context.Context, Action) (ExecutionResult, error) {
	return ExecutionResult{}, e.err
}
