package actions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Kind string

const (
	GitHubIssue Kind = "github_issue"
	LocalPlan   Kind = "local_plan"
)

func (k Kind) Valid() bool {
	switch k {
	case GitHubIssue, LocalPlan:
		return true
	default:
		return false
	}
}

type Status string

const (
	Proposed  Status = "proposed"
	Approved  Status = "approved"
	Executing Status = "executing"
	Completed Status = "completed"
	Failed    Status = "failed"
	Rejected  Status = "rejected"
)

func (s Status) Valid() bool {
	switch s {
	case Proposed, Approved, Executing, Completed, Failed, Rejected:
		return true
	default:
		return false
	}
}

var (
	ErrInvalidAction       = errors.New("invalid action")
	ErrActionNotFound      = errors.New("action not found")
	ErrApprovalRequired    = errors.New("explicit approval is required")
	ErrActionRejected      = errors.New("action was rejected")
	ErrActionsDisabled     = errors.New("external actions are disabled")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with an existing action")
	ErrActionInProgress    = errors.New("action is already executing")
	ErrActionNotClaimable  = errors.New("action cannot be claimed")
)

type Action struct {
	ID              string            `json:"id"`
	TenantID        string            `json:"tenant_id"`
	UserID          string            `json:"user_id"`
	ProjectID       string            `json:"project_id,omitempty"`
	TaskID          string            `json:"task_id,omitempty"`
	Kind            Kind              `json:"kind"`
	Target          string            `json:"target"`
	Payload         map[string]string `json:"payload,omitempty"`
	IdempotencyKey  string            `json:"idempotency_key"`
	RequestHash     string            `json:"request_hash"`
	Status          Status            `json:"status"`
	ApprovedBy      string            `json:"approved_by,omitempty"`
	ApprovalID      string            `json:"approval_id,omitempty"`
	ApprovalNote    string            `json:"approval_note,omitempty"`
	ApprovedAt      time.Time         `json:"approved_at,omitempty"`
	ResultReference string            `json:"result_reference,omitempty"`
	Result          string            `json:"result,omitempty"`
	Error           string            `json:"error,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

type Approval struct {
	ID         string
	ApprovedBy string
	Note       string
	At         time.Time
}

type ExecutionResult struct {
	Reference string
	Output    string
}

type Executor interface {
	Execute(context.Context, Action) (ExecutionResult, error)
}

// Repository is the durable action boundary. ClaimForExecution must perform
// an atomic approved -> executing transition in a graph implementation so two
// daemon instances cannot both perform the same external write.
type Repository interface {
	Get(context.Context, string, string) (Action, error)
	// Create must be atomic on the tenant/user/idempotency-key tuple and return
	// the existing action when the key has already been used.
	Create(context.Context, Action) (Action, error)
	// Approve must atomically transition a proposed or retryable action to
	// approved and return the stored action for idempotent repeat calls.
	Approve(context.Context, string, string, Approval) (Action, error)
	Save(context.Context, Action) error
	ClaimForExecution(context.Context, string, string) (Action, error)
}

type Service struct {
	repository            Repository
	externalWritesEnabled bool
	now                   func() time.Time
}

func NewService(repository Repository, externalWritesEnabled bool) *Service {
	return &Service{
		repository:            repository,
		externalWritesEnabled: externalWritesEnabled,
		now:                   func() time.Time { return time.Now().UTC() },
	}
}

// Propose is idempotent by (tenant, user, idempotency key). Reusing a key for
// a different typed request is rejected rather than silently changing the
// action a later approval refers to.
func (s *Service) Propose(ctx context.Context, action Action) (Action, error) {
	if s == nil || s.repository == nil {
		return Action{}, fmt.Errorf("%w: repository is required", ErrInvalidAction)
	}
	if action.ID == "" {
		return Action{}, fmt.Errorf("%w: action ID is required", ErrInvalidAction)
	}
	if action.Status == "" {
		action.Status = Proposed
	}
	action.Status = Proposed
	if action.RequestHash == "" {
		action.RequestHash = fingerprint(action)
	}
	if action.CreatedAt.IsZero() {
		action.CreatedAt = s.now().UTC()
	}
	if action.UpdatedAt.IsZero() {
		action.UpdatedAt = action.CreatedAt
	}
	if err := validateAction(action); err != nil {
		return Action{}, err
	}
	created, err := s.repository.Create(ctx, action)
	if err != nil {
		return Action{}, fmt.Errorf("create action %q: %w", action.ID, err)
	}
	if created.RequestHash != action.RequestHash {
		return Action{}, fmt.Errorf("%w: key %q", ErrIdempotencyConflict, action.IdempotencyKey)
	}
	return cloneAction(created), nil
}

// Approve is the only domain operation that can move a proposed action into
// the executable state. Repeated approval calls return the existing approved
// action without changing its original audit fields.
func (s *Service) Approve(ctx context.Context, userID, actionID string, approval Approval) (Action, error) {
	if s == nil || s.repository == nil {
		return Action{}, fmt.Errorf("%w: repository is required", ErrInvalidAction)
	}
	if strings.TrimSpace(approval.ApprovedBy) == "" {
		return Action{}, fmt.Errorf("%w: approving actor is required", ErrInvalidAction)
	}
	action, err := s.repository.Get(ctx, userID, actionID)
	if err != nil {
		return Action{}, fmt.Errorf("get action %q: %w", actionID, err)
	}
	switch action.Status {
	case Approved, Executing, Completed:
		return cloneAction(action), nil
	case Rejected:
		return Action{}, fmt.Errorf("%w: %q", ErrActionRejected, actionID)
	case Proposed, Failed:
		if approval.At.IsZero() {
			approval.At = s.now().UTC()
		}
		approved, approveErr := s.repository.Approve(ctx, userID, actionID, approval)
		if approveErr == nil {
			return cloneAction(approved), nil
		}
		// Another instance may have won the atomic transition between the
		// initial read and Approve. Re-read before reporting a failure so the
		// operation remains idempotent.
		current, getErr := s.repository.Get(ctx, userID, actionID)
		if getErr == nil && (current.Status == Approved || current.Status == Executing || current.Status == Completed) {
			return cloneAction(current), nil
		}
		return Action{}, fmt.Errorf("approve action %q: %w", actionID, approveErr)
	default:
		return Action{}, fmt.Errorf("%w: unknown status %q", ErrInvalidAction, action.Status)
	}
}

// Execute enforces configuration and explicit approval before invoking an
// external provider. Completed actions return their stored result without
// invoking the provider again, which makes retrying a request idempotent.
func (s *Service) Execute(ctx context.Context, userID, actionID string, executor Executor) (Action, error) {
	if s == nil || s.repository == nil {
		return Action{}, fmt.Errorf("%w: repository is required", ErrInvalidAction)
	}
	if !s.externalWritesEnabled {
		return Action{}, ErrActionsDisabled
	}
	if executor == nil {
		return Action{}, fmt.Errorf("%w: executor is required", ErrInvalidAction)
	}
	action, err := s.repository.Get(ctx, userID, actionID)
	if err != nil {
		return Action{}, fmt.Errorf("get action %q: %w", actionID, err)
	}
	if action.Status == Completed {
		return cloneAction(action), nil
	}
	if action.Status != Approved {
		if action.Status == Executing {
			return Action{}, fmt.Errorf("%w: %q", ErrActionInProgress, actionID)
		}
		return Action{}, fmt.Errorf("%w: action %q is %s", ErrApprovalRequired, actionID, action.Status)
	}
	claimed, err := s.repository.ClaimForExecution(ctx, userID, actionID)
	if err != nil {
		return Action{}, fmt.Errorf("claim action %q: %w", actionID, err)
	}
	if claimed.Status == Completed {
		return cloneAction(claimed), nil
	}
	if claimed.Status != Executing {
		return Action{}, fmt.Errorf("%w: action %q returned status %s", ErrActionNotClaimable, actionID, claimed.Status)
	}
	result, executeErr := executor.Execute(ctx, cloneAction(claimed))
	if executeErr != nil {
		claimed.Status = Failed
		claimed.Error = executeErr.Error()
		claimed.UpdatedAt = s.now().UTC()
		if saveErr := s.repository.Save(ctx, claimed); saveErr != nil {
			return Action{}, fmt.Errorf("execute action %q failed (%v) and persist failed state: %w", actionID, executeErr, saveErr)
		}
		return Action{}, fmt.Errorf("execute action %q: %w", actionID, executeErr)
	}
	claimed.Status = Completed
	claimed.ResultReference = result.Reference
	claimed.Result = result.Output
	claimed.Error = ""
	claimed.UpdatedAt = s.now().UTC()
	if err := s.repository.Save(ctx, claimed); err != nil {
		return Action{}, fmt.Errorf("persist completed action %q: %w", actionID, err)
	}
	return cloneAction(claimed), nil
}

func validateAction(action Action) error {
	if strings.TrimSpace(action.ID) == "" || strings.TrimSpace(action.UserID) == "" {
		return fmt.Errorf("%w: ID and user ID are required", ErrInvalidAction)
	}
	if !action.Kind.Valid() {
		return fmt.Errorf("%w: unknown action kind %q", ErrInvalidAction, action.Kind)
	}
	if strings.TrimSpace(action.Target) == "" {
		return fmt.Errorf("%w: target is required", ErrInvalidAction)
	}
	if strings.TrimSpace(action.IdempotencyKey) == "" {
		return fmt.Errorf("%w: idempotency key is required", ErrInvalidAction)
	}
	if !action.Status.Valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidAction, action.Status)
	}
	return nil
}

func fingerprint(action Action) string {
	keys := make([]string, 0, len(action.Payload))
	for key := range action.Payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	canonical := struct {
		Kind    Kind              `json:"kind"`
		Target  string            `json:"target"`
		Payload map[string]string `json:"payload"`
	}{Kind: action.Kind, Target: action.Target, Payload: make(map[string]string, len(keys))}
	for _, key := range keys {
		canonical.Payload[key] = action.Payload[key]
	}
	encoded, _ := json.Marshal(canonical)
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}

func cloneAction(action Action) Action {
	if action.Payload != nil {
		action.Payload = make(map[string]string, len(action.Payload))
		for key, value := range action.Payload {
			action.Payload[key] = value
		}
	}
	return action
}
