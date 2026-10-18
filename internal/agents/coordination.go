package agents

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type AgentType string

const (
	ScoutAgent     AgentType = "scout"
	CuratorAgent   AgentType = "graph_curator"
	RelevanceAgent AgentType = "relevance"
	PlannerAgent   AgentType = "planner"
	MemoryAgent    AgentType = "memory_reviewer"
	CodingAgent    AgentType = "coding_worker"
)

func (a AgentType) Valid() bool {
	switch a {
	case ScoutAgent, CuratorAgent, RelevanceAgent, PlannerAgent, MemoryAgent, CodingAgent:
		return true
	default:
		return false
	}
}

type TaskStatus string

const (
	Pending   TaskStatus = "pending"
	Running   TaskStatus = "running"
	Blocked   TaskStatus = "blocked"
	Completed TaskStatus = "completed"
	Failed    TaskStatus = "failed"
	Cancelled TaskStatus = "cancelled"
)

func (s TaskStatus) Valid() bool {
	switch s {
	case Pending, Running, Blocked, Completed, Failed, Cancelled:
		return true
	default:
		return false
	}
}

var (
	ErrInvalidTask    = errors.New("invalid agent task")
	ErrTaskNotFound   = errors.New("agent task not found")
	ErrInvalidHandoff = errors.New("invalid agent handoff")
	ErrTaskLeaseHeld  = errors.New("agent task lease is held by another worker")
)

type Task struct {
	ID                       string     `json:"id"`
	TenantID                 string     `json:"tenant_id"`
	UserID                   string     `json:"user_id"`
	ProjectID                string     `json:"project_id,omitempty"`
	SessionID                string     `json:"session_id,omitempty"`
	Agent                    AgentType  `json:"agent"`
	Status                   TaskStatus `json:"status"`
	Priority                 int        `json:"priority"`
	CorrelationID            string     `json:"correlation_id,omitempty"`
	InputRefs                []string   `json:"input_refs,omitempty"`
	OutputRefs               []string   `json:"output_refs,omitempty"`
	DependencyIDs            []string   `json:"dependency_ids,omitempty"`
	Phase                    string     `json:"phase,omitempty"`
	Progress                 int        `json:"progress"`
	LeaseOwner               string     `json:"lease_owner,omitempty"`
	LeaseExpiresAt           time.Time  `json:"lease_expires_at,omitempty"`
	ConversationID           string     `json:"conversation_id,omitempty"`
	TurnID                   string     `json:"turn_id,omitempty"`
	Provider                 string     `json:"provider,omitempty"`
	ProviderSessionID        string     `json:"provider_session_id,omitempty"`
	ProviderEventCursor      string     `json:"provider_event_cursor,omitempty"`
	ProviderTerminalObserved bool       `json:"provider_terminal_observed,omitempty"`
	CodingTitle              string     `json:"coding_title,omitempty"`
	CodingPrompt             string     `json:"coding_prompt,omitempty"`
	CodingExternalContext    string     `json:"coding_external_context,omitempty"`
	RepositoryPath           string     `json:"repository_path,omitempty"`
	WorkspacePath            string     `json:"workspace_path,omitempty"`
	BaseSHA                  string     `json:"base_sha,omitempty"`
	WorkspaceCleanupState    string     `json:"workspace_cleanup_state,omitempty"`
	RunID                    string     `json:"run_id,omitempty"`
	RetryCount               int        `json:"retry_count"`
	Error                    string     `json:"error,omitempty"`
	CreatedAt                time.Time  `json:"created_at"`
	UpdatedAt                time.Time  `json:"updated_at"`
}

// Handoff is a durable transfer record. The target task is persisted by the
// same repository operation as the HANDED_OFF_TO relationship.
type Handoff struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	UserID        string    `json:"user_id"`
	FromTaskID    string    `json:"from_task_id"`
	ToTaskID      string    `json:"to_task_id"`
	FromAgent     AgentType `json:"from_agent"`
	ToAgent       AgentType `json:"to_agent"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Notes         string    `json:"notes,omitempty"`
	InputRefs     []string  `json:"input_refs,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type HandoffRequest struct {
	ID            string
	UserID        string
	FromTaskID    string
	ToTaskID      string
	ToAgent       AgentType
	CorrelationID string
	Notes         string
	InputRefs     []string
}

// Repository owns all durable coordination state. PersistHandoff is one
// operation so a graph-backed implementation can create the target task and
// its HANDED_OFF_TO edge without exposing a partial in-memory handoff.
type Repository interface {
	SaveTask(context.Context, Task) error
	GetTask(context.Context, string, string) (Task, error)
	PersistHandoff(context.Context, Task, Handoff) error
}

// TaskListRepository and TaskControlRepository are optional capabilities so
// existing lightweight coordination repositories remain source-compatible.
type TaskListRepository interface {
	ListTasks(context.Context, string, string, string, int) ([]Task, error)
}

type TaskControlRepository interface {
	HeartbeatTask(context.Context, Task, string, time.Time) (Task, error)
}

// StaleRecoveryRepository is optional so lightweight test repositories do not
// need to implement operational recovery behavior.
type StaleRecoveryRepository interface {
	RecoverStaleRunning(context.Context, string, string, time.Time) (int, error)
}

type Coordinator struct {
	repository Repository
	now        func() time.Time
}

func NewCoordinator(repository Repository) *Coordinator {
	return &Coordinator{repository: repository, now: func() time.Time { return time.Now().UTC() }}
}

func (c *Coordinator) CreateTask(ctx context.Context, task Task) (Task, error) {
	if c == nil || c.repository == nil {
		return Task{}, fmt.Errorf("%w: repository is required", ErrInvalidTask)
	}
	if task.Status == "" {
		task.Status = Pending
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = c.now().UTC()
	}
	if task.UpdatedAt.IsZero() {
		task.UpdatedAt = task.CreatedAt
	}
	if err := validateTask(task); err != nil {
		return Task{}, err
	}
	task = cloneTask(task)
	if err := c.repository.SaveTask(ctx, task); err != nil {
		return Task{}, fmt.Errorf("save task %q: %w", task.ID, err)
	}
	return task, nil
}

func (c *Coordinator) GetTask(ctx context.Context, userID, taskID string) (Task, error) {
	if c == nil || c.repository == nil {
		return Task{}, fmt.Errorf("%w: repository is required", ErrTaskNotFound)
	}
	if strings.TrimSpace(userID) == "" || strings.TrimSpace(taskID) == "" {
		return Task{}, fmt.Errorf("%w: user and task IDs are required", ErrTaskNotFound)
	}
	task, err := c.repository.GetTask(ctx, userID, taskID)
	if err != nil {
		return Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	return cloneTask(task), nil
}

func (c *Coordinator) GetTaskForScope(ctx context.Context, scopeTenant, scopeUser, taskID string) (Task, error) {
	task, err := c.GetTask(ctx, scopeUser, taskID)
	if err != nil {
		return Task{}, err
	}
	if task.TenantID != scopeTenant || task.UserID != scopeUser {
		return Task{}, fmt.Errorf("%w: task is outside the requested scope", ErrTaskNotFound)
	}
	return task, nil
}

func (c *Coordinator) ListTasks(ctx context.Context, tenantID, userID, projectID string, limit int) ([]Task, error) {
	if c == nil || c.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidTask)
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(userID) == "" {
		return nil, fmt.Errorf("%w: tenant and user IDs are required", ErrInvalidTask)
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		return nil, fmt.Errorf("%w: limit cannot exceed 100", ErrInvalidTask)
	}
	repository, ok := c.repository.(TaskListRepository)
	if !ok {
		return nil, fmt.Errorf("%w: task listing is not supported", ErrInvalidTask)
	}
	values, err := repository.ListTasks(ctx, tenantID, userID, projectID, limit)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	for index := range values {
		if err := validateTask(values[index]); err != nil {
			return nil, err
		}
		if values[index].TenantID != tenantID || values[index].UserID != userID {
			return nil, fmt.Errorf("%w: repository returned a task outside the requested scope", ErrInvalidTask)
		}
	}
	return values, nil
}

func (c *Coordinator) Transition(ctx context.Context, task Task, status TaskStatus, phase string, progress int, failure string) (Task, error) {
	if c == nil || c.repository == nil {
		return Task{}, fmt.Errorf("%w: repository is required", ErrInvalidTask)
	}
	if !status.Valid() {
		return Task{}, fmt.Errorf("%w: unknown status %q", ErrInvalidTask, status)
	}
	if progress < 0 || progress > 100 {
		return Task{}, fmt.Errorf("%w: progress must be between 0 and 100", ErrInvalidTask)
	}
	if task.Status == Completed && status != Completed {
		return Task{}, fmt.Errorf("%w: completed task cannot transition", ErrInvalidTask)
	}
	if task.Status == Cancelled && status != Cancelled && status != Pending {
		return Task{}, fmt.Errorf("%w: cancelled task can only remain cancelled or be explicitly retried", ErrInvalidTask)
	}
	if status == Pending && task.Status != Failed && task.Status != Cancelled && task.Status != Blocked {
		return Task{}, fmt.Errorf("%w: only failed, blocked, or cancelled tasks can be retried", ErrInvalidTask)
	}
	previousStatus := task.Status
	task.Status = status
	if status == Pending && (previousStatus == Failed || previousStatus == Cancelled || previousStatus == Blocked) {
		task.RetryCount++
	}
	task.Phase = strings.TrimSpace(phase)
	task.Progress = progress
	task.Error = strings.TrimSpace(failure)
	task.UpdatedAt = c.now().UTC()
	if status == Completed {
		task.Progress = 100
	}
	if err := validateTask(task); err != nil {
		return Task{}, err
	}
	if err := c.repository.SaveTask(ctx, task); err != nil {
		return Task{}, fmt.Errorf("transition task: %w", err)
	}
	return cloneTask(task), nil
}

func (c *Coordinator) Heartbeat(ctx context.Context, task Task, owner string, until time.Time) (Task, error) {
	if c == nil || c.repository == nil {
		return Task{}, fmt.Errorf("%w: repository is required", ErrInvalidTask)
	}
	owner = strings.TrimSpace(owner)
	if owner == "" || until.IsZero() {
		return Task{}, fmt.Errorf("%w: lease owner and expiry are required", ErrInvalidTask)
	}
	if task.Status != Running {
		return Task{}, fmt.Errorf("%w: only running tasks can renew a lease", ErrInvalidTask)
	}
	if heartbeat, ok := c.repository.(TaskControlRepository); ok {
		return heartbeat.HeartbeatTask(ctx, task, owner, until)
	}
	task.LeaseOwner = owner
	task.LeaseExpiresAt = until.UTC()
	task.UpdatedAt = c.now().UTC()
	if err := c.repository.SaveTask(ctx, task); err != nil {
		return Task{}, fmt.Errorf("heartbeat task: %w", err)
	}
	return cloneTask(task), nil
}

func (c *Coordinator) Handoff(ctx context.Context, request HandoffRequest) (Handoff, error) {
	if c == nil || c.repository == nil {
		return Handoff{}, fmt.Errorf("%w: repository is required", ErrInvalidHandoff)
	}
	if strings.TrimSpace(request.UserID) == "" || strings.TrimSpace(request.FromTaskID) == "" || strings.TrimSpace(request.ToTaskID) == "" || strings.TrimSpace(request.ID) == "" {
		return Handoff{}, fmt.Errorf("%w: user, handoff, source task, and target task IDs are required", ErrInvalidHandoff)
	}
	if request.FromTaskID == request.ToTaskID {
		return Handoff{}, fmt.Errorf("%w: source and target tasks must differ", ErrInvalidHandoff)
	}
	if !request.ToAgent.Valid() {
		return Handoff{}, fmt.Errorf("%w: unknown target agent %q", ErrInvalidHandoff, request.ToAgent)
	}
	source, err := c.repository.GetTask(ctx, request.UserID, request.FromTaskID)
	if err != nil {
		return Handoff{}, fmt.Errorf("get source task %q: %w", request.FromTaskID, err)
	}
	if source.Status == Completed || source.Status == Cancelled {
		return Handoff{}, fmt.Errorf("%w: task %q is already %s", ErrInvalidHandoff, source.ID, source.Status)
	}
	now := c.now().UTC()
	target := Task{
		ID:            request.ToTaskID,
		TenantID:      source.TenantID,
		UserID:        source.UserID,
		ProjectID:     source.ProjectID,
		SessionID:     source.SessionID,
		Agent:         request.ToAgent,
		Status:        Pending,
		Priority:      source.Priority,
		CorrelationID: request.CorrelationID,
		InputRefs:     append([]string(nil), request.InputRefs...),
		DependencyIDs: []string{source.ID},
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if len(target.InputRefs) == 0 {
		target.InputRefs = append([]string(nil), source.OutputRefs...)
	}
	handoff := Handoff{
		ID:            request.ID,
		TenantID:      source.TenantID,
		UserID:        source.UserID,
		FromTaskID:    source.ID,
		ToTaskID:      target.ID,
		FromAgent:     source.Agent,
		ToAgent:       target.Agent,
		CorrelationID: request.CorrelationID,
		Notes:         request.Notes,
		InputRefs:     append([]string(nil), target.InputRefs...),
		CreatedAt:     now,
	}
	if err := validateTask(target); err != nil {
		return Handoff{}, err
	}
	if err := validateHandoff(handoff); err != nil {
		return Handoff{}, err
	}
	if err := c.repository.PersistHandoff(ctx, target, handoff); err != nil {
		return Handoff{}, fmt.Errorf("persist handoff %q: %w", handoff.ID, err)
	}
	return cloneHandoff(handoff), nil
}

func (c *Coordinator) RecoverStaleRunning(ctx context.Context, tenantID, userID string, before time.Time) (int, error) {
	if c == nil || c.repository == nil {
		return 0, fmt.Errorf("%w: repository is required", ErrInvalidTask)
	}
	recovery, ok := c.repository.(StaleRecoveryRepository)
	if !ok {
		return 0, fmt.Errorf("%w: stale task recovery is not supported", ErrInvalidTask)
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(userID) == "" || before.IsZero() {
		return 0, fmt.Errorf("%w: tenant, user, and stale cutoff are required", ErrInvalidTask)
	}
	return recovery.RecoverStaleRunning(ctx, tenantID, userID, before)
}

func validateTask(task Task) error {
	if strings.TrimSpace(task.ID) == "" || strings.TrimSpace(task.TenantID) == "" || strings.TrimSpace(task.UserID) == "" {
		return fmt.Errorf("%w: ID, tenant ID, and user ID are required", ErrInvalidTask)
	}
	if !task.Agent.Valid() {
		return fmt.Errorf("%w: unknown agent %q", ErrInvalidTask, task.Agent)
	}
	if !task.Status.Valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidTask, task.Status)
	}
	if task.Priority < 0 {
		return fmt.Errorf("%w: priority cannot be negative", ErrInvalidTask)
	}
	if task.Progress < 0 || task.Progress > 100 {
		return fmt.Errorf("%w: progress must be between 0 and 100", ErrInvalidTask)
	}
	if len([]rune(task.Error)) > 2000 {
		return fmt.Errorf("%w: error is too large", ErrInvalidTask)
	}
	return nil
}

func validateHandoff(handoff Handoff) error {
	if strings.TrimSpace(handoff.ID) == "" || strings.TrimSpace(handoff.UserID) == "" || strings.TrimSpace(handoff.FromTaskID) == "" || strings.TrimSpace(handoff.ToTaskID) == "" {
		return fmt.Errorf("%w: IDs are required", ErrInvalidHandoff)
	}
	if handoff.FromTaskID == handoff.ToTaskID {
		return fmt.Errorf("%w: source and target tasks must differ", ErrInvalidHandoff)
	}
	if !handoff.FromAgent.Valid() || !handoff.ToAgent.Valid() {
		return fmt.Errorf("%w: source and target agents are required", ErrInvalidHandoff)
	}
	return nil
}

func cloneTask(task Task) Task {
	task.InputRefs = append([]string(nil), task.InputRefs...)
	task.OutputRefs = append([]string(nil), task.OutputRefs...)
	task.DependencyIDs = append([]string(nil), task.DependencyIDs...)
	return task
}

func cloneHandoff(handoff Handoff) Handoff {
	handoff.InputRefs = append([]string(nil), handoff.InputRefs...)
	return handoff
}
