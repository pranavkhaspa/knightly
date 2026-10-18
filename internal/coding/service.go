package coding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pranavkhaspa/knightly/internal/agents"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

type TaskCoordinator interface {
	GetTaskForScope(context.Context, string, string, string) (agents.Task, error)
	Transition(context.Context, agents.Task, agents.TaskStatus, string, int, string) (agents.Task, error)
	Heartbeat(context.Context, agents.Task, string, time.Time) (agents.Task, error)
}

type EventSink func(context.Context, identity.Scope, agents.Task, string, map[string]string) error

type ServiceConfig struct {
	RunTimeout        time.Duration
	LeaseOwner        string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	Now               func() time.Time
}

// Service orchestrates one coding task at a time through the existing durable
// task coordinator, workspace manager, provider adapter, and graph repository.
type Service struct {
	coordinator       TaskCoordinator
	workspaces        *WorkspaceManager
	adapter           AgentAdapter
	records           RecordRepository
	eventSink         EventSink
	timeout           time.Duration
	leaseOwner        string
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	now               func() time.Time
	mu                sync.Mutex
	cancels           map[string]context.CancelFunc
	handles           map[string]RunHandle
}

func NewService(coordinator TaskCoordinator, workspaces *WorkspaceManager, adapter AgentAdapter, records RecordRepository, eventSink EventSink, cfg ServiceConfig) (*Service, error) {
	if coordinator == nil || workspaces == nil || adapter == nil || records == nil {
		return nil, fmt.Errorf("%w: coordinator, workspace manager, adapter, and record repository are required", ErrInvalidRequest)
	}
	if cfg.RunTimeout <= 0 {
		cfg.RunTimeout = 2 * time.Hour
	}
	if strings.TrimSpace(cfg.LeaseOwner) == "" {
		cfg.LeaseOwner = "coding-worker"
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 2 * time.Minute
	}
	if cfg.HeartbeatInterval <= 0 || cfg.HeartbeatInterval >= cfg.LeaseDuration {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{coordinator: coordinator, workspaces: workspaces, adapter: adapter, records: records, eventSink: eventSink, timeout: cfg.RunTimeout, leaseOwner: cfg.LeaseOwner, leaseDuration: cfg.LeaseDuration, heartbeatInterval: cfg.HeartbeatInterval, now: cfg.Now, cancels: make(map[string]context.CancelFunc), handles: make(map[string]RunHandle)}, nil
}

// Start creates an isolated workspace and starts OpenCode for an existing,
// scoped NightShift task. It returns after the provider has accepted the prompt;
// progress and final artifacts are handled asynchronously.
func (s *Service) Start(ctx context.Context, scope identity.Scope, taskID string, request RunRequest) (agents.Task, error) {
	if s == nil {
		return agents.Task{}, fmt.Errorf("%w: coding service is nil", ErrInvalidRequest)
	}
	if err := scope.Validate(); err != nil {
		return agents.Task{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	task, err := s.coordinator.GetTaskForScope(ctx, scope.TenantID, scope.UserID, taskID)
	if err != nil {
		return agents.Task{}, err
	}
	if task.Status == agents.Running || task.Status == agents.Completed || task.Status == agents.Cancelled {
		return agents.Task{}, fmt.Errorf("%w: task cannot start from status %q", ErrInvalidRequest, task.Status)
	}
	previousRunID := task.RunID
	resumeOnly := task.ProviderSessionID != "" && (task.Error == "recovered stale running task" || task.Phase == "retrying")
	terminalObserved := resumeOnly && (task.ProviderTerminalObserved || isTerminalProviderPhase(task.Phase))
	request.TaskID = task.ID
	request.ProjectID = task.ProjectID
	request.RepositoryPath = strings.TrimSpace(request.RepositoryPath)
	request.BaseSHA = strings.TrimSpace(request.BaseSHA)
	if strings.TrimSpace(request.Title) == "" || strings.TrimSpace(request.Prompt) == "" || len(request.Prompt) > DefaultMaxPromptBytes || len(request.ExternalContext) > DefaultMaxContextBytes || request.RepositoryPath == "" || !commitSHAPattern.MatchString(request.BaseSHA) {
		return agents.Task{}, fmt.Errorf("%w: title, bounded prompt, repository path, and base SHA are required", ErrInvalidRequest)
	}

	// Claim the durable task lease before creating a workspace. This is the
	// critical multi-instance guard: a competing worker must lose the lease
	// race before it can create a worktree or launch a provider session.
	leaseCandidate := task
	leaseCandidate.Status = agents.Running
	leasedTask, leaseErr := s.coordinator.Heartbeat(ctx, leaseCandidate, s.leaseOwner, s.now().UTC().Add(s.leaseDuration))
	if leaseErr != nil {
		return agents.Task{}, fmt.Errorf("claim coding task lease: %w", leaseErr)
	}
	task = leasedTask
	task.Provider = s.adapter.Name()
	task, err = s.coordinator.Transition(ctx, task, agents.Running, "preparing_workspace", 2, "")
	if err != nil {
		return agents.Task{}, fmt.Errorf("persist coding task claim: %w", err)
	}

	workspace, err := s.workspaces.Get(task.ID)
	createdWorkspace := false
	if err != nil && task.WorkspacePath != "" {
		workspace, err = s.workspaces.Attach(Workspace{TaskID: task.ID, RepositoryPath: task.RepositoryPath, Path: task.WorkspacePath, BaseSHA: task.BaseSHA, CleanupState: task.WorkspaceCleanupState})
	}
	if err != nil {
		workspace, err = s.workspaces.Create(ctx, task.ID, request.RepositoryPath, request.BaseSHA)
		if err != nil {
			return agents.Task{}, s.failSetup(ctx, scope, task, "workspace_creation_failed", err)
		}
		createdWorkspace = true
	} else if !samePath(workspace.RepositoryPath, request.RepositoryPath) || !strings.EqualFold(workspace.BaseSHA, request.BaseSHA) {
		return agents.Task{}, s.failSetup(ctx, scope, task, "workspace_conflict", ErrWorkspaceConflict)
	}
	request.WorkspacePath = workspace.Path
	if err := validateRunRequest(request); err != nil {
		if createdWorkspace {
			_ = s.workspaces.Cleanup(ctx, task.ID)
		}
		return agents.Task{}, s.failSetup(ctx, scope, task, "coding_request_invalid", err)
	}
	if err := s.records.SaveWorkspace(ctx, workspace, scope.TenantID, scope.UserID); err != nil {
		if createdWorkspace {
			_ = s.workspaces.Cleanup(ctx, task.ID)
		}
		return agents.Task{}, s.failSetup(ctx, scope, task, "workspace_persistence_failed", err)
	}

	task.RepositoryPath = workspace.RepositoryPath
	task.WorkspacePath = workspace.Path
	task.BaseSHA = workspace.BaseSHA
	task.WorkspaceCleanupState = workspace.CleanupState
	task.ProviderTerminalObserved = terminalObserved
	task.Progress = 5
	task.UpdatedAt = s.now().UTC()
	task, err = s.coordinator.Transition(ctx, task, agents.Running, "workspace_prepared", 5, "")
	if err != nil {
		return agents.Task{}, fmt.Errorf("persist coding task workspace state: %w", err)
	}

	runID := stableID("run", task.ID, fmt.Sprint(task.RetryCount))
	if terminalObserved && previousRunID != "" {
		runID = previousRunID
	}
	run := RunRecord{ID: runID, TenantID: scope.TenantID, UserID: scope.UserID, ProjectID: task.ProjectID, TaskID: task.ID, Provider: s.adapter.Name(), Status: "starting", WorkspaceID: workspace.TaskID, EventCursor: task.ProviderEventCursor, StartedAt: s.now().UTC(), UpdatedAt: s.now().UTC()}
	task.RunID = runID
	task, err = s.coordinator.Transition(ctx, task, agents.Running, "starting_agent", 10, "")
	if err != nil {
		return agents.Task{}, fmt.Errorf("persist coding run state: %w", err)
	}
	if err := s.records.SaveRun(ctx, run); err != nil {
		return agents.Task{}, fmt.Errorf("persist coding run: %w", err)
	}

	request.WorkspacePath = workspace.Path
	var handle RunHandle
	var startErr error
	if task.ProviderSessionID != "" {
		resumeRequest := request
		if resumeOnly {
			resumeRequest.Prompt = ""
		}
		handle, startErr = s.adapter.Resume(ctx, task.ProviderSessionID, resumeRequest)
	} else {
		handle, startErr = s.adapter.Start(ctx, request)
	}
	handle.EventCursor = task.ProviderEventCursor
	handle.TerminalObserved = terminalObserved
	if handle.ProviderSessionID != "" {
		task.ProviderSessionID = handle.ProviderSessionID
		run.ProviderSessionID = handle.ProviderSessionID
	}
	if startErr != nil {
		run.Status, run.Error, run.UpdatedAt = "failed", boundedError(startErr), s.now().UTC()
		_ = s.records.SaveRun(ctx, run)
		task, _ = s.coordinator.Transition(ctx, task, agents.Failed, "provider_start_failed", task.Progress, boundedError(startErr))
		s.emit(context.Background(), scope, task, "task.failed", map[string]string{"phase": "provider_start_failed"})
		return task, startErr
	}
	task, err = s.coordinator.Transition(ctx, task, agents.Running, "agent_running", 15, "")
	if err != nil {
		return agents.Task{}, fmt.Errorf("persist OpenCode session on task: %w", err)
	}
	run.Status, run.UpdatedAt = "running", s.now().UTC()
	if err := s.records.SaveRun(ctx, run); err != nil {
		// The provider session ID is already durable on the task, so a later
		// recovery can inspect it rather than blindly creating another session.
		_ = s.adapter.Cancel(context.Background(), handle)
		failed, transitionErr := s.coordinator.Transition(context.Background(), task, agents.Failed, "run_persistence_failed", task.Progress, boundedError(err))
		if transitionErr == nil {
			s.emit(context.Background(), scope, failed, "task.failed", map[string]string{"phase": "run_persistence_failed"})
		}
		return task, fmt.Errorf("persist OpenCode run status: %w", err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), s.timeout)
	s.mu.Lock()
	if previous := s.cancels[task.ID]; previous != nil {
		previous()
	}
	s.cancels[task.ID] = cancel
	s.handles[task.ID] = handle
	s.mu.Unlock()
	go s.finishRun(runCtx, cancel, scope, task, request, handle, run)
	return task, nil
}

func (s *Service) failSetup(ctx context.Context, scope identity.Scope, task agents.Task, phase string, cause error) error {
	latest, readErr := s.coordinator.GetTaskForScope(context.Background(), scope.TenantID, scope.UserID, task.ID)
	if readErr == nil && latest.Status == agents.Running {
		failed, transitionErr := s.coordinator.Transition(context.Background(), latest, agents.Failed, phase, latest.Progress, boundedError(cause))
		if transitionErr == nil {
			s.emit(context.Background(), scope, failed, "task.failed", map[string]string{"phase": phase})
		}
	}
	return fmt.Errorf("%s: %w", phase, cause)
}

// FailQueued records a queue/setup failure and notifies the originating
// NightShift conversation so a task cannot silently disappear from the UI.
func (s *Service) FailQueued(ctx context.Context, scope identity.Scope, task agents.Task, cause error) (agents.Task, error) {
	if s == nil || cause == nil {
		return agents.Task{}, fmt.Errorf("coding service and failure cause are required")
	}
	latest, err := s.coordinator.GetTaskForScope(ctx, scope.TenantID, scope.UserID, task.ID)
	if err != nil {
		return agents.Task{}, err
	}
	if latest.Status != agents.Pending {
		return latest, nil
	}
	failed, err := s.coordinator.Transition(ctx, latest, agents.Failed, "coding_queue_failed", latest.Progress, boundedError(cause))
	if err != nil {
		return agents.Task{}, err
	}
	s.emit(context.Background(), scope, failed, "task.failed", map[string]string{"phase": "coding_queue_failed"})
	return failed, nil
}

func (s *Service) Cancel(ctx context.Context, scope identity.Scope, taskID string) (agents.Task, error) {
	task, err := s.coordinator.GetTaskForScope(ctx, scope.TenantID, scope.UserID, taskID)
	if err != nil {
		return agents.Task{}, err
	}
	s.mu.Lock()
	cancel := s.cancels[taskID]
	s.mu.Unlock()
	if task.ProviderSessionID != "" {
		if err := s.adapter.Cancel(ctx, RunHandle{TaskID: task.ID, ProjectID: task.ProjectID, Provider: task.Provider, ProviderSessionID: task.ProviderSessionID, WorkspacePath: task.WorkspacePath}); err != nil && !errors.Is(err, context.Canceled) {
			return agents.Task{}, fmt.Errorf("interrupt coding provider: %w", err)
		}
	}
	if cancel != nil {
		cancel()
	}
	updated, err := s.coordinator.Transition(ctx, task, agents.Cancelled, "cancelled", task.Progress, "cancelled by user")
	if err != nil {
		return agents.Task{}, err
	}
	if updated.RunID != "" {
		run := RunRecord{ID: updated.RunID, TenantID: scope.TenantID, UserID: scope.UserID, ProjectID: updated.ProjectID, TaskID: updated.ID, Provider: updated.Provider, ProviderSessionID: updated.ProviderSessionID, EventCursor: updated.ProviderEventCursor, Status: "cancelled", WorkspaceID: updated.ID, UpdatedAt: s.now().UTC()}
		_ = s.records.SaveRun(ctx, run)
	}
	s.emit(context.Background(), scope, updated, "task.cancelled", map[string]string{"phase": "cancelled"})
	return updated, nil
}

// Close interrupts active provider sessions and cancels local stream readers
// during daemon shutdown. Task state remains durable for startup recovery.
func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	handles := make([]RunHandle, 0, len(s.handles))
	cancels := make([]context.CancelFunc, 0, len(s.cancels))
	for _, handle := range s.handles {
		handles = append(handles, handle)
	}
	for _, cancel := range s.cancels {
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()
	var firstErr error
	for _, handle := range handles {
		if err := s.adapter.Cancel(ctx, handle); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, cancel := range cancels {
		cancel()
	}
	return firstErr
}

func (s *Service) maintainLease(ctx context.Context, scope identity.Scope, taskID string, cancel context.CancelFunc) {
	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			latest, err := s.coordinator.GetTaskForScope(ctx, scope.TenantID, scope.UserID, taskID)
			if err == nil {
				_, err = s.coordinator.Heartbeat(ctx, latest, s.leaseOwner, s.now().UTC().Add(s.leaseDuration))
			}
			if err != nil {
				cancel()
				return
			}
		}
	}
}

func (s *Service) finishRun(ctx context.Context, cancel context.CancelFunc, scope identity.Scope, task agents.Task, request RunRequest, handle RunHandle, run RunRecord) {
	defer cancel()
	defer func() { s.mu.Lock(); delete(s.cancels, task.ID); delete(s.handles, task.ID); s.mu.Unlock() }()
	fail := func(phase string, cause error) {
		latest, err := s.coordinator.GetTaskForScope(context.Background(), scope.TenantID, scope.UserID, task.ID)
		// A user cancel makes the provider report an abort; that must not be
		// recorded as a failure on top of the cancelled state.
		if err == nil && latest.Status == agents.Cancelled {
			return
		}
		run.Status, run.Error, run.UpdatedAt = "failed", boundedError(cause), s.now().UTC()
		_ = s.records.SaveRun(context.Background(), run)
		if err == nil {
			latest, _ = s.coordinator.Transition(context.Background(), latest, agents.Failed, phase, latest.Progress, boundedError(cause))
			if workspace, workspaceErr := s.workspaces.UpdateCleanupState(task.ID, "retained_for_review"); workspaceErr == nil {
				latest.WorkspaceCleanupState = workspace.CleanupState
				latest, _ = s.coordinator.Transition(context.Background(), latest, agents.Failed, phase, latest.Progress, boundedError(cause))
				_ = s.records.SaveWorkspace(context.Background(), workspace, scope.TenantID, scope.UserID)
			}
			s.emit(context.Background(), scope, latest, "task.failed", map[string]string{"phase": phase})
		}
	}

	go s.maintainLease(ctx, scope, task.ID, cancel)
	if !handle.TerminalObserved {
		events, eventErr := s.adapter.Events(ctx, handle)
		if eventErr != nil {
			fail("event_stream_failed", eventErr)
			return
		}
		for {
			select {
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					fail("run_timeout", ctx.Err())
				}
				return
			case event, ok := <-events:
				if !ok {
					fail("provider_event_stream_closed", errors.New("OpenCode event stream closed before a terminal event"))
					return
				}
				if event.Error != "" {
					fail("provider_event_stream_failed", errors.New(boundedText(event.Error, 300)))
					return
				}
				if event.Sequence != "" {
					task.ProviderEventCursor = boundedText(event.Sequence, 200)
					updated, persistErr := s.coordinator.Transition(ctx, task, agents.Running, boundedText(event.Type, 80), task.Progress, "")
					if persistErr != nil {
						fail("event_cursor_persistence_failed", persistErr)
						return
					}
					task = updated
					run.EventCursor, run.UpdatedAt = task.ProviderEventCursor, s.now().UTC()
					if persistErr := s.records.SaveRun(ctx, run); persistErr != nil {
						fail("event_cursor_persistence_failed", persistErr)
						return
					}
				}
				// Do not forward raw provider text: it may contain private code, secrets,
				// or untrusted repository instructions. Publish bounded status metadata.
				eventData := map[string]string{"provider_event": boundedText(event.Type, 80), "task_id": task.ID}
				if event.Sequence != "" {
					eventData["provider_sequence"] = boundedText(event.Sequence, 200)
				}
				s.emit(ctx, scope, task, "agent.output", eventData)
				if event.Terminal {
					latest, _ := s.coordinator.GetTaskForScope(ctx, scope.TenantID, scope.UserID, task.ID)
					latest.ProviderTerminalObserved = true
					latest, _ = s.coordinator.Transition(ctx, latest, agents.Running, "provider_idle", latest.Progress, "")
					task = latest
					run.Status, run.UpdatedAt = "provider_idle", s.now().UTC()
					_ = s.records.SaveRun(ctx, run)
					goto collect
				}
			}
		}
	}
collect:
	var err error
	// Keep what the coding agent actually said, so a run is never a black box.
	var summaryRefs []string
	if summarizer, ok := s.adapter.(interface {
		Summary(context.Context, RunHandle) (string, error)
	}); ok {
		if text, summaryErr := summarizer.Summary(ctx, handle); summaryErr == nil && strings.TrimSpace(text) != "" {
			summaryID := stableID("artifact", run.ID, "summary")
			if saveErr := s.records.SaveArtifact(ctx, Artifact{ID: summaryID, TenantID: scope.TenantID, UserID: scope.UserID, ProjectID: task.ProjectID, TaskID: task.ID, TurnID: task.TurnID, RunID: run.ID, Kind: "assistant_summary", Content: boundedText(text, 16*1024), Metadata: map[string]string{"provider": s.adapter.Name()}, CreatedAt: s.now().UTC()}); saveErr == nil {
				summaryRefs = append(summaryRefs, summaryID)
			}
		}
	}
	diff, diffErr := s.adapter.Diff(ctx, handle)
	if diffErr != nil {
		fail("diff_collection_failed", diffErr)
		return
	}
	diffJSON, _ := json.Marshal(diff)
	diffArtifactID := stableID("artifact", run.ID, "diff")
	if err := s.records.SaveArtifact(ctx, Artifact{ID: diffArtifactID, TenantID: scope.TenantID, UserID: scope.UserID, ProjectID: task.ProjectID, TaskID: task.ID, TurnID: task.TurnID, RunID: run.ID, Kind: "diff", Content: string(diffJSON), Metadata: map[string]string{"files": fmt.Sprint(len(diff.Files)), "truncated": fmt.Sprint(diff.Truncated)}, CreatedAt: s.now().UTC()}); err != nil {
		fail("diff_persistence_failed", err)
		return
	}
	latest, reloadErr := s.coordinator.GetTaskForScope(ctx, scope.TenantID, scope.UserID, task.ID)
	if reloadErr != nil {
		fail("task_reload_failed", reloadErr)
		return
	}
	latest.OutputRefs = appendUnique(latest.OutputRefs, append([]string{diffArtifactID}, summaryRefs...)...)
	latest, reloadErr = s.coordinator.Transition(ctx, latest, agents.Running, "diff_collected", 70, "")
	if reloadErr != nil {
		fail("diff_reference_persistence_failed", reloadErr)
		return
	}
	s.emit(ctx, scope, latest, "task.updated", map[string]string{"phase": "diff_collected"})

	// OpenCode finished but changed nothing: that is not a successful coding
	// task. Block it for review instead of running tests on an untouched tree.
	if len(diff.Files) == 0 {
		if workspace, workspaceErr := s.workspaces.UpdateCleanupState(task.ID, "retained_for_review"); workspaceErr == nil {
			_ = s.records.SaveWorkspace(ctx, workspace, scope.TenantID, scope.UserID)
			latest.WorkspaceCleanupState = workspace.CleanupState
		}
		blocked, blockErr := s.coordinator.Transition(ctx, latest, agents.Blocked, "no_changes_review_required", latest.Progress, "OpenCode finished without changing any files; see the assistant summary artifact")
		if blockErr != nil {
			fail("task_completion_persistence_failed", blockErr)
			return
		}
		run.Status, run.UpdatedAt = "blocked", s.now().UTC()
		_ = s.records.SaveRun(ctx, run)
		s.emit(ctx, scope, blocked, "agent.approval_required", map[string]string{"reason": "no_changes", "task_id": task.ID})
		return
	}

	testResult, testErr := s.adapter.Tests(ctx, handle)
	if testErr != nil {
		fail("test_execution_failed", testErr)
		return
	}
	testJSON, _ := json.Marshal(testResult)
	testArtifactID := stableID("artifact", run.ID, "tests")
	if err := s.records.SaveArtifact(ctx, Artifact{ID: testArtifactID, TenantID: scope.TenantID, UserID: scope.UserID, ProjectID: task.ProjectID, TaskID: task.ID, TurnID: task.TurnID, RunID: run.ID, Kind: "test_result", Content: string(testJSON), Metadata: map[string]string{"passed": fmt.Sprint(testResult.Passed), "exit_code": fmt.Sprint(testResult.ExitCode)}, CreatedAt: s.now().UTC()}); err != nil {
		fail("test_persistence_failed", err)
		return
	}
	latest, reloadErr = s.coordinator.GetTaskForScope(ctx, scope.TenantID, scope.UserID, task.ID)
	if reloadErr != nil {
		fail("task_reload_failed", reloadErr)
		return
	}
	latest.OutputRefs = appendUnique(latest.OutputRefs, testArtifactID)
	latest, reloadErr = s.coordinator.Transition(ctx, latest, agents.Running, "tests_completed", 90, "")
	if reloadErr != nil {
		fail("test_reference_persistence_failed", reloadErr)
		return
	}
	s.emit(ctx, scope, latest, "task.updated", map[string]string{"phase": "tests_completed", "tests_passed": fmt.Sprint(testResult.Passed)})

	workspace, workspaceErr := s.workspaces.UpdateCleanupState(task.ID, "retained_for_review")
	if workspaceErr != nil {
		fail("workspace_state_persistence_failed", workspaceErr)
		return
	}
	if err := s.records.SaveWorkspace(ctx, workspace, scope.TenantID, scope.UserID); err != nil {
		fail("workspace_state_persistence_failed", err)
		return
	}
	latest, err = s.coordinator.GetTaskForScope(ctx, scope.TenantID, scope.UserID, task.ID)
	if err != nil {
		fail("task_reload_failed", err)
		return
	}
	latest.WorkspaceCleanupState = workspace.CleanupState
	latest.OutputRefs = appendUnique(latest.OutputRefs, diffArtifactID, testArtifactID)
	status, phase, runStatus, progress := agents.Completed, "completed", "completed", 100
	if !testResult.Passed {
		status, phase, runStatus, progress = agents.Blocked, "tests_failed_review_required", "blocked", latest.Progress
	}
	latest, err = s.coordinator.Transition(ctx, latest, status, phase, progress, "")
	if err != nil {
		fail("task_completion_persistence_failed", err)
		return
	}
	run.Status, run.UpdatedAt = runStatus, s.now().UTC()
	if err := s.records.SaveRun(ctx, run); err != nil {
		s.emit(ctx, scope, latest, "task.updated", map[string]string{"warning": "run status persistence failed"})
		return
	}
	if !testResult.Passed {
		s.emit(ctx, scope, latest, "agent.approval_required", map[string]string{"reason": "tests_failed", "task_id": task.ID})
	} else {
		s.emit(ctx, scope, latest, "task.completed", map[string]string{"task_id": task.ID})
	}
}

func (s *Service) emit(ctx context.Context, scope identity.Scope, task agents.Task, eventType string, data map[string]string) {
	if s.eventSink != nil {
		_ = s.eventSink(ctx, scope, task, eventType, data)
	}
}

func stableID(prefix string, values ...string) string {
	hash := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return prefix + "_" + hex.EncodeToString(hash[:12])
}
func boundedError(err error) string {
	if err == nil {
		return ""
	}
	return boundedText(err.Error(), 2000)
}
func boundedText(value string, max int) string {
	value = strings.TrimSpace(value)
	if len(value) > max {
		return value[:max]
	}
	return value
}
func appendUnique(values []string, additions ...string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, value := range additions {
		if _, ok := seen[value]; !ok {
			values = append(values, value)
			seen[value] = struct{}{}
		}
	}
	return values
}

func isTerminalProviderPhase(phase string) bool {
	phase = strings.ToLower(strings.TrimSpace(phase))
	return strings.Contains(phase, "idle") || strings.Contains(phase, "completed") || strings.Contains(phase, "terminal")
}
