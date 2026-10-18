package coding

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/agents"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

type fakeTaskCoordinator struct {
	mu   sync.Mutex
	task agents.Task
}

func (f *fakeTaskCoordinator) GetTaskForScope(_ context.Context, tenantID, userID, taskID string) (agents.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.task.ID != taskID || f.task.TenantID != tenantID || f.task.UserID != userID {
		return agents.Task{}, agents.ErrTaskNotFound
	}
	return f.task, nil
}
func (f *fakeTaskCoordinator) Heartbeat(_ context.Context, task agents.Task, owner string, until time.Time) (agents.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.task.LeaseOwner != "" && f.task.LeaseOwner != owner && f.task.LeaseExpiresAt.After(time.Now()) {
		return agents.Task{}, agents.ErrTaskLeaseHeld
	}
	f.task.LeaseOwner, f.task.LeaseExpiresAt = owner, until
	return f.task, nil
}

func (f *fakeTaskCoordinator) Transition(_ context.Context, task agents.Task, status agents.TaskStatus, phase string, progress int, failure string) (agents.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task.Status, task.Phase, task.Progress, task.Error, task.UpdatedAt = status, phase, progress, failure, time.Now().UTC()
	f.task = task
	return task, nil
}

type fakeCodingRecords struct {
	mu         sync.Mutex
	workspaces []Workspace
	runs       []RunRecord
	artifacts  []Artifact
	done       chan struct{}
}

func (f *fakeCodingRecords) SaveWorkspace(_ context.Context, w Workspace, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.workspaces = append(f.workspaces, w)
	return nil
}
func (f *fakeCodingRecords) SaveRun(_ context.Context, run RunRecord) error {
	f.mu.Lock()
	f.runs = append(f.runs, run)
	f.mu.Unlock()
	if run.Status == "completed" {
		select {
		case f.done <- struct{}{}:
		default:
		}
	}
	return nil
}
func (f *fakeCodingRecords) SaveArtifact(_ context.Context, artifact Artifact) error {
	f.mu.Lock()
	f.artifacts = append(f.artifacts, artifact)
	f.mu.Unlock()
	return nil
}
func (f *fakeCodingRecords) GetArtifact(context.Context, string, string, string) (Artifact, error) {
	return Artifact{}, ErrArtifactNotFound
}

type fakeCodingAdapter struct{}

func (fakeCodingAdapter) Name() string { return "fake" }
func (fakeCodingAdapter) Capabilities(context.Context) (Capabilities, error) {
	return Capabilities{Events: true, Cancel: true, Resume: true, Diff: true, TestResults: true}, nil
}
func (fakeCodingAdapter) Start(_ context.Context, req RunRequest) (RunHandle, error) {
	return RunHandle{TaskID: req.TaskID, ProjectID: req.ProjectID, Provider: "fake", ProviderSessionID: "session-1", WorkspacePath: req.WorkspacePath}, nil
}
func (fakeCodingAdapter) Events(_ context.Context, handle RunHandle) (<-chan AgentEvent, error) {
	ch := make(chan AgentEvent, 1)
	ch <- AgentEvent{Type: "session.idle", TaskID: handle.TaskID, Terminal: true}
	close(ch)
	return ch, nil
}
func (fakeCodingAdapter) Cancel(context.Context, RunHandle) error { return nil }
func (fakeCodingAdapter) Resume(ctx context.Context, session string, req RunRequest) (RunHandle, error) {
	return fakeCodingAdapter{}.Start(ctx, req)
}
func (fakeCodingAdapter) Diff(_ context.Context, handle RunHandle) (DiffResult, error) {
	return DiffResult{TaskID: handle.TaskID, SessionID: handle.ProviderSessionID, Files: []FileDiff{{Path: "main.go", Patch: "+fixed"}}}, nil
}
func (fakeCodingAdapter) Tests(_ context.Context, handle RunHandle) (TestResult, error) {
	return TestResult{TaskID: handle.TaskID, Passed: true, ExitCode: 0}, nil
}

func TestServiceRunsTaskAndPersistsArtifacts(t *testing.T) {
	repo, sha := initTestRepo(t)
	manager, err := NewWorkspaceManager(WorkspaceManagerConfig{Root: filepath.Join(t.TempDir(), "workspaces"), AllowedRepositories: []string{repo}})
	if err != nil {
		t.Fatal(err)
	}
	scope := identity.Scope{TenantID: "tenant-1", UserID: "user-1"}
	coordinator := &fakeTaskCoordinator{task: agents.Task{ID: "task-1", TenantID: scope.TenantID, UserID: scope.UserID, ProjectID: "project-1", Agent: agents.PlannerAgent, Status: agents.Pending}}
	records := &fakeCodingRecords{done: make(chan struct{}, 1)}
	service, err := NewService(coordinator, manager, fakeCodingAdapter{}, records, nil, ServiceConfig{RunTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.Start(context.Background(), scope, "task-1", RunRequest{RepositoryPath: repo, BaseSHA: sha, Title: "Fix bug", Prompt: "Fix the bug"})
	if err != nil {
		t.Fatal(err)
	}
	if started.Status != agents.Running || started.ProviderSessionID != "session-1" {
		t.Fatalf("unexpected started task: %#v", started)
	}
	select {
	case <-records.done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for persisted artifacts")
	}
	coordinator.mu.Lock()
	final := coordinator.task
	coordinator.mu.Unlock()
	if final.Status != agents.Completed {
		t.Fatalf("final status = %s, want completed; phase=%s error=%s", final.Status, final.Phase, final.Error)
	}
	if len(final.OutputRefs) != 2 {
		t.Fatalf("output refs = %v, want diff and test artifact", final.OutputRefs)
	}
	if _, err := os.Stat(filepath.Join(started.WorkspacePath, "main.go")); err != nil {
		t.Fatalf("expected workspace to remain available for review: %v", err)
	}
}
