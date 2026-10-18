package coding

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/agents"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

type queueTestRepository struct {
	mu    sync.Mutex
	tasks map[string]agents.Task
}

func (r *queueTestRepository) SaveTask(_ context.Context, task agents.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tasks == nil {
		r.tasks = make(map[string]agents.Task)
	}
	r.tasks[task.ID] = task
	return nil
}

func (r *queueTestRepository) GetTask(_ context.Context, userID, taskID string) (agents.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	task, ok := r.tasks[taskID]
	if !ok || task.UserID != userID {
		return agents.Task{}, agents.ErrTaskNotFound
	}
	return task, nil
}

func (r *queueTestRepository) PersistHandoff(context.Context, agents.Task, agents.Handoff) error {
	return errors.New("not used")
}

func (r *queueTestRepository) ListTasks(_ context.Context, tenantID, userID, projectID string, limit int) ([]agents.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []agents.Task
	for _, task := range r.tasks {
		if task.TenantID == tenantID && task.UserID == userID &&
			(projectID == "" || task.ProjectID == projectID) {
			out = append(out, task)
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *queueTestRepository) HeartbeatTask(_ context.Context, task agents.Task, owner string, until time.Time) (agents.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stored, ok := r.tasks[task.ID]
	if !ok {
		return agents.Task{}, agents.ErrTaskNotFound
	}
	if stored.LeaseOwner != "" && stored.LeaseOwner != owner && stored.LeaseExpiresAt.After(time.Now()) {
		return agents.Task{}, agents.ErrTaskLeaseHeld
	}

	stored.LeaseOwner, stored.LeaseExpiresAt = owner, until
	r.tasks[task.ID] = stored
	return stored, nil
}

func TestQueueWorkerExecutesPersistedCodingTask(t *testing.T) {
	repoPath, _ := initTestRepo(t)
	scope := identity.Scope{TenantID: "tenant-queue", UserID: "user-queue"}

	repository := &queueTestRepository{tasks: map[string]agents.Task{
		"task-queue": {
			ID: "task-queue", TenantID: scope.TenantID, UserID: scope.UserID,
			ProjectID: "project-queue", Agent: agents.CodingAgent,
			Status: agents.Pending, Phase: "coding_queued",
			CodingTitle: "Fix a bug", CodingPrompt: "Fix the bug",
			RepositoryPath: repoPath, CreatedAt: time.Now().UTC(),
		},
	}}

	coordinator := agents.NewCoordinator(repository)
	manager, err := NewWorkspaceManager(WorkspaceManagerConfig{
		Root:                filepath.Join(t.TempDir(), "workspaces"),
		AllowedRepositories: []string{repoPath},
	})
	if err != nil {
		t.Fatal(err)
	}

	records := &fakeCodingRecords{done: make(chan struct{}, 2)}
	service, err := NewService(
		coordinator, manager, fakeCodingAdapter{}, records, nil,
		ServiceConfig{RunTimeout: 5 * time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}

	worker := &QueueWorker{
		Coordinator: coordinator, Service: service, Workspaces: manager,
		Scope: scope, Repositories: map[string]string{"project-queue": repoPath},
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	select {
	case <-records.done:
	case <-time.After(3 * time.Second):
		t.Fatal("queued coding task did not finish")
	}

	final, err := coordinator.GetTaskForScope(
		context.Background(), scope.TenantID, scope.UserID, "task-queue",
	)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != agents.Completed {
		t.Fatalf("task status = %s, phase=%s error=%s", final.Status, final.Phase, final.Error)
	}
	if final.WorkspacePath == "" || len(final.OutputRefs) != 2 {
		t.Fatalf("expected workspace and diff/test refs, got %#v", final)
	}
}

func TestQueueWorkerCompletesAgainstFakeOpenCodeServer(t *testing.T) {
	repoPath, _ := initTestRepo(t)
	scope := identity.Scope{TenantID: "tenant-e2e", UserID: "user-e2e"}

	repository := &queueTestRepository{tasks: map[string]agents.Task{
		"task-e2e": {
			ID: "task-e2e", TenantID: scope.TenantID, UserID: scope.UserID,
			ProjectID: "project-e2e", Agent: agents.CodingAgent,
			Status: agents.Pending, Phase: "coding_queued",
			CodingTitle: "Fix a bug", CodingPrompt: "Fix the bug",
			RepositoryPath: repoPath, CreatedAt: time.Now().UTC(),
		},
	}}

	coordinator := agents.NewCoordinator(repository)
	manager, err := NewWorkspaceManager(WorkspaceManagerConfig{
		Root:                filepath.Join(t.TempDir(), "workspaces"),
		AllowedRepositories: []string{repoPath},
	})
	if err != nil {
		t.Fatal(err)
	}

	var sessionWorkspace string
	var workspaceMu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			directory := r.URL.Query().Get("directory")
			if directory == "" {
				t.Error("OpenCode session must receive a workspace directory")
			}
			if directory == repoPath {
				t.Error("OpenCode must receive the isolated worktree, not the source repository")
			}

			workspaceMu.Lock()
			sessionWorkspace = directory
			workspaceMu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"id": "ses_e2e", "directory": directory})

		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_e2e/prompt_async":
			// Simulate OpenCode editing a file inside the isolated worktree.
			workspaceMu.Lock()
			directory := sessionWorkspace
			workspaceMu.Unlock()
			if err := os.WriteFile(filepath.Join(directory, "opencode_edit.txt"), []byte("edited by fake OpenCode\n"), 0o644); err != nil {
				t.Errorf("simulate OpenCode edit: %v", err)
			}
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodGet && r.URL.Path == "/session/ses_e2e/message":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"info":{"id":"msg_1","role":"user","time":{"created":1}},"parts":[{"type":"text","text":"Fix the bug"}]},{"info":{"id":"msg_2","role":"assistant","time":{"created":2,"completed":3}},"parts":[{"type":"text","text":"Fixed the bug in opencode_edit.txt"}]}]`))

		case r.Method == http.MethodGet && r.URL.Path == "/session/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	runner := &CommandTestRunner{
		Workspaces: manager,
		CommandsByProject: map[string][]string{
			"project-e2e": {"go", "test", "./..."},
		},
		Timeout: 10 * time.Second,
	}

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{
		BaseURL: server.URL, TestRunner: runner, PollInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	records := &fakeCodingRecords{done: make(chan struct{}, 2)}
	service, err := NewService(
		coordinator, manager, adapter, records, nil,
		ServiceConfig{RunTimeout: 5 * time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}

	worker := &QueueWorker{
		Coordinator: coordinator, Service: service, Workspaces: manager,
		Scope: scope, Repositories: map[string]string{"project-e2e": repoPath},
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	select {
	case <-records.done:
	case <-time.After(4 * time.Second):
		t.Fatal("fake OpenCode coding run did not finish")
	}

	final, err := coordinator.GetTaskForScope(
		context.Background(), scope.TenantID, scope.UserID, "task-e2e",
	)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != agents.Completed {
		t.Fatalf("task status = %s, phase=%s error=%s", final.Status, final.Phase, final.Error)
	}
	if len(records.artifacts) != 3 {
		t.Fatalf("persisted artifacts = %d, want diff, assistant summary and test result", len(records.artifacts))
	}
	kinds := map[string]bool{}
	for _, artifact := range records.artifacts {
		kinds[artifact.Kind] = true
	}
	for _, kind := range []string{"diff", "assistant_summary", "test_result"} {
		if !kinds[kind] {
			t.Fatalf("missing %s artifact: %#v", kind, records.artifacts)
		}
	}
}
