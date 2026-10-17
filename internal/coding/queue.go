package coding

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/agents"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

// QueueWorker treats durable FalkorDB tasks as the queue. It processes at most
// one coding task at a time, so a restart does not lose queued work and a
// second task is not started while the first coding run is active.
type QueueWorker struct {
	Coordinator  *agents.Coordinator
	Service      *Service
	Workspaces   *WorkspaceManager
	Scope        identity.Scope
	Repositories map[string]string
	PollInterval time.Duration
	OnError      func(error)
}

func (w *QueueWorker) Run(ctx context.Context) {
	if w == nil || w.Coordinator == nil || w.Service == nil || w.Workspaces == nil {
		return
	}
	interval := w.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := w.ProcessOnce(ctx); err != nil && w.OnError != nil && ctx.Err() == nil {
			w.OnError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ProcessOnce claims the oldest queued coding task for a configured project.
// The durable task is the source of truth; this method may safely be called
// again after process restart.
func (w *QueueWorker) ProcessOnce(ctx context.Context) error {
	if w == nil || w.Coordinator == nil || w.Service == nil || w.Workspaces == nil {
		return fmt.Errorf("coding queue worker is not configured")
	}
	if err := w.Scope.Validate(); err != nil {
		return err
	}
	projectIDs := make([]string, 0, len(w.Repositories))
	for projectID := range w.Repositories {
		projectIDs = append(projectIDs, projectID)
	}
	sort.Strings(projectIDs)
	var candidates []agents.Task
	for _, projectID := range projectIDs {
		tasks, err := w.Coordinator.ListTasks(ctx, w.Scope.TenantID, w.Scope.UserID, projectID, 100)
		if err != nil {
			return fmt.Errorf("list coding queue for project %s: %w", projectID, err)
		}
		for _, task := range tasks {
			if task.Status == agents.Running && task.Provider == "opencode" {
				if !task.LeaseExpiresAt.IsZero() && !task.LeaseExpiresAt.After(time.Now().UTC()) {
					recovered, recoverErr := w.Coordinator.Transition(ctx, task, agents.Failed, task.Phase, task.Progress, "recovered stale running task")
					if recoverErr != nil {
						return fmt.Errorf("mark expired coding lease for %s: %w", task.ID, recoverErr)
					}
					if recovered.ProviderSessionID == "" {
						w.Service.emit(context.Background(), w.Scope, recovered, "task.failed", map[string]string{"phase": "stale_worker_recovery"})
					}
					task = recovered
				} else {
					return nil
				}
			}
			// Automatically recover only stale coding runs with a durable provider
			// session. Other failed tasks remain explicitly retry-gated.
			if task.Status == agents.Failed && task.Error == "recovered stale running task" && task.ProviderSessionID != "" && strings.TrimSpace(task.CodingPrompt) != "" {
				requeued, requeueErr := w.Coordinator.Transition(ctx, task, agents.Pending, "retrying", task.Progress, "")
				if requeueErr != nil {
					return fmt.Errorf("requeue stale coding task %s: %w", task.ID, requeueErr)
				}
				task = requeued
			}
			if task.Status == agents.Pending && (task.Phase == "coding_queued" || task.Phase == "retrying") && strings.TrimSpace(task.CodingPrompt) != "" {
				candidates = append(candidates, task)
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		if !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].ID < candidates[j].ID
	})
	task := candidates[0]
	repository := strings.TrimSpace(w.Repositories[task.ProjectID])
	if repository == "" {
		return w.failQueued(ctx, task, errors.New("no repository is configured for this project"))
	}
	if task.RepositoryPath != "" && !samePath(task.RepositoryPath, repository) {
		return w.failQueued(ctx, task, errors.New("queued task repository differs from trusted project configuration"))
	}
	baseSHA := strings.TrimSpace(task.BaseSHA)
	if baseSHA == "" {
		var err error
		baseSHA, err = w.Workspaces.HeadSHA(ctx, repository)
		if err != nil {
			return w.failQueued(ctx, task, err)
		}
	}
	request := RunRequest{
		TaskID: task.ID, ProjectID: task.ProjectID, Title: task.CodingTitle,
		Prompt: task.CodingPrompt, ExternalContext: task.CodingExternalContext,
		RepositoryPath: repository, BaseSHA: baseSHA,
	}
	if strings.TrimSpace(request.Title) == "" {
		request.Title = task.CodingPrompt
	}
	started, err := w.Service.Start(ctx, w.Scope, task.ID, request)
	if err != nil {
		latest, readErr := w.Coordinator.GetTaskForScope(ctx, w.Scope.TenantID, w.Scope.UserID, task.ID)
		if readErr == nil && latest.Status == agents.Pending && !errors.Is(err, agents.ErrTaskLeaseHeld) {
			return w.failQueued(ctx, latest, err)
		}
		return err
	}
	_ = started
	return nil
}

func (w *QueueWorker) failQueued(ctx context.Context, task agents.Task, cause error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if task.Status != agents.Pending {
		return cause
	}
	if _, err := w.Service.FailQueued(ctx, w.Scope, task, cause); err != nil {
		return fmt.Errorf("coding queue failure (%v), persist failure: %w", cause, err)
	}
	return cause
}
