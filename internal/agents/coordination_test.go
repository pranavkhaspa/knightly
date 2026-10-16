package agents

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeRepository struct {
	tasks        map[string]Task
	handoffs     []Handoff
	persistCalls int
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{tasks: make(map[string]Task)}
}

func (f *fakeRepository) SaveTask(_ context.Context, task Task) error {
	f.tasks[task.ID] = cloneTask(task)
	return nil
}

func (f *fakeRepository) GetTask(_ context.Context, userID, taskID string) (Task, error) {
	task, ok := f.tasks[taskID]
	if !ok || task.UserID != userID {
		return Task{}, ErrTaskNotFound
	}
	return cloneTask(task), nil
}

func (f *fakeRepository) PersistHandoff(_ context.Context, task Task, handoff Handoff) error {
	f.persistCalls++
	f.tasks[task.ID] = cloneTask(task)
	f.handoffs = append(f.handoffs, cloneHandoff(handoff))
	return nil
}

func TestHandoffPersistsTargetTaskAndRelationshipRecord(t *testing.T) {
	repository := newFakeRepository()
	coordinator := NewCoordinator(repository)
	coordinator.now = func() time.Time { return time.Date(2026, 10, 16, 11, 0, 0, 0, time.UTC) }

	_, err := coordinator.CreateTask(context.Background(), Task{
		ID:         "task_relevance",
		TenantID:   "tenant_demo",
		UserID:     "user_demo",
		ProjectID:  "project_nightshift",
		Agent:      RelevanceAgent,
		Status:     Running,
		Priority:   2,
		OutputRefs: []string{"recommendation_1", "evidence_1"},
	})
	if err != nil {
		t.Fatalf("create source task: %v", err)
	}

	handoff, err := coordinator.Handoff(context.Background(), HandoffRequest{
		ID:         "handoff_1",
		UserID:     "user_demo",
		FromTaskID: "task_relevance",
		ToTaskID:   "task_planner",
		ToAgent:    PlannerAgent,
		Notes:      "Turn the recommendation into a bounded plan.",
	})
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	if handoff.FromAgent != RelevanceAgent || handoff.ToAgent != PlannerAgent {
		t.Fatalf("handoff agents = %q -> %q", handoff.FromAgent, handoff.ToAgent)
	}
	target := repository.tasks["task_planner"]
	if target.Agent != PlannerAgent || target.Status != Pending {
		t.Fatalf("target task = %#v", target)
	}
	if len(target.InputRefs) != 2 || target.InputRefs[0] != "recommendation_1" {
		t.Fatalf("target input refs = %#v", target.InputRefs)
	}
	if len(target.DependencyIDs) != 1 || target.DependencyIDs[0] != "task_relevance" {
		t.Fatalf("target dependencies = %#v", target.DependencyIDs)
	}
	if repository.persistCalls != 1 || len(repository.handoffs) != 1 {
		t.Fatalf("persist calls = %d, handoffs = %d", repository.persistCalls, len(repository.handoffs))
	}
}

func TestHandoffRejectsCompletedSource(t *testing.T) {
	repository := newFakeRepository()
	coordinator := NewCoordinator(repository)
	_, err := coordinator.CreateTask(context.Background(), Task{
		ID:       "task_done",
		TenantID: "tenant_demo",
		UserID:   "user_demo",
		Agent:    ScoutAgent,
		Status:   Completed,
		Priority: 1,
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	_, err = coordinator.Handoff(context.Background(), HandoffRequest{
		ID:         "handoff_done",
		UserID:     "user_demo",
		FromTaskID: "task_done",
		ToTaskID:   "task_next",
		ToAgent:    PlannerAgent,
	})
	if !errors.Is(err, ErrInvalidHandoff) {
		t.Fatalf("handoff error = %v, want ErrInvalidHandoff", err)
	}
}
