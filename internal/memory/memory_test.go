package memory

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeRepository struct {
	items      map[string]Memory
	supersedes []struct {
		previous string
		current  string
	}
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{items: make(map[string]Memory)}
}

func (f *fakeRepository) Save(_ context.Context, item Memory) error {
	f.items[item.ID] = cloneMemory(item)
	return nil
}

func (f *fakeRepository) Supersede(_ context.Context, previousID string, replacement Memory) error {
	if _, ok := f.items[previousID]; !ok {
		return ErrMemoryNotFound
	}
	replacement.SupersedesID = previousID
	f.items[replacement.ID] = cloneMemory(replacement)
	f.supersedes = append(f.supersedes, struct {
		previous string
		current  string
	}{previous: previousID, current: replacement.ID})
	return nil
}

func (f *fakeRepository) Get(_ context.Context, userID, id string) (Memory, error) {
	item, ok := f.items[id]
	if !ok || item.UserID != userID {
		return Memory{}, ErrMemoryNotFound
	}
	return cloneMemory(item), nil
}

func (f *fakeRepository) Retrieve(_ context.Context, _ RetrievalQuery) ([]Memory, error) {
	items := make([]Memory, 0, len(f.items))
	for _, item := range f.items {
		items = append(items, cloneMemory(item))
	}
	return items, nil
}

func TestEvolvePersistsTypedMemoryAndHistory(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository)
	service.now = func() time.Time { return time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC) }

	previous, err := service.Remember(context.Background(), Memory{
		ID:        "memory_decision_v1",
		TenantID:  "tenant_demo",
		UserID:    "user_demo",
		ProjectID: "project_nightshift",
		Type:      Semantic,
		Content:   "FalkorDB is the authoritative product store.",
	})
	if err != nil {
		t.Fatalf("remember previous memory: %v", err)
	}

	current, err := service.Evolve(context.Background(), previous.ID, Memory{
		ID:          "memory_decision_v2",
		TenantID:    previous.TenantID,
		UserID:      previous.UserID,
		ProjectID:   previous.ProjectID,
		Type:        Semantic,
		Content:     "FalkorDB remains authoritative; transport buffers cannot answer product queries.",
		EvidenceIDs: []string{"evidence_architecture"},
	})
	if err != nil {
		t.Fatalf("evolve memory: %v", err)
	}
	if current.SupersedesID != previous.ID {
		t.Fatalf("replacement supersedes %q, want %q", current.SupersedesID, previous.ID)
	}
	if got := repository.items[previous.ID].SupersedesID; got != "" {
		t.Fatalf("previous memory was overwritten with supersedes ID %q", got)
	}
	if len(repository.supersedes) != 1 || repository.supersedes[0].current != current.ID {
		t.Fatalf("SUPERSEDES history was not recorded: %#v", repository.supersedes)
	}
	if got := repository.items[current.ID].Type; got != Semantic {
		t.Fatalf("replacement type = %q, want %q", got, Semantic)
	}
}

func TestEvolveRejectsTypeChange(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository)
	_, err := service.Remember(context.Background(), Memory{ID: "m1", UserID: "u1", Type: Episodic, Content: "a"})
	if err != nil {
		t.Fatalf("remember: %v", err)
	}
	_, err = service.Evolve(context.Background(), "m1", Memory{ID: "m2", UserID: "u1", Type: Procedural, Content: "b"})
	if !errors.Is(err, ErrInvalidSupersession) {
		t.Fatalf("evolve error = %v, want ErrInvalidSupersession", err)
	}
}

func TestRetrieveAppliesByteBoundAfterRepositoryResult(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository)
	_, _ = service.Remember(context.Background(), Memory{ID: "m1", UserID: "u1", Type: Episodic, Content: "12345"})
	_, _ = service.Remember(context.Background(), Memory{ID: "m2", UserID: "u1", Type: Episodic, Content: "67890"})

	items, err := service.Retrieve(context.Background(), RetrievalQuery{UserID: "u1", Limit: 10, MaxBytes: 5})
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("retrieved %d memories under 5-byte bound, want 1", len(items))
	}
}
