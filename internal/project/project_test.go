package project

import (
	"context"
	"errors"
	"testing"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type fakeRepository struct {
	values []Project
}

func (f *fakeRepository) Save(_ context.Context, value Project) error {
	f.values = append(f.values, clone(value))
	return nil
}

func (f *fakeRepository) Get(_ context.Context, scope identity.Scope, id string) (Project, error) {
	for _, value := range f.values {
		if value.ID == id && sameScope(value.TenantID, value.UserID, scope) {
			return clone(value), nil
		}
	}
	return Project{}, ErrProjectNotFound
}

func (f *fakeRepository) List(_ context.Context, scope identity.Scope, limit int) ([]Project, error) {
	result := make([]Project, 0, limit)
	for _, value := range f.values {
		if sameScope(value.TenantID, value.UserID, scope) {
			result = append(result, clone(value))
		}
	}
	return result, nil
}

func TestSaveDefaultsStatusAndRejectsRepositoryCredentials(t *testing.T) {
	_, err := NewService(&fakeRepository{}).Save(context.Background(), Project{
		ID: "project-a", TenantID: "tenant-a", UserID: "user-a", Name: "NightShift",
		RepositoryURL: "https://user:secret@example.com/repo",
	})
	if !errors.Is(err, ErrInvalidProject) {
		t.Fatalf("Save() error = %v, want ErrInvalidProject", err)
	}
}

func TestListPreservesUserScope(t *testing.T) {
	repository := &fakeRepository{}
	service := NewService(repository)
	for _, value := range []Project{
		{ID: "project-a", TenantID: "tenant-a", UserID: "user-a", Name: "A"},
		{ID: "project-b", TenantID: "tenant-b", UserID: "user-b", Name: "B"},
	} {
		if _, err := service.Save(context.Background(), value); err != nil {
			t.Fatalf("Save(%q): %v", value.ID, err)
		}
	}
	values, err := service.List(context.Background(), identity.Scope{TenantID: "tenant-a", UserID: "user-a"}, 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(values) != 1 || values[0].ID != "project-a" {
		t.Fatalf("List() = %#v, want only project-a", values)
	}
}

func TestListRejectsUnboundedLimit(t *testing.T) {
	_, err := NewService(&fakeRepository{}).List(context.Background(), identity.Scope{TenantID: "tenant-a", UserID: "user-a"}, MaxListLimit+1)
	if !errors.Is(err, ErrInvalidProject) {
		t.Fatalf("List() error = %v, want ErrInvalidProject", err)
	}
}
