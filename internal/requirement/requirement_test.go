package requirement

import (
	"context"
	"errors"
	"testing"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type fakeRepository struct {
	values []Requirement
}

func (f *fakeRepository) Save(_ context.Context, value Requirement) error {
	f.values = append(f.values, clone(value))
	return nil
}

func (f *fakeRepository) Get(_ context.Context, scope identity.Scope, id string) (Requirement, error) {
	for _, value := range f.values {
		if value.ID == id && sameScope(value, scope) {
			return clone(value), nil
		}
	}
	return Requirement{}, ErrRequirementNotFound
}

func (f *fakeRepository) List(_ context.Context, scope identity.Scope, projectID string, limit int) ([]Requirement, error) {
	result := make([]Requirement, 0, limit)
	for _, value := range f.values {
		if sameScope(value, scope) && value.ProjectID == projectID {
			result = append(result, clone(value))
		}
	}
	return result, nil
}

func TestSaveRequiresAProjectAndBoundsPriority(t *testing.T) {
	_, err := NewService(&fakeRepository{}).Save(context.Background(), Requirement{
		ID: "requirement-a", TenantID: "tenant-a", UserID: "user-a", Title: "Need graph context", Priority: 101,
	})
	if !errors.Is(err, ErrInvalidRequirement) {
		t.Fatalf("Save() error = %v, want ErrInvalidRequirement", err)
	}
}

func TestListUsesProjectAndUserScope(t *testing.T) {
	repository := &fakeRepository{}
	service := NewService(repository)
	for _, value := range []Requirement{
		{ID: "requirement-a", TenantID: "tenant-a", UserID: "user-a", ProjectID: "project-a", Title: "A"},
		{ID: "requirement-b", TenantID: "tenant-b", UserID: "user-b", ProjectID: "project-a", Title: "B"},
	} {
		if _, err := service.Save(context.Background(), value); err != nil {
			t.Fatalf("Save(%q): %v", value.ID, err)
		}
	}
	values, err := service.List(context.Background(), identity.Scope{TenantID: "tenant-a", UserID: "user-a"}, "project-a", 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(values) != 1 || values[0].ID != "requirement-a" {
		t.Fatalf("List() = %#v, want only requirement-a", values)
	}
}
