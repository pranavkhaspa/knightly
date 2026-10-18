package profile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type fakeRepository struct {
	value Profile
}

func (f *fakeRepository) Save(_ context.Context, value Profile) error {
	f.value = clone(value)
	return nil
}

func (f *fakeRepository) Get(_ context.Context, scope identity.Scope) (Profile, error) {
	if f.value.UserID == "" || f.value.TenantID != scope.TenantID || f.value.UserID != scope.UserID {
		return Profile{}, ErrProfileNotFound
	}
	return clone(f.value), nil
}

func TestSaveBoundsFieldsAndCopiesMutableInput(t *testing.T) {
	repository := &fakeRepository{}
	service := NewService(repository)
	service.now = func() time.Time { return time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC) }
	value := Profile{
		TenantID: "tenant-a", UserID: "user-a", DisplayName: "Builder",
		Interests: []string{"graphs"}, ContentPreferences: map[string]string{"format": "brief"},
		QuietHours: QuietHours{Start: "22:00", End: "07:00", Timezone: "UTC"},
	}
	saved, err := service.Save(context.Background(), value)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	value.Interests[0] = "mutated"
	value.ContentPreferences["format"] = "mutated"
	if repository.value.Interests[0] != "graphs" || repository.value.ContentPreferences["format"] != "brief" {
		t.Fatal("repository retained caller-owned mutable profile fields")
	}
	if !saved.CreatedAt.Equal(service.now()) || !saved.UpdatedAt.Equal(service.now()) {
		t.Fatalf("timestamps were not initialized: %#v", saved)
	}
}

func TestSaveRejectsOversizedProfileList(t *testing.T) {
	values := make([]string, MaxProfileListItems+1)
	_, err := NewService(&fakeRepository{}).Save(context.Background(), Profile{
		TenantID: "tenant-a", UserID: "user-a", DisplayName: "Builder", Interests: values,
	})
	if !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("Save() error = %v, want ErrInvalidProfile", err)
	}
}

func TestGetDoesNotReturnAProfileFromAnotherScope(t *testing.T) {
	repository := &fakeRepository{value: Profile{TenantID: "tenant-a", UserID: "user-a", DisplayName: "Builder"}}
	_, err := NewService(repository).Get(context.Background(), identity.Scope{TenantID: "tenant-b", UserID: "user-a"})
	if !errors.Is(err, ErrProfileNotFound) {
		t.Fatalf("Get() error = %v, want ErrProfileNotFound", err)
	}
}
