package falkor

import (
	"context"
	"errors"
	"testing"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

func TestUserGraphNameIsStableAndDoesNotContainIdentity(t *testing.T) {
	first := UserGraphName("tenant-a", "user@example.com")
	second := UserGraphName("tenant-a", "user@example.com")
	if first != second {
		t.Fatalf("graph name is not stable: %q != %q", first, second)
	}
	if first[:13] != "nightshift_u_" {
		t.Fatalf("unexpected graph name prefix: %q", first)
	}
	if first == "nightshift_u_user@example.com" || first == "nightshift_u_tenant-a" {
		t.Fatal("graph name leaked an identity value")
	}
}

type fakeMappingStore struct {
	scope identity.Scope
	name  string
}

func (f fakeMappingStore) LookupUserGraph(_ context.Context, scope identity.Scope) (string, error) {
	if scope != f.scope {
		return "", ErrUserGraphNotMapped
	}
	return f.name, nil
}

func TestGraphRouterUsesControlMappingForAuthenticatedScope(t *testing.T) {
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}
	name := UserGraphName(scope.TenantID, scope.UserID)
	router := NewGraphRouter(fakeMappingStore{scope: scope, name: name})
	resolved, err := router.Resolve(context.Background(), scope)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved != name {
		t.Fatalf("Resolve() = %q, want control mapping %q", resolved, name)
	}
}

func TestGraphRouterRejectsControlGraphMapping(t *testing.T) {
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}
	_, err := NewGraphRouter(fakeMappingStore{scope: scope, name: "nightshift_control"}).Resolve(context.Background(), scope)
	if !errors.Is(err, ErrInvalidUserGraph) {
		t.Fatalf("Resolve() error = %v, want ErrInvalidUserGraph", err)
	}
}

func TestGraphRouterHasNoRequestGraphInput(t *testing.T) {
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}
	router := NewGraphRouter(fakeMappingStore{scope: scope, name: "nightshift_u_controlled"})
	resolved, err := router.Resolve(context.Background(), scope)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved != "nightshift_u_controlled" {
		t.Fatalf("Resolve() = %q, want only the trusted mapping", resolved)
	}
}
