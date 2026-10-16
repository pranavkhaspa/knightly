package session

import (
	"context"
	"errors"
	"testing"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type fakeRepository struct {
	sessions  map[string]Session
	decisions map[string]Decision
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{sessions: make(map[string]Session), decisions: make(map[string]Decision)}
}

func (f *fakeRepository) SaveSession(_ context.Context, value Session) error {
	f.sessions[value.ID] = cloneSession(value)
	return nil
}

func (f *fakeRepository) GetSession(_ context.Context, scope identity.Scope, id string) (Session, error) {
	value, ok := f.sessions[id]
	if !ok || !sameScope(value.TenantID, value.UserID, scope) {
		return Session{}, ErrSessionNotFound
	}
	return cloneSession(value), nil
}

func (f *fakeRepository) SaveDecision(_ context.Context, value Decision) error {
	if _, ok := f.sessions[value.SessionID]; !ok {
		return ErrSessionNotFound
	}
	f.decisions[value.ID] = cloneDecision(value)
	return nil
}

func (f *fakeRepository) GetDecision(_ context.Context, scope identity.Scope, id string) (Decision, error) {
	value, ok := f.decisions[id]
	if !ok || !sameScope(value.TenantID, value.UserID, scope) {
		return Decision{}, ErrDecisionNotFound
	}
	return cloneDecision(value), nil
}

func (f *fakeRepository) ListDecisions(_ context.Context, scope identity.Scope, sessionID string, limit int) ([]Decision, error) {
	result := make([]Decision, 0, limit)
	for _, value := range f.decisions {
		if sameScope(value.TenantID, value.UserID, scope) && (sessionID == "" || value.SessionID == sessionID) {
			result = append(result, cloneDecision(value))
		}
	}
	return result, nil
}

func TestSessionAndDecisionSurviveThroughTypedFakeRepository(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository)
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}
	createdSession, err := service.SaveSession(context.Background(), Session{ID: "session-a", TenantID: scope.TenantID, UserID: scope.UserID, Title: "Setup"})
	if err != nil {
		t.Fatalf("SaveSession() error = %v", err)
	}
	createdDecision, err := service.SaveDecision(context.Background(), Decision{
		ID: "decision-a", TenantID: scope.TenantID, UserID: scope.UserID, SessionID: createdSession.ID,
		Subject: "Graph store", Choice: "FalkorDB", EvidenceIDs: []string{"evidence-a"},
	})
	if err != nil {
		t.Fatalf("SaveDecision() error = %v", err)
	}
	found, err := service.GetDecision(context.Background(), scope, createdDecision.ID)
	if err != nil {
		t.Fatalf("GetDecision() error = %v", err)
	}
	if found.UserID != scope.UserID || found.SessionID != createdSession.ID || found.Status != DecisionProposed {
		t.Fatalf("GetDecision() = %#v, want scoped proposed decision", found)
	}
}

func TestDecisionRejectsUnboundedEvidenceAndCrossScopeLookup(t *testing.T) {
	repository := newFakeRepository()
	service := NewService(repository)
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}
	if _, err := service.SaveSession(context.Background(), Session{ID: "session-a", TenantID: scope.TenantID, UserID: scope.UserID, Title: "Setup"}); err != nil {
		t.Fatalf("SaveSession() error = %v", err)
	}
	_, err := service.SaveDecision(context.Background(), Decision{
		ID: "decision-a", TenantID: scope.TenantID, UserID: scope.UserID, SessionID: "session-a", Subject: "A", Choice: "B",
		EvidenceIDs: make([]string, MaxEvidenceIDs+1),
	})
	if !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("SaveDecision() error = %v, want ErrInvalidDecision", err)
	}
	if _, err := service.GetSession(context.Background(), identity.Scope{TenantID: "tenant-b", UserID: "user-a"}, "session-a"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("GetSession() error = %v, want ErrSessionNotFound", err)
	}
}
