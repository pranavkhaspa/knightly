package chat

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type turnMemoryRepository struct {
	turns map[string]Turn
}

func newTurnMemoryRepository() *turnMemoryRepository {
	return &turnMemoryRepository{turns: make(map[string]Turn)}
}

func (r *turnMemoryRepository) SaveTurn(_ context.Context, value Turn) error {
	for id, existing := range r.turns {
		if id != value.ID && existing.TenantID == value.TenantID && existing.UserID == value.UserID && existing.ConversationID == value.ConversationID && existing.IdempotencyKey == value.IdempotencyKey {
			return errors.New("duplicate idempotency key")
		}
	}
	r.turns[value.ID] = value
	return nil
}

func (r *turnMemoryRepository) GetTurn(_ context.Context, scope identity.Scope, id string) (Turn, error) {
	value, ok := r.turns[id]
	if !ok || value.TenantID != scope.TenantID || value.UserID != scope.UserID {
		return Turn{}, ErrTurnNotFound
	}
	return value, nil
}

func (r *turnMemoryRepository) GetTurnByIdempotency(_ context.Context, scope identity.Scope, conversationID, key string) (Turn, error) {
	for _, value := range r.turns {
		if value.TenantID == scope.TenantID && value.UserID == scope.UserID && value.ConversationID == conversationID && value.IdempotencyKey == key {
			return value, nil
		}
	}
	return Turn{}, ErrTurnNotFound
}

func (r *turnMemoryRepository) ListTurns(_ context.Context, scope identity.Scope, conversationID string, limit int) ([]Turn, error) {
	values := make([]Turn, 0, limit)
	for _, value := range r.turns {
		if value.TenantID == scope.TenantID && value.UserID == scope.UserID && value.ConversationID == conversationID {
			values = append(values, value)
		}
	}
	return values, nil
}

func TestTurnServiceIdempotencyAndTerminalState(t *testing.T) {
	repository := newTurnMemoryRepository()
	service := NewTurnService(repository)
	service.now = func() time.Time { return time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC) }
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}

	first, created, err := service.Begin(context.Background(), scope, "conversation-a", "request-1", "message-1")
	if err != nil || !created {
		t.Fatalf("first Begin() = %#v, created=%v, err=%v", first, created, err)
	}
	replay, created, err := service.Begin(context.Background(), scope, "conversation-a", "request-1", "message-1")
	if err != nil || created || replay.ID != first.ID {
		t.Fatalf("replay Begin() = %#v, created=%v, err=%v", replay, created, err)
	}
	if _, err := service.Start(context.Background(), scope, first.ID); err != nil {
		t.Fatal(err)
	}
	completed, err := service.Complete(context.Background(), scope, first.ID, "message-2")
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != TurnStatusCompleted || completed.AssistantMessageID != "message-2" || completed.CompletedAt.IsZero() {
		t.Fatalf("completed turn = %#v", completed)
	}
	second, created, err := service.Begin(context.Background(), scope, "conversation-a", "request-1", "message-1")
	if err != nil || created || second.Status != TurnStatusCompleted {
		t.Fatalf("terminal replay = %#v, created=%v, err=%v", second, created, err)
	}
}

func TestTurnServiceCancellationPreservesTerminalState(t *testing.T) {
	repository := newTurnMemoryRepository()
	service := NewTurnService(repository)
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}
	turn, _, err := service.Begin(context.Background(), scope, "conversation-a", "request-2", "message-3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Cancel(context.Background(), scope, turn.ID); err != nil {
		t.Fatal(err)
	}
	cancelled, err := service.Get(context.Background(), scope, turn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != TurnStatusCancelled || cancelled.TerminalResult != "cancelled" {
		t.Fatalf("cancelled turn = %#v", cancelled)
	}
	completed, err := service.Complete(context.Background(), scope, turn.ID, "message-ignored")
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != TurnStatusCancelled || completed.AssistantMessageID != "" {
		t.Fatalf("terminal cancellation was overwritten = %#v", completed)
	}
}
