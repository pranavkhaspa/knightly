package chat

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type memoryRepository struct {
	conversations map[string]Conversation
	messages      []Message
	events        []Event
	sequence      int64
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{conversations: make(map[string]Conversation)}
}

func (r *memoryRepository) SaveConversation(_ context.Context, value Conversation) error {
	r.conversations[value.ID] = value
	return nil
}

func (r *memoryRepository) GetConversation(_ context.Context, scope identity.Scope, id string) (Conversation, error) {
	value, ok := r.conversations[id]
	if !ok || value.TenantID != scope.TenantID || value.UserID != scope.UserID {
		return Conversation{}, ErrConversationNotFound
	}
	return value, nil
}

func (r *memoryRepository) ListConversations(_ context.Context, scope identity.Scope, limit int) ([]Conversation, error) {
	values := make([]Conversation, 0, limit)
	for _, value := range r.conversations {
		if value.TenantID == scope.TenantID && value.UserID == scope.UserID {
			values = append(values, value)
		}
	}
	return values, nil
}

func (r *memoryRepository) SaveMessage(_ context.Context, value Message) error {
	value.Sequence = int64(len(r.messages) + 1)
	r.messages = append(r.messages, value)
	return nil
}

func (r *memoryRepository) ListMessages(_ context.Context, scope identity.Scope, conversationID string, limit int) ([]Message, error) {
	values := make([]Message, 0, limit)
	for _, value := range r.messages {
		if value.TenantID == scope.TenantID && value.UserID == scope.UserID && value.ConversationID == conversationID {
			values = append(values, value)
		}
	}
	return values, nil
}

func (r *memoryRepository) AppendEvent(_ context.Context, value Event) (Event, error) {
	r.sequence++
	value.Sequence = r.sequence
	r.events = append(r.events, value)
	return value, nil
}

func (r *memoryRepository) ListEvents(_ context.Context, scope identity.Scope, conversationID string, after int64, limit int) ([]Event, error) {
	values := make([]Event, 0, limit)
	for _, value := range r.events {
		if value.TenantID == scope.TenantID && value.UserID == scope.UserID && value.ConversationID == conversationID && value.Sequence > after {
			values = append(values, value)
		}
	}
	return values, nil
}

func TestServicePersistsConversationMessageAndReplayableEvent(t *testing.T) {
	repository := newMemoryRepository()
	service := NewService(repository)
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}

	conversation, err := service.CreateConversation(context.Background(), scope, "project-a", "  Search updates  ")
	if err != nil {
		t.Fatal(err)
	}
	if conversation.Title != "Search updates" || conversation.ProjectID != "project-a" {
		t.Fatalf("conversation = %+v", conversation)
	}

	message, err := service.AddMessage(context.Background(), scope, Message{
		ConversationID: conversation.ID,
		Role:           RoleUser,
		Content:        "What changed?",
	})
	if err != nil {
		t.Fatal(err)
	}
	if message.ID == "" || message.TenantID != scope.TenantID || message.UserID != scope.UserID {
		t.Fatalf("message scope/id = %+v", message)
	}

	first, err := service.AppendEvent(context.Background(), scope, Event{
		ID:             "event-one",
		ConversationID: conversation.ID,
		Type:           MessageCreated,
		Data:           map[string]string{"message_id": message.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.AppendEvent(context.Background(), scope, Event{
		ID:             "event-two",
		ConversationID: conversation.ID,
		Type:           TurnStarted,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("event sequences = %d, %d", first.Sequence, second.Sequence)
	}

	events, err := service.ListEvents(context.Background(), scope, conversation.ID, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != "event-two" {
		t.Fatalf("replayed events = %+v", events)
	}
}

func TestServiceRejectsCrossScopeLookupAndOversizedMessage(t *testing.T) {
	service := NewService(newMemoryRepository())
	created, err := service.CreateConversation(context.Background(), identity.Scope{TenantID: "tenant-a", UserID: "user-a"}, "", "title")
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.GetConversation(context.Background(), identity.Scope{TenantID: "tenant-b", UserID: "user-b"}, created.ID)
	if !errors.Is(err, ErrConversationNotFound) && !errors.Is(err, ErrInvalidConversation) {
		t.Fatalf("cross-scope error = %v", err)
	}

	tooLong := make([]rune, MaxMessage+1)
	for index := range tooLong {
		tooLong[index] = 'x'
	}
	_, err = service.AddMessage(context.Background(), identity.Scope{TenantID: "tenant-a", UserID: "user-a"}, Message{
		ConversationID: created.ID,
		Role:           RoleUser,
		Content:        string(tooLong),
		CreatedAt:      time.Now(),
	})
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("oversized message error = %v", err)
	}
}
