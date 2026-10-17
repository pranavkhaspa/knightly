// Package chat contains the durable conversation boundary used by the web
// chat and by provider-backed turns. It deliberately has no HTTP or FalkorDB
// dependencies; those concerns live in the application and repository layers.
package chat

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	MaxTitle         = 160
	MaxMessage       = 32000
	MaxEventData     = 16000
	MaxCitationCount = 20
	DefaultLimit     = 50
	MaxLimit         = 100
)

var (
	ErrInvalidConversation  = errors.New("invalid conversation")
	ErrConversationNotFound = errors.New("conversation not found")
	ErrInvalidMessage       = errors.New("invalid chat message")
	ErrMessageNotFound      = errors.New("chat message not found")
	ErrInvalidEvent         = errors.New("invalid chat event")
)

type ConversationStatus string

const (
	ConversationActive   ConversationStatus = "active"
	ConversationArchived ConversationStatus = "archived"
)

func (s ConversationStatus) Valid() bool {
	return s == ConversationActive || s == ConversationArchived
}

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleSystem    Role = "system"
	RoleTool      Role = "tool"
	RoleProgress  Role = "progress"
)

func (r Role) Valid() bool {
	switch r {
	case RoleUser, RoleAssistant, RoleSystem, RoleTool, RoleProgress:
		return true
	default:
		return false
	}
}

type EventType string

const (
	ConversationCreated EventType = "conversation.created"
	MessageCreated      EventType = "message.created"
	TurnStarted         EventType = "turn.started"
	TurnProgress        EventType = "turn.progress"
	AssistantCompleted  EventType = "assistant.completed"
	TurnCompleted       EventType = "turn.completed"
	TurnFailed          EventType = "turn.failed"
)

type Citation struct {
	URL     string `json:"url"`
	Title   string `json:"title,omitempty"`
	Source  string `json:"source,omitempty"`
	Excerpt string `json:"excerpt,omitempty"`
	// Origin marks the citation as untrusted external content (for example
	// "web", "mail", "calendar", or "github"). An empty origin means the text
	// did not come from an external provider.
	Origin string `json:"origin,omitempty"`
}

type Conversation struct {
	ID              string             `json:"id"`
	TenantID        string             `json:"tenant_id"`
	UserID          string             `json:"user_id"`
	ProjectID       string             `json:"project_id,omitempty"`
	Title           string             `json:"title"`
	Status          ConversationStatus `json:"status"`
	EventSequence   int64              `json:"event_sequence"`
	MessageSequence int64              `json:"message_sequence"`
	CreatedAt       time.Time          `json:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at"`
}

type Message struct {
	ID             string     `json:"id"`
	TenantID       string     `json:"tenant_id"`
	UserID         string     `json:"user_id"`
	ConversationID string     `json:"conversation_id"`
	TurnID         string     `json:"turn_id,omitempty"`
	Sequence       int64      `json:"sequence"`
	Role           Role       `json:"role"`
	Content        string     `json:"content"`
	Citations      []Citation `json:"citations,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

type Event struct {
	ID             string            `json:"event_id"`
	TenantID       string            `json:"tenant_id"`
	UserID         string            `json:"user_id"`
	ConversationID string            `json:"conversation_id"`
	TurnID         string            `json:"turn_id,omitempty"`
	TaskID         string            `json:"task_id,omitempty"`
	RunID          string            `json:"run_id,omitempty"`
	Sequence       int64             `json:"sequence"`
	Type           EventType         `json:"event_type"`
	Data           map[string]string `json:"data,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
}

type Repository interface {
	SaveConversation(context.Context, Conversation) error
	GetConversation(context.Context, identity.Scope, string) (Conversation, error)
	ListConversations(context.Context, identity.Scope, int) ([]Conversation, error)
	SaveMessage(context.Context, Message) error
	ListMessages(context.Context, identity.Scope, string, int) ([]Message, error)
	AppendEvent(context.Context, Event) (Event, error)
	ListEvents(context.Context, identity.Scope, string, int64, int) ([]Event, error)
}

type Service struct {
	repository Repository
	now        func() time.Time
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository, now: func() time.Time { return time.Now().UTC() }}
}

func (s *Service) CreateConversation(ctx context.Context, scope identity.Scope, projectID, title string) (Conversation, error) {
	if s == nil || s.repository == nil {
		return Conversation{}, fmt.Errorf("%w: repository is required", ErrInvalidConversation)
	}
	if err := scope.Validate(); err != nil {
		return Conversation{}, fmt.Errorf("%w: %v", ErrInvalidConversation, err)
	}
	value := Conversation{
		ID:        newID("conversation"),
		TenantID:  scope.TenantID,
		UserID:    scope.UserID,
		ProjectID: strings.TrimSpace(projectID),
		Title:     strings.TrimSpace(title),
		Status:    ConversationActive,
		CreatedAt: s.now().UTC(),
		UpdatedAt: s.now().UTC(),
	}
	if value.Title == "" {
		value.Title = "New conversation"
	}
	if err := validateConversation(value); err != nil {
		return Conversation{}, err
	}
	if err := s.repository.SaveConversation(ctx, value); err != nil {
		return Conversation{}, fmt.Errorf("save conversation: %w", err)
	}
	return value, nil
}

func (s *Service) UpdateConversation(ctx context.Context, scope identity.Scope, value Conversation) (Conversation, error) {
	if s == nil || s.repository == nil {
		return Conversation{}, fmt.Errorf("%w: repository is required", ErrInvalidConversation)
	}
	if err := scope.Validate(); err != nil {
		return Conversation{}, fmt.Errorf("%w: %v", ErrInvalidConversation, err)
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	value.ID = strings.TrimSpace(value.ID)
	value.ProjectID = strings.TrimSpace(value.ProjectID)
	value.Title = strings.TrimSpace(value.Title)
	if value.Status == "" {
		value.Status = ConversationActive
	}
	value.UpdatedAt = s.now().UTC()
	if err := validateConversation(value); err != nil {
		return Conversation{}, err
	}
	if err := s.repository.SaveConversation(ctx, value); err != nil {
		return Conversation{}, fmt.Errorf("update conversation: %w", err)
	}
	return value, nil
}

func (s *Service) GetConversation(ctx context.Context, scope identity.Scope, id string) (Conversation, error) {
	if s == nil || s.repository == nil {
		return Conversation{}, fmt.Errorf("%w: repository is required", ErrConversationNotFound)
	}
	if err := validateLookup(scope, id, ErrConversationNotFound); err != nil {
		return Conversation{}, err
	}
	value, err := s.repository.GetConversation(ctx, scope, strings.TrimSpace(id))
	if err != nil {
		return Conversation{}, fmt.Errorf("get conversation: %w", err)
	}
	if value.TenantID != scope.TenantID || value.UserID != scope.UserID {
		return Conversation{}, fmt.Errorf("%w: repository returned another scope", ErrInvalidConversation)
	}
	return value, nil
}

func (s *Service) ListConversations(ctx context.Context, scope identity.Scope, limit int) ([]Conversation, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidConversation)
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = DefaultLimit
	}
	if limit < 1 || limit > MaxLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidConversation, MaxLimit)
	}
	values, err := s.repository.ListConversations(ctx, scope, limit)
	if err != nil {
		return nil, fmt.Errorf("list conversations: %w", err)
	}
	return values, nil
}

func (s *Service) AddMessage(ctx context.Context, scope identity.Scope, value Message) (Message, error) {
	if s == nil || s.repository == nil {
		return Message{}, fmt.Errorf("%w: repository is required", ErrInvalidMessage)
	}
	if err := scope.Validate(); err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	value.ID = strings.TrimSpace(value.ID)
	if value.ID == "" {
		value.ID = newID("message")
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	value.ConversationID = strings.TrimSpace(value.ConversationID)
	value.TurnID = strings.TrimSpace(value.TurnID)
	value.Content = strings.TrimSpace(value.Content)
	if value.CreatedAt.IsZero() {
		value.CreatedAt = s.now().UTC()
	}
	if err := validateMessage(value); err != nil {
		return Message{}, err
	}
	if err := s.repository.SaveMessage(ctx, value); err != nil {
		return Message{}, fmt.Errorf("save message: %w", err)
	}
	return value, nil
}

func (s *Service) ListMessages(ctx context.Context, scope identity.Scope, conversationID string, limit int) ([]Message, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidMessage)
	}
	if err := validateLookup(scope, conversationID, ErrMessageNotFound); err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = DefaultLimit
	}
	if limit < 1 || limit > MaxLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidMessage, MaxLimit)
	}
	values, err := s.repository.ListMessages(ctx, scope, conversationID, limit)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	return values, nil
}

func (s *Service) AppendEvent(ctx context.Context, scope identity.Scope, value Event) (Event, error) {
	if s == nil || s.repository == nil {
		return Event{}, fmt.Errorf("%w: repository is required", ErrInvalidEvent)
	}
	if err := scope.Validate(); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	value.ID = strings.TrimSpace(value.ID)
	if value.ID == "" {
		value.ID = newID("event")
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	value.ConversationID = strings.TrimSpace(value.ConversationID)
	value.Type = EventType(strings.TrimSpace(string(value.Type)))
	if value.CreatedAt.IsZero() {
		value.CreatedAt = s.now().UTC()
	}
	if err := validateEvent(value); err != nil {
		return Event{}, err
	}
	stored, err := s.repository.AppendEvent(ctx, value)
	if err != nil {
		return Event{}, fmt.Errorf("append event: %w", err)
	}
	return stored, nil
}

func (s *Service) ListEvents(ctx context.Context, scope identity.Scope, conversationID string, after int64, limit int) ([]Event, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidEvent)
	}
	if err := validateLookup(scope, conversationID, ErrInvalidEvent); err != nil {
		return nil, err
	}
	if after < 0 {
		return nil, fmt.Errorf("%w: event cursor cannot be negative", ErrInvalidEvent)
	}
	if limit == 0 {
		limit = MaxLimit
	}
	if limit < 1 || limit > MaxLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidEvent, MaxLimit)
	}
	values, err := s.repository.ListEvents(ctx, scope, conversationID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	return values, nil
}

func validateConversation(value Conversation) error {
	if err := identity.ValidateID("conversation ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConversation, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConversation, err)
	}
	if value.ProjectID != "" {
		if err := identity.ValidateID("project ID", value.ProjectID); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidConversation, err)
		}
	}
	if value.Title == "" || len([]rune(value.Title)) > MaxTitle {
		return fmt.Errorf("%w: title is required and must be at most %d characters", ErrInvalidConversation, MaxTitle)
	}
	if !value.Status.Valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidConversation, value.Status)
	}
	return nil
}

func validateMessage(value Message) error {
	if err := identity.ValidateID("message ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if err := identity.ValidateID("conversation ID", value.ConversationID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if value.TurnID != "" {
		if err := identity.ValidateID("turn ID", value.TurnID); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
		}
	}
	if !value.Role.Valid() || value.Content == "" || len([]rune(value.Content)) > MaxMessage {
		return fmt.Errorf("%w: role/content is invalid or content exceeds its bound", ErrInvalidMessage)
	}
	if len(value.Citations) > MaxCitationCount {
		return fmt.Errorf("%w: too many citations", ErrInvalidMessage)
	}
	return nil
}

func validateEvent(value Event) error {
	if err := identity.ValidateID("event ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if err := identity.ValidateID("conversation ID", value.ConversationID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if value.TurnID != "" {
		if err := identity.ValidateID("turn ID", value.TurnID); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidEvent, err)
		}
	}
	if value.Type == "" || len([]rune(string(value.Type))) > 80 {
		return fmt.Errorf("%w: event type is required and bounded", ErrInvalidEvent)
	}
	for key, item := range value.Data {
		if len([]rune(key)) > 80 || len([]rune(item)) > MaxEventData {
			return fmt.Errorf("%w: event data is too large", ErrInvalidEvent)
		}
	}
	return nil
}

func validateLookup(scope identity.Scope, id string, notFound error) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID("ID", id); err != nil {
		return fmt.Errorf("%w: %v", notFound, err)
	}
	return nil
}

func newID(prefix string) string {
	var bytes [12]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(bytes[:])
}
