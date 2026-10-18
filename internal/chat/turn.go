package chat

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	MaxIdempotencyKey = 200
	MaxTurnError      = 2000
)

var (
	ErrInvalidTurn  = errors.New("invalid chat turn")
	ErrTurnNotFound = errors.New("chat turn not found")
	ErrTurnConflict = errors.New("chat turn idempotency key conflict")
)

type TurnStatus string

const (
	TurnStatusPending   TurnStatus = "pending"
	TurnStatusRunning   TurnStatus = "running"
	TurnStatusCompleted TurnStatus = "completed"
	TurnStatusFailed    TurnStatus = "failed"
	TurnStatusCancelled TurnStatus = "cancelled"
)

func (s TurnStatus) Valid() bool {
	switch s {
	case TurnStatusPending, TurnStatusRunning, TurnStatusCompleted, TurnStatusFailed, TurnStatusCancelled:
		return true
	default:
		return false
	}
}

func (s TurnStatus) Terminal() bool {
	return s == TurnStatusCompleted || s == TurnStatusFailed || s == TurnStatusCancelled
}

type Turn struct {
	ID                 string     `json:"id"`
	TenantID           string     `json:"tenant_id"`
	UserID             string     `json:"user_id"`
	ConversationID     string     `json:"conversation_id"`
	IdempotencyKey     string     `json:"idempotency_key"`
	Status             TurnStatus `json:"status"`
	UserMessageID      string     `json:"user_message_id,omitempty"`
	AssistantMessageID string     `json:"assistant_message_id,omitempty"`
	TerminalResult     string     `json:"terminal_result,omitempty"`
	Error              string     `json:"error,omitempty"`
	CancelRequested    bool       `json:"cancel_requested,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	CompletedAt        time.Time  `json:"completed_at,omitempty"`
}

type TurnRepository interface {
	SaveTurn(context.Context, Turn) error
	GetTurn(context.Context, identity.Scope, string) (Turn, error)
	GetTurnByIdempotency(context.Context, identity.Scope, string, string) (Turn, error)
	ListTurns(context.Context, identity.Scope, string, int) ([]Turn, error)
}

type AtomicTurnRepository interface {
	CreateOrGetTurn(context.Context, Turn) (Turn, bool, error)
}

type ConditionalTurnRepository interface {
	TransitionTurn(context.Context, identity.Scope, string, string, string, string) (Turn, error)
}

type TurnService struct {
	repository TurnRepository
	now        func() time.Time
}

func NewTurnService(repository TurnRepository) *TurnService {
	return &TurnService{repository: repository, now: func() time.Time { return time.Now().UTC() }}
}

// Begin returns an existing turn for a repeated idempotency key. The boolean
// reports whether a new durable turn was created.
func (s *TurnService) Begin(ctx context.Context, scope identity.Scope, conversationID, key, userMessageID string) (Turn, bool, error) {
	if s == nil || s.repository == nil {
		return Turn{}, false, fmt.Errorf("%w: repository is required", ErrInvalidTurn)
	}
	if err := scope.Validate(); err != nil {
		return Turn{}, false, fmt.Errorf("%w: %v", ErrInvalidTurn, err)
	}
	conversationID = strings.TrimSpace(conversationID)
	key = strings.TrimSpace(key)
	userMessageID = strings.TrimSpace(userMessageID)
	if err := identity.ValidateID("conversation ID", conversationID); err != nil {
		return Turn{}, false, fmt.Errorf("%w: %v", ErrInvalidTurn, err)
	}
	if key == "" || len([]rune(key)) > MaxIdempotencyKey {
		return Turn{}, false, fmt.Errorf("%w: idempotency key is required and bounded", ErrInvalidTurn)
	}
	if userMessageID != "" {
		if err := identity.ValidateID("user message ID", userMessageID); err != nil {
			return Turn{}, false, fmt.Errorf("%w: %v", ErrInvalidTurn, err)
		}
	}
	if existing, err := s.repository.GetTurnByIdempotency(ctx, scope, conversationID, key); err == nil {
		if existing.UserMessageID != "" && userMessageID != "" && existing.UserMessageID != userMessageID {
			return Turn{}, false, fmt.Errorf("%w: existing turn has another user message", ErrTurnConflict)
		}
		return existing, false, nil
	} else if !errors.Is(err, ErrTurnNotFound) {
		return Turn{}, false, fmt.Errorf("find idempotent turn: %w", err)
	}
	now := s.now().UTC()
	turn := Turn{
		ID: newID("turn"), TenantID: scope.TenantID, UserID: scope.UserID,
		ConversationID: conversationID, IdempotencyKey: key, Status: TurnStatusPending,
		UserMessageID: userMessageID, CreatedAt: now, UpdatedAt: now,
	}
	if err := validateTurn(turn); err != nil {
		return Turn{}, false, err
	}
	if atomicRepository, ok := s.repository.(AtomicTurnRepository); ok {
		stored, created, err := atomicRepository.CreateOrGetTurn(ctx, turn)
		if err != nil {
			return Turn{}, false, fmt.Errorf("create or get turn: %w", err)
		}
		if err := validateTurn(stored); err != nil {
			return Turn{}, false, err
		}
		if !created && stored.UserMessageID != "" && userMessageID != "" && stored.UserMessageID != userMessageID {
			return Turn{}, false, fmt.Errorf("%w: existing turn has another user message", ErrTurnConflict)
		}
		return stored, created, nil
	}
	if err := s.repository.SaveTurn(ctx, turn); err != nil {
		// A concurrent request may have won the idempotency race. Read it back
		// so retries still return one durable turn rather than duplicating work.
		if existing, readErr := s.repository.GetTurnByIdempotency(ctx, scope, conversationID, key); readErr == nil {
			return existing, false, nil
		}
		return Turn{}, false, fmt.Errorf("save turn: %w", err)
	}
	return turn, true, nil
}

func (s *TurnService) Get(ctx context.Context, scope identity.Scope, id string) (Turn, error) {
	if s == nil || s.repository == nil {
		return Turn{}, fmt.Errorf("%w: repository is required", ErrTurnNotFound)
	}
	if err := validateTurnLookup(scope, id); err != nil {
		return Turn{}, err
	}
	value, err := s.repository.GetTurn(ctx, scope, strings.TrimSpace(id))
	if err != nil {
		return Turn{}, fmt.Errorf("get turn: %w", err)
	}
	if err := validateTurn(value); err != nil {
		return Turn{}, err
	}
	return value, nil
}

func (s *TurnService) Start(ctx context.Context, scope identity.Scope, id string) (Turn, error) {
	if repository, ok := s.repository.(ConditionalTurnRepository); ok {
		return repository.TransitionTurn(ctx, scope, id, "start", "", "")
	}
	return s.update(ctx, scope, id, func(value *Turn) {
		if value.Status == TurnStatusPending {
			value.Status = TurnStatusRunning
		}
	})
}

func (s *TurnService) Complete(ctx context.Context, scope identity.Scope, id, resultID string) (Turn, error) {
	if repository, ok := s.repository.(ConditionalTurnRepository); ok {
		return repository.TransitionTurn(ctx, scope, id, "complete", resultID, "")
	}
	return s.update(ctx, scope, id, func(value *Turn) {
		if value.Status.Terminal() {
			return
		}
		if value.CancelRequested {
			value.Status = TurnStatusCancelled
			value.TerminalResult = "cancelled"
			return
		}
		value.Status = TurnStatusCompleted
		value.AssistantMessageID = strings.TrimSpace(resultID)
		value.TerminalResult = value.AssistantMessageID
	})
}

func (s *TurnService) Fail(ctx context.Context, scope identity.Scope, id string, cause error) (Turn, error) {
	if repository, ok := s.repository.(ConditionalTurnRepository); ok {
		return repository.TransitionTurn(ctx, scope, id, "fail", "", boundedTurnError(cause))
	}
	return s.update(ctx, scope, id, func(value *Turn) {
		if value.Status.Terminal() {
			return
		}
		if value.CancelRequested {
			value.Status = TurnStatusCancelled
			value.TerminalResult = "cancelled"
			return
		}
		value.Status = TurnStatusFailed
		value.Error = boundedTurnError(cause)
		value.TerminalResult = "failed"
	})
}

func (s *TurnService) Cancel(ctx context.Context, scope identity.Scope, id string) (Turn, error) {
	if repository, ok := s.repository.(ConditionalTurnRepository); ok {
		return repository.TransitionTurn(ctx, scope, id, "cancel", "", "")
	}
	return s.update(ctx, scope, id, func(value *Turn) {
		if value.Status.Terminal() {
			return
		}
		value.CancelRequested = true
		if value.Status == TurnStatusPending {
			value.Status = TurnStatusCancelled
			value.TerminalResult = "cancelled"
		}
	})
}

func (s *TurnService) List(ctx context.Context, scope identity.Scope, conversationID string, limit int) ([]Turn, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidTurn)
	}
	if err := validateTurnLookup(scope, conversationID); err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = DefaultLimit
	}
	if limit < 1 || limit > MaxLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidTurn, MaxLimit)
	}
	return s.repository.ListTurns(ctx, scope, conversationID, limit)
}

func (s *TurnService) update(ctx context.Context, scope identity.Scope, id string, mutate func(*Turn)) (Turn, error) {
	value, err := s.Get(ctx, scope, id)
	if err != nil {
		return Turn{}, err
	}
	mutate(&value)
	value.UpdatedAt = s.now().UTC()
	if value.Status.Terminal() && value.CompletedAt.IsZero() {
		value.CompletedAt = value.UpdatedAt
	}
	if err := validateTurn(value); err != nil {
		return Turn{}, err
	}
	if err := s.repository.SaveTurn(ctx, value); err != nil {
		return Turn{}, fmt.Errorf("save turn: %w", err)
	}
	return value, nil
}

func validateTurn(value Turn) error {
	if err := identity.ValidateID("turn ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTurn, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTurn, err)
	}
	if err := identity.ValidateID("conversation ID", value.ConversationID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTurn, err)
	}
	if strings.TrimSpace(value.IdempotencyKey) == "" || len([]rune(value.IdempotencyKey)) > MaxIdempotencyKey {
		return fmt.Errorf("%w: idempotency key is required and bounded", ErrInvalidTurn)
	}
	if !value.Status.Valid() {
		return fmt.Errorf("%w: invalid status %q", ErrInvalidTurn, value.Status)
	}
	if len([]rune(value.Error)) > MaxTurnError {
		return fmt.Errorf("%w: error is too large", ErrInvalidTurn)
	}
	return nil
}

func validateTurnLookup(scope identity.Scope, id string) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTurn, err)
	}
	if err := identity.ValidateID("turn ID", id); err != nil {
		return fmt.Errorf("%w: %v", ErrTurnNotFound, err)
	}
	return nil
}

func boundedTurnError(cause error) string {
	if cause == nil {
		return "turn failed"
	}
	value := strings.TrimSpace(cause.Error())
	if len([]rune(value)) > MaxTurnError {
		return string([]rune(value)[:MaxTurnError])
	}
	return value
}
