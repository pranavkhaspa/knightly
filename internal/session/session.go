package session

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	MaxTitle         = 160
	MaxSummary       = 12000
	MaxDecisionText  = 16000
	MaxEvidenceIDs   = 50
	DefaultListLimit = 50
	MaxListLimit     = 100
)

var (
	ErrInvalidSession   = errors.New("invalid session")
	ErrSessionNotFound  = errors.New("session not found")
	ErrInvalidDecision  = errors.New("invalid decision")
	ErrDecisionNotFound = errors.New("decision not found")
)

type SessionStatus string

const (
	SessionActive    SessionStatus = "active"
	SessionCompleted SessionStatus = "completed"
	SessionAbandoned SessionStatus = "abandoned"
)

func (s SessionStatus) Valid() bool {
	switch s {
	case SessionActive, SessionCompleted, SessionAbandoned:
		return true
	default:
		return false
	}
}

type DecisionStatus string

const (
	DecisionProposed DecisionStatus = "proposed"
	DecisionAccepted DecisionStatus = "accepted"
	DecisionRejected DecisionStatus = "rejected"
	DecisionDeferred DecisionStatus = "deferred"
)

func (s DecisionStatus) Valid() bool {
	switch s {
	case DecisionProposed, DecisionAccepted, DecisionRejected, DecisionDeferred:
		return true
	default:
		return false
	}
}

type Session struct {
	ID        string        `json:"id"`
	TenantID  string        `json:"tenant_id"`
	UserID    string        `json:"user_id"`
	ProjectID string        `json:"project_id,omitempty"`
	Title     string        `json:"title"`
	Summary   string        `json:"summary,omitempty"`
	Status    SessionStatus `json:"status"`
	StartedAt time.Time     `json:"started_at"`
	EndedAt   time.Time     `json:"ended_at,omitempty"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

type Decision struct {
	ID          string         `json:"id"`
	TenantID    string         `json:"tenant_id"`
	UserID      string         `json:"user_id"`
	SessionID   string         `json:"session_id"`
	ProjectID   string         `json:"project_id,omitempty"`
	Subject     string         `json:"subject"`
	Choice      string         `json:"choice"`
	Rationale   string         `json:"rationale,omitempty"`
	Status      DecisionStatus `json:"status"`
	EvidenceIDs []string       `json:"evidence_ids,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

type SessionRepository interface {
	SaveSession(context.Context, Session) error
	GetSession(context.Context, identity.Scope, string) (Session, error)
}

type DecisionRepository interface {
	SaveDecision(context.Context, Decision) error
	GetDecision(context.Context, identity.Scope, string) (Decision, error)
	ListDecisions(context.Context, identity.Scope, string, int) ([]Decision, error)
}

type Repository interface {
	SessionRepository
	DecisionRepository
}

type Service struct {
	repository Repository
	now        func() time.Time
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository, now: func() time.Time { return time.Now().UTC() }}
}

func (s *Service) SaveSession(ctx context.Context, value Session) (Session, error) {
	if s == nil || s.repository == nil {
		return Session{}, fmt.Errorf("%w: repository is required", ErrInvalidSession)
	}
	value = cloneSession(value)
	value.ID = identity.NormalizeID(value.ID)
	value.TenantID = identity.NormalizeID(value.TenantID)
	value.UserID = identity.NormalizeID(value.UserID)
	value.ProjectID = identity.NormalizeID(value.ProjectID)
	value.Title = strings.TrimSpace(value.Title)
	value.Summary = strings.TrimSpace(value.Summary)
	if value.Status == "" {
		value.Status = SessionActive
	}
	if value.StartedAt.IsZero() {
		value.StartedAt = s.now().UTC()
	}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = value.StartedAt
	}
	value.UpdatedAt = s.now().UTC()
	if err := validateSession(value); err != nil {
		return Session{}, err
	}
	if err := s.repository.SaveSession(ctx, value); err != nil {
		return Session{}, fmt.Errorf("save session %q: %w", value.ID, err)
	}
	return cloneSession(value), nil
}

func (s *Service) GetSession(ctx context.Context, scope identity.Scope, id string) (Session, error) {
	if s == nil || s.repository == nil {
		return Session{}, fmt.Errorf("%w: repository is required", ErrSessionNotFound)
	}
	if err := validateLookup(scope, id, ErrSessionNotFound, "session ID"); err != nil {
		return Session{}, err
	}
	value, err := s.repository.GetSession(ctx, scope, strings.TrimSpace(id))
	if err != nil {
		return Session{}, fmt.Errorf("get session %q: %w", id, err)
	}
	if !sameScope(value.TenantID, value.UserID, scope) {
		return Session{}, fmt.Errorf("%w: repository returned a different user scope", ErrInvalidSession)
	}
	return cloneSession(value), nil
}

func (s *Service) SaveDecision(ctx context.Context, value Decision) (Decision, error) {
	if s == nil || s.repository == nil {
		return Decision{}, fmt.Errorf("%w: repository is required", ErrInvalidDecision)
	}
	value = cloneDecision(value)
	value.ID = identity.NormalizeID(value.ID)
	value.TenantID = identity.NormalizeID(value.TenantID)
	value.UserID = identity.NormalizeID(value.UserID)
	value.SessionID = identity.NormalizeID(value.SessionID)
	value.ProjectID = identity.NormalizeID(value.ProjectID)
	value.Subject = strings.TrimSpace(value.Subject)
	value.Choice = strings.TrimSpace(value.Choice)
	value.Rationale = strings.TrimSpace(value.Rationale)
	if value.Status == "" {
		value.Status = DecisionProposed
	}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = s.now().UTC()
	}
	value.UpdatedAt = s.now().UTC()
	if err := validateDecision(value); err != nil {
		return Decision{}, err
	}
	if err := s.repository.SaveDecision(ctx, value); err != nil {
		return Decision{}, fmt.Errorf("save decision %q: %w", value.ID, err)
	}
	return cloneDecision(value), nil
}

func (s *Service) GetDecision(ctx context.Context, scope identity.Scope, id string) (Decision, error) {
	if s == nil || s.repository == nil {
		return Decision{}, fmt.Errorf("%w: repository is required", ErrDecisionNotFound)
	}
	if err := validateLookup(scope, id, ErrDecisionNotFound, "decision ID"); err != nil {
		return Decision{}, err
	}
	value, err := s.repository.GetDecision(ctx, scope, strings.TrimSpace(id))
	if err != nil {
		return Decision{}, fmt.Errorf("get decision %q: %w", id, err)
	}
	if !sameScope(value.TenantID, value.UserID, scope) {
		return Decision{}, fmt.Errorf("%w: repository returned a different user scope", ErrInvalidDecision)
	}
	return cloneDecision(value), nil
}

func (s *Service) ListDecisions(ctx context.Context, scope identity.Scope, sessionID string, limit int) ([]Decision, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidDecision)
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if sessionID != "" {
		if err := identity.ValidateID("session ID", sessionID); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidDecision, err)
		}
	}
	if limit == 0 {
		limit = DefaultListLimit
	}
	if limit < 1 || limit > MaxListLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidDecision, MaxListLimit)
	}
	values, err := s.repository.ListDecisions(ctx, scope, strings.TrimSpace(sessionID), limit)
	if err != nil {
		return nil, fmt.Errorf("list decisions: %w", err)
	}
	result := make([]Decision, 0, len(values))
	for _, value := range values {
		if !sameScope(value.TenantID, value.UserID, scope) {
			return nil, fmt.Errorf("%w: repository returned a decision from another user scope", ErrInvalidDecision)
		}
		result = append(result, cloneDecision(value))
	}
	return result, nil
}

func validateSession(value Session) error {
	if err := identity.ValidateID("session ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSession, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSession, err)
	}
	if value.ProjectID != "" {
		if err := identity.ValidateID("project ID", value.ProjectID); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidSession, err)
		}
	}
	if value.Title == "" || len([]rune(value.Title)) > MaxTitle || len([]rune(value.Summary)) > MaxSummary {
		return fmt.Errorf("%w: title is required and summary/title exceed their bounds", ErrInvalidSession)
	}
	if !value.Status.Valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidSession, value.Status)
	}
	if !value.EndedAt.IsZero() && value.EndedAt.Before(value.StartedAt) {
		return fmt.Errorf("%w: ended time precedes start time", ErrInvalidSession)
	}
	return nil
}

func validateDecision(value Decision) error {
	if err := identity.ValidateID("decision ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDecision, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDecision, err)
	}
	if err := identity.ValidateID("session ID", value.SessionID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDecision, err)
	}
	if value.ProjectID != "" {
		if err := identity.ValidateID("project ID", value.ProjectID); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidDecision, err)
		}
	}
	if value.Subject == "" || value.Choice == "" || len([]rune(value.Subject)) > MaxTitle || len([]rune(value.Choice)) > MaxTitle || len([]rune(value.Rationale)) > MaxDecisionText {
		return fmt.Errorf("%w: subject and choice are required and text exceeds its bound", ErrInvalidDecision)
	}
	if !value.Status.Valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidDecision, value.Status)
	}
	if len(value.EvidenceIDs) > MaxEvidenceIDs {
		return fmt.Errorf("%w: at most %d evidence IDs are allowed", ErrInvalidDecision, MaxEvidenceIDs)
	}
	for _, evidenceID := range value.EvidenceIDs {
		if err := identity.ValidateID("evidence ID", evidenceID); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidDecision, err)
		}
	}
	return nil
}

func validateLookup(scope identity.Scope, id string, notFound error, label string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID(label, id); err != nil {
		return fmt.Errorf("%w: %v", notFound, err)
	}
	return nil
}

func sameScope(tenantID, userID string, scope identity.Scope) bool {
	return tenantID == scope.TenantID && userID == scope.UserID
}

func cloneSession(value Session) Session { return value }

func cloneDecision(value Decision) Decision {
	value.EvidenceIDs = append([]string(nil), value.EvidenceIDs...)
	return value
}
