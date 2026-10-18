package requirement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	MaxTitle         = 240
	MaxDescription   = 8000
	MaxListItems     = 50
	MaxListValue     = 256
	DefaultListLimit = 50
	MaxListLimit     = 100
)

var (
	ErrInvalidRequirement  = errors.New("invalid requirement")
	ErrRequirementNotFound = errors.New("requirement not found")
)

type Status string

const (
	Open       Status = "open"
	InProgress Status = "in_progress"
	Blocked    Status = "blocked"
	Done       Status = "done"
	Archived   Status = "archived"
)

func (s Status) Valid() bool {
	switch s {
	case Open, InProgress, Blocked, Done, Archived:
		return true
	default:
		return false
	}
}

type Requirement struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	UserID        string    `json:"user_id"`
	ProjectID     string    `json:"project_id"`
	Title         string    `json:"title"`
	Description   string    `json:"description,omitempty"`
	Status        Status    `json:"status"`
	Priority      int       `json:"priority"`
	TechnologyIDs []string  `json:"technology_ids,omitempty"`
	Constraints   []string  `json:"constraints,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Repository interface {
	Save(context.Context, Requirement) error
	Get(context.Context, identity.Scope, string) (Requirement, error)
	List(context.Context, identity.Scope, string, int) ([]Requirement, error)
}

type RequirementRepository = Repository

type Service struct {
	repository Repository
	now        func() time.Time
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository, now: func() time.Time { return time.Now().UTC() }}
}

func (s *Service) Save(ctx context.Context, value Requirement) (Requirement, error) {
	if s == nil || s.repository == nil {
		return Requirement{}, fmt.Errorf("%w: repository is required", ErrInvalidRequirement)
	}
	value = clone(value)
	value.ID = identity.NormalizeID(value.ID)
	value.TenantID = identity.NormalizeID(value.TenantID)
	value.UserID = identity.NormalizeID(value.UserID)
	value.ProjectID = identity.NormalizeID(value.ProjectID)
	value.Title = strings.TrimSpace(value.Title)
	value.Description = strings.TrimSpace(value.Description)
	if value.Status == "" {
		value.Status = Open
	}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = s.now().UTC()
	}
	value.UpdatedAt = s.now().UTC()
	if err := validate(value); err != nil {
		return Requirement{}, err
	}
	if err := s.repository.Save(ctx, value); err != nil {
		return Requirement{}, fmt.Errorf("save requirement %q: %w", value.ID, err)
	}
	return clone(value), nil
}

func (s *Service) Get(ctx context.Context, scope identity.Scope, id string) (Requirement, error) {
	if s == nil || s.repository == nil {
		return Requirement{}, fmt.Errorf("%w: repository is required", ErrRequirementNotFound)
	}
	if err := scope.Validate(); err != nil {
		return Requirement{}, err
	}
	if err := identity.ValidateID("requirement ID", id); err != nil {
		return Requirement{}, fmt.Errorf("%w: %v", ErrRequirementNotFound, err)
	}
	value, err := s.repository.Get(ctx, scope, strings.TrimSpace(id))
	if err != nil {
		return Requirement{}, fmt.Errorf("get requirement %q: %w", id, err)
	}
	if !sameScope(value, scope) {
		return Requirement{}, fmt.Errorf("%w: repository returned a different user scope", ErrInvalidRequirement)
	}
	return clone(value), nil
}

func (s *Service) List(ctx context.Context, scope identity.Scope, projectID string, limit int) ([]Requirement, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidRequirement)
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := identity.ValidateID("project ID", projectID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidRequirement, err)
	}
	if limit == 0 {
		limit = DefaultListLimit
	}
	if limit < 1 || limit > MaxListLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidRequirement, MaxListLimit)
	}
	values, err := s.repository.List(ctx, scope, strings.TrimSpace(projectID), limit)
	if err != nil {
		return nil, fmt.Errorf("list requirements: %w", err)
	}
	result := make([]Requirement, 0, len(values))
	for _, value := range values {
		if !sameScope(value, scope) {
			return nil, fmt.Errorf("%w: repository returned a requirement from another user scope", ErrInvalidRequirement)
		}
		result = append(result, clone(value))
	}
	return result, nil
}

func validate(value Requirement) error {
	if err := identity.ValidateID("requirement ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequirement, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequirement, err)
	}
	if err := identity.ValidateID("project ID", value.ProjectID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequirement, err)
	}
	if value.Title == "" || len([]rune(value.Title)) > MaxTitle {
		return fmt.Errorf("%w: title is required and must be at most %d characters", ErrInvalidRequirement, MaxTitle)
	}
	if len([]rune(value.Description)) > MaxDescription {
		return fmt.Errorf("%w: description is too long", ErrInvalidRequirement)
	}
	if !value.Status.Valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidRequirement, value.Status)
	}
	if value.Priority < 0 || value.Priority > 100 {
		return fmt.Errorf("%w: priority must be between 0 and 100", ErrInvalidRequirement)
	}
	for name, values := range map[string][]string{"technology IDs": value.TechnologyIDs, "constraints": value.Constraints} {
		if len(values) > MaxListItems {
			return fmt.Errorf("%w: %s cannot contain more than %d items", ErrInvalidRequirement, name, MaxListItems)
		}
		for _, item := range values {
			if strings.TrimSpace(item) == "" || len([]rune(item)) > MaxListValue {
				return fmt.Errorf("%w: %s values must be between 1 and %d characters", ErrInvalidRequirement, name, MaxListValue)
			}
		}
	}
	return nil
}

func sameScope(value Requirement, scope identity.Scope) bool {
	return value.TenantID == scope.TenantID && value.UserID == scope.UserID
}

func clone(value Requirement) Requirement {
	value.TechnologyIDs = append([]string(nil), value.TechnologyIDs...)
	value.Constraints = append([]string(nil), value.Constraints...)
	return value
}
