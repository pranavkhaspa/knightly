package project

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	MaxName          = 160
	MaxDescription   = 8000
	MaxPRDText       = 32768
	MaxListItems     = 50
	MaxListValue     = 256
	MaxRepositoryURL = 2048
	DefaultListLimit = 20
	MaxListLimit     = 100
)

var (
	ErrInvalidProject  = errors.New("invalid project")
	ErrProjectNotFound = errors.New("project not found")
)

type Status string

const (
	Draft     Status = "draft"
	Active    Status = "active"
	Paused    Status = "paused"
	Completed Status = "completed"
	Archived  Status = "archived"
)

func (s Status) Valid() bool {
	switch s {
	case Draft, Active, Paused, Completed, Archived:
		return true
	default:
		return false
	}
}

type Project struct {
	ID              string    `json:"id"`
	TenantID        string    `json:"tenant_id"`
	UserID          string    `json:"user_id"`
	Name            string    `json:"name"`
	Description     string    `json:"description,omitempty"`
	PRDText         string    `json:"prd_text,omitempty"`
	Technologies    []string  `json:"technologies,omitempty"`
	Constraints     []string  `json:"constraints,omitempty"`
	Status          Status    `json:"status"`
	RepositoryURL   string    `json:"repository_url,omitempty"`
	DesiredOutcomes []string  `json:"desired_outcomes,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type Repository interface {
	Save(context.Context, Project) error
	Get(context.Context, identity.Scope, string) (Project, error)
	List(context.Context, identity.Scope, int) ([]Project, error)
}

type ProjectRepository = Repository

type Service struct {
	repository Repository
	now        func() time.Time
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository, now: func() time.Time { return time.Now().UTC() }}
}

func (s *Service) Save(ctx context.Context, value Project) (Project, error) {
	if s == nil || s.repository == nil {
		return Project{}, fmt.Errorf("%w: repository is required", ErrInvalidProject)
	}
	value = clone(value)
	value.ID = identity.NormalizeID(value.ID)
	value.TenantID = identity.NormalizeID(value.TenantID)
	value.UserID = identity.NormalizeID(value.UserID)
	value.Name = strings.TrimSpace(value.Name)
	value.Description = strings.TrimSpace(value.Description)
	value.PRDText = strings.TrimSpace(value.PRDText)
	value.RepositoryURL = strings.TrimSpace(value.RepositoryURL)
	if value.Status == "" {
		value.Status = Draft
	}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = s.now().UTC()
	}
	value.UpdatedAt = s.now().UTC()
	if err := validate(value); err != nil {
		return Project{}, err
	}
	if err := s.repository.Save(ctx, value); err != nil {
		return Project{}, fmt.Errorf("save project %q: %w", value.ID, err)
	}
	return clone(value), nil
}

func (s *Service) Get(ctx context.Context, scope identity.Scope, id string) (Project, error) {
	if s == nil || s.repository == nil {
		return Project{}, fmt.Errorf("%w: repository is required", ErrProjectNotFound)
	}
	if err := scope.Validate(); err != nil {
		return Project{}, err
	}
	if err := identity.ValidateID("project ID", id); err != nil {
		return Project{}, fmt.Errorf("%w: %v", ErrProjectNotFound, err)
	}
	value, err := s.repository.Get(ctx, scope, strings.TrimSpace(id))
	if err != nil {
		return Project{}, fmt.Errorf("get project %q: %w", id, err)
	}
	if !sameScope(value.TenantID, value.UserID, scope) {
		return Project{}, fmt.Errorf("%w: repository returned a different user scope", ErrInvalidProject)
	}
	return clone(value), nil
}

func (s *Service) List(ctx context.Context, scope identity.Scope, limit int) ([]Project, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidProject)
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = DefaultListLimit
	}
	if limit < 1 || limit > MaxListLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidProject, MaxListLimit)
	}
	values, err := s.repository.List(ctx, scope, limit)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	result := make([]Project, 0, len(values))
	for _, value := range values {
		if !sameScope(value.TenantID, value.UserID, scope) {
			return nil, fmt.Errorf("%w: repository returned a project from another user scope", ErrInvalidProject)
		}
		result = append(result, clone(value))
	}
	return result, nil
}

func validate(value Project) error {
	if err := identity.ValidateID("project ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidProject, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidProject, err)
	}
	if value.Name == "" || len([]rune(value.Name)) > MaxName {
		return fmt.Errorf("%w: name is required and must be at most %d characters", ErrInvalidProject, MaxName)
	}
	if len([]rune(value.Description)) > MaxDescription || len([]rune(value.PRDText)) > MaxPRDText {
		return fmt.Errorf("%w: description or PRD exceeds its bound", ErrInvalidProject)
	}
	if !value.Status.Valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidProject, value.Status)
	}
	if len([]rune(value.RepositoryURL)) > MaxRepositoryURL {
		return fmt.Errorf("%w: repository URL is too long", ErrInvalidProject)
	}
	if value.RepositoryURL != "" {
		parsed, err := url.Parse(value.RepositoryURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
			return fmt.Errorf("%w: repository URL must be an http(s) URL without credentials", ErrInvalidProject)
		}
	}
	for name, values := range map[string][]string{
		"technologies": value.Technologies, "constraints": value.Constraints, "desired outcomes": value.DesiredOutcomes,
	} {
		if len(values) > MaxListItems {
			return fmt.Errorf("%w: %s cannot contain more than %d items", ErrInvalidProject, name, MaxListItems)
		}
		for _, item := range values {
			if strings.TrimSpace(item) == "" || len([]rune(item)) > MaxListValue {
				return fmt.Errorf("%w: %s values must be between 1 and %d characters", ErrInvalidProject, name, MaxListValue)
			}
		}
	}
	return nil
}

func sameScope(tenantID, userID string, scope identity.Scope) bool {
	return tenantID == scope.TenantID && userID == scope.UserID
}

func clone(value Project) Project {
	value.Technologies = append([]string(nil), value.Technologies...)
	value.Constraints = append([]string(nil), value.Constraints...)
	value.DesiredOutcomes = append([]string(nil), value.DesiredOutcomes...)
	return value
}
