package profile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	MaxDisplayName       = 120
	MaxProfileListItems  = 50
	MaxProfileListValue  = 120
	MaxPreferenceEntries = 32
	MaxPreferenceKey     = 64
	MaxPreferenceValue   = 256
	MaxTimezone          = 80
)

var (
	ErrInvalidProfile  = errors.New("invalid user profile")
	ErrProfileNotFound = errors.New("user profile not found")
)

type QuietHours struct {
	Start    string `json:"start,omitempty"`
	End      string `json:"end,omitempty"`
	Timezone string `json:"timezone,omitempty"`
}

// Profile is the structured user context stored on the user graph's User
// node. Preferences are bounded maps/lists rather than an unbounded blob.
type Profile struct {
	TenantID                string            `json:"tenant_id"`
	UserID                  string            `json:"user_id"`
	DisplayName             string            `json:"display_name"`
	Interests               []string          `json:"interests,omitempty"`
	Skills                  []string          `json:"skills,omitempty"`
	PreferredTechnologies   []string          `json:"preferred_technologies,omitempty"`
	Goals                   []string          `json:"goals,omitempty"`
	LocationPreferences     []string          `json:"location_preferences,omitempty"`
	ContentPreferences      map[string]string `json:"content_preferences,omitempty"`
	QuietHours              QuietHours        `json:"quiet_hours,omitempty"`
	NotificationPreferences map[string]string `json:"notification_preferences,omitempty"`
	CreatedAt               time.Time         `json:"created_at"`
	UpdatedAt               time.Time         `json:"updated_at"`
}

type Repository interface {
	Save(context.Context, Profile) error
	Get(context.Context, identity.Scope) (Profile, error)
}

type ProfileRepository = Repository

type Service struct {
	repository Repository
	now        func() time.Time
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository, now: func() time.Time { return time.Now().UTC() }}
}

func (s *Service) Save(ctx context.Context, value Profile) (Profile, error) {
	if s == nil || s.repository == nil {
		return Profile{}, fmt.Errorf("%w: repository is required", ErrInvalidProfile)
	}
	value = clone(value)
	value.TenantID = identity.NormalizeID(value.TenantID)
	value.UserID = identity.NormalizeID(value.UserID)
	value.DisplayName = strings.TrimSpace(value.DisplayName)
	if value.CreatedAt.IsZero() {
		value.CreatedAt = s.now().UTC()
	}
	value.UpdatedAt = s.now().UTC()
	if err := validate(value); err != nil {
		return Profile{}, err
	}
	if err := s.repository.Save(ctx, value); err != nil {
		return Profile{}, fmt.Errorf("save profile %q: %w", value.UserID, err)
	}
	return clone(value), nil
}

func (s *Service) Get(ctx context.Context, scope identity.Scope) (Profile, error) {
	if s == nil || s.repository == nil {
		return Profile{}, fmt.Errorf("%w: repository is required", ErrProfileNotFound)
	}
	if err := scope.Validate(); err != nil {
		return Profile{}, err
	}
	value, err := s.repository.Get(ctx, scope)
	if err != nil {
		return Profile{}, fmt.Errorf("get profile %q: %w", scope.UserID, err)
	}
	if value.TenantID != scope.TenantID || value.UserID != scope.UserID {
		return Profile{}, fmt.Errorf("%w: repository returned a different user scope", ErrInvalidProfile)
	}
	return clone(value), nil
}

func validate(value Profile) error {
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidProfile, err)
	}
	if value.DisplayName == "" || len([]rune(value.DisplayName)) > MaxDisplayName {
		return fmt.Errorf("%w: display name is required and must be at most %d characters", ErrInvalidProfile, MaxDisplayName)
	}
	for name, values := range map[string][]string{
		"interests": value.Interests, "skills": value.Skills,
		"preferred technologies": value.PreferredTechnologies, "goals": value.Goals,
		"location preferences": value.LocationPreferences,
	} {
		if len(values) > MaxProfileListItems {
			return fmt.Errorf("%w: %s cannot contain more than %d items", ErrInvalidProfile, name, MaxProfileListItems)
		}
		for _, item := range values {
			if strings.TrimSpace(item) == "" || len([]rune(item)) > MaxProfileListValue {
				return fmt.Errorf("%w: %s values must be between 1 and %d characters", ErrInvalidProfile, name, MaxProfileListValue)
			}
		}
	}
	if err := validatePreferences("content preferences", value.ContentPreferences); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidProfile, err)
	}
	if err := validatePreferences("notification preferences", value.NotificationPreferences); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidProfile, err)
	}
	if (value.QuietHours.Start == "") != (value.QuietHours.End == "") {
		return fmt.Errorf("%w: quiet hours require both start and end", ErrInvalidProfile)
	}
	for field, hour := range map[string]string{"quiet hours start": value.QuietHours.Start, "quiet hours end": value.QuietHours.End} {
		if hour != "" {
			if _, err := time.Parse("15:04", hour); err != nil {
				return fmt.Errorf("%w: %s must use HH:MM", ErrInvalidProfile, field)
			}
		}
	}
	if len([]rune(value.QuietHours.Timezone)) > MaxTimezone {
		return fmt.Errorf("%w: timezone is too long", ErrInvalidProfile)
	}
	return nil
}

func validatePreferences(name string, values map[string]string) error {
	if len(values) > MaxPreferenceEntries {
		return fmt.Errorf("%s cannot contain more than %d entries", name, MaxPreferenceEntries)
	}
	for key, value := range values {
		if strings.TrimSpace(key) == "" || len([]rune(key)) > MaxPreferenceKey || len([]rune(value)) > MaxPreferenceValue {
			return fmt.Errorf("%s keys or values exceed their bounds", name)
		}
	}
	return nil
}

func clone(value Profile) Profile {
	value.Interests = append([]string(nil), value.Interests...)
	value.Skills = append([]string(nil), value.Skills...)
	value.PreferredTechnologies = append([]string(nil), value.PreferredTechnologies...)
	value.Goals = append([]string(nil), value.Goals...)
	value.LocationPreferences = append([]string(nil), value.LocationPreferences...)
	value.ContentPreferences = cloneMap(value.ContentPreferences)
	value.NotificationPreferences = cloneMap(value.NotificationPreferences)
	return value
}

func cloneMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
