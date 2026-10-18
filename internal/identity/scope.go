// Package identity contains the bounded identity values shared by user-scoped
// domain services. It deliberately has no storage or transport dependencies.
package identity

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxIdentityLength = 128

var ErrInvalidScope = errors.New("invalid user scope")

// Scope is the only identity input accepted by routed repositories. A graph
// name is intentionally absent: it is resolved from the trusted control graph.
type Scope struct {
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
}

func (s Scope) Validate() error {
	if err := ValidateID("tenant ID", s.TenantID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidScope, err)
	}
	if err := ValidateID("user ID", s.UserID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidScope, err)
	}
	return nil
}

func ValidateID(label, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", label)
	}
	if utf8.RuneCountInString(value) > MaxIdentityLength {
		return fmt.Errorf("%s must be at most %d characters", label, MaxIdentityLength)
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return fmt.Errorf("%s contains whitespace or control characters", label)
		}
	}
	return nil
}

func NormalizeID(value string) string {
	return strings.TrimSpace(value)
}
