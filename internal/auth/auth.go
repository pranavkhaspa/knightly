// Package auth provides the HTTP boundary for deployment authentication and
// trusted request identity. It deliberately does not read scope or approval
// values from request headers, query parameters, or JSON bodies.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

var (
	ErrInvalidConfig   = errors.New("invalid authentication configuration")
	ErrUnauthenticated = errors.New("authenticated identity is required")
)

// Config describes one trusted deployment identity. Token authentication and
// HTTP Basic authentication may be enabled together; either valid credential
// form is accepted. Scope and ApproverID are always supplied by configuration.
//
// When Resolver is set it replaces the static identity: the resolver verifies
// per-request evidence (an OIDC token or a trusted-proxy signature) and supplies
// the tenant, user, and approver for that request. The static Scope and
// ApproverID are then used only as the installation fallback for local
// loopback-only development.
type Config struct {
	Token                 string
	Username              string
	Password              string
	Scope                 identity.Scope
	ApproverID            string
	RequireAuthentication bool
	PublicPaths           []string
	Resolver              IdentityResolver
}

// Identity is the authenticated deployment identity used by handlers to route
// graph access and record approvals. It cannot be populated from a request.
type Identity struct {
	Scope      identity.Scope
	ApproverID string
}

type Middleware struct {
	token                 string
	username              string
	password              string
	identity              Identity
	requireAuthentication bool
	publicPaths           map[string]struct{}
	resolver              IdentityResolver
}

// New validates the trusted identity and returns an HTTP middleware policy.
// A policy without credentials is valid for a validated loopback-only bind.
func New(config Config) (*Middleware, error) {
	config.Scope.TenantID = identity.NormalizeID(config.Scope.TenantID)
	config.Scope.UserID = identity.NormalizeID(config.Scope.UserID)
	config.ApproverID = identity.NormalizeID(config.ApproverID)
	if err := config.Scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := identity.ValidateID("approver ID", config.ApproverID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if (strings.TrimSpace(config.Username) == "") != (strings.TrimSpace(config.Password) == "") {
		return nil, fmt.Errorf("%w: username and password must be set together", ErrInvalidConfig)
	}
	if err := validateCredential(config.Token, "token"); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := validateCredential(config.Username, "username"); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := validateCredential(config.Password, "password"); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	enabled := strings.TrimSpace(config.Token) != "" || (strings.TrimSpace(config.Username) != "" && strings.TrimSpace(config.Password) != "")
	// A configured resolver verifies per-request identity evidence, so it
	// satisfies the authentication requirement on its own.
	if config.RequireAuthentication && !enabled && config.Resolver == nil {
		return nil, fmt.Errorf("%w: credentials or a resolver are required", ErrInvalidConfig)
	}

	publicPaths := make(map[string]struct{}, len(config.PublicPaths))
	for _, path := range config.PublicPaths {
		path = strings.TrimSpace(path)
		if path != "" {
			publicPaths[path] = struct{}{}
		}
	}
	return &Middleware{
		token:                 config.Token,
		username:              config.Username,
		password:              config.Password,
		identity:              Identity{Scope: config.Scope, ApproverID: config.ApproverID},
		requireAuthentication: config.RequireAuthentication,
		publicPaths:           publicPaths,
		resolver:              config.Resolver,
	}, nil
}

// Wrap adds security headers, authenticates protected requests, and attaches
// the resolved identity to the request context. When a resolver is configured
// the resolver is the authentication authority and its failure fails closed.
// PublicPaths should be used only for narrowly scoped operational endpoints
// such as /health/live.
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		setSecurityHeaders(writer)
		if m == nil || next == nil {
			http.Error(writer, "authentication middleware unavailable", http.StatusInternalServerError)
			return
		}
		if _, public := m.publicPaths[request.URL.Path]; public {
			next.ServeHTTP(writer, request)
			return
		}
		if m.resolver != nil {
			resolved, err := m.resolver.Resolve(request.Context(), request)
			if err != nil {
				writer.Header().Set("WWW-Authenticate", `Bearer realm="nightshift"`)
				http.Error(writer, "authentication required", http.StatusUnauthorized)
				return
			}
			request = request.WithContext(withIdentity(request.Context(), resolved))
			next.ServeHTTP(writer, request)
			return
		}
		if !m.authenticated(request) {
			writer.Header().Set("WWW-Authenticate", m.challenge())
			http.Error(writer, "authentication required", http.StatusUnauthorized)
			return
		}
		request = request.WithContext(withIdentity(request.Context(), m.identity))
		next.ServeHTTP(writer, request)
	})
}

// Handler is an alias for Wrap so the policy can be passed directly in code
// that names HTTP middleware handlers explicitly.
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return m.Wrap(next)
}

// IdentityFromRequest returns the identity installed by Wrap. It never falls
// back to request data, making missing middleware wiring fail closed.
func IdentityFromRequest(request *http.Request) (Identity, error) {
	if request == nil {
		return Identity{}, ErrUnauthenticated
	}
	value, ok := IdentityFromContext(request.Context())
	if !ok {
		return Identity{}, ErrUnauthenticated
	}
	return value, nil
}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	if ctx == nil {
		return Identity{}, false
	}
	value, ok := ctx.Value(identityContextKey{}).(Identity)
	return value, ok
}

// ScopeFromRequest returns the trusted graph scope. Handlers should use this
// for every repository call instead of accepting tenant/user request fields.
func ScopeFromRequest(request *http.Request) (identity.Scope, error) {
	value, err := IdentityFromRequest(request)
	if err != nil {
		return identity.Scope{}, err
	}
	return value.Scope, nil
}

// ApproverFromRequest returns the trusted approval actor. An approval handler
// must pass this value to the action service and ignore any client-supplied
// approved_by field.
func ApproverFromRequest(request *http.Request) (string, error) {
	value, err := IdentityFromRequest(request)
	if err != nil {
		return "", err
	}
	if value.ApproverID == "" {
		return "", fmt.Errorf("%w: approver identity is not configured", ErrUnauthenticated)
	}
	return value.ApproverID, nil
}

type identityContextKey struct{}

func withIdentity(ctx context.Context, value Identity) context.Context {
	return context.WithValue(ctx, identityContextKey{}, value)
}

func (m *Middleware) authenticated(request *http.Request) bool {
	if m == nil {
		return false
	}
	if m.token == "" && m.username == "" && m.password == "" {
		return !m.requireAuthentication
	}
	if bearerToken, ok := bearer(request.Header.Get("Authorization")); ok && constantTimeEqual(bearerToken, m.token) {
		return true
	}
	username, password, ok := request.BasicAuth()
	return ok && m.username != "" && m.password != "" && constantTimeEqual(username, m.username) && constantTimeEqual(password, m.password)
}

func (m *Middleware) challenge() string {
	if m != nil && m.token != "" && m.username == "" {
		return `Bearer realm="nightshift"`
	}
	if m != nil && m.username != "" && m.token == "" {
		return `Basic realm="nightshift"`
	}
	return `Bearer realm="nightshift", Basic realm="nightshift"`
}

func bearer(value string) (string, bool) {
	scheme, token, ok := strings.Cut(value, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", false
	}
	return strings.TrimSpace(token), true
}

func constantTimeEqual(left, right string) bool {
	leftHash := sha256.Sum256([]byte(left))
	rightHash := sha256.Sum256([]byte(right))
	return subtle.ConstantTimeCompare(leftHash[:], rightHash[:]) == 1
}

func validateCredential(value, label string) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("%s must not contain a newline", label)
	}
	return nil
}

func setSecurityHeaders(writer http.ResponseWriter) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Permissions-Policy", "camera=(), geolocation=(), microphone=()")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("X-Frame-Options", "DENY")
	writer.Header().Add("Vary", "Authorization")
}
