package mail

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/pranavkhaspa/knightly/internal/googleauth"
)

// GmailReadOnlyScope is the only Google scope used in phase P0. It cannot
// send, delete, label, archive, or modify a mailbox.
const GmailReadOnlyScope = "https://www.googleapis.com/auth/gmail.readonly"

const ProviderGmail = "gmail"

// maxTokenResponse is retained so tests and callers share the provider bound.
const maxTokenResponse = googleauth.MaxTokenResponse

var (
	ErrOAuthConfig      = googleauth.ErrInvalidConfig
	ErrOAuthExchange    = googleauth.ErrExchange
	ErrInsecureOAuthURL = googleauth.ErrInsecureEndpoint
	ErrScopeMissing     = errors.New("granted OAuth scope is missing gmail.readonly")
)

// OAuthConfig configures the read-only Gmail OAuth client.
type OAuthConfig struct {
	ClientID     string
	ClientSecret string
	RedirectURL  string
	AuthURL      string
	TokenURL     string
	Client       *http.Client
	Clock        func() time.Time
}

// OAuthClient wraps the shared Google OAuth client and enforces the Gmail
// read-only scope on every grant.
type OAuthClient struct {
	inner *googleauth.Client
}

func NewOAuthClient(config OAuthConfig) (*OAuthClient, error) {
	inner, err := googleauth.NewClient(googleauth.Config{
		ClientID: config.ClientID, ClientSecret: config.ClientSecret, RedirectURL: config.RedirectURL,
		AuthURL: config.AuthURL, TokenURL: config.TokenURL, Client: config.Client, Clock: config.Clock,
	})
	if err != nil {
		return nil, err
	}
	return &OAuthClient{inner: inner}, nil
}

// AuthCodeURL builds the consent URL for the read-only scope.
func (c *OAuthClient) AuthCodeURL(state string) (string, error) {
	if c == nil || c.inner == nil {
		return "", fmt.Errorf("%w: client is not initialized", ErrOAuthConfig)
	}
	return c.inner.AuthCodeURL(state, []string{GmailReadOnlyScope})
}

// Exchange redeems an authorization code and rejects any grant that is not
// exactly the Gmail read-only scope.
func (c *OAuthClient) Exchange(ctx context.Context, code string) (TokenSet, error) {
	if c == nil || c.inner == nil {
		return TokenSet{}, fmt.Errorf("%w: client is not initialized", ErrOAuthConfig)
	}
	tokens, err := c.inner.Exchange(ctx, code)
	if err != nil {
		return TokenSet{}, err
	}
	if tokens.Scope != "" && !hasReadOnlyScope(tokens.Scope) {
		return TokenSet{}, ErrScopeMissing
	}
	return tokens, nil
}

// Refresh exchanges a refresh token for a new access token.
func (c *OAuthClient) Refresh(ctx context.Context, tokens TokenSet) (TokenSet, error) {
	if c == nil || c.inner == nil {
		return TokenSet{}, fmt.Errorf("%w: client is not initialized", ErrOAuthConfig)
	}
	return c.inner.Refresh(ctx, tokens)
}

func hasReadOnlyScope(scope string) bool { return googleauth.HasScope(scope, GmailReadOnlyScope) }

func isLoopbackHost(host string) bool { return googleauth.IsLoopbackHost(host) }

func validateOAuthEndpoint(raw, fallback string) (*url.URL, error) {
	return googleauth.ValidateEndpoint(raw, fallback)
}
