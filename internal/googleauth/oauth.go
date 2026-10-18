// Package googleauth contains the shared, read-only Google OAuth client and a
// protected local token store used by Google-backed providers (Gmail, Calendar,
// and later). It never logs or persists token material outside the owner-only
// credential file.
package googleauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	GoogleAuthEndpoint  = "https://accounts.google.com/o/oauth2/v2/auth"
	GoogleTokenEndpoint = "https://oauth2.googleapis.com/token"

	MaxTokenResponse    = int64(64 * 1024)
	MaxOAuthStateLength = 256
)

var (
	ErrInvalidConfig    = errors.New("invalid OAuth configuration")
	ErrExchange         = errors.New("OAuth token exchange failed")
	ErrInsecureEndpoint = errors.New("OAuth endpoint must use HTTPS")
)

// Config configures the OAuth client. Endpoints are overridable so tests can
// point at a loopback fake server.
type Config struct {
	ClientID     string
	ClientSecret string
	RedirectURL  string
	AuthURL      string
	TokenURL     string
	Client       *http.Client
	Clock        func() time.Time
}

type Client struct {
	clientID     string
	clientSecret string
	redirectURL  string
	authURL      *url.URL
	tokenURL     *url.URL
	client       *http.Client
	now          func() time.Time
}

func NewClient(config Config) (*Client, error) {
	if strings.TrimSpace(config.ClientID) == "" || strings.TrimSpace(config.ClientSecret) == "" {
		return nil, fmt.Errorf("%w: client ID and client secret are required", ErrInvalidConfig)
	}
	if strings.TrimSpace(config.RedirectURL) == "" {
		return nil, fmt.Errorf("%w: redirect URL is required", ErrInvalidConfig)
	}
	redirect, err := url.Parse(config.RedirectURL)
	if err != nil || (redirect.Scheme != "http" && redirect.Scheme != "https") || redirect.Host == "" {
		return nil, fmt.Errorf("%w: redirect URL must be an http(s) URL", ErrInvalidConfig)
	}
	authURL, err := ValidateEndpoint(config.AuthURL, GoogleAuthEndpoint)
	if err != nil {
		return nil, err
	}
	tokenURL, err := ValidateEndpoint(config.TokenURL, GoogleTokenEndpoint)
	if err != nil {
		return nil, err
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	now := config.Clock
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Client{
		clientID: config.ClientID, clientSecret: config.ClientSecret, redirectURL: config.RedirectURL,
		authURL: authURL, tokenURL: tokenURL, client: client, now: now,
	}, nil
}

// AuthCodeURL builds the consent URL for the given scopes. The caller supplies
// an opaque, single-use state value.
func (c *Client) AuthCodeURL(state string, scopes []string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("%w: client is not initialized", ErrInvalidConfig)
	}
	state = strings.TrimSpace(state)
	if state == "" || len(state) > MaxOAuthStateLength || strings.ContainsAny(state, "\r\n") {
		return "", fmt.Errorf("%w: state is required and bounded", ErrInvalidConfig)
	}
	scope := strings.TrimSpace(strings.Join(scopes, " "))
	if scope == "" {
		return "", fmt.Errorf("%w: at least one scope is required", ErrInvalidConfig)
	}
	query := c.authURL.Query()
	query.Set("client_id", c.clientID)
	query.Set("redirect_uri", c.redirectURL)
	query.Set("response_type", "code")
	query.Set("scope", scope)
	query.Set("access_type", "offline")
	query.Set("include_granted_scopes", "true")
	query.Set("prompt", "consent")
	query.Set("state", state)
	endpoint := *c.authURL
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

// Exchange redeems an authorization code for tokens.
func (c *Client) Exchange(ctx context.Context, code string) (TokenSet, error) {
	code = strings.TrimSpace(code)
	if code == "" || strings.ContainsAny(code, "\r\n") {
		return TokenSet{}, fmt.Errorf("%w: authorization code is required", ErrInvalidConfig)
	}
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {c.redirectURL},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
	}
	return c.postToken(ctx, form)
}

// Refresh exchanges a refresh token for a new access token, preserving the
// refresh token when the provider does not rotate it.
func (c *Client) Refresh(ctx context.Context, tokens TokenSet) (TokenSet, error) {
	if strings.TrimSpace(tokens.RefreshToken) == "" {
		return TokenSet{}, fmt.Errorf("%w: refresh token is required", ErrInvalidConfig)
	}
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {tokens.RefreshToken},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
	}
	refreshed, err := c.postToken(ctx, form)
	if err != nil {
		return TokenSet{}, err
	}
	if refreshed.RefreshToken == "" {
		refreshed.RefreshToken = tokens.RefreshToken
	}
	return refreshed, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
	ExpiresIn    int64  `json:"expires_in"`
}

func (c *Client) postToken(ctx context.Context, form url.Values) (TokenSet, error) {
	if c == nil || c.client == nil {
		return TokenSet{}, fmt.Errorf("%w: client is not initialized", ErrInvalidConfig)
	}
	if ctx == nil {
		return TokenSet{}, fmt.Errorf("%w: context is required", ErrInvalidConfig)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenURL.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return TokenSet{}, fmt.Errorf("create OAuth token request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return TokenSet{}, fmt.Errorf("%w: transport error", ErrExchange)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxTokenResponse+1))
	if err != nil {
		return TokenSet{}, fmt.Errorf("read OAuth token response: %w", err)
	}
	if int64(len(body)) > MaxTokenResponse {
		return TokenSet{}, fmt.Errorf("%w: token response is too large", ErrExchange)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return TokenSet{}, fmt.Errorf("%w: provider returned HTTP %d", ErrExchange, response.StatusCode)
	}
	var decoded tokenResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return TokenSet{}, fmt.Errorf("%w: decode token response", ErrExchange)
	}
	if strings.TrimSpace(decoded.AccessToken) == "" {
		return TokenSet{}, fmt.Errorf("%w: provider returned no access token", ErrExchange)
	}
	tokens := TokenSet{
		AccessToken:  decoded.AccessToken,
		RefreshToken: decoded.RefreshToken,
		TokenType:    decoded.TokenType,
		Scope:        decoded.Scope,
	}
	if decoded.ExpiresIn > 0 {
		tokens.Expiry = c.now().Add(time.Duration(decoded.ExpiresIn) * time.Second)
	}
	return tokens, nil
}

// HasScope reports whether a space-delimited scope string grants target.
func HasScope(scope, target string) bool {
	for _, value := range strings.Fields(scope) {
		if value == target {
			return true
		}
	}
	return false
}

// ValidateEndpoint resolves an endpoint URL, requiring HTTPS for any
// non-loopback host.
func ValidateEndpoint(raw, fallback string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		raw = fallback
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil {
		return nil, fmt.Errorf("%w: endpoint must be an absolute http(s) URL", ErrInvalidConfig)
	}
	if parsed.Scheme == "http" && !IsLoopbackHost(parsed.Hostname()) {
		return nil, fmt.Errorf("%w: %s", ErrInsecureEndpoint, parsed.Hostname())
	}
	return parsed, nil
}

// IsLoopbackHost reports whether a host literal is the local machine.
func IsLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
