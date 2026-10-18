package config

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// Config contains only deployment configuration. Durable product state belongs
// in FalkorDB, not in this process or a second local database.
type Config struct {
	Listen                  string
	Environment             string
	InstanceID              string
	TenantID                string
	UserID                  string
	ProjectID               string
	FalkorURL               string
	ControlGraph            string
	UserGraph               string
	DataDir                 string
	SearchURL               string
	OpenCodeURL             string
	OpenCodeToken           string // Optional Bearer token for a trusted reverse proxy.
	OpenCodeServerUsername  string
	OpenCodeServerPassword  string
	OpenCodeModel           string
	OpenCodePolicyConfirmed bool
	WorkspaceRoot           string
	AllowedRepositories     []string
	CodingRepositories      map[string]string
	CodingTestCommands      map[string][]string
	CodingRunTimeoutSeconds int
	QueryTimeoutMillis      int
	ActionsEnabled          bool

	// Gmail read-only OAuth. Tokens are stored only in the protected local
	// credential file below, never in FalkorDB.
	MailCredentialsFile string
	GmailClientID       string
	GmailClientSecret   string
	GmailRedirectURL    string

	// Timezone is the IANA location used to normalize calendar windows and
	// event times. Invalid or empty values fall back to UTC.
	Timezone string

	// ReminderWebhookURL, when set, enables an approval-gated webhook sink for
	// reminder delivery. Empty keeps the sink disabled.
	ReminderWebhookURL string

	// GitHub read/write integration. GitHubToken is a fine-grained personal
	// access token that is never stored in FalkorDB. GitHubRepositories is the
	// allowlist of "owner/name" repositories the API may read or write.
	GitHubToken        string
	GitHubRepositories []string
	GitHubAPIBase      string

	// AuthToken or the username/password pair protects a non-loopback HTTP
	// listener. The scope and approver are deployment identities, not request
	// fields; the auth middleware places them in the request context.
	AuthToken      string
	AuthUsername   string
	AuthPassword   string
	AuthTenantID   string
	AuthUserID     string
	AuthApproverID string
	AuthRequired   bool

	// Verified per-request identity. Exactly one of OIDC or trusted-proxy may be
	// configured; when neither is set the deployment-wide Auth* identity is used
	// for loopback-only single-user operation.
	OIDCIssuer          string
	OIDCAudience        string
	OIDCJWKSURL         string
	OIDCTenantClaim     string
	TrustedProxySecret  string
	TrustedProxyHeaders TrustedProxyHeaders

	// Observability. LogFormat is "json" (default) or "text"; LogLevel is one of
	// debug, info, warn, or error. OTLPEndpoint, when set, enables non-blocking
	// OTLP/HTTP metrics export. MetricsPublic exposes GET /metrics without
	// credentials and must only be enabled on a trusted scrape network.
	LogFormat     string
	LogLevel      string
	OTLPEndpoint  string
	MetricsPublic bool
}

// TrustedProxyHeaders customizes the identity headers a trusted proxy sets. The
// proxy must sign them with TrustedProxySecret.
type TrustedProxyHeaders struct {
	Tenant    string
	User      string
	Approver  string
	Signature string
	Timestamp string
}

func Load() (Config, error) {
	c := Config{
		Listen:                  envOr("NIGHTSHIFT_LISTEN", "127.0.0.1:8080"),
		Environment:             envOr("NIGHTSHIFT_ENV", "development"),
		InstanceID:              instanceID(),
		TenantID:                envOr("NIGHTSHIFT_TENANT_ID", "tenant_demo"),
		UserID:                  envOr("NIGHTSHIFT_USER_ID", "user_demo"),
		ProjectID:               envOr("NIGHTSHIFT_PROJECT_ID", "project_nightshift"),
		FalkorURL:               envOr("FALKORDB_URL", "redis://127.0.0.1:6379"),
		ControlGraph:            envOr("FALKORDB_CONTROL_GRAPH", "nightshift_control"),
		UserGraph:               envOr("FALKORDB_GRAPH", "nightshift_u_demo"),
		DataDir:                 envOr("NIGHTSHIFT_DATA_DIR", "./data"),
		SearchURL:               strings.TrimSpace(os.Getenv("NIGHTSHIFT_SEARCH_URL")),
		OpenCodeURL:             strings.TrimSpace(os.Getenv("NIGHTSHIFT_OPENCODE_URL")),
		OpenCodeToken:           os.Getenv("NIGHTSHIFT_OPENCODE_TOKEN"),
		OpenCodeServerUsername:  strings.TrimSpace(os.Getenv("NIGHTSHIFT_OPENCODE_SERVER_USERNAME")),
		OpenCodeServerPassword:  os.Getenv("NIGHTSHIFT_OPENCODE_SERVER_PASSWORD"),
		OpenCodeModel:           envOr("NIGHTSHIFT_OPENCODE_MODEL", "opencode/big-pickle"),
		OpenCodePolicyConfirmed: strings.EqualFold(strings.TrimSpace(os.Getenv("NIGHTSHIFT_OPENCODE_POLICY_CONFIRMED")), "true"),
		WorkspaceRoot:           envOr("NIGHTSHIFT_WORKSPACE_ROOT", "../nightshift-workspaces"),
		CodingRunTimeoutSeconds: 7200,
		QueryTimeoutMillis:      5000,
		AuthToken:               os.Getenv("NIGHTSHIFT_AUTH_TOKEN"),
		AuthUsername:            os.Getenv("NIGHTSHIFT_AUTH_USERNAME"),
		AuthPassword:            os.Getenv("NIGHTSHIFT_AUTH_PASSWORD"),
		AuthTenantID:            envOr("NIGHTSHIFT_AUTH_TENANT_ID", envOr("NIGHTSHIFT_TENANT_ID", "tenant_demo")),
		AuthUserID:              envOr("NIGHTSHIFT_AUTH_USER_ID", envOr("NIGHTSHIFT_USER_ID", "user_demo")),
		AuthApproverID:          envOr("NIGHTSHIFT_AUTH_APPROVER_ID", envOr("NIGHTSHIFT_USER_ID", "user_demo")),
		GmailClientID:           strings.TrimSpace(os.Getenv("GMAIL_CLIENT_ID")),
		GmailClientSecret:       os.Getenv("GMAIL_CLIENT_SECRET"),
		GmailRedirectURL:        strings.TrimSpace(os.Getenv("GMAIL_REDIRECT_URL")),
		Timezone:                envOr("NIGHTSHIFT_TIMEZONE", "UTC"),
		ReminderWebhookURL:      strings.TrimSpace(os.Getenv("NIGHTSHIFT_REMINDER_WEBHOOK_URL")),
		GitHubToken:             os.Getenv("GITHUB_TOKEN"),
		GitHubRepositories:      splitList(os.Getenv("GITHUB_REPOSITORIES")),
		GitHubAPIBase:           envOr("GITHUB_API_BASE", "https://api.github.com"),
		OIDCIssuer:              strings.TrimSpace(os.Getenv("NIGHTSHIFT_OIDC_ISSUER")),
		OIDCAudience:            strings.TrimSpace(os.Getenv("NIGHTSHIFT_OIDC_AUDIENCE")),
		OIDCJWKSURL:             strings.TrimSpace(os.Getenv("NIGHTSHIFT_OIDC_JWKS_URL")),
		OIDCTenantClaim:         strings.TrimSpace(os.Getenv("NIGHTSHIFT_OIDC_TENANT_CLAIM")),
		TrustedProxySecret:      os.Getenv("NIGHTSHIFT_TRUSTED_PROXY_SECRET"),
		TrustedProxyHeaders: TrustedProxyHeaders{
			Tenant:    strings.TrimSpace(os.Getenv("NIGHTSHIFT_TRUSTED_PROXY_TENANT_HEADER")),
			User:      strings.TrimSpace(os.Getenv("NIGHTSHIFT_TRUSTED_PROXY_USER_HEADER")),
			Approver:  strings.TrimSpace(os.Getenv("NIGHTSHIFT_TRUSTED_PROXY_APPROVER_HEADER")),
			Signature: strings.TrimSpace(os.Getenv("NIGHTSHIFT_TRUSTED_PROXY_SIGNATURE_HEADER")),
			Timestamp: strings.TrimSpace(os.Getenv("NIGHTSHIFT_TRUSTED_PROXY_TIMESTAMP_HEADER")),
		},
		LogFormat:     envOr("NIGHTSHIFT_LOG_FORMAT", "json"),
		LogLevel:      envOr("NIGHTSHIFT_LOG_LEVEL", "info"),
		OTLPEndpoint:  strings.TrimSpace(os.Getenv("NIGHTSHIFT_OTLP_ENDPOINT")),
		MetricsPublic: false,
	}
	c.MailCredentialsFile = strings.TrimSpace(os.Getenv("NIGHTSHIFT_MAIL_CREDENTIALS_FILE"))
	if c.MailCredentialsFile == "" {
		c.MailCredentialsFile = filepath.Join(c.DataDir, "mail-credentials.json")
	}

	if raw := strings.TrimSpace(os.Getenv("NIGHTSHIFT_ALLOWED_REPOSITORIES_JSON")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.AllowedRepositories); err != nil {
			return Config{}, fmt.Errorf("NIGHTSHIFT_ALLOWED_REPOSITORIES_JSON must be a JSON array of repository paths")
		}
	}
	if raw := strings.TrimSpace(os.Getenv("NIGHTSHIFT_CODING_PROJECT_REPOSITORIES_JSON")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.CodingRepositories); err != nil {
			return Config{}, fmt.Errorf("NIGHTSHIFT_CODING_PROJECT_REPOSITORIES_JSON must map project IDs to local repository paths")
		}
	}
	if raw := strings.TrimSpace(os.Getenv("NIGHTSHIFT_CODING_TEST_COMMANDS_JSON")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.CodingTestCommands); err != nil {
			return Config{}, fmt.Errorf("NIGHTSHIFT_CODING_TEST_COMMANDS_JSON must map project IDs to JSON argument arrays")
		}
	}
	if value := os.Getenv("NIGHTSHIFT_CODING_RUN_TIMEOUT_SECONDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 86400 {
			return Config{}, fmt.Errorf("NIGHTSHIFT_CODING_RUN_TIMEOUT_SECONDS must be between 1 and 86400")
		}
		c.CodingRunTimeoutSeconds = parsed
	}

	if value := os.Getenv("FALKORDB_QUERY_TIMEOUT_MS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 60000 {
			return Config{}, fmt.Errorf("FALKORDB_QUERY_TIMEOUT_MS must be between 1 and 60000")
		}
		c.QueryTimeoutMillis = parsed
	}

	if value := os.Getenv("NIGHTSHIFT_ACTIONS_ENABLED"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("NIGHTSHIFT_ACTIONS_ENABLED must be true or false")
		}
		c.ActionsEnabled = parsed
	}

	if value := os.Getenv("NIGHTSHIFT_METRICS_PUBLIC"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("NIGHTSHIFT_METRICS_PUBLIC must be true or false")
		}
		c.MetricsPublic = parsed
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}

	return c, nil
}

// Validate checks the deployment boundary before any network or graph client
// is initialized. A non-loopback listener must have a configured bearer token
// or a complete HTTP Basic credential pair.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("configuration is required")
	}
	if err := validateListen(c.Listen); err != nil {
		return err
	}
	if strings.TrimSpace(c.FalkorURL) == "" || strings.TrimSpace(c.ControlGraph) == "" || strings.TrimSpace(c.UserGraph) == "" || strings.TrimSpace(c.DataDir) == "" || strings.TrimSpace(c.ProjectID) == "" {
		return fmt.Errorf("FalkorDB URL, graph names, data directory, and project ID must be non-empty")
	}
	if strings.TrimSpace(c.OpenCodeURL) != "" {
		if !c.OpenCodePolicyConfirmed {
			return fmt.Errorf("OpenCode requires NIGHTSHIFT_OPENCODE_POLICY_CONFIRMED=true after configuring the trusted server policy")
		}
		if strings.TrimSpace(c.WorkspaceRoot) == "" || len(c.AllowedRepositories) == 0 {
			return fmt.Errorf("OpenCode requires NIGHTSHIFT_WORKSPACE_ROOT and NIGHTSHIFT_ALLOWED_REPOSITORIES_JSON")
		}
		if len(c.CodingTestCommands) == 0 {
			return fmt.Errorf("OpenCode requires NIGHTSHIFT_CODING_TEST_COMMANDS_JSON with an allowlisted test command for each project")
		}
		if len(c.CodingRepositories) == 0 {
			return fmt.Errorf("OpenCode requires NIGHTSHIFT_CODING_PROJECT_REPOSITORIES_JSON mapping project IDs to local repository paths")
		}
		allowed := make(map[string]struct{}, len(c.AllowedRepositories))
		for _, repository := range c.AllowedRepositories {
			absolute, err := filepath.Abs(repository)
			if err != nil {
				return fmt.Errorf("invalid allowlisted repository path: %w", err)
			}
			allowed[filepath.Clean(absolute)] = struct{}{}
		}
		for projectID, repository := range c.CodingRepositories {
			if strings.TrimSpace(projectID) == "" || strings.TrimSpace(repository) == "" {
				return fmt.Errorf("coding project repository mappings require non-empty project IDs and paths")
			}
			abs, err := filepath.Abs(repository)
			if err != nil {
				return fmt.Errorf("invalid coding repository for project %s: %w", projectID, err)
			}
			if _, ok := allowed[filepath.Clean(abs)]; !ok {
				return fmt.Errorf("coding repository for project %s is not in NIGHTSHIFT_ALLOWED_REPOSITORIES_JSON", projectID)
			}
			if _, ok := c.CodingTestCommands[projectID]; !ok {
				return fmt.Errorf("coding project %s has no configured test command", projectID)
			}
		}
		for projectID, command := range c.CodingTestCommands {
			if strings.TrimSpace(projectID) == "" || len(command) == 0 || strings.TrimSpace(command[0]) == "" {
				return fmt.Errorf("coding test commands require a project ID and non-empty argument vector")
			}
			for _, argument := range command {
				if strings.ContainsAny(argument, "\r\n") {
					return fmt.Errorf("coding test command arguments must not contain newlines")
				}
			}
		}
	}
	if err := validateSecret("NIGHTSHIFT_OPENCODE_TOKEN", c.OpenCodeToken); err != nil {
		return err
	}
	if err := validateSecret("NIGHTSHIFT_OPENCODE_SERVER_PASSWORD", c.OpenCodeServerPassword); err != nil {
		return err
	}
	if c.OpenCodeServerPassword != "" && c.OpenCodeServerUsername == "" {
		c.OpenCodeServerUsername = "opencode"
	}
	if c.OpenCodeServerPassword == "" && c.OpenCodeServerUsername != "" {
		return fmt.Errorf("NIGHTSHIFT_OPENCODE_SERVER_USERNAME requires NIGHTSHIFT_OPENCODE_SERVER_PASSWORD")
	}
	if strings.TrimSpace(c.OpenCodeModel) != "" {
		parts := strings.SplitN(strings.TrimSpace(c.OpenCodeModel), "/", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return fmt.Errorf("NIGHTSHIFT_OPENCODE_MODEL must use provider/model format, for example opencode/big-pickle")
		}
	}
	if err := (identity.Scope{TenantID: strings.TrimSpace(c.TenantID), UserID: strings.TrimSpace(c.UserID)}).Validate(); err != nil {
		return fmt.Errorf("invalid installation scope: %w", err)
	}

	if (strings.TrimSpace(c.AuthUsername) == "") != (strings.TrimSpace(c.AuthPassword) == "") {
		return fmt.Errorf("NIGHTSHIFT_AUTH_USERNAME and NIGHTSHIFT_AUTH_PASSWORD must be set together")
	}
	if err := validateSecret("NIGHTSHIFT_AUTH_TOKEN", c.AuthToken); err != nil {
		return err
	}
	if err := validateSecret("NIGHTSHIFT_AUTH_USERNAME", c.AuthUsername); err != nil {
		return err
	}
	if err := validateSecret("NIGHTSHIFT_AUTH_PASSWORD", c.AuthPassword); err != nil {
		return err
	}
	c.AuthTenantID = strings.TrimSpace(c.AuthTenantID)
	c.AuthUserID = strings.TrimSpace(c.AuthUserID)
	c.AuthApproverID = strings.TrimSpace(c.AuthApproverID)
	if err := identity.ValidateID("NIGHTSHIFT_AUTH_TENANT_ID", c.AuthTenantID); err != nil {
		return err
	}
	if err := identity.ValidateID("NIGHTSHIFT_AUTH_USER_ID", c.AuthUserID); err != nil {
		return err
	}
	if err := identity.ValidateID("NIGHTSHIFT_AUTH_APPROVER_ID", c.AuthApproverID); err != nil {
		return err
	}

	c.AuthRequired = !ListenIsLoopback(c.Listen)
	if c.AuthRequired && !c.authConfigured() {
		return fmt.Errorf("non-loopback NIGHTSHIFT_LISTEN requires NIGHTSHIFT_AUTH_TOKEN or a complete NIGHTSHIFT_AUTH_USERNAME/NIGHTSHIFT_AUTH_PASSWORD pair")
	}

	if (c.GmailClientID == "") != (c.GmailClientSecret == "") {
		return fmt.Errorf("GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET must be set together")
	}
	if err := validateSecret("GMAIL_CLIENT_SECRET", c.GmailClientSecret); err != nil {
		return err
	}
	if c.GmailClientID != "" {
		if c.GmailRedirectURL == "" {
			return fmt.Errorf("GMAIL_REDIRECT_URL is required when Gmail OAuth is configured")
		}
		redirect, err := url.Parse(c.GmailRedirectURL)
		if err != nil || (redirect.Scheme != "http" && redirect.Scheme != "https") || redirect.Host == "" {
			return fmt.Errorf("GMAIL_REDIRECT_URL must be an absolute http(s) URL")
		}
	}
	if strings.TrimSpace(c.MailCredentialsFile) == "" || strings.ContainsAny(c.MailCredentialsFile, "\r\n") {
		return fmt.Errorf("NIGHTSHIFT_MAIL_CREDENTIALS_FILE must be a non-empty path")
	}
	if strings.TrimSpace(c.Timezone) == "" {
		c.Timezone = "UTC"
	}
	if _, err := time.LoadLocation(c.Timezone); err != nil {
		return fmt.Errorf("NIGHTSHIFT_TIMEZONE must be a valid IANA location such as UTC or America/New_York")
	}
	if c.ReminderWebhookURL != "" {
		if err := validateSecret("NIGHTSHIFT_REMINDER_WEBHOOK_URL", c.ReminderWebhookURL); err != nil {
			return err
		}
		webhook, err := url.Parse(c.ReminderWebhookURL)
		if err != nil || (webhook.Scheme != "http" && webhook.Scheme != "https") || webhook.Host == "" {
			return fmt.Errorf("NIGHTSHIFT_REMINDER_WEBHOOK_URL must be an absolute http(s) URL")
		}
	}
	if err := validateSecret("GITHUB_TOKEN", c.GitHubToken); err != nil {
		return err
	}
	c.GitHubToken = strings.TrimSpace(c.GitHubToken)
	c.GitHubAPIBase = strings.TrimSpace(c.GitHubAPIBase)
	if c.GitHubAPIBase == "" {
		c.GitHubAPIBase = "https://api.github.com"
	}
	base, err := url.Parse(c.GitHubAPIBase)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return fmt.Errorf("GITHUB_API_BASE must be an absolute http(s) URL")
	}
	normalized := make([]string, 0, len(c.GitHubRepositories))
	seen := map[string]struct{}{}
	for _, repository := range c.GitHubRepositories {
		repository = strings.ToLower(strings.TrimSpace(repository))
		parts := strings.Split(repository, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("GITHUB_REPOSITORIES entries must be owner/name, got %q", repository)
		}
		if _, ok := seen[repository]; ok {
			continue
		}
		seen[repository] = struct{}{}
		normalized = append(normalized, repository)
	}
	c.GitHubRepositories = normalized
	if err := validateIdentityProviders(c); err != nil {
		return err
	}
	if err := validateObservability(c); err != nil {
		return err
	}
	return nil
}

func validateObservability(c *Config) error {
	switch strings.ToLower(strings.TrimSpace(c.LogFormat)) {
	case "", "json":
		c.LogFormat = "json"
	case "text":
		c.LogFormat = "text"
	default:
		return fmt.Errorf("NIGHTSHIFT_LOG_FORMAT must be json or text")
	}
	switch strings.ToLower(strings.TrimSpace(c.LogLevel)) {
	case "", "info":
		c.LogLevel = "info"
	case "debug":
		c.LogLevel = "debug"
	case "warn", "warning":
		c.LogLevel = "warn"
	case "error":
		c.LogLevel = "error"
	default:
		return fmt.Errorf("NIGHTSHIFT_LOG_LEVEL must be debug, info, warn, or error")
	}
	c.OTLPEndpoint = strings.TrimSpace(c.OTLPEndpoint)
	if c.OTLPEndpoint != "" {
		if err := validateSecret("NIGHTSHIFT_OTLP_ENDPOINT", c.OTLPEndpoint); err != nil {
			return err
		}
		endpoint, err := url.Parse(c.OTLPEndpoint)
		if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
			return fmt.Errorf("NIGHTSHIFT_OTLP_ENDPOINT must be an absolute http(s) URL")
		}
	}
	return nil
}

func validateIdentityProviders(c *Config) error {
	for label, value := range map[string]string{
		"NIGHTSHIFT_OIDC_ISSUER": c.OIDCIssuer, "NIGHTSHIFT_OIDC_AUDIENCE": c.OIDCAudience,
		"NIGHTSHIFT_OIDC_JWKS_URL": c.OIDCJWKSURL, "NIGHTSHIFT_OIDC_TENANT_CLAIM": c.OIDCTenantClaim,
		"NIGHTSHIFT_TRUSTED_PROXY_SECRET": c.TrustedProxySecret,
	} {
		if err := validateSecret(label, value); err != nil {
			return err
		}
	}
	oidcEnabled := c.OIDCIssuer != "" || c.OIDCJWKSURL != "" || c.OIDCAudience != ""
	proxyEnabled := c.TrustedProxySecret != ""
	if oidcEnabled && proxyEnabled {
		return fmt.Errorf("configure either NIGHTSHIFT_OIDC_ISSUER or NIGHTSHIFT_TRUSTED_PROXY_SECRET, not both")
	}
	if oidcEnabled {
		if c.OIDCIssuer == "" || c.OIDCJWKSURL == "" || c.OIDCAudience == "" {
			return fmt.Errorf("NIGHTSHIFT_OIDC_ISSUER, NIGHTSHIFT_OIDC_AUDIENCE, and NIGHTSHIFT_OIDC_JWKS_URL must be set together")
		}
		jwks, err := url.Parse(c.OIDCJWKSURL)
		if err != nil || (jwks.Scheme != "http" && jwks.Scheme != "https") || jwks.Host == "" {
			return fmt.Errorf("NIGHTSHIFT_OIDC_JWKS_URL must be an absolute http(s) URL")
		}
	}
	if proxyEnabled {
		for label, value := range map[string]string{
			"NIGHTSHIFT_TRUSTED_PROXY_TENANT_HEADER":    c.TrustedProxyHeaders.Tenant,
			"NIGHTSHIFT_TRUSTED_PROXY_USER_HEADER":      c.TrustedProxyHeaders.User,
			"NIGHTSHIFT_TRUSTED_PROXY_APPROVER_HEADER":  c.TrustedProxyHeaders.Approver,
			"NIGHTSHIFT_TRUSTED_PROXY_SIGNATURE_HEADER": c.TrustedProxyHeaders.Signature,
			"NIGHTSHIFT_TRUSTED_PROXY_TIMESTAMP_HEADER": c.TrustedProxyHeaders.Timestamp,
		} {
			if value == "" {
				continue
			}
			if strings.ContainsAny(value, " \t\r\n") {
				return fmt.Errorf("%s must be a valid HTTP header name", label)
			}
		}
	}
	return nil
}

// ListenIsLoopback reports whether an HTTP listener is explicitly restricted
// to the local host. Empty-host listeners such as :8080 intentionally return
// false because they normally expose every interface.
func ListenIsLoopback(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil || host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c Config) authConfigured() bool {
	return strings.TrimSpace(c.AuthToken) != "" || (strings.TrimSpace(c.AuthUsername) != "" && strings.TrimSpace(c.AuthPassword) != "")
}

func validateListen(listen string) error {
	if strings.TrimSpace(listen) == "" {
		return fmt.Errorf("NIGHTSHIFT_LISTEN must be a host:port address")
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil || port == "" {
		return fmt.Errorf("NIGHTSHIFT_LISTEN must be a host:port address")
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 0 || parsedPort > 65535 {
		return fmt.Errorf("NIGHTSHIFT_LISTEN port must be between 0 and 65535")
	}
	return nil
}

func validateSecret(label, value string) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("%s must not contain a newline", label)
	}
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// splitList parses a comma or whitespace separated list, trimming blanks.
func splitList(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		if trimmed := strings.TrimSpace(field); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func instanceID() string {
	if value := os.Getenv("NIGHTSHIFT_INSTANCE_ID"); value != "" {
		return value
	}
	// Container runtimes normally provide a per-process hostname. Using it as
	// the fallback avoids every scaled web/worker instance claiming the same
	// coordination owner while keeping local development configuration-free.
	if value := os.Getenv("HOSTNAME"); value != "" {
		return value
	}
	if value, err := os.Hostname(); err == nil && value != "" {
		return value
	}
	return "local-dev"
}
