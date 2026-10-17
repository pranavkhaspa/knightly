package config

import (
	"strings"
	"testing"
)

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"NIGHTSHIFT_LISTEN", "NIGHTSHIFT_ENV", "NIGHTSHIFT_INSTANCE_ID", "HOSTNAME",
		"NIGHTSHIFT_TENANT_ID", "NIGHTSHIFT_USER_ID", "NIGHTSHIFT_PROJECT_ID",
		"FALKORDB_URL", "FALKORDB_CONTROL_GRAPH", "FALKORDB_GRAPH", "NIGHTSHIFT_DATA_DIR",
		"FALKORDB_QUERY_TIMEOUT_MS", "NIGHTSHIFT_ACTIONS_ENABLED", "NIGHTSHIFT_AUTH_TOKEN",
		"NIGHTSHIFT_SEARCH_URL",
		"NIGHTSHIFT_MAIL_CREDENTIALS_FILE", "GMAIL_CLIENT_ID", "GMAIL_CLIENT_SECRET", "GMAIL_REDIRECT_URL",
		"NIGHTSHIFT_TIMEZONE", "NIGHTSHIFT_REMINDER_WEBHOOK_URL",
		"GITHUB_TOKEN", "GITHUB_REPOSITORIES", "GITHUB_API_BASE",
		"NIGHTSHIFT_OIDC_ISSUER", "NIGHTSHIFT_OIDC_AUDIENCE", "NIGHTSHIFT_OIDC_JWKS_URL",
		"NIGHTSHIFT_OIDC_TENANT_CLAIM", "NIGHTSHIFT_TRUSTED_PROXY_SECRET",
		"NIGHTSHIFT_TRUSTED_PROXY_TENANT_HEADER", "NIGHTSHIFT_TRUSTED_PROXY_USER_HEADER",
		"NIGHTSHIFT_TRUSTED_PROXY_APPROVER_HEADER", "NIGHTSHIFT_TRUSTED_PROXY_SIGNATURE_HEADER",
		"NIGHTSHIFT_TRUSTED_PROXY_TIMESTAMP_HEADER",
		"NIGHTSHIFT_AUTH_USERNAME", "NIGHTSHIFT_AUTH_PASSWORD", "NIGHTSHIFT_AUTH_TENANT_ID",
		"NIGHTSHIFT_AUTH_USER_ID", "NIGHTSHIFT_AUTH_APPROVER_ID",
		"NIGHTSHIFT_LOG_FORMAT", "NIGHTSHIFT_LOG_LEVEL", "NIGHTSHIFT_OTLP_ENDPOINT", "NIGHTSHIFT_METRICS_PUBLIC",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadDefaultsToSafeLocalConfiguration(t *testing.T) {
	clearConfigEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.FalkorURL != "redis://127.0.0.1:6379" || c.DataDir != "./data" {
		t.Fatalf("unexpected defaults: %#v", c)
	}
	if c.ActionsEnabled || c.AuthRequired {
		t.Fatalf("unsafe defaults: %#v", c)
	}
	if c.AuthTenantID != "tenant_demo" || c.AuthUserID != "user_demo" || c.AuthApproverID != "user_demo" {
		t.Fatalf("unexpected trusted identity defaults: %#v", c)
	}
}

func TestLoadUsesExplicitInstanceAndInstallationScope(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_INSTANCE_ID", "vps-worker-02")
	t.Setenv("NIGHTSHIFT_TENANT_ID", "tenant_acme")
	t.Setenv("NIGHTSHIFT_USER_ID", "user_alice")
	t.Setenv("NIGHTSHIFT_PROJECT_ID", "project_platform")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.InstanceID != "vps-worker-02" || c.TenantID != "tenant_acme" || c.UserID != "user_alice" || c.ProjectID != "project_platform" {
		t.Fatalf("unexpected configured scope: %#v", c)
	}
}

func TestLoadAllowsExplicitlyEnabledActionsForApprovalWorkflow(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_ACTIONS_ENABLED", "true")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() rejected approval-gated action mode: %v", err)
	}
}

func TestLoadRejectsInvalidInstallationScope(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_USER_ID", "user with spaces")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an invalid configured user scope")
	}
}

func TestLoadRejectsNonLoopbackWithoutAuthentication(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_LISTEN", "0.0.0.0:8080")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a non-loopback listener without authentication")
	}
}

func TestLoadRequiresCompleteBasicCredentials(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_AUTH_USERNAME", "operator")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an incomplete Basic credential pair")
	}
}

func TestLoadAcceptsTokenForNonLoopback(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_LISTEN", ":8080")
	t.Setenv("NIGHTSHIFT_AUTH_TOKEN", "a-long-enough-for-this-test-token")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !c.AuthRequired {
		t.Fatal("non-loopback listener must mark authentication as required")
	}
}

func TestListenIsLoopbackRecognizesOnlyExplicitLocalAddresses(t *testing.T) {
	for _, listen := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080"} {
		if !ListenIsLoopback(listen) {
			t.Fatalf("ListenIsLoopback(%q) = false", listen)
		}
	}
	for _, listen := range []string{"0.0.0.0:8080", "[::]:8080", ":8080"} {
		if ListenIsLoopback(listen) {
			t.Fatalf("ListenIsLoopback(%q) = true", listen)
		}
	}
}

func TestLoadRejectsUntrustedAuthIdentitySyntax(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_AUTH_USER_ID", "user with spaces")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a user identity containing spaces")
	}
}

func TestLoadDefaultsMailCredentialsFile(t *testing.T) {
	clearConfigEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.MailCredentialsFile == "" || !strings.Contains(c.MailCredentialsFile, "mail-credentials.json") {
		t.Fatalf("mail credentials default = %q", c.MailCredentialsFile)
	}
}

func TestLoadRequiresCompleteGmailOAuthConfiguration(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("GMAIL_CLIENT_ID", "client-id")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted GMAIL_CLIENT_ID without a client secret")
	}
	clearConfigEnv(t)
	t.Setenv("GMAIL_CLIENT_ID", "client-id")
	t.Setenv("GMAIL_CLIENT_SECRET", "client-secret")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted Gmail OAuth without a redirect URL")
	}
	clearConfigEnv(t)
	t.Setenv("GMAIL_CLIENT_ID", "client-id")
	t.Setenv("GMAIL_CLIENT_SECRET", "client-secret")
	t.Setenv("GMAIL_REDIRECT_URL", "https://nightshift.example.com/api/v1/mail/accounts/callback")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() rejected complete Gmail OAuth configuration: %v", err)
	}
}

func TestLoadTimezoneDefaultsToUTCAndRejectsInvalid(t *testing.T) {
	clearConfigEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.Timezone != "UTC" {
		t.Fatalf("timezone default = %q, want UTC", c.Timezone)
	}
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_TIMEZONE", "America/New_York")
	c, err = Load()
	if err != nil {
		t.Fatalf("Load() rejected valid timezone: %v", err)
	}
	if c.Timezone != "America/New_York" {
		t.Fatalf("timezone = %q, want America/New_York", c.Timezone)
	}
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_TIMEZONE", "Not/AZone")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an invalid IANA timezone")
	}
}

func TestLoadValidatesReminderWebhookURL(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_REMINDER_WEBHOOK_URL", "https://hooks.example.com/reminders")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() rejected a valid webhook URL: %v", err)
	}
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_REMINDER_WEBHOOK_URL", "not-a-url")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a malformed webhook URL")
	}
}

func TestLoadNormalizesGitHubConfiguration(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("GITHUB_TOKEN", "ghp_example_token")
	t.Setenv("GITHUB_REPOSITORIES", "Owner/Alpha, owner/beta ; Owner/Alpha")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(c.GitHubRepositories) != 2 {
		t.Fatalf("repositories = %v, want 2 unique entries", c.GitHubRepositories)
	}
	if c.GitHubRepositories[0] != "owner/alpha" || c.GitHubRepositories[1] != "owner/beta" {
		t.Fatalf("repositories = %v, want normalized owner/name entries", c.GitHubRepositories)
	}
	if c.GitHubAPIBase != "https://api.github.com" {
		t.Fatalf("api base = %q, want default", c.GitHubAPIBase)
	}

	clearConfigEnv(t)
	t.Setenv("GITHUB_REPOSITORIES", "invalid-entry")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an invalid repository entry")
	}
}

func TestLoadValidatesIdentityProviders(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_OIDC_ISSUER", "https://issuer.example")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an incomplete OIDC configuration")
	}

	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_OIDC_ISSUER", "https://issuer.example")
	t.Setenv("NIGHTSHIFT_OIDC_AUDIENCE", "nightshift")
	t.Setenv("NIGHTSHIFT_OIDC_JWKS_URL", "https://issuer.example/.well-known/jwks.json")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() rejected a complete OIDC configuration: %v", err)
	}
	if c.OIDCIssuer != "https://issuer.example" || c.OIDCTenantClaim != "" {
		t.Fatalf("oidc config = %+v", c)
	}

	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_TRUSTED_PROXY_SECRET", "proxy-secret")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() rejected a trusted-proxy configuration: %v", err)
	}

	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_OIDC_ISSUER", "https://issuer.example")
	t.Setenv("NIGHTSHIFT_OIDC_AUDIENCE", "nightshift")
	t.Setenv("NIGHTSHIFT_OIDC_JWKS_URL", "https://issuer.example/.well-known/jwks.json")
	t.Setenv("NIGHTSHIFT_TRUSTED_PROXY_SECRET", "proxy-secret")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted both OIDC and trusted-proxy configuration")
	}
}

func TestLoadValidatesObservabilityConfiguration(t *testing.T) {
	clearConfigEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.LogFormat != "json" || c.LogLevel != "info" || c.OTLPEndpoint != "" || c.MetricsPublic {
		t.Fatalf("unexpected observability defaults: %#v", c)
	}

	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_LOG_FORMAT", "text")
	t.Setenv("NIGHTSHIFT_LOG_LEVEL", "debug")
	t.Setenv("NIGHTSHIFT_OTLP_ENDPOINT", "http://otel-collector:4318")
	t.Setenv("NIGHTSHIFT_METRICS_PUBLIC", "true")
	c, err = Load()
	if err != nil {
		t.Fatalf("Load() rejected a valid observability configuration: %v", err)
	}
	if c.LogFormat != "text" || c.LogLevel != "debug" || c.OTLPEndpoint != "http://otel-collector:4318" || !c.MetricsPublic {
		t.Fatalf("observability config = %#v", c)
	}

	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_LOG_LEVEL", "verbose")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an invalid log level")
	}

	clearConfigEnv(t)
	t.Setenv("NIGHTSHIFT_OTLP_ENDPOINT", "not-a-url")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an invalid OTLP endpoint")
	}
}
