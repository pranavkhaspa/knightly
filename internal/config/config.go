package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// Config contains only deployment configuration. Durable product state belongs
// in FalkorDB, not in this process or a second local database.
type Config struct {
	Listen             string
	Environment        string
	InstanceID         string
	TenantID           string
	UserID             string
	ProjectID          string
	FalkorURL          string
	ControlGraph       string
	UserGraph          string
	DataDir            string
	QueryTimeoutMillis int
	ActionsEnabled     bool

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
}

func Load() (Config, error) {
	c := Config{
		Listen:             envOr("NIGHTSHIFT_LISTEN", "127.0.0.1:8080"),
		Environment:        envOr("NIGHTSHIFT_ENV", "development"),
		InstanceID:         instanceID(),
		TenantID:           envOr("NIGHTSHIFT_TENANT_ID", "tenant_demo"),
		UserID:             envOr("NIGHTSHIFT_USER_ID", "user_demo"),
		ProjectID:          envOr("NIGHTSHIFT_PROJECT_ID", "project_nightshift"),
		FalkorURL:          envOr("FALKORDB_URL", "redis://127.0.0.1:6379"),
		ControlGraph:       envOr("FALKORDB_CONTROL_GRAPH", "nightshift_control"),
		UserGraph:          envOr("FALKORDB_GRAPH", "nightshift_u_demo"),
		DataDir:            envOr("NIGHTSHIFT_DATA_DIR", "./data"),
		QueryTimeoutMillis: 5000,
		AuthToken:          os.Getenv("NIGHTSHIFT_AUTH_TOKEN"),
		AuthUsername:       os.Getenv("NIGHTSHIFT_AUTH_USERNAME"),
		AuthPassword:       os.Getenv("NIGHTSHIFT_AUTH_PASSWORD"),
		AuthTenantID:       envOr("NIGHTSHIFT_AUTH_TENANT_ID", envOr("NIGHTSHIFT_TENANT_ID", "tenant_demo")),
		AuthUserID:         envOr("NIGHTSHIFT_AUTH_USER_ID", envOr("NIGHTSHIFT_USER_ID", "user_demo")),
		AuthApproverID:     envOr("NIGHTSHIFT_AUTH_APPROVER_ID", envOr("NIGHTSHIFT_USER_ID", "user_demo")),
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
