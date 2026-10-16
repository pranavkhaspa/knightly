package config

import "testing"

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"NIGHTSHIFT_LISTEN", "NIGHTSHIFT_ENV", "NIGHTSHIFT_INSTANCE_ID", "HOSTNAME",
		"NIGHTSHIFT_TENANT_ID", "NIGHTSHIFT_USER_ID", "NIGHTSHIFT_PROJECT_ID",
		"FALKORDB_URL", "FALKORDB_CONTROL_GRAPH", "FALKORDB_GRAPH", "NIGHTSHIFT_DATA_DIR",
		"FALKORDB_QUERY_TIMEOUT_MS", "NIGHTSHIFT_ACTIONS_ENABLED", "NIGHTSHIFT_AUTH_TOKEN",
		"NIGHTSHIFT_SEARCH_URL",
		"NIGHTSHIFT_AUTH_USERNAME", "NIGHTSHIFT_AUTH_PASSWORD", "NIGHTSHIFT_AUTH_TENANT_ID",
		"NIGHTSHIFT_AUTH_USER_ID", "NIGHTSHIFT_AUTH_APPROVER_ID",
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
