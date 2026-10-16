package config

import "testing"

func TestLoadDefaultsToSafeLocalConfiguration(t *testing.T) {
	for _, key := range []string{
		"NIGHTSHIFT_LISTEN",
		"NIGHTSHIFT_ENV",
		"NIGHTSHIFT_INSTANCE_ID",
		"HOSTNAME",
		"NIGHTSHIFT_TENANT_ID",
		"NIGHTSHIFT_USER_ID",
		"NIGHTSHIFT_PROJECT_ID",
		"FALKORDB_URL",
		"FALKORDB_CONTROL_GRAPH",
		"FALKORDB_GRAPH",
		"NIGHTSHIFT_DATA_DIR",
		"FALKORDB_QUERY_TIMEOUT_MS",
		"NIGHTSHIFT_ACTIONS_ENABLED",
	} {
		t.Setenv(key, "")
	}

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.FalkorURL != "redis://127.0.0.1:6379" {
		t.Fatalf("unexpected default FalkorDB URL: %q", c.FalkorURL)
	}
	if c.ActionsEnabled {
		t.Fatal("external actions must default to disabled")
	}
	if c.InstanceID == "" {
		t.Fatal("instance ID must have a non-empty fallback")
	}
	if c.DataDir != "./data" {
		t.Fatalf("unexpected data directory: %q", c.DataDir)
	}
}

func TestLoadUsesExplicitInstanceID(t *testing.T) {
	t.Setenv("NIGHTSHIFT_INSTANCE_ID", "vps-worker-02")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.InstanceID != "vps-worker-02" {
		t.Fatalf("InstanceID = %q, want explicit value", c.InstanceID)
	}
}

func TestLoadAllowsExplicitlyEnabledActionsForApprovalWorkflow(t *testing.T) {
	t.Setenv("NIGHTSHIFT_ACTIONS_ENABLED", "true")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() rejected approval-gated action mode: %v", err)
	}
}

func TestLoadUsesTrustedInstallationScope(t *testing.T) {
	t.Setenv("NIGHTSHIFT_TENANT_ID", "tenant_acme")
	t.Setenv("NIGHTSHIFT_USER_ID", "user_alice")
	t.Setenv("NIGHTSHIFT_PROJECT_ID", "project_platform")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.TenantID != "tenant_acme" || c.UserID != "user_alice" || c.ProjectID != "project_platform" {
		t.Fatalf("scope = %q/%q project=%q", c.TenantID, c.UserID, c.ProjectID)
	}
}

func TestLoadRejectsInvalidInstallationScope(t *testing.T) {
	t.Setenv("NIGHTSHIFT_USER_ID", "user with spaces")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an invalid configured user scope")
	}
}
