package config

import "testing"

func TestLoadDefaultsToSafeLocalConfiguration(t *testing.T) {
	for _, key := range []string{
		"NIGHTSHIFT_LISTEN",
		"NIGHTSHIFT_ENV",
		"NIGHTSHIFT_INSTANCE_ID",
		"FALKORDB_URL",
		"FALKORDB_CONTROL_GRAPH",
		"FALKORDB_GRAPH",
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
}

func TestLoadRejectsEnabledActionsInFoundation(t *testing.T) {
	t.Setenv("NIGHTSHIFT_ACTIONS_ENABLED", "true")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted unsupported external actions")
	}
}
