package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config contains only deployment configuration. Durable product state belongs
// in FalkorDB, not in this process or a second local database.
type Config struct {
	Listen             string
	Environment        string
	InstanceID         string
	FalkorURL          string
	ControlGraph       string
	UserGraph          string
	QueryTimeoutMillis int
	ActionsEnabled     bool
}

func Load() (Config, error) {
	c := Config{
		Listen:             envOr("NIGHTSHIFT_LISTEN", "127.0.0.1:8080"),
		Environment:        envOr("NIGHTSHIFT_ENV", "development"),
		InstanceID:         instanceID(),
		FalkorURL:          envOr("FALKORDB_URL", "redis://127.0.0.1:6379"),
		ControlGraph:       envOr("FALKORDB_CONTROL_GRAPH", "nightshift_control"),
		UserGraph:          envOr("FALKORDB_GRAPH", "nightshift_u_demo"),
		QueryTimeoutMillis: 5000,
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

	if c.ActionsEnabled {
		return Config{}, fmt.Errorf("external actions are not available in the foundation and must remain disabled")
	}
	if c.Listen == "" || c.FalkorURL == "" || c.ControlGraph == "" || c.UserGraph == "" {
		return Config{}, fmt.Errorf("listen, FalkorDB URL, and graph names must be non-empty")
	}

	return c, nil
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
