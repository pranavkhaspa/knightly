package falkor

import (
	"context"
	"fmt"
	"strings"
	"time"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/redis/go-redis/v9"
)

const (
	demoUserID       = "user_demo"
	demoProjectID    = "project_nightshift"
	demoInterestID   = "interest_graph_agents"
	demoRequirement  = "requirement_graph_context"
	demoTechnologyID = "technology_falkordb"
)

// Client is the only package allowed to construct FalkorDB Cypher. Keeping
// this boundary small makes graph dependence visible and reviewable.
type Client struct {
	db           *dbclient.FalkorDB
	control      *dbclient.Graph
	user         *dbclient.Graph
	controlName  string
	userName     string
	queryTimeout int
}

type Status struct {
	Available         bool   `json:"available"`
	ControlGraph      string `json:"control_graph"`
	UserGraph         string `json:"user_graph"`
	NodeCount         int64  `json:"node_count,omitempty"`
	RelationshipCount int64  `json:"relationship_count,omitempty"`
	Error             string `json:"error,omitempty"`
}

func New(url, controlGraph, userGraph string, queryTimeoutMillis int) (*Client, error) {
	optionsURL := url
	if strings.HasPrefix(optionsURL, "falkor://") {
		optionsURL = "redis://" + strings.TrimPrefix(optionsURL, "falkor://")
	}
	if strings.HasPrefix(optionsURL, "falkors://") {
		optionsURL = "rediss://" + strings.TrimPrefix(optionsURL, "falkors://")
	}
	options, err := redis.ParseURL(optionsURL)
	if err != nil {
		return nil, fmt.Errorf("parse FalkorDB URL: %w", err)
	}
	// Construct the Redis transport directly so client creation is lazy. The
	// official FalkorDB client performs a sentinel probe during construction,
	// which would delay /health/live whenever a remote graph is offline.
	db := &dbclient.FalkorDB{Conn: redis.NewClient(options)}
	return &Client{
		db:           db,
		control:      db.SelectGraph(controlGraph),
		user:         db.SelectGraph(userGraph),
		controlName:  controlGraph,
		userName:     userGraph,
		queryTimeout: queryTimeoutMillis,
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.db == nil || c.db.Conn == nil {
		return nil
	}
	return c.db.Conn.Close()
}

// AddHook installs a go-redis hook on the underlying connection so callers can
// observe command latency and errors without wrapping every repository.
func (c *Client) AddHook(hook redis.Hook) {
	if c == nil || c.db == nil || c.db.Conn == nil || hook == nil {
		return
	}
	c.db.Conn.AddHook(hook)
}

func (c *Client) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.db.Conn.Ping(ctx).Err()
}

func (c *Client) Status(ctx context.Context) Status {
	status := Status{
		ControlGraph: c.controlName,
		UserGraph:    c.userName,
	}
	if err := c.Ping(ctx); err != nil {
		status.Error = err.Error()
		return status
	}
	status.Available = true

	nodeResult, err := c.user.ROQuery(
		"MATCH (n) RETURN count(n) AS nodes",
		nil,
		c.options(),
	)
	if err != nil {
		status.Available = false
		status.Error = err.Error()
		return status
	}
	if nodeResult.Next() {
		record := nodeResult.Record()
		status.NodeCount = int64Value(recordValue(record, "nodes"))
	}

	relationshipResult, err := c.user.ROQuery(
		"MATCH ()-[r]->() RETURN count(r) AS relationships",
		nil,
		c.options(),
	)
	if err != nil {
		status.Available = false
		status.Error = err.Error()
		return status
	}
	if relationshipResult.Next() {
		status.RelationshipCount = int64Value(recordValue(relationshipResult.Record(), "relationships"))
	}
	return status
}

// Initialize creates the minimum graph context needed for a meaningful demo.
// It is idempotent and deliberately stores product state only in FalkorDB.
func (c *Client) Initialize() error {
	return c.InitializeFor(context.Background(), identity.Scope{TenantID: "tenant_demo", UserID: demoUserID})
}

// InitializeFor creates the control-plane mapping and the minimum user graph
// context for the trusted installation scope. Initialize historically seeded
// the demo user; keeping it as a wrapper preserves the integration-test and
// local fixture API while the daemon can now initialize a non-demo installation
// without writing demo records into the wrong scope.
func (c *Client) InitializeFor(ctx context.Context, scope identity.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("initialize scope: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.db == nil || c.control == nil {
		return fmt.Errorf("FalkorDB client is not configured")
	}
	graphName := c.userName
	if scope.TenantID != "tenant_demo" || scope.UserID != demoUserID {
		graphName = UserGraphName(scope.TenantID, scope.UserID)
	}
	updatedAt := time.Now().UTC().UnixMilli()
	if _, err := c.control.Query(`
		MERGE (s:Schema {id: "nightshift"})
		SET s.version = "0.1-foundation", s.updated_at = $updated_at
		MERGE (mapping:UserGraph {tenant_id: $tenant_id, user_id: $user_id})
		SET mapping.graph_name = $graph_name, mapping.updated_at = $updated_at
	`, map[string]interface{}{"tenant_id": scope.TenantID, "user_id": scope.UserID, "graph_name": graphName, "updated_at": updatedAt}, c.options()); err != nil {
		return fmt.Errorf("initialize control graph: %w", err)
	}

	// Repoint the client before any user-scoped repository is used. The
	// repositories intentionally retain a pointer to Client, so this keeps the
	// existing construction API while making the selected graph authoritative.
	if c.userName != graphName {
		c.user = c.db.SelectGraph(graphName)
		c.userName = graphName
	}

	if scope.TenantID != "tenant_demo" || scope.UserID != demoUserID {
		if _, err := c.user.Query(`
			MERGE (u:User {id: $user_id, tenant_id: $tenant_id})
			ON CREATE SET u.created_at = $updated_at
			SET u.tenant_id = $tenant_id, u.updated_at = $updated_at
		`, map[string]interface{}{
			"user_id": scope.UserID, "tenant_id": scope.TenantID, "updated_at": updatedAt,
		}, c.options()); err != nil {
			return fmt.Errorf("initialize user graph: %w", err)
		}
		return nil
	}

	if _, err := c.user.Query(`
		MERGE (u:User {id: $user_id, tenant_id: $tenant_id})
		SET u.tenant_id = $tenant_id, u.display_name = $display_name,
		    u.updated_at = $updated_at
		MERGE (p:Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
		SET p.tenant_id = $tenant_id, p.user_id = $user_id,
		    p.name = $project_name, p.status = "active", p.updated_at = $updated_at
		MERGE (u)-[:OWNS]->(p)
		MERGE (interest:Interest {id: $interest_id, tenant_id: $tenant_id, user_id: $user_id})
		SET interest.name = "Graph-powered agents", interest.normalized_name = "graph agents"
		MERGE (u)-[:INTERESTED_IN]->(interest)
		MERGE (r:Requirement {id: $requirement_id, tenant_id: $tenant_id, user_id: $user_id})
		SET r.tenant_id = $tenant_id, r.user_id = $user_id,
		    r.title = "Use connected context for explainable recommendations",
		    r.status = "open", r.priority = 1
		MERGE (p)-[:HAS_REQUIREMENT]->(r)
		MERGE (tech:Technology {id: $technology_id, tenant_id: $tenant_id, user_id: $user_id})
		SET tech.tenant_id = $tenant_id, tech.user_id = $user_id,
		    tech.name = "FalkorDB", tech.normalized_name = "falkordb", tech.category = "graph database"
		MERGE (r)-[:NEEDS]->(tech)
	`, map[string]interface{}{
		"user_id":        scope.UserID,
		"tenant_id":      scope.TenantID,
		"display_name":   "Demo Builder",
		"project_id":     demoProjectID,
		"project_name":   "NightShift",
		"interest_id":    demoInterestID,
		"requirement_id": demoRequirement,
		"technology_id":  demoTechnologyID,
		"updated_at":     updatedAt,
	}, c.options()); err != nil {
		return fmt.Errorf("initialize user graph: %w", err)
	}
	return nil
}

var _ UserGraphSelector = (*Client)(nil)

func (c *Client) SelectUserGraphFor(ctx context.Context, tenantID, userID string) (*dbclient.Graph, error) {
	return c.SelectUserGraph(ctx, identity.Scope{TenantID: tenantID, UserID: userID})
}

func (c *Client) options() *dbclient.QueryOptions {
	return dbclient.NewQueryOptions().SetTimeout(c.queryTimeout)
}

// QueryOptions lets routed repositories reuse the configured bounded query
// timeout without exposing the FalkorDB client itself to domain packages.
func (c *Client) QueryOptions() *dbclient.QueryOptions {
	if c == nil {
		return dbclient.NewQueryOptions()
	}
	return c.options()
}

func recordValue(record interface {
	Get(string) (interface{}, bool)
}, key string) interface{} {
	value, _ := record.Get(key)
	return value
}

func int64Value(value interface{}) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

// Keep the time import tied to this package's query lifecycle so callers can
// use a bounded readiness probe without inventing an unbounded wait.
func ReadinessContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 2*time.Second)
}
