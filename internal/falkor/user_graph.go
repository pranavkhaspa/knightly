package falkor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

var (
	ErrUserGraphNotMapped = errors.New("user graph is not mapped")
	ErrInvalidUserGraph   = errors.New("invalid user graph mapping")
)

// UserGraphSelector is the integration seam for all user-scoped FalkorDB
// repositories. Implementations resolve the graph from trusted control-plane
// state; callers cannot provide a graph name.
type UserGraphSelector interface {
	SelectUserGraph(context.Context, identity.Scope) (*dbclient.Graph, error)
}

// UserGraphMappingStore is the control-plane lookup seam used by GraphRouter.
// A request never participates in this lookup beyond its already-authenticated
// tenant and user scope.
type UserGraphMappingStore interface {
	LookupUserGraph(context.Context, identity.Scope) (string, error)
}

type GraphRouter struct {
	mappings UserGraphMappingStore
}

func NewGraphRouter(mappings UserGraphMappingStore) *GraphRouter {
	return &GraphRouter{mappings: mappings}
}

func (r *GraphRouter) Resolve(ctx context.Context, scope identity.Scope) (string, error) {
	if r == nil || r.mappings == nil {
		return "", errors.New("user graph mapping store is not configured")
	}
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	graphName, err := r.mappings.LookupUserGraph(ctx, scope)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := validateGraphName(graphName); err != nil {
		return "", err
	}
	return graphName, nil
}

type queryOptionsProvider interface {
	QueryOptions() *dbclient.QueryOptions
}

func queryOptionsFor(selector UserGraphSelector) *dbclient.QueryOptions {
	if provider, ok := selector.(queryOptionsProvider); ok {
		if options := provider.QueryOptions(); options != nil {
			return options
		}
	}
	return dbclient.NewQueryOptions()
}

// EnsureUserGraph creates the stable control-plane mapping for a user. It is
// intended for setup flows after the caller has authenticated the user. The
// graph name is derived here and is never accepted from a request.
func (c *Client) EnsureUserGraph(ctx context.Context, scope identity.Scope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if c == nil || c.control == nil {
		return "", errors.New("FalkorDB control graph is not configured")
	}
	graphName := UserGraphName(scope.TenantID, scope.UserID)
	result, err := c.control.Query(`
		MERGE (mapping:UserGraph {tenant_id: $tenant_id, user_id: $user_id})
		SET mapping.graph_name = $graph_name, mapping.updated_at = timestamp()
		RETURN mapping.graph_name AS graph_name
	`, map[string]interface{}{
		"tenant_id":  scope.TenantID,
		"user_id":    scope.UserID,
		"graph_name": graphName,
	}, c.options())
	if err != nil {
		return "", fmt.Errorf("ensure user graph mapping: %w", err)
	}
	if err := requireQueryRow(result, "ensure user graph mapping"); err != nil {
		return "", err
	}
	return graphName, nil
}

// SelectUserGraph looks up the graph only through the control graph. The
// mapping is checked before selection so malformed control data cannot turn
// into an unexpected graph name. A request-supplied graph name has no input
// path to this method.
func (c *Client) SelectUserGraph(ctx context.Context, scope identity.Scope) (*dbclient.Graph, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if c == nil || c.control == nil || c.db == nil {
		return nil, errors.New("FalkorDB graph routing is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	graphName, err := c.resolveUserGraphName(ctx, scope)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.db.SelectGraph(graphName), nil
}

func (c *Client) resolveUserGraphName(ctx context.Context, scope identity.Scope) (string, error) {
	graphName, err := NewGraphRouter(c).Resolve(ctx, scope)
	if err != nil {
		return "", err
	}
	return graphName, nil
}

func (c *Client) LookupUserGraph(ctx context.Context, scope identity.Scope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if c == nil || c.control == nil {
		return "", errors.New("FalkorDB control graph is not configured")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	result, err := c.control.ROQuery(`
		MATCH (mapping:UserGraph {tenant_id: $tenant_id, user_id: $user_id})
		RETURN mapping.graph_name AS graph_name
		LIMIT 1
	`, map[string]interface{}{
		"tenant_id": scope.TenantID,
		"user_id":   scope.UserID,
	}, c.options())
	if err != nil {
		return "", fmt.Errorf("resolve user graph mapping: %w", err)
	}
	if result == nil || !result.Next() {
		return "", fmt.Errorf("%w: tenant %q user %q", ErrUserGraphNotMapped, scope.TenantID, scope.UserID)
	}
	return stringValue(recordValue(result.Record(), "graph_name")), nil
}

func validateGraphName(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%w: graph name is empty", ErrInvalidUserGraph)
	}
	if len(value) > 128 {
		return fmt.Errorf("%w: graph name exceeds 128 characters", ErrInvalidUserGraph)
	}
	for _, character := range value {
		if unicode.IsLetter(character) || unicode.IsDigit(character) || character == '_' || character == '-' {
			continue
		}
		return fmt.Errorf("%w: graph name contains unsupported character", ErrInvalidUserGraph)
	}
	if strings.HasPrefix(value, "nightshift_control") {
		return fmt.Errorf("%w: control graph cannot be selected as a user graph", ErrInvalidUserGraph)
	}
	return nil
}
