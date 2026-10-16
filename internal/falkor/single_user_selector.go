package falkor

import (
	"context"
	"errors"
	"fmt"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

// SingleUserGraphSelector binds routed repositories to the trusted graph
// configured for this daemon installation. It keeps tenant/user filtering in
// the repositories while preventing a request from selecting a graph name.
// The control-plane selector remains available for multi-user integrations.
type SingleUserGraphSelector struct {
	client *Client
	scope  identity.Scope
}

var _ UserGraphSelector = (*SingleUserGraphSelector)(nil)

func NewSingleUserGraphSelector(client *Client, scope identity.Scope) (*SingleUserGraphSelector, error) {
	if client == nil || client.user == nil {
		return nil, errors.New("FalkorDB user graph is not configured")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return &SingleUserGraphSelector{client: client, scope: scope}, nil
}

func (s *SingleUserGraphSelector) SelectUserGraph(ctx context.Context, scope identity.Scope) (*dbclient.Graph, error) {
	if s == nil || s.client == nil || s.client.user == nil {
		return nil, errors.New("single-user graph selector is not configured")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if scope != s.scope {
		return nil, fmt.Errorf("requested scope is outside this installation")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.client.user, nil
}

func (s *SingleUserGraphSelector) QueryOptions() *dbclient.QueryOptions {
	if s == nil || s.client == nil {
		return dbclient.NewQueryOptions()
	}
	return s.client.QueryOptions()
}
