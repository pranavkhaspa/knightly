package falkor

import (
	"context"
	"errors"
	"fmt"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

func routedGraph(ctx context.Context, selector UserGraphSelector, scope identity.Scope, operation string) (*dbclient.Graph, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if selector == nil {
		return nil, fmt.Errorf("%s repository is not configured", operation)
	}
	graph, err := selector.SelectUserGraph(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("select user graph for %s: %w", operation, err)
	}
	if graph == nil {
		return nil, errors.New("selected user graph is nil")
	}
	return graph, nil
}
