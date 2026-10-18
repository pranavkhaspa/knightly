package falkor

import (
	"context"
	"fmt"
	"time"

	"github.com/pranavkhaspa/knightly/internal/github"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

const GitHubObjectSaveQuery = `
	UNWIND $rows AS row
	MERGE (a:ProviderAccount {id: row.account_id, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET a.created_at = $observed_at
	SET a.provider = "github", a.resource = row.repository, a.status = "connected", a.updated_at = $observed_at
	MERGE (r:Repository {id: row.repository, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET r.created_at = $observed_at
	SET r.full_name = row.repository, r.updated_at = $observed_at
	MERGE (e:ExternalObject {id: row.id, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET e.created_at = $observed_at
	SET e.provider = row.provider,
	    e.object_type = row.object_type,
	    e.object_id = row.object_id,
	    e.repository = row.repository,
	    e.title = row.title,
	    e.summary = row.summary,
	    e.link = row.link,
	    e.sha = row.sha,
	    e.state = row.state,
	    e.untrusted_origin = row.untrusted_origin,
	    e.observed_at = row.observed_at,
	    e.updated_at = $observed_at
	MERGE (a)-[:HAS_OBJECT]->(e)
	MERGE (e)-[:IN_REPOSITORY]->(r)
	RETURN count(e) AS stored
`

const GitHubObjectListQuery = `
	MATCH (e:ExternalObject {tenant_id: $tenant_id, user_id: $user_id})
	WHERE ($repository = "" OR e.repository = $repository)
	  AND ($object_type = "" OR e.object_type = $object_type)
	RETURN e.id AS id, e.tenant_id AS tenant_id, e.user_id AS user_id, e.provider AS provider,
	       e.object_type AS object_type, e.object_id AS object_id, e.repository AS repository,
	       e.title AS title, e.summary AS summary, e.link AS link, e.sha AS sha, e.state AS state,
	       e.untrusted_origin AS untrusted_origin, e.observed_at AS observed_at
	ORDER BY e.observed_at DESC
	LIMIT $limit
`

// GitHubObjectRepository persists bounded GitHub external objects and their
// provider-account ownership in the routed user graph.
type GitHubObjectRepository struct {
	selector UserGraphSelector
	now      func() time.Time
}

var _ github.ObjectStore = (*GitHubObjectRepository)(nil)

func NewGitHubObjectRepository(selector UserGraphSelector) *GitHubObjectRepository {
	return &GitHubObjectRepository{selector: selector, now: func() time.Time { return time.Now().UTC() }}
}

func (r *GitHubObjectRepository) SaveExternalObjects(ctx context.Context, scope identity.Scope, objects []github.ExternalObject) ([]github.ExternalObject, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return nil, nil
	}
	graph, err := routedGraph(ctx, r.selector, scope, "github object save")
	if err != nil {
		return nil, err
	}
	observedAt := r.now().UTC()
	rows := make([]map[string]interface{}, 0, len(objects))
	for _, object := range objects {
		rows = append(rows, map[string]interface{}{
			"id": object.ID, "account_id": "github:" + object.Repository, "provider": object.Provider,
			"object_type": string(object.ObjectType), "object_id": object.ObjectID, "repository": object.Repository,
			"title": object.Title, "summary": object.Summary, "link": object.Link, "sha": object.SHA,
			"state": object.State, "untrusted_origin": object.UntrustedOrigin, "observed_at": optionalMillis(object.ObservedAt),
		})
	}
	if _, err := graph.Query(GitHubObjectSaveQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "rows": rows, "observed_at": optionalMillis(observedAt),
	}, queryOptionsFor(r.selector)); err != nil {
		return nil, fmt.Errorf("save GitHub objects in graph: %w", err)
	}
	return objects, nil
}

func (r *GitHubObjectRepository) ListExternalObjects(ctx context.Context, scope identity.Scope, query github.ListObjectsQuery) ([]github.ExternalObject, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "github object list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(GitHubObjectListQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID,
		"repository": github.NormalizeRepository(query.Repository), "object_type": string(query.ObjectType),
		"limit": query.Limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list GitHub objects from graph: %w", err)
	}
	objects := make([]github.ExternalObject, 0)
	for result != nil && result.Next() {
		objects = append(objects, externalObjectFromRecord(result.Record()))
	}
	return objects, nil
}

func externalObjectFromRecord(record recordReader) github.ExternalObject {
	value := github.ExternalObject{
		ID:              stringValue(recordField(record, "id")),
		TenantID:        stringValue(recordField(record, "tenant_id")),
		UserID:          stringValue(recordField(record, "user_id")),
		Provider:        stringValue(recordField(record, "provider")),
		ObjectType:      github.ObjectType(stringValue(recordField(record, "object_type"))),
		ObjectID:        stringValue(recordField(record, "object_id")),
		Repository:      stringValue(recordField(record, "repository")),
		Title:           stringValue(recordField(record, "title")),
		Summary:         stringValue(recordField(record, "summary")),
		Link:            stringValue(recordField(record, "link")),
		SHA:             stringValue(recordField(record, "sha")),
		State:           stringValue(recordField(record, "state")),
		UntrustedOrigin: stringValue(recordField(record, "untrusted_origin")),
		ObservedAt:      timeValue(recordField(record, "observed_at")),
	}
	if value.UntrustedOrigin == "" {
		value.UntrustedOrigin = github.UntrustedOrigin
	}
	return value
}
