package falkor

import (
	"context"
	"fmt"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/requirement"
)

type RequirementRepository struct {
	selector UserGraphSelector
}

var _ requirement.Repository = (*RequirementRepository)(nil)

func NewRequirementRepository(selector UserGraphSelector) *RequirementRepository {
	return &RequirementRepository{selector: selector}
}

func (r *RequirementRepository) Save(ctx context.Context, value requirement.Requirement) error {
	if r == nil {
		return fmt.Errorf("requirement repository is not configured")
	}
	scope := identity.Scope{TenantID: value.TenantID, UserID: value.UserID}
	graph, err := routedGraph(ctx, r.selector, scope, "requirement save")
	if err != nil {
		return err
	}
	result, err := graph.Query(`
		MATCH (p:Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (r:Requirement {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET r.created_at = $created_at
		SET r.project_id = $project_id,
		    r.title = $title,
		    r.description = $description,
		    r.status = $status,
		    r.priority = $priority,
		    r.technology_ids = $technology_ids,
		    r.constraints = $constraints,
		    r.updated_at = $updated_at
		MERGE (p)-[:HAS_REQUIREMENT]->(r)
		RETURN r.id AS id
	`, requirementParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save requirement in graph: %w", err)
	}
	if err := requireQueryRow(result, "save requirement"); err != nil {
		return fmt.Errorf("%w: project %q may not exist in this user scope", err, value.ProjectID)
	}
	if _, err := graph.Query(`
		MATCH (r:Requirement {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		OPTIONAL MATCH (r)-[old:NEEDS]->(:Technology)
		DELETE old
	`, map[string]interface{}{"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID}, queryOptionsFor(r.selector)); err != nil {
		return fmt.Errorf("replace requirement technologies: %w", err)
	}
	if len(value.TechnologyIDs) > 0 {
		if _, err := graph.Query(`
			MATCH (r:Requirement {id: $id, tenant_id: $tenant_id, user_id: $user_id})
			UNWIND $technology_ids AS technology_id
			MERGE (technology:Technology {id: technology_id, tenant_id: $tenant_id, user_id: $user_id})
			MERGE (r)-[:NEEDS]->(technology)
		`, map[string]interface{}{
			"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID, "technology_ids": value.TechnologyIDs,
		}, queryOptionsFor(r.selector)); err != nil {
			return fmt.Errorf("link requirement technologies: %w", err)
		}
	}
	return nil
}

func (r *RequirementRepository) Get(ctx context.Context, scope identity.Scope, id string) (requirement.Requirement, error) {
	if r == nil {
		return requirement.Requirement{}, fmt.Errorf("requirement repository is not configured")
	}
	graph, err := routedGraph(ctx, r.selector, scope, "requirement get")
	if err != nil {
		return requirement.Requirement{}, err
	}
	result, err := graph.ROQuery(requirementQuery+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": id, "project_id": "",
	}, queryOptionsFor(r.selector))
	if err != nil {
		return requirement.Requirement{}, fmt.Errorf("get requirement from graph: %w", err)
	}
	if err := requireQueryRow(result, "get requirement"); err != nil {
		return requirement.Requirement{}, fmt.Errorf("%w: %s", requirement.ErrRequirementNotFound, err)
	}
	return requirementFromRecord(result.Record()), nil
}

func (r *RequirementRepository) List(ctx context.Context, scope identity.Scope, projectID string, limit int) ([]requirement.Requirement, error) {
	if r == nil {
		return nil, fmt.Errorf("requirement repository is not configured")
	}
	graph, err := routedGraph(ctx, r.selector, scope, "requirement list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(requirementQuery+" ORDER BY coalesce(r.priority, 999999), r.id LIMIT $limit", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": "", "project_id": projectID, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list requirements from graph: %w", err)
	}
	values := make([]requirement.Requirement, 0, limit)
	for result.Next() {
		values = append(values, requirementFromRecord(result.Record()))
	}
	return values, nil
}

func requirementParams(value requirement.Requirement) map[string]interface{} {
	return map[string]interface{}{
		"id":             value.ID,
		"tenant_id":      value.TenantID,
		"user_id":        value.UserID,
		"project_id":     value.ProjectID,
		"title":          value.Title,
		"description":    value.Description,
		"status":         string(value.Status),
		"priority":       value.Priority,
		"technology_ids": value.TechnologyIDs,
		"constraints":    value.Constraints,
		"created_at":     optionalMillis(value.CreatedAt),
		"updated_at":     optionalMillis(value.UpdatedAt),
	}
}

const requirementQuery = `
	MATCH (r:Requirement {tenant_id: $tenant_id, user_id: $user_id})
	WHERE ($id = "" OR r.id = $id) AND ($project_id = "" OR r.project_id = $project_id)
	RETURN r.id AS id, r.tenant_id AS tenant_id, r.user_id AS user_id,
	       r.project_id AS project_id, r.title AS title, r.description AS description,
	       r.status AS status, r.priority AS priority, r.technology_ids AS technology_ids,
	       r.constraints AS constraints, r.created_at AS created_at, r.updated_at AS updated_at
`

func requirementFromRecord(record recordReader) requirement.Requirement {
	return requirement.Requirement{
		ID:            stringValue(recordField(record, "id")),
		TenantID:      stringValue(recordField(record, "tenant_id")),
		UserID:        stringValue(recordField(record, "user_id")),
		ProjectID:     stringValue(recordField(record, "project_id")),
		Title:         stringValue(recordField(record, "title")),
		Description:   stringValue(recordField(record, "description")),
		Status:        requirement.Status(stringValue(recordField(record, "status"))),
		Priority:      int(recordInt64(recordField(record, "priority"))),
		TechnologyIDs: stringSliceValue(recordField(record, "technology_ids")),
		Constraints:   stringSliceValue(recordField(record, "constraints")),
		CreatedAt:     timeValue(recordField(record, "created_at")),
		UpdatedAt:     timeValue(recordField(record, "updated_at")),
	}
}
