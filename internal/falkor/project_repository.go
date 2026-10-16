package falkor

import (
	"context"
	"fmt"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/project"
)

type ProjectRepository struct {
	selector UserGraphSelector
}

var _ project.Repository = (*ProjectRepository)(nil)

func NewProjectRepository(selector UserGraphSelector) *ProjectRepository {
	return &ProjectRepository{selector: selector}
}

func (r *ProjectRepository) Save(ctx context.Context, value project.Project) error {
	if r == nil {
		return fmt.Errorf("project repository is not configured")
	}
	scope := identity.Scope{TenantID: value.TenantID, UserID: value.UserID}
	graph, err := routedGraph(ctx, r.selector, scope, "project save")
	if err != nil {
		return err
	}
	result, err := graph.Query(`
		MERGE (u:User {id: $user_id, tenant_id: $tenant_id})
		MERGE (p:Project {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET p.created_at = $created_at
		SET p.name = $name,
		    p.description = $description,
		    p.prd_text = $prd_text,
		    p.technologies = $technologies,
		    p.constraints = $constraints,
		    p.status = $status,
		    p.repository_url = $repository_url,
		    p.desired_outcomes = $desired_outcomes,
		    p.updated_at = $updated_at
		MERGE (u)-[:OWNS]->(p)
		RETURN p.id AS id
	`, projectParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save project in graph: %w", err)
	}
	return requireQueryRow(result, "save project")
}

func (r *ProjectRepository) Get(ctx context.Context, scope identity.Scope, id string) (project.Project, error) {
	if r == nil {
		return project.Project{}, fmt.Errorf("project repository is not configured")
	}
	graph, err := routedGraph(ctx, r.selector, scope, "project get")
	if err != nil {
		return project.Project{}, err
	}
	result, err := graph.ROQuery(projectQuery+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": id,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return project.Project{}, fmt.Errorf("get project from graph: %w", err)
	}
	if err := requireQueryRow(result, "get project"); err != nil {
		return project.Project{}, fmt.Errorf("%w: %s", project.ErrProjectNotFound, err)
	}
	return projectFromRecord(result.Record()), nil
}

func (r *ProjectRepository) List(ctx context.Context, scope identity.Scope, limit int) ([]project.Project, error) {
	if r == nil {
		return nil, fmt.Errorf("project repository is not configured")
	}
	graph, err := routedGraph(ctx, r.selector, scope, "project list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(projectQuery+" ORDER BY coalesce(p.updated_at, 0) DESC, p.id LIMIT $limit", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": "", "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list projects from graph: %w", err)
	}
	values := make([]project.Project, 0, limit)
	for result.Next() {
		values = append(values, projectFromRecord(result.Record()))
	}
	return values, nil
}

func projectParams(value project.Project) map[string]interface{} {
	return map[string]interface{}{
		"id":               value.ID,
		"tenant_id":        value.TenantID,
		"user_id":          value.UserID,
		"name":             value.Name,
		"description":      value.Description,
		"prd_text":         value.PRDText,
		"technologies":     value.Technologies,
		"constraints":      value.Constraints,
		"status":           string(value.Status),
		"repository_url":   value.RepositoryURL,
		"desired_outcomes": value.DesiredOutcomes,
		"created_at":       value.CreatedAt.UnixMilli(),
		"updated_at":       value.UpdatedAt.UnixMilli(),
	}
}

const projectQuery = `
	MATCH (p:Project {tenant_id: $tenant_id, user_id: $user_id})
	WHERE $id = "" OR p.id = $id
	RETURN p.id AS id, p.tenant_id AS tenant_id, p.user_id AS user_id,
	       p.name AS name, p.description AS description, p.prd_text AS prd_text,
	       p.technologies AS technologies, p.constraints AS constraints,
	       p.status AS status, p.repository_url AS repository_url,
	       p.desired_outcomes AS desired_outcomes, p.created_at AS created_at,
	       p.updated_at AS updated_at
`

func projectFromRecord(record recordReader) project.Project {
	return project.Project{
		ID:              stringValue(recordField(record, "id")),
		TenantID:        stringValue(recordField(record, "tenant_id")),
		UserID:          stringValue(recordField(record, "user_id")),
		Name:            stringValue(recordField(record, "name")),
		Description:     stringValue(recordField(record, "description")),
		PRDText:         stringValue(recordField(record, "prd_text")),
		Technologies:    stringSliceValue(recordField(record, "technologies")),
		Constraints:     stringSliceValue(recordField(record, "constraints")),
		Status:          project.Status(stringValue(recordField(record, "status"))),
		RepositoryURL:   stringValue(recordField(record, "repository_url")),
		DesiredOutcomes: stringSliceValue(recordField(record, "desired_outcomes")),
		CreatedAt:       timeValue(recordField(record, "created_at")),
		UpdatedAt:       timeValue(recordField(record, "updated_at")),
	}
}
