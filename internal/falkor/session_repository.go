package falkor

import (
	"context"
	"fmt"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/session"
)

type SessionRepository struct {
	selector UserGraphSelector
}

var _ session.SessionRepository = (*SessionRepository)(nil)

func NewSessionRepository(selector UserGraphSelector) *SessionRepository {
	return &SessionRepository{selector: selector}
}

// SessionDecisionRepository is the composite seam used by session.Service.
// The separate repositories remain available when an integration only needs
// one record family.
type SessionDecisionRepository struct {
	*SessionRepository
	*DecisionRepository
}

var _ session.Repository = (*SessionDecisionRepository)(nil)

func NewSessionDecisionRepository(selector UserGraphSelector) *SessionDecisionRepository {
	return &SessionDecisionRepository{
		SessionRepository:  NewSessionRepository(selector),
		DecisionRepository: NewDecisionRepository(selector),
	}
}

func (r *SessionRepository) SaveSession(ctx context.Context, value session.Session) error {
	if r == nil {
		return fmt.Errorf("session repository is not configured")
	}
	scope := identity.Scope{TenantID: value.TenantID, UserID: value.UserID}
	graph, err := routedGraph(ctx, r.selector, scope, "session save")
	if err != nil {
		return err
	}
	if value.ProjectID != "" {
		if err := scopedProjectExists(graph, r.selector, scope, value.ProjectID); err != nil {
			return fmt.Errorf("validate session project: %w", err)
		}
	}
	result, err := graph.Query(`
		MERGE (s:Session {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET s.created_at = $created_at
		SET s.project_id = $project_id,
		    s.title = $title,
		    s.summary = $summary,
		    s.status = $status,
		    s.started_at = $started_at,
		    s.ended_at = $ended_at,
		    s.updated_at = $updated_at
		RETURN s.id AS id
	`, sessionParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save session in graph: %w", err)
	}
	return requireQueryRow(result, "save session")
}

func (r *SessionRepository) GetSession(ctx context.Context, scope identity.Scope, id string) (session.Session, error) {
	if r == nil {
		return session.Session{}, fmt.Errorf("session repository is not configured")
	}
	graph, err := routedGraph(ctx, r.selector, scope, "session get")
	if err != nil {
		return session.Session{}, err
	}
	result, err := graph.ROQuery(sessionQuery+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": id,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return session.Session{}, fmt.Errorf("get session from graph: %w", err)
	}
	if err := requireQueryRow(result, "get session"); err != nil {
		return session.Session{}, fmt.Errorf("%w: %s", session.ErrSessionNotFound, err)
	}
	return sessionFromRecord(result.Record()), nil
}

func scopedProjectExists(graph *dbclient.Graph, selector UserGraphSelector, scope identity.Scope, projectID string) error {
	result, err := graph.ROQuery(`
		MATCH (p:Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
		RETURN p.id AS id
		LIMIT 1
	`, map[string]interface{}{"project_id": projectID, "tenant_id": scope.TenantID, "user_id": scope.UserID}, queryOptionsFor(selector))
	if err != nil {
		return err
	}
	if err := requireQueryRow(result, "find project"); err != nil {
		return fmt.Errorf("project %q is not in this user scope", projectID)
	}
	return nil
}

func sessionParams(value session.Session) map[string]interface{} {
	return map[string]interface{}{
		"id":         value.ID,
		"tenant_id":  value.TenantID,
		"user_id":    value.UserID,
		"project_id": value.ProjectID,
		"title":      value.Title,
		"summary":    value.Summary,
		"status":     string(value.Status),
		"started_at": value.StartedAt.UnixMilli(),
		"ended_at":   value.EndedAt.UnixMilli(),
		"created_at": value.CreatedAt.UnixMilli(),
		"updated_at": value.UpdatedAt.UnixMilli(),
	}
}

const sessionQuery = `
	MATCH (s:Session {id: $id, tenant_id: $tenant_id, user_id: $user_id})
	RETURN s.id AS id, s.tenant_id AS tenant_id, s.user_id AS user_id,
	       s.project_id AS project_id, s.title AS title, s.summary AS summary,
	       s.status AS status, s.started_at AS started_at, s.ended_at AS ended_at,
	       s.created_at AS created_at, s.updated_at AS updated_at
`

func sessionFromRecord(record recordReader) session.Session {
	return session.Session{
		ID:        stringValue(recordField(record, "id")),
		TenantID:  stringValue(recordField(record, "tenant_id")),
		UserID:    stringValue(recordField(record, "user_id")),
		ProjectID: stringValue(recordField(record, "project_id")),
		Title:     stringValue(recordField(record, "title")),
		Summary:   stringValue(recordField(record, "summary")),
		Status:    session.SessionStatus(stringValue(recordField(record, "status"))),
		StartedAt: timeValue(recordField(record, "started_at")),
		EndedAt:   timeValue(recordField(record, "ended_at")),
		CreatedAt: timeValue(recordField(record, "created_at")),
		UpdatedAt: timeValue(recordField(record, "updated_at")),
	}
}
