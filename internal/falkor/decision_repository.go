package falkor

import (
	"context"
	"fmt"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/session"
)

type DecisionRepository struct {
	selector UserGraphSelector
}

var _ session.DecisionRepository = (*DecisionRepository)(nil)

func NewDecisionRepository(selector UserGraphSelector) *DecisionRepository {
	return &DecisionRepository{selector: selector}
}

func (r *DecisionRepository) SaveDecision(ctx context.Context, value session.Decision) error {
	if r == nil {
		return fmt.Errorf("decision repository is not configured")
	}
	scope := identity.Scope{TenantID: value.TenantID, UserID: value.UserID}
	graph, err := routedGraph(ctx, r.selector, scope, "decision save")
	if err != nil {
		return err
	}
	if err := scopedSessionExists(graph, r.selector, scope, value.SessionID); err != nil {
		return fmt.Errorf("validate decision session: %w", err)
	}
	if value.ProjectID != "" {
		if err := scopedProjectExists(graph, r.selector, scope, value.ProjectID); err != nil {
			return fmt.Errorf("validate decision project: %w", err)
		}
	}
	result, err := graph.Query(`
		MERGE (d:Decision {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET d.created_at = $created_at
		SET d.session_id = $session_id,
		    d.project_id = $project_id,
		    d.subject = $subject,
		    d.choice = $choice,
		    d.rationale = $rationale,
		    d.status = $status,
		    d.evidence_ids = $evidence_ids,
		    d.updated_at = $updated_at
		RETURN d.id AS id
	`, decisionParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save decision in graph: %w", err)
	}
	if err := requireQueryRow(result, "save decision"); err != nil {
		return err
	}
	if _, err := graph.Query(`
		MATCH (s:Session {id: $session_id, tenant_id: $tenant_id, user_id: $user_id})
		MATCH (d:Decision {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (s)-[:RECORDED]->(d)
	`, map[string]interface{}{
		"session_id": value.SessionID, "id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID,
	}, queryOptionsFor(r.selector)); err != nil {
		return fmt.Errorf("link decision session: %w", err)
	}
	if value.ProjectID != "" {
		if _, err := graph.Query(`
			MATCH (p:Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
			MATCH (d:Decision {id: $id, tenant_id: $tenant_id, user_id: $user_id})
			MERGE (d)-[:ABOUT]->(p)
		`, map[string]interface{}{
			"project_id": value.ProjectID, "id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID,
		}, queryOptionsFor(r.selector)); err != nil {
			return fmt.Errorf("link decision project: %w", err)
		}
	}
	if len(value.EvidenceIDs) > 0 {
		result, err := graph.ROQuery(`
			UNWIND $evidence_ids AS evidence_id
			MATCH (e:Evidence {id: evidence_id, tenant_id: $tenant_id, user_id: $user_id})
			RETURN count(e) = size($evidence_ids) AS complete
		`, map[string]interface{}{
			"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID, "evidence_ids": value.EvidenceIDs,
		}, queryOptionsFor(r.selector))
		if err != nil {
			return fmt.Errorf("validate decision evidence: %w", err)
		}
		if !result.Next() || !boolValue(recordValue(result.Record(), "complete")) {
			return fmt.Errorf("decision evidence is missing or outside this user scope")
		}
		if _, err := graph.Query(`
			MATCH (d:Decision {id: $id, tenant_id: $tenant_id, user_id: $user_id})
			UNWIND $evidence_ids AS evidence_id
			MATCH (e:Evidence {id: evidence_id, tenant_id: $tenant_id, user_id: $user_id})
			MERGE (d)-[:BASED_ON]->(e)
		`, map[string]interface{}{
			"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID, "evidence_ids": value.EvidenceIDs,
		}, queryOptionsFor(r.selector)); err != nil {
			return fmt.Errorf("link decision evidence: %w", err)
		}
	}
	return nil
}

func (r *DecisionRepository) GetDecision(ctx context.Context, scope identity.Scope, id string) (session.Decision, error) {
	if r == nil {
		return session.Decision{}, fmt.Errorf("decision repository is not configured")
	}
	graph, err := routedGraph(ctx, r.selector, scope, "decision get")
	if err != nil {
		return session.Decision{}, err
	}
	result, err := graph.ROQuery(decisionQuery+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": id, "session_id": "",
	}, queryOptionsFor(r.selector))
	if err != nil {
		return session.Decision{}, fmt.Errorf("get decision from graph: %w", err)
	}
	if err := requireQueryRow(result, "get decision"); err != nil {
		return session.Decision{}, fmt.Errorf("%w: %s", session.ErrDecisionNotFound, err)
	}
	return decisionFromRecord(result.Record()), nil
}

func (r *DecisionRepository) ListDecisions(ctx context.Context, scope identity.Scope, sessionID string, limit int) ([]session.Decision, error) {
	if r == nil {
		return nil, fmt.Errorf("decision repository is not configured")
	}
	graph, err := routedGraph(ctx, r.selector, scope, "decision list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(decisionQuery+" ORDER BY coalesce(d.updated_at, 0) DESC, d.id LIMIT $limit", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": "", "session_id": sessionID, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list decisions from graph: %w", err)
	}
	values := make([]session.Decision, 0, limit)
	for result.Next() {
		values = append(values, decisionFromRecord(result.Record()))
	}
	return values, nil
}

func scopedSessionExists(graph *dbclient.Graph, selector UserGraphSelector, scope identity.Scope, sessionID string) error {
	result, err := graph.ROQuery(`
		MATCH (s:Session {id: $session_id, tenant_id: $tenant_id, user_id: $user_id})
		RETURN s.id AS id
		LIMIT 1
	`, map[string]interface{}{"session_id": sessionID, "tenant_id": scope.TenantID, "user_id": scope.UserID}, queryOptionsFor(selector))
	if err != nil {
		return err
	}
	if err := requireQueryRow(result, "find session"); err != nil {
		return fmt.Errorf("session %q is not in this user scope", sessionID)
	}
	return nil
}

func decisionParams(value session.Decision) map[string]interface{} {
	return map[string]interface{}{
		"id":           value.ID,
		"tenant_id":    value.TenantID,
		"user_id":      value.UserID,
		"session_id":   value.SessionID,
		"project_id":   value.ProjectID,
		"subject":      value.Subject,
		"choice":       value.Choice,
		"rationale":    value.Rationale,
		"status":       string(value.Status),
		"evidence_ids": value.EvidenceIDs,
		"created_at":   optionalMillis(value.CreatedAt),
		"updated_at":   optionalMillis(value.UpdatedAt),
	}
}

const decisionQuery = `
	MATCH (d:Decision {tenant_id: $tenant_id, user_id: $user_id})
	WHERE ($id = "" OR d.id = $id) AND ($session_id = "" OR d.session_id = $session_id)
	RETURN d.id AS id, d.tenant_id AS tenant_id, d.user_id AS user_id,
	       d.session_id AS session_id, d.project_id AS project_id,
	       d.subject AS subject, d.choice AS choice, d.rationale AS rationale,
	       d.status AS status, d.evidence_ids AS evidence_ids,
	       d.created_at AS created_at, d.updated_at AS updated_at
`

func decisionFromRecord(record recordReader) session.Decision {
	return session.Decision{
		ID:          stringValue(recordField(record, "id")),
		TenantID:    stringValue(recordField(record, "tenant_id")),
		UserID:      stringValue(recordField(record, "user_id")),
		SessionID:   stringValue(recordField(record, "session_id")),
		ProjectID:   stringValue(recordField(record, "project_id")),
		Subject:     stringValue(recordField(record, "subject")),
		Choice:      stringValue(recordField(record, "choice")),
		Rationale:   stringValue(recordField(record, "rationale")),
		Status:      session.DecisionStatus(stringValue(recordField(record, "status"))),
		EvidenceIDs: stringSliceValue(recordField(record, "evidence_ids")),
		CreatedAt:   timeValue(recordField(record, "created_at")),
		UpdatedAt:   timeValue(recordField(record, "updated_at")),
	}
}
