package falkor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/agents"
)

type AgentRepository struct {
	client *Client
}

var _ agents.Repository = (*AgentRepository)(nil)

func NewAgentRepository(client *Client) *AgentRepository {
	return &AgentRepository{client: client}
}

func (r *AgentRepository) SaveTask(ctx context.Context, task agents.Task) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(task.TenantID) == "" || strings.TrimSpace(task.UserID) == "" {
		return fmt.Errorf("task tenant and user IDs are required")
	}
	if task.ProjectID != "" {
		if err := r.validateProject(ctx, task.TenantID, task.UserID, task.ProjectID); err != nil {
			return fmt.Errorf("validate task project: %w", err)
		}
	}
	result, err := r.client.user.Query(`
		MERGE (t:Task {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET t.created_at = $created_at
		SET t.tenant_id = $tenant_id,
		    t.project_id = $project_id,
		    t.session_id = $session_id,
		    t.agent_type = $agent_type,
		    t.status = $status,
		    t.priority = $priority,
		    t.correlation_id = $correlation_id,
		    t.input_refs = $input_refs,
		    t.output_refs = $output_refs,
		    t.dependency_ids = $dependency_ids,
		    t.phase = $phase,
		    t.progress = $progress,
		    t.lease_owner = $lease_owner,
		    t.lease_expires_at = $lease_expires_at,
		    t.conversation_id = $conversation_id,
		    t.turn_id = $turn_id,
		    t.provider = $provider,
		    t.provider_session_id = $provider_session_id,
		    t.retry_count = $retry_count,
		    t.error = $error,
		    t.updated_at = $updated_at
		MERGE (a:Agent {id: $agent_type, tenant_id: $tenant_id, user_id: $user_id})
		SET a.agent_type = $agent_type, a.tenant_id = $tenant_id, a.user_id = $user_id, a.updated_at = $updated_at
		MERGE (t)-[:ASSIGNED_TO]->(a)
		RETURN t.id AS id
	`, taskParams(task), r.client.options())
	if err != nil {
		return fmt.Errorf("save task in graph: %w", err)
	}
	return requireQueryRow(result, "save task")
}

func (r *AgentRepository) GetTask(ctx context.Context, userID, taskID string) (agents.Task, error) {
	if err := r.ready(ctx); err != nil {
		return agents.Task{}, err
	}
	result, err := r.client.user.ROQuery(`
		MATCH (u:User {id: $user_id})
		MATCH (t:Task {id: $id, user_id: $user_id})
		WHERE t.tenant_id = u.tenant_id
		RETURN t.id AS id, t.tenant_id AS tenant_id, t.user_id AS user_id,
		       t.project_id AS project_id, t.session_id AS session_id,
		       t.agent_type AS agent_type, t.status AS status,
		       t.priority AS priority, t.correlation_id AS correlation_id,
		       t.input_refs AS input_refs, t.output_refs AS output_refs,
		       t.dependency_ids AS dependency_ids, t.retry_count AS retry_count,
		       t.phase AS phase, t.progress AS progress, t.lease_owner AS lease_owner,
		       t.lease_expires_at AS lease_expires_at, t.conversation_id AS conversation_id,
		       t.turn_id AS turn_id, t.provider AS provider, t.provider_session_id AS provider_session_id,
		       t.error AS error, t.created_at AS created_at, t.updated_at AS updated_at
		LIMIT 1
	`, map[string]interface{}{"id": taskID, "user_id": userID}, r.client.options())
	if err != nil {
		return agents.Task{}, fmt.Errorf("get task from graph: %w", err)
	}
	if err := requireQueryRow(result, "get task"); err != nil {
		return agents.Task{}, fmt.Errorf("%w: %s", agents.ErrTaskNotFound, err)
	}
	return taskFromRecord(result.Record()), nil
}

func (r *AgentRepository) ListTasks(ctx context.Context, tenantID, userID, projectID string, limit int) ([]agents.Task, error) {
	if err := r.ready(ctx); err != nil {
		return nil, err
	}
	result, err := r.client.user.ROQuery(`
		MATCH (t:Task {tenant_id: $tenant_id, user_id: $user_id})
		WHERE ($project_id = "" OR t.project_id = $project_id)
		RETURN t.id AS id, t.tenant_id AS tenant_id, t.user_id AS user_id,
		       t.project_id AS project_id, t.session_id AS session_id,
		       t.agent_type AS agent_type, t.status AS status,
		       t.priority AS priority, t.correlation_id AS correlation_id,
		       t.input_refs AS input_refs, t.output_refs AS output_refs,
		       t.dependency_ids AS dependency_ids, t.retry_count AS retry_count,
		       t.phase AS phase, t.progress AS progress, t.lease_owner AS lease_owner,
		       t.lease_expires_at AS lease_expires_at, t.conversation_id AS conversation_id,
		       t.turn_id AS turn_id, t.provider AS provider, t.provider_session_id AS provider_session_id,
		       t.error AS error, t.created_at AS created_at, t.updated_at AS updated_at
		ORDER BY coalesce(t.updated_at, 0) DESC, t.id LIMIT $limit
	`, map[string]interface{}{"tenant_id": tenantID, "user_id": userID, "project_id": projectID, "limit": limit}, r.client.options())
	if err != nil {
		return nil, fmt.Errorf("list tasks from graph: %w", err)
	}
	values := make([]agents.Task, 0, limit)
	for result.Next() {
		values = append(values, taskFromRecord(result.Record()))
	}
	return values, nil
}

func (r *AgentRepository) HeartbeatTask(ctx context.Context, task agents.Task, owner string, until time.Time) (agents.Task, error) {
	if err := r.ready(ctx); err != nil {
		return agents.Task{}, err
	}
	result, err := r.client.user.Query(`
		MATCH (t:Task {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		SET t.lease_owner = $lease_owner, t.lease_expires_at = $lease_expires_at, t.updated_at = $updated_at
		RETURN t.id AS id, t.tenant_id AS tenant_id, t.user_id AS user_id,
		       t.project_id AS project_id, t.session_id AS session_id,
		       t.agent_type AS agent_type, t.status AS status, t.priority AS priority,
		       t.correlation_id AS correlation_id, t.input_refs AS input_refs, t.output_refs AS output_refs,
		       t.dependency_ids AS dependency_ids, t.retry_count AS retry_count,
		       t.phase AS phase, t.progress AS progress, t.lease_owner AS lease_owner,
		       t.lease_expires_at AS lease_expires_at, t.conversation_id AS conversation_id,
		       t.turn_id AS turn_id, t.provider AS provider, t.provider_session_id AS provider_session_id,
		       t.error AS error, t.created_at AS created_at, t.updated_at AS updated_at
	`, map[string]interface{}{
		"id": task.ID, "tenant_id": task.TenantID, "user_id": task.UserID,
		"lease_owner": owner, "lease_expires_at": until.UTC().UnixMilli(), "updated_at": time.Now().UTC().UnixMilli(),
	}, r.client.options())
	if err != nil {
		return agents.Task{}, fmt.Errorf("heartbeat task in graph: %w", err)
	}
	if err := requireQueryRow(result, "heartbeat task"); err != nil {
		return agents.Task{}, fmt.Errorf("%w: %s", agents.ErrTaskNotFound, err)
	}
	return taskFromRecord(result.Record()), nil
}

func (r *AgentRepository) PersistHandoff(ctx context.Context, target agents.Task, handoff agents.Handoff) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(target.TenantID) == "" || strings.TrimSpace(target.UserID) == "" {
		return fmt.Errorf("handoff tenant and user IDs are required")
	}
	params := taskParams(target)
	params["from_task_id"] = handoff.FromTaskID
	params["handoff_id"] = handoff.ID
	params["from_agent"] = string(handoff.FromAgent)
	params["to_agent"] = string(handoff.ToAgent)
	params["handoff_notes"] = handoff.Notes
	params["handoff_created_at"] = optionalMillis(handoff.CreatedAt)
	result, err := r.client.user.Query(`
		MATCH (source:Task {id: $from_task_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (target:Task {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET target.created_at = $created_at
		SET target.tenant_id = $tenant_id,
		    target.project_id = $project_id,
		    target.session_id = $session_id,
		    target.agent_type = $agent_type,
		    target.status = $status,
		    target.priority = $priority,
		    target.correlation_id = $correlation_id,
		    target.input_refs = $input_refs,
		    target.output_refs = $output_refs,
		    target.dependency_ids = $dependency_ids,
		    target.phase = $phase,
		    target.progress = $progress,
		    target.lease_owner = $lease_owner,
		    target.lease_expires_at = $lease_expires_at,
		    target.conversation_id = $conversation_id,
		    target.turn_id = $turn_id,
		    target.provider = $provider,
		    target.provider_session_id = $provider_session_id,
		    target.retry_count = $retry_count,
		    target.error = $error,
		    target.updated_at = $updated_at
		MERGE (a:Agent {id: $agent_type, tenant_id: $tenant_id, user_id: $user_id})
		SET a.agent_type = $agent_type, a.tenant_id = $tenant_id, a.user_id = $user_id, a.updated_at = $updated_at
		MERGE (target)-[:ASSIGNED_TO]->(a)
		MERGE (source)-[h:HANDED_OFF_TO {handoff_id: $handoff_id}]->(target)
		SET h.tenant_id = $tenant_id, h.user_id = $user_id,
		    h.from_agent = $from_agent, h.to_agent = $to_agent,
		    h.notes = $handoff_notes, h.created_at = $handoff_created_at
		RETURN target.id AS id
	`, params, r.client.options())
	if err != nil {
		return fmt.Errorf("persist handoff in graph: %w", err)
	}
	return requireQueryRow(result, "persist handoff")
}

// RecoverStaleRunning marks tasks whose worker stopped heartbeating as failed.
// A failed task is intentionally left for the existing explicit retry path;
// this helper never silently re-runs agent work.
func (r *AgentRepository) RecoverStaleRunning(ctx context.Context, tenantID, userID string, before time.Time) (int, error) {
	if err := r.ready(ctx); err != nil {
		return 0, err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(userID) == "" || before.IsZero() {
		return 0, fmt.Errorf("tenant, user, and stale cutoff are required")
	}
	result, err := r.client.user.Query(`
		MATCH (t:Task {tenant_id: $tenant_id, user_id: $user_id, status: "running"})
		WHERE coalesce(t.updated_at, t.created_at, 0) < $cutoff
		SET t.status = "failed", t.error = "recovered stale running task",
		    t.retry_count = coalesce(t.retry_count, 0) + 1, t.updated_at = $recovered_at
		RETURN count(t) AS recovered
	`, map[string]interface{}{
		"tenant_id":    tenantID,
		"user_id":      userID,
		"cutoff":       before.UTC().UnixMilli(),
		"recovered_at": time.Now().UTC().UnixMilli(),
	}, r.client.options())
	if err != nil {
		return 0, fmt.Errorf("recover stale running tasks: %w", err)
	}
	if !result.Next() {
		return 0, nil
	}
	return int(recordInt64(recordValue(result.Record(), "recovered"))), nil
}

func (r *AgentRepository) validateProject(ctx context.Context, tenantID, userID, projectID string) error {
	result, err := r.client.user.ROQuery(`
		MATCH (p:Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
		RETURN p.id AS id
		LIMIT 1
	`, map[string]interface{}{"project_id": projectID, "tenant_id": tenantID, "user_id": userID}, r.client.options())
	if err != nil {
		return err
	}
	if err := requireQueryRow(result, "find task project"); err != nil {
		return fmt.Errorf("project %q is not in the task scope", projectID)
	}
	return nil
}

func (r *AgentRepository) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.client == nil || r.client.user == nil {
		return fmt.Errorf("agent graph repository is not configured")
	}
	return nil
}

func taskParams(task agents.Task) map[string]interface{} {
	return map[string]interface{}{
		"id":                  task.ID,
		"tenant_id":           task.TenantID,
		"user_id":             task.UserID,
		"project_id":          task.ProjectID,
		"session_id":          task.SessionID,
		"agent_type":          string(task.Agent),
		"status":              string(task.Status),
		"priority":            task.Priority,
		"correlation_id":      task.CorrelationID,
		"input_refs":          task.InputRefs,
		"output_refs":         task.OutputRefs,
		"dependency_ids":      task.DependencyIDs,
		"phase":               task.Phase,
		"progress":            task.Progress,
		"lease_owner":         task.LeaseOwner,
		"lease_expires_at":    optionalMillis(task.LeaseExpiresAt),
		"conversation_id":     task.ConversationID,
		"turn_id":             task.TurnID,
		"provider":            task.Provider,
		"provider_session_id": task.ProviderSessionID,
		"retry_count":         task.RetryCount,
		"error":               task.Error,
		"created_at":          optionalMillis(task.CreatedAt),
		"updated_at":          optionalMillis(task.UpdatedAt),
	}
}

func taskFromRecord(record recordReader) agents.Task {
	return agents.Task{
		ID:                stringValue(recordField(record, "id")),
		TenantID:          stringValue(recordField(record, "tenant_id")),
		UserID:            stringValue(recordField(record, "user_id")),
		ProjectID:         stringValue(recordField(record, "project_id")),
		SessionID:         stringValue(recordField(record, "session_id")),
		Agent:             agents.AgentType(stringValue(recordField(record, "agent_type"))),
		Status:            agents.TaskStatus(stringValue(recordField(record, "status"))),
		Priority:          int(recordInt64(recordField(record, "priority"))),
		CorrelationID:     stringValue(recordField(record, "correlation_id")),
		InputRefs:         stringSliceValue(recordField(record, "input_refs")),
		OutputRefs:        stringSliceValue(recordField(record, "output_refs")),
		DependencyIDs:     stringSliceValue(recordField(record, "dependency_ids")),
		Phase:             stringValue(recordField(record, "phase")),
		Progress:          int(recordInt64(recordField(record, "progress"))),
		LeaseOwner:        stringValue(recordField(record, "lease_owner")),
		LeaseExpiresAt:    timeValue(recordField(record, "lease_expires_at")),
		ConversationID:    stringValue(recordField(record, "conversation_id")),
		TurnID:            stringValue(recordField(record, "turn_id")),
		Provider:          stringValue(recordField(record, "provider")),
		ProviderSessionID: stringValue(recordField(record, "provider_session_id")),
		RetryCount:        int(recordInt64(recordField(record, "retry_count"))),
		Error:             stringValue(recordField(record, "error")),
		CreatedAt:         timeValue(recordField(record, "created_at")),
		UpdatedAt:         timeValue(recordField(record, "updated_at")),
	}
}

func recordInt64(value interface{}) int64 {
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
