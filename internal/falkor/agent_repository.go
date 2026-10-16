package falkor

import (
	"context"
	"fmt"

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
	result, err := r.client.user.Query(`
		MERGE (t:Task {id: $id, user_id: $user_id})
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
		    t.retry_count = $retry_count,
		    t.error = $error,
		    t.updated_at = $updated_at
		MERGE (a:Agent {id: $agent_type})
		SET a.agent_type = $agent_type, a.updated_at = $updated_at
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
		MATCH (t:Task {id: $id, user_id: $user_id})
		RETURN t.id AS id, t.tenant_id AS tenant_id, t.user_id AS user_id,
		       t.project_id AS project_id, t.session_id AS session_id,
		       t.agent_type AS agent_type, t.status AS status,
		       t.priority AS priority, t.correlation_id AS correlation_id,
		       t.input_refs AS input_refs, t.output_refs AS output_refs,
		       t.dependency_ids AS dependency_ids, t.retry_count AS retry_count,
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

func (r *AgentRepository) PersistHandoff(ctx context.Context, target agents.Task, handoff agents.Handoff) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	params := taskParams(target)
	params["from_task_id"] = handoff.FromTaskID
	params["handoff_id"] = handoff.ID
	params["from_agent"] = string(handoff.FromAgent)
	params["to_agent"] = string(handoff.ToAgent)
	params["handoff_notes"] = handoff.Notes
	params["handoff_created_at"] = handoff.CreatedAt.UnixMilli()
	result, err := r.client.user.Query(`
		MATCH (source:Task {id: $from_task_id, user_id: $user_id})
		MERGE (target:Task {id: $id, user_id: $user_id})
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
		    target.retry_count = $retry_count,
		    target.error = $error,
		    target.updated_at = $updated_at
		MERGE (a:Agent {id: $agent_type})
		SET a.agent_type = $agent_type, a.updated_at = $updated_at
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
		"id":             task.ID,
		"tenant_id":      task.TenantID,
		"user_id":        task.UserID,
		"project_id":     task.ProjectID,
		"session_id":     task.SessionID,
		"agent_type":     string(task.Agent),
		"status":         string(task.Status),
		"priority":       task.Priority,
		"correlation_id": task.CorrelationID,
		"input_refs":     task.InputRefs,
		"output_refs":    task.OutputRefs,
		"dependency_ids": task.DependencyIDs,
		"retry_count":    task.RetryCount,
		"error":          task.Error,
		"created_at":     task.CreatedAt.UnixMilli(),
		"updated_at":     task.UpdatedAt.UnixMilli(),
	}
}

func taskFromRecord(record recordReader) agents.Task {
	return agents.Task{
		ID:            stringValue(recordField(record, "id")),
		TenantID:      stringValue(recordField(record, "tenant_id")),
		UserID:        stringValue(recordField(record, "user_id")),
		ProjectID:     stringValue(recordField(record, "project_id")),
		SessionID:     stringValue(recordField(record, "session_id")),
		Agent:         agents.AgentType(stringValue(recordField(record, "agent_type"))),
		Status:        agents.TaskStatus(stringValue(recordField(record, "status"))),
		Priority:      int(recordInt64(recordField(record, "priority"))),
		CorrelationID: stringValue(recordField(record, "correlation_id")),
		InputRefs:     stringSliceValue(recordField(record, "input_refs")),
		OutputRefs:    stringSliceValue(recordField(record, "output_refs")),
		DependencyIDs: stringSliceValue(recordField(record, "dependency_ids")),
		RetryCount:    int(recordInt64(recordField(record, "retry_count"))),
		Error:         stringValue(recordField(record, "error")),
		CreatedAt:     timeValue(recordField(record, "created_at")),
		UpdatedAt:     timeValue(recordField(record, "updated_at")),
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
