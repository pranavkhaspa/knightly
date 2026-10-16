package falkor

import (
	"context"
	"fmt"

	"github.com/pranavkhaspa/knightly/internal/mcp"
)

type ToolCallRepository struct {
	client *Client
}

var _ mcp.ToolCallRepository = (*ToolCallRepository)(nil)

func NewToolCallRepository(client *Client) *ToolCallRepository {
	return &ToolCallRepository{client: client}
}

func (r *ToolCallRepository) Record(ctx context.Context, call mcp.ToolCall) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	result, err := r.client.user.Query(`
		MERGE (tc:ToolCall {id: $id})
		ON CREATE SET tc.started_at = $started_at
		SET tc.name = $name,
		    tc.request_id = $request_id,
		    tc.tenant_id = $tenant_id,
		    tc.user_id = $user_id,
		    tc.session_id = $session_id,
		    tc.task_id = $task_id,
		    tc.input_type = $input_type,
		    tc.status = $status,
		    tc.error = $error
		RETURN tc.id AS id
	`, toolCallParams(call), r.client.options())
	if err != nil {
		return fmt.Errorf("record tool call in graph: %w", err)
	}
	if err := requireQueryRow(result, "record tool call"); err != nil {
		return err
	}
	return r.linkToolCall(ctx, call)
}

func (r *ToolCallRepository) Complete(ctx context.Context, call mcp.ToolCall) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	result, err := r.client.user.Query(`
		MATCH (tc:ToolCall {id: $id})
		SET tc.status = $status, tc.error = $error, tc.completed_at = $completed_at
		RETURN tc.id AS id
	`, map[string]interface{}{
		"id":           call.ID,
		"status":       string(call.Status),
		"error":        call.Error,
		"completed_at": call.CompletedAt.UnixMilli(),
	}, r.client.options())
	if err != nil {
		return fmt.Errorf("complete tool call in graph: %w", err)
	}
	return requireQueryRow(result, "complete tool call")
}

func (r *ToolCallRepository) linkToolCall(ctx context.Context, call mcp.ToolCall) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if call.SessionID != "" {
		if _, err := r.client.user.Query(`
			MATCH (tc:ToolCall {id: $tool_call_id})
			MATCH (s:Session {id: $session_id})
			MERGE (s)-[:CONTAINS]->(tc)
		`, map[string]interface{}{"tool_call_id": call.ID, "session_id": call.SessionID}, r.client.options()); err != nil {
			return fmt.Errorf("link tool call session in graph: %w", err)
		}
	}
	if call.TaskID != "" {
		if _, err := r.client.user.Query(`
			MATCH (tc:ToolCall {id: $tool_call_id})
			MATCH (t:Task {id: $task_id, user_id: $user_id})
			MERGE (tc)-[:EXECUTED_FOR]->(t)
		`, map[string]interface{}{"tool_call_id": call.ID, "task_id": call.TaskID, "user_id": call.UserID}, r.client.options()); err != nil {
			return fmt.Errorf("link tool call task in graph: %w", err)
		}
	}
	return nil
}

func (r *ToolCallRepository) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.client == nil || r.client.user == nil {
		return fmt.Errorf("tool call graph repository is not configured")
	}
	return nil
}

func toolCallParams(call mcp.ToolCall) map[string]interface{} {
	return map[string]interface{}{
		"id":         call.ID,
		"name":       string(call.Name),
		"request_id": call.RequestID,
		"tenant_id":  call.TenantID,
		"user_id":    call.UserID,
		"session_id": call.SessionID,
		"task_id":    call.TaskID,
		"input_type": call.InputType,
		"status":     string(call.Status),
		"error":      call.Error,
		"started_at": call.StartedAt.UnixMilli(),
	}
}
