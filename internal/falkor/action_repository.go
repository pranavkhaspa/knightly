package falkor

import (
	"context"
	"fmt"
	"time"

	"github.com/pranavkhaspa/knightly/internal/actions"
)

type ActionRepository struct {
	client *Client
}

var _ actions.Repository = (*ActionRepository)(nil)

func NewActionRepository(client *Client) *ActionRepository {
	return &ActionRepository{client: client}
}

func (r *ActionRepository) Get(ctx context.Context, userID, actionID string) (actions.Action, error) {
	if err := r.ready(ctx); err != nil {
		return actions.Action{}, err
	}
	result, err := r.client.user.ROQuery(`
		MATCH (a:Action)
		WHERE a.id = $id AND a.user_id = $user_id
		`+actionReturn+`
		LIMIT 1
	`, map[string]interface{}{"id": actionID, "user_id": userID}, r.client.options())
	if err != nil {
		return actions.Action{}, fmt.Errorf("get action from graph: %w", err)
	}
	if err := requireQueryRow(result, "get action"); err != nil {
		return actions.Action{}, fmt.Errorf("%w: %s", actions.ErrActionNotFound, err)
	}
	return actionFromRecord(result.Record()), nil
}

// Create atomically claims the idempotency tuple. The graph MERGE key is not
// the caller's action ID so concurrent retries with different generated IDs
// still resolve to one durable action.
func (r *ActionRepository) Create(ctx context.Context, action actions.Action) (actions.Action, error) {
	if err := r.ready(ctx); err != nil {
		return actions.Action{}, err
	}
	result, err := r.client.user.Query(`
		MERGE (a:Action {tenant_id: $tenant_id, user_id: $user_id, idempotency_key: $idempotency_key})
		ON CREATE SET a.id = $id,
		              a.created_at = $created_at,
		              a.project_id = $project_id,
		              a.task_id = $task_id,
		              a.kind = $kind,
		              a.target = $target,
		              a.payload_json = $payload_json,
		              a.request_hash = $request_hash,
		              a.status = $status,
		              a.approved_by = $approved_by,
		              a.approval_id = $approval_id,
		              a.approval_note = $approval_note,
		              a.approved_at = $approved_at,
		              a.result_reference = $result_reference,
		              a.result = $result,
		              a.error = $error,
		              a.updated_at = $updated_at
		`+actionReturn,
		actionParams(action), r.client.options())
	if err != nil {
		return actions.Action{}, fmt.Errorf("create action in graph: %w", err)
	}
	if err := requireQueryRow(result, "create action"); err != nil {
		return actions.Action{}, err
	}
	return actionFromRecord(result.Record()), nil
}

func (r *ActionRepository) Approve(ctx context.Context, userID, actionID string, approval actions.Approval) (actions.Action, error) {
	if err := r.ready(ctx); err != nil {
		return actions.Action{}, err
	}
	result, err := r.client.user.Query(`
		MATCH (a:Action)
		WHERE a.id = $id AND a.user_id = $user_id AND a.status IN ["proposed", "failed"]
		SET a.status = "approved",
		    a.approved_by = $approved_by,
		    a.approval_id = $approval_id,
		    a.approval_note = $approval_note,
		    a.approved_at = $approved_at,
		    a.error = "",
		    a.updated_at = $updated_at
		`+actionReturn,
		map[string]interface{}{
			"id":            actionID,
			"user_id":       userID,
			"approved_by":   approval.ApprovedBy,
			"approval_id":   approval.ID,
			"approval_note": approval.Note,
			"approved_at":   approval.At.UnixMilli(),
			"updated_at":    time.Now().UTC().UnixMilli(),
		}, r.client.options())
	if err == nil && result != nil && result.Next() {
		return actionFromRecord(result.Record()), nil
	}
	if err != nil {
		return actions.Action{}, fmt.Errorf("approve action in graph: %w", err)
	}
	current, getErr := r.Get(ctx, userID, actionID)
	if getErr != nil {
		return actions.Action{}, getErr
	}
	return current, actions.ErrActionNotClaimable
}

func (r *ActionRepository) FindByIdempotencyKey(ctx context.Context, tenantID, userID, key string) (actions.Action, error) {
	if err := r.ready(ctx); err != nil {
		return actions.Action{}, err
	}
	result, err := r.client.user.ROQuery(`
		MATCH (a:Action)
		WHERE a.tenant_id = $tenant_id AND a.user_id = $user_id
		  AND a.idempotency_key = $idempotency_key
		`+actionReturn+`
		LIMIT 1
	`, map[string]interface{}{"tenant_id": tenantID, "user_id": userID, "idempotency_key": key}, r.client.options())
	if err != nil {
		return actions.Action{}, fmt.Errorf("find action idempotency key in graph: %w", err)
	}
	if err := requireQueryRow(result, "find action by idempotency key"); err != nil {
		return actions.Action{}, fmt.Errorf("%w: %s", actions.ErrActionNotFound, err)
	}
	return actionFromRecord(result.Record()), nil
}

func (r *ActionRepository) Save(ctx context.Context, action actions.Action) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	result, err := r.client.user.Query(`
		MERGE (a:Action {id: $id, user_id: $user_id})
		ON CREATE SET a.created_at = $created_at
		SET a.tenant_id = $tenant_id,
		    a.project_id = $project_id,
		    a.task_id = $task_id,
		    a.kind = $kind,
		    a.target = $target,
		    a.payload_json = $payload_json,
		    a.idempotency_key = $idempotency_key,
		    a.request_hash = $request_hash,
		    a.status = $status,
		    a.approved_by = $approved_by,
		    a.approval_id = $approval_id,
		    a.approval_note = $approval_note,
		    a.approved_at = $approved_at,
		    a.result_reference = $result_reference,
		    a.result = $result,
		    a.error = $error,
		    a.updated_at = $updated_at
		RETURN a.id AS id
	`, actionParams(action), r.client.options())
	if err != nil {
		return fmt.Errorf("save action in graph: %w", err)
	}
	if err := requireQueryRow(result, "save action"); err != nil {
		return err
	}
	if action.Status == actions.Approved || action.Status == actions.Executing || action.Status == actions.Completed {
		if _, err := r.client.user.Query(`
			MATCH (u:User {id: $user_id})
			MATCH (a:Action {id: $id, user_id: $user_id})
			MERGE (u)-[approval:APPROVED]->(a)
			SET approval.approved_at = $approved_at, approval.approval_id = $approval_id,
			    approval.note = $approval_note
		`, map[string]interface{}{
			"user_id":       action.UserID,
			"id":            action.ID,
			"approved_at":   action.ApprovedAt.UnixMilli(),
			"approval_id":   action.ApprovalID,
			"approval_note": action.ApprovalNote,
		}, r.client.options()); err != nil {
			return fmt.Errorf("link action approval in graph: %w", err)
		}
	}
	return nil
}

func (r *ActionRepository) ClaimForExecution(ctx context.Context, userID, actionID string) (actions.Action, error) {
	if err := r.ready(ctx); err != nil {
		return actions.Action{}, err
	}
	result, err := r.client.user.Query(`
		MATCH (a:Action)
		WHERE a.id = $id AND a.user_id = $user_id AND a.status = "approved"
		SET a.status = "executing", a.updated_at = $updated_at
		`+actionReturn, map[string]interface{}{"id": actionID, "user_id": userID, "updated_at": timeNowMillis()}, r.client.options())
	if err == nil && result != nil && result.Next() {
		return actionFromRecord(result.Record()), nil
	}
	if err != nil {
		return actions.Action{}, fmt.Errorf("claim action in graph: %w", err)
	}
	current, getErr := r.Get(ctx, userID, actionID)
	if getErr != nil {
		return actions.Action{}, getErr
	}
	if current.Status == actions.Completed {
		return current, nil
	}
	return current, actions.ErrActionNotClaimable
}

func (r *ActionRepository) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.client == nil || r.client.user == nil {
		return fmt.Errorf("action graph repository is not configured")
	}
	return nil
}

func actionParams(action actions.Action) map[string]interface{} {
	return map[string]interface{}{
		"id":               action.ID,
		"tenant_id":        action.TenantID,
		"user_id":          action.UserID,
		"project_id":       action.ProjectID,
		"task_id":          action.TaskID,
		"kind":             string(action.Kind),
		"target":           action.Target,
		"payload_json":     jsonMap(action.Payload),
		"idempotency_key":  action.IdempotencyKey,
		"request_hash":     action.RequestHash,
		"status":           string(action.Status),
		"approved_by":      action.ApprovedBy,
		"approval_id":      action.ApprovalID,
		"approval_note":    action.ApprovalNote,
		"approved_at":      action.ApprovedAt.UnixMilli(),
		"result_reference": action.ResultReference,
		"result":           action.Result,
		"error":            action.Error,
		"created_at":       action.CreatedAt.UnixMilli(),
		"updated_at":       action.UpdatedAt.UnixMilli(),
	}
}

func actionFromRecord(record recordReader) actions.Action {
	return actions.Action{
		ID:              stringValue(recordField(record, "id")),
		TenantID:        stringValue(recordField(record, "tenant_id")),
		UserID:          stringValue(recordField(record, "user_id")),
		ProjectID:       stringValue(recordField(record, "project_id")),
		TaskID:          stringValue(recordField(record, "task_id")),
		Kind:            actions.Kind(stringValue(recordField(record, "kind"))),
		Target:          stringValue(recordField(record, "target")),
		Payload:         stringMap(recordField(record, "payload_json")),
		IdempotencyKey:  stringValue(recordField(record, "idempotency_key")),
		RequestHash:     stringValue(recordField(record, "request_hash")),
		Status:          actions.Status(stringValue(recordField(record, "status"))),
		ApprovedBy:      stringValue(recordField(record, "approved_by")),
		ApprovalID:      stringValue(recordField(record, "approval_id")),
		ApprovalNote:    stringValue(recordField(record, "approval_note")),
		ApprovedAt:      timeValue(recordField(record, "approved_at")),
		ResultReference: stringValue(recordField(record, "result_reference")),
		Result:          stringValue(recordField(record, "result")),
		Error:           stringValue(recordField(record, "error")),
		CreatedAt:       timeValue(recordField(record, "created_at")),
		UpdatedAt:       timeValue(recordField(record, "updated_at")),
	}
}

func stringMap(value interface{}) map[string]string {
	result := parseStringMap(value)
	if result == nil {
		return nil
	}
	return result
}

func timeNowMillis() int64 {
	return time.Now().UTC().UnixMilli()
}

const actionReturn = `
	RETURN a.id AS id, a.tenant_id AS tenant_id, a.user_id AS user_id,
	       a.project_id AS project_id, a.task_id AS task_id, a.kind AS kind,
	       a.target AS target, a.payload_json AS payload_json,
	       a.idempotency_key AS idempotency_key, a.request_hash AS request_hash,
	       a.status AS status, a.approved_by AS approved_by,
	       a.approval_id AS approval_id, a.approval_note AS approval_note,
	       a.approved_at AS approved_at,
	       a.result_reference AS result_reference, a.result AS result,
	       a.error AS error, a.created_at AS created_at, a.updated_at AS updated_at
`
