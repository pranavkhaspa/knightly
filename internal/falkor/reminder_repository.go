package falkor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/reminder"
)

const ReminderSaveQuery = `
	MERGE (r:Reminder {id: $id, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET r.created_at = $created_at
	SET r.title = $title,
	    r.body = $body,
	    r.due_at = $due_at,
	    r.timezone = $timezone,
	    r.recurrence_json = $recurrence_json,
	    r.quiet_hours_json = $quiet_hours_json,
	    r.status = $status,
	    r.source = $source,
	    r.source_ref = $source_ref,
	    r.idempotency_key = $idempotency_key,
	    r.attempts = $attempts,
	    r.lease_until = $lease_until,
	    r.delivered_at = $delivered_at,
	    r.approved_by = $approved_by,
	    r.last_error = $last_error,
	    r.updated_at = $updated_at
	RETURN r.id AS id
`

const ReminderGetQuery = `
	MATCH (r:Reminder {id: $id, tenant_id: $tenant_id, user_id: $user_id})
	RETURN ` + reminderReturn + `
	LIMIT 1
`

const ReminderListQuery = `
	MATCH (r:Reminder {tenant_id: $tenant_id, user_id: $user_id})
	WHERE $status = "" OR r.status = $status
	RETURN ` + reminderReturn + `
	ORDER BY r.due_at ASC
	LIMIT $limit
`

const ReminderClaimDueQuery = `
	MATCH (r:Reminder {tenant_id: $tenant_id, user_id: $user_id, status: "scheduled"})
	WHERE coalesce(r.due_at, 0) <= $now AND coalesce(r.lease_until, 0) < $now
	WITH r ORDER BY r.due_at ASC LIMIT $limit
	SET r.lease_until = $lease_until, r.updated_at = $now
	RETURN ` + reminderReturn

const InboxItemSaveQuery = `
	MERGE (i:InboxItem {id: $id, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET i.created_at = $created_at,
	              i.kind = $kind,
	              i.title = $title,
	              i.body = $body,
	              i.reminder_id = $reminder_id,
	              i.due_at = $due_at,
	              i.idempotency_key = $idempotency_key
	WITH i
	OPTIONAL MATCH (r:Reminder {id: $reminder_id, tenant_id: $tenant_id, user_id: $user_id})
	FOREACH (_ IN CASE WHEN r IS NULL THEN [] ELSE [1] END | MERGE (r)-[:DELIVERED_AS]->(i))
	RETURN i.id AS id
`

const InboxItemListQuery = `
	MATCH (i:InboxItem {tenant_id: $tenant_id, user_id: $user_id})
	RETURN i.id AS id, i.tenant_id AS tenant_id, i.user_id AS user_id, i.kind AS kind,
	       i.title AS title, i.body AS body, i.reminder_id AS reminder_id,
	       i.due_at AS due_at, i.idempotency_key AS idempotency_key, i.created_at AS created_at
	ORDER BY i.created_at DESC
	LIMIT $limit
`

const reminderReturn = `
	r.id AS id, r.tenant_id AS tenant_id, r.user_id AS user_id,
	r.title AS title, r.body AS body, r.due_at AS due_at, r.timezone AS timezone,
	r.recurrence_json AS recurrence_json, r.quiet_hours_json AS quiet_hours_json,
	r.status AS status, r.source AS source, r.source_ref AS source_ref,
	r.idempotency_key AS idempotency_key, r.attempts AS attempts,
	r.lease_until AS lease_until, r.delivered_at AS delivered_at,
	r.approved_by AS approved_by, r.last_error AS last_error,
	r.created_at AS created_at, r.updated_at AS updated_at
`

// ReminderRepository persists reminders and inbox items in the routed user
// graph. Delivery deduplication relies on the atomic lease in ClaimDue and the
// idempotent MERGE on the occurrence-keyed inbox item.
type ReminderRepository struct {
	selector UserGraphSelector
	now      func() time.Time
}

var (
	_ reminder.Store      = (*ReminderRepository)(nil)
	_ reminder.InboxStore = (*ReminderRepository)(nil)
)

func NewReminderRepository(selector UserGraphSelector) *ReminderRepository {
	return &ReminderRepository{selector: selector, now: func() time.Time { return time.Now().UTC() }}
}

func (r *ReminderRepository) Save(ctx context.Context, scope identity.Scope, value reminder.Reminder) (reminder.Reminder, error) {
	if err := scope.Validate(); err != nil {
		return reminder.Reminder{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "reminder save")
	if err != nil {
		return reminder.Reminder{}, err
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	if _, err := graph.Query(ReminderSaveQuery, reminderParams(value), queryOptionsFor(r.selector)); err != nil {
		return reminder.Reminder{}, fmt.Errorf("save reminder in graph: %w", err)
	}
	return value, nil
}

func (r *ReminderRepository) Get(ctx context.Context, scope identity.Scope, id string) (reminder.Reminder, error) {
	if err := scope.Validate(); err != nil {
		return reminder.Reminder{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "reminder get")
	if err != nil {
		return reminder.Reminder{}, err
	}
	result, err := graph.ROQuery(ReminderGetQuery, map[string]interface{}{
		"id": id, "tenant_id": scope.TenantID, "user_id": scope.UserID,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return reminder.Reminder{}, fmt.Errorf("get reminder from graph: %w", err)
	}
	if result == nil || !result.Next() {
		return reminder.Reminder{}, fmt.Errorf("%w: %s", reminder.ErrReminderNotFound, id)
	}
	return reminderFromRecord(result.Record()), nil
}

func (r *ReminderRepository) List(ctx context.Context, scope identity.Scope, query reminder.ListQuery) ([]reminder.Reminder, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "reminder list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(ReminderListQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "status": string(query.Status), "limit": query.Limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list reminders from graph: %w", err)
	}
	values := make([]reminder.Reminder, 0)
	for result != nil && result.Next() {
		values = append(values, reminderFromRecord(result.Record()))
	}
	return values, nil
}

// ClaimDue atomically leases every due reminder so a second instance or a
// post-restart pass cannot deliver the same occurrence concurrently.
func (r *ReminderRepository) ClaimDue(ctx context.Context, scope identity.Scope, now, leaseUntil time.Time, limit int) ([]reminder.Reminder, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "reminder claim due")
	if err != nil {
		return nil, err
	}
	result, err := graph.Query(ReminderClaimDueQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID,
		"now": optionalMillis(now), "lease_until": optionalMillis(leaseUntil), "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("claim due reminders in graph: %w", err)
	}
	values := make([]reminder.Reminder, 0)
	for result != nil && result.Next() {
		values = append(values, reminderFromRecord(result.Record()))
	}
	return values, nil
}

func (r *ReminderRepository) SaveInboxItem(ctx context.Context, scope identity.Scope, item reminder.InboxItem) (reminder.InboxItem, error) {
	if err := scope.Validate(); err != nil {
		return reminder.InboxItem{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "reminder inbox save")
	if err != nil {
		return reminder.InboxItem{}, err
	}
	item.TenantID = scope.TenantID
	item.UserID = scope.UserID
	if item.CreatedAt.IsZero() {
		item.CreatedAt = r.now().UTC()
	}
	if _, err := graph.Query(InboxItemSaveQuery, map[string]interface{}{
		"id": item.ID, "tenant_id": scope.TenantID, "user_id": scope.UserID,
		"kind": item.Kind, "title": item.Title, "body": item.Body, "reminder_id": item.ReminderID,
		"due_at": optionalMillis(item.DueAt), "idempotency_key": item.IdempotencyKey,
		"created_at": optionalMillis(item.CreatedAt),
	}, queryOptionsFor(r.selector)); err != nil {
		return reminder.InboxItem{}, fmt.Errorf("save inbox item in graph: %w", err)
	}
	return item, nil
}

func (r *ReminderRepository) ListInboxItems(ctx context.Context, scope identity.Scope, limit int) ([]reminder.InboxItem, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "reminder inbox list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(InboxItemListQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list inbox items from graph: %w", err)
	}
	items := make([]reminder.InboxItem, 0)
	for result != nil && result.Next() {
		items = append(items, inboxItemFromRecord(result.Record()))
	}
	return items, nil
}

func reminderParams(value reminder.Reminder) map[string]interface{} {
	return map[string]interface{}{
		"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID,
		"title": value.Title, "body": value.Body, "due_at": optionalMillis(value.DueAt),
		"timezone": value.Timezone, "recurrence_json": encodeRecurrence(value.Recurrence),
		"quiet_hours_json": encodeQuietHours(value.QuietHours), "status": string(value.Status),
		"source": value.Source, "source_ref": value.SourceRef, "idempotency_key": value.IdempotencyKey,
		"attempts": value.Attempts, "lease_until": optionalMillis(value.LeaseUntil),
		"delivered_at": optionalMillis(value.DeliveredAt), "approved_by": value.ApprovedBy,
		"last_error": value.LastError, "created_at": optionalMillis(value.CreatedAt),
		"updated_at": optionalMillis(value.UpdatedAt),
	}
}

func reminderFromRecord(record recordReader) reminder.Reminder {
	return reminder.Reminder{
		ID:             stringValue(recordField(record, "id")),
		TenantID:       stringValue(recordField(record, "tenant_id")),
		UserID:         stringValue(recordField(record, "user_id")),
		Title:          stringValue(recordField(record, "title")),
		Body:           stringValue(recordField(record, "body")),
		DueAt:          timeValue(recordField(record, "due_at")),
		Timezone:       stringValue(recordField(record, "timezone")),
		Recurrence:     decodeRecurrence(stringValue(recordField(record, "recurrence_json"))),
		QuietHours:     decodeQuietHours(stringValue(recordField(record, "quiet_hours_json"))),
		Status:         reminder.Status(stringValue(recordField(record, "status"))),
		Source:         stringValue(recordField(record, "source")),
		SourceRef:      stringValue(recordField(record, "source_ref")),
		IdempotencyKey: stringValue(recordField(record, "idempotency_key")),
		Attempts:       int(recordInt64(recordField(record, "attempts"))),
		LeaseUntil:     timeValue(recordField(record, "lease_until")),
		DeliveredAt:    timeValue(recordField(record, "delivered_at")),
		ApprovedBy:     stringValue(recordField(record, "approved_by")),
		LastError:      stringValue(recordField(record, "last_error")),
		CreatedAt:      timeValue(recordField(record, "created_at")),
		UpdatedAt:      timeValue(recordField(record, "updated_at")),
	}
}

func inboxItemFromRecord(record recordReader) reminder.InboxItem {
	return reminder.InboxItem{
		ID:             stringValue(recordField(record, "id")),
		TenantID:       stringValue(recordField(record, "tenant_id")),
		UserID:         stringValue(recordField(record, "user_id")),
		Kind:           stringValue(recordField(record, "kind")),
		Title:          stringValue(recordField(record, "title")),
		Body:           stringValue(recordField(record, "body")),
		ReminderID:     stringValue(recordField(record, "reminder_id")),
		DueAt:          timeValue(recordField(record, "due_at")),
		IdempotencyKey: stringValue(recordField(record, "idempotency_key")),
		CreatedAt:      timeValue(recordField(record, "created_at")),
	}
}

func encodeRecurrence(value reminder.Recurrence) string {
	if value.Unit == "" {
		return ""
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func decodeRecurrence(encoded string) reminder.Recurrence {
	if encoded == "" {
		return reminder.Recurrence{}
	}
	var value reminder.Recurrence
	if err := json.Unmarshal([]byte(encoded), &value); err != nil {
		return reminder.Recurrence{}
	}
	return value
}

func encodeQuietHours(value reminder.QuietHours) string {
	if value.Start == "" && value.End == "" {
		return ""
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func decodeQuietHours(encoded string) reminder.QuietHours {
	if encoded == "" {
		return reminder.QuietHours{}
	}
	var value reminder.QuietHours
	if err := json.Unmarshal([]byte(encoded), &value); err != nil {
		return reminder.QuietHours{}
	}
	return value
}
