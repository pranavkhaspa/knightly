package falkor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/chat"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

// ChatRepository persists conversations, messages, and replayable events in a
// routed user graph. Sequence allocation happens inside FalkorDB so separate
// HTTP workers cannot assign the same stream sequence.
type ChatRepository struct {
	selector UserGraphSelector
}

var _ chat.Repository = (*ChatRepository)(nil)
var _ chat.TurnRepository = (*ChatRepository)(nil)

func NewChatRepository(selector UserGraphSelector) *ChatRepository {
	return &ChatRepository{selector: selector}
}

func (r *ChatRepository) SaveConversation(ctx context.Context, value chat.Conversation) error {
	graph, err := routedGraph(ctx, r.selector, identity.Scope{TenantID: value.TenantID, UserID: value.UserID}, "conversation save")
	if err != nil {
		return err
	}
	result, err := graph.Query(`
		MERGE (c:Conversation {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET c.created_at = $created_at,
		              c.event_sequence = 0,
		              c.message_sequence = 0
		SET c.project_id = $project_id,
		    c.title = $title,
		    c.status = $status,
		    c.updated_at = $updated_at
		RETURN c.id AS id
	`, conversationParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save conversation in graph: %w", err)
	}
	return requireQueryRow(result, "save conversation")
}

func (r *ChatRepository) GetConversation(ctx context.Context, scope identity.Scope, id string) (chat.Conversation, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "conversation get")
	if err != nil {
		return chat.Conversation{}, err
	}
	result, err := graph.ROQuery(conversationQuery+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": id,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return chat.Conversation{}, fmt.Errorf("get conversation from graph: %w", err)
	}
	if err := requireQueryRow(result, "get conversation"); err != nil {
		return chat.Conversation{}, fmt.Errorf("%w: %s", chat.ErrConversationNotFound, err)
	}
	return conversationFromRecord(result.Record()), nil
}

func (r *ChatRepository) ListConversations(ctx context.Context, scope identity.Scope, limit int) ([]chat.Conversation, error) {
	return r.ListConversationsAfter(ctx, scope, "", limit)
}

func (r *ChatRepository) ListConversationsAfter(ctx context.Context, scope identity.Scope, after string, limit int) ([]chat.Conversation, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "conversation list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(conversationListQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": "", "after": after, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list conversations from graph: %w", err)
	}
	values := make([]chat.Conversation, 0, limit)
	for result.Next() {
		values = append(values, conversationFromRecord(result.Record()))
	}
	return values, nil
}

func (r *ChatRepository) SaveMessage(ctx context.Context, value chat.Message) error {
	graph, err := routedGraph(ctx, r.selector, identity.Scope{TenantID: value.TenantID, UserID: value.UserID}, "message save")
	if err != nil {
		return err
	}
	result, err := graph.Query(`
		MATCH (c:Conversation {id: $conversation_id, tenant_id: $tenant_id, user_id: $user_id})
		OPTIONAL MATCH (existing:Message {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		WITH c, existing
		WHERE existing IS NULL OR existing.conversation_id = $conversation_id
		MERGE (m:Message {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET m.sequence = coalesce(c.message_sequence, 0) + 1,
		              c.message_sequence = coalesce(c.message_sequence, 0) + 1,
		              m.created_at = $created_at
		SET m.tenant_id = $tenant_id,
		    m.user_id = $user_id,
		    m.conversation_id = $conversation_id,
		    m.turn_id = $turn_id,
		    m.role = $role,
		    m.content = $content,
		    m.citations_json = $citations_json,
		    m.tool_call_ids_json = $tool_call_ids_json,
		    m.task_ids_json = $task_ids_json
		MERGE (c)-[:CONTAINS]->(m)
		RETURN m.id AS id
	`, messageParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save message in graph: %w", err)
	}
	if err := requireQueryRow(result, "save message"); err != nil {
		return fmt.Errorf("%w: conversation is missing", chat.ErrMessageNotFound)
	}
	return nil
}

func (r *ChatRepository) ListMessages(ctx context.Context, scope identity.Scope, conversationID string, limit int) ([]chat.Message, error) {
	return r.listMessages(ctx, scope, conversationID, 0, limit)
}

func (r *ChatRepository) ListMessagesAfter(ctx context.Context, scope identity.Scope, conversationID string, after int64, limit int) ([]chat.Message, error) {
	return r.listMessages(ctx, scope, conversationID, after, limit)
}

func (r *ChatRepository) listMessages(ctx context.Context, scope identity.Scope, conversationID string, after int64, limit int) ([]chat.Message, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "message list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(`
		MATCH (c:Conversation {id: $conversation_id, tenant_id: $tenant_id, user_id: $user_id})-[:CONTAINS]->(m:Message)
		WHERE coalesce(m.sequence, 0) > $after
		RETURN m.id AS id, m.tenant_id AS tenant_id, m.user_id AS user_id,
		       m.conversation_id AS conversation_id, m.turn_id AS turn_id,
		       m.sequence AS sequence, m.role AS role, m.content AS content,
		       m.citations_json AS citations_json, m.tool_call_ids_json AS tool_call_ids_json,
		       m.task_ids_json AS task_ids_json, m.created_at AS created_at
		ORDER BY coalesce(m.sequence, 0), m.id
		LIMIT $limit
	`, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID,
		"conversation_id": conversationID, "after": after, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list messages from graph: %w", err)
	}
	values := make([]chat.Message, 0, limit)
	for result.Next() {
		values = append(values, messageFromRecord(result.Record()))
	}
	return values, nil
}

func (r *ChatRepository) SaveTurn(ctx context.Context, value chat.Turn) error {
	graph, err := routedGraph(ctx, r.selector, identity.Scope{TenantID: value.TenantID, UserID: value.UserID}, "turn save")
	if err != nil {
		return err
	}
	result, err := graph.Query(`
		MATCH (c:Conversation {id: $conversation_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (t:Turn {tenant_id: $tenant_id, user_id: $user_id, conversation_id: $conversation_id, idempotency_key: $idempotency_key})
		ON CREATE SET t.id = $id, t.created_at = $created_at
		SET t.conversation_id = $conversation_id,
		    t.idempotency_key = $idempotency_key,
		    t.status = $status,
		    t.user_message_id = $user_message_id,
		    t.assistant_message_id = $assistant_message_id,
		    t.terminal_result = $terminal_result,
		    t.error = $error,
		    t.cancel_requested = $cancel_requested,
		    t.updated_at = $updated_at,
		    t.completed_at = $completed_at
		MERGE (c)-[:HAS_TURN]->(t)
		RETURN t.id AS id
	`, turnParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save turn in graph: %w", err)
	}
	if err := requireQueryRow(result, "save turn"); err != nil {
		return fmt.Errorf("%w: conversation is missing", chat.ErrTurnNotFound)
	}
	return nil
}

func (r *ChatRepository) CreateOrGetTurn(ctx context.Context, value chat.Turn) (chat.Turn, bool, error) {
	graph, err := routedGraph(ctx, r.selector, identity.Scope{TenantID: value.TenantID, UserID: value.UserID}, "turn create")
	if err != nil {
		return chat.Turn{}, false, err
	}
	result, err := graph.Query(`
		MATCH (c:Conversation {id: $conversation_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (t:Turn {tenant_id: $tenant_id, user_id: $user_id, conversation_id: $conversation_id, idempotency_key: $idempotency_key})
		ON CREATE SET t.id = $id, t.created_by_id = $id, t.created_at = $created_at,
		              t.status = $status, t.user_message_id = $user_message_id,
		              t.cancel_requested = $cancel_requested, t.updated_at = $updated_at
		MERGE (c)-[:HAS_TURN]->(t)
		`+turnReturn+`, t.created_by_id AS created_by_id`, turnParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return chat.Turn{}, false, fmt.Errorf("create turn in graph: %w", err)
	}
	if err := requireQueryRow(result, "create turn"); err != nil {
		return chat.Turn{}, false, fmt.Errorf("%w: conversation is missing", chat.ErrTurnNotFound)
	}
	stored := turnFromRecord(result.Record())
	return stored, stringValue(recordField(result.Record(), "created_by_id")) == value.ID, nil
}

func (r *ChatRepository) TransitionTurn(ctx context.Context, scope identity.Scope, id, action, resultID, failure string) (chat.Turn, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "turn transition")
	if err != nil {
		return chat.Turn{}, err
	}
	transitionAt := time.Now().UTC().UnixMilli()
	result, err := graph.Query(`
		MATCH (t:Turn {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		SET t.status = CASE
			WHEN $action = "start" AND t.status = "pending" THEN "running"
			WHEN $action = "complete" AND t.status IN ["pending", "running"] THEN CASE WHEN coalesce(t.cancel_requested, false) THEN "cancelled" ELSE "completed" END
			WHEN $action = "fail" AND t.status IN ["pending", "running"] THEN CASE WHEN coalesce(t.cancel_requested, false) THEN "cancelled" ELSE "failed" END
			WHEN $action = "cancel" AND t.status = "pending" THEN "cancelled"
			ELSE t.status END,
		    t.cancel_requested = CASE WHEN $action = "cancel" AND t.status IN ["pending", "running"] THEN true ELSE coalesce(t.cancel_requested, false) END,
		    t.assistant_message_id = CASE WHEN $action = "complete" AND t.status IN ["pending", "running"] AND NOT coalesce(t.cancel_requested, false) THEN $result_id ELSE t.assistant_message_id END,
		    t.error = CASE WHEN $action = "fail" AND t.status IN ["pending", "running"] AND NOT coalesce(t.cancel_requested, false) THEN $failure ELSE t.error END,
		    t.terminal_result = CASE
			WHEN $action = "cancel" AND t.status = "pending" THEN "cancelled"
			WHEN $action = "complete" AND t.status IN ["pending", "running"] THEN CASE WHEN coalesce(t.cancel_requested, false) THEN "cancelled" ELSE $result_id END
			WHEN $action = "fail" AND t.status IN ["pending", "running"] THEN CASE WHEN coalesce(t.cancel_requested, false) THEN "cancelled" ELSE "failed" END
			ELSE t.terminal_result END,
		    t.updated_at = $transition_at,
		    t.completed_at = CASE WHEN $action IN ["complete", "fail"] OR ($action = "cancel" AND t.status = "pending") THEN coalesce(t.completed_at, $transition_at) ELSE t.completed_at END
		`+turnReturn, map[string]interface{}{
		"id": id, "tenant_id": scope.TenantID, "user_id": scope.UserID, "action": action,
		"result_id": resultID, "failure": failure, "transition_at": transitionAt,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return chat.Turn{}, fmt.Errorf("transition turn in graph: %w", err)
	}
	if err := requireQueryRow(result, "transition turn"); err != nil {
		return chat.Turn{}, fmt.Errorf("%w: %s", chat.ErrTurnNotFound, err)
	}
	return turnFromRecord(result.Record()), nil
}

func (r *ChatRepository) GetTurn(ctx context.Context, scope identity.Scope, id string) (chat.Turn, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "turn get")
	if err != nil {
		return chat.Turn{}, err
	}
	result, err := graph.ROQuery(turnQuery+" AND t.id = $id"+turnReturn+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": id,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return chat.Turn{}, fmt.Errorf("get turn from graph: %w", err)
	}
	if err := requireQueryRow(result, "get turn"); err != nil {
		return chat.Turn{}, fmt.Errorf("%w: %s", chat.ErrTurnNotFound, err)
	}
	return turnFromRecord(result.Record()), nil
}

func (r *ChatRepository) GetTurnByIdempotency(ctx context.Context, scope identity.Scope, conversationID, key string) (chat.Turn, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "turn idempotency lookup")
	if err != nil {
		return chat.Turn{}, err
	}
	result, err := graph.ROQuery(turnQuery+" AND t.conversation_id = $conversation_id AND t.idempotency_key = $idempotency_key"+turnReturn+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "conversation_id": conversationID, "idempotency_key": key,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return chat.Turn{}, fmt.Errorf("find turn in graph: %w", err)
	}
	if err := requireQueryRow(result, "find turn"); err != nil {
		return chat.Turn{}, fmt.Errorf("%w: %s", chat.ErrTurnNotFound, err)
	}
	return turnFromRecord(result.Record()), nil
}

func (r *ChatRepository) ListTurns(ctx context.Context, scope identity.Scope, conversationID string, limit int) ([]chat.Turn, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "turn list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(turnQuery+" AND t.conversation_id = $conversation_id"+turnReturn+" ORDER BY coalesce(t.created_at, 0), t.id LIMIT $limit", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "conversation_id": conversationID, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list turns from graph: %w", err)
	}
	values := make([]chat.Turn, 0, limit)
	for result.Next() {
		values = append(values, turnFromRecord(result.Record()))
	}
	return values, nil
}

func (r *ChatRepository) AppendEvent(ctx context.Context, value chat.Event) (chat.Event, error) {
	graph, err := routedGraph(ctx, r.selector, identity.Scope{TenantID: value.TenantID, UserID: value.UserID}, "event append")
	if err != nil {
		return chat.Event{}, err
	}
	result, err := graph.Query(`
		MATCH (c:Conversation {id: $conversation_id, tenant_id: $tenant_id, user_id: $user_id})
		OPTIONAL MATCH (existing:Event {id: $event_id, tenant_id: $tenant_id, user_id: $user_id})
		WITH c, existing
		WHERE existing IS NULL OR existing.conversation_id = $conversation_id
		MERGE (e:Event {id: $event_id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET e.sequence = coalesce(c.event_sequence, 0) + 1,
		              c.event_sequence = coalesce(c.event_sequence, 0) + 1,
		              e.created_at = $created_at,
		              e.conversation_id = $conversation_id,
		              e.turn_id = $turn_id,
		              e.task_id = $task_id,
		              e.run_id = $run_id,
		              e.event_type = $event_type,
		              e.data_json = $data_json
		MERGE (c)-[:EMITS]->(e)
		RETURN e.id AS event_id, e.sequence AS sequence, e.created_at AS created_at,
		       e.conversation_id AS conversation_id, e.turn_id AS turn_id,
		       e.task_id AS task_id, e.run_id AS run_id, e.event_type AS event_type,
		       e.data_json AS data_json
	`, eventParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return chat.Event{}, fmt.Errorf("append event in graph: %w", err)
	}
	if err := requireQueryRow(result, "append event"); err != nil {
		return chat.Event{}, fmt.Errorf("%w: conversation is missing", chat.ErrInvalidEvent)
	}
	return eventFromRecord(result.Record(), value.TenantID, value.UserID), nil
}

func (r *ChatRepository) ListEvents(ctx context.Context, scope identity.Scope, conversationID string, after int64, limit int) ([]chat.Event, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "event list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(`
		MATCH (c:Conversation {id: $conversation_id, tenant_id: $tenant_id, user_id: $user_id})-[:EMITS]->(e:Event)
		WHERE coalesce(e.sequence, 0) > $after
		RETURN e.id AS event_id, e.sequence AS sequence, e.created_at AS created_at,
		       e.conversation_id AS conversation_id, e.turn_id AS turn_id,
		       e.task_id AS task_id, e.run_id AS run_id, e.event_type AS event_type,
		       e.data_json AS data_json
		ORDER BY e.sequence, e.id
		LIMIT $limit
	`, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID,
		"conversation_id": conversationID, "after": after, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list events from graph: %w", err)
	}
	values := make([]chat.Event, 0, limit)
	for result.Next() {
		values = append(values, eventFromRecord(result.Record(), scope.TenantID, scope.UserID))
	}
	return values, nil
}

const conversationQuery = `
	MATCH (c:Conversation {tenant_id: $tenant_id, user_id: $user_id})
	WHERE $id = "" OR c.id = $id
	RETURN c.id AS id, c.tenant_id AS tenant_id, c.user_id AS user_id,
	       c.project_id AS project_id, c.title AS title, c.status AS status,
	       c.event_sequence AS event_sequence, c.message_sequence AS message_sequence,
	       c.created_at AS created_at, c.updated_at AS updated_at
`

const conversationListQuery = `
	MATCH (c:Conversation {tenant_id: $tenant_id, user_id: $user_id})
	WHERE $after = "" OR c.id < $after
	RETURN c.id AS id, c.tenant_id AS tenant_id, c.user_id AS user_id,
	       c.project_id AS project_id, c.title AS title, c.status AS status,
	       c.event_sequence AS event_sequence, c.message_sequence AS message_sequence,
	       c.created_at AS created_at, c.updated_at AS updated_at
	ORDER BY coalesce(c.updated_at, 0) DESC, c.id
	LIMIT $limit
`

const turnQuery = `
	MATCH (t:Turn {tenant_id: $tenant_id, user_id: $user_id})
	WHERE true
	`

const turnReturn = `
	RETURN t.id AS id, t.tenant_id AS tenant_id, t.user_id AS user_id,
	       t.conversation_id AS conversation_id, t.idempotency_key AS idempotency_key,
	       t.status AS status, t.user_message_id AS user_message_id,
	       t.assistant_message_id AS assistant_message_id, t.terminal_result AS terminal_result,
	       t.error AS error, t.cancel_requested AS cancel_requested,
	       t.created_at AS created_at, t.updated_at AS updated_at, t.completed_at AS completed_at
`

func conversationParams(value chat.Conversation) map[string]interface{} {
	return map[string]interface{}{
		"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID,
		"project_id": value.ProjectID, "title": value.Title, "status": string(value.Status),
		"created_at": optionalMillis(value.CreatedAt), "updated_at": optionalMillis(value.UpdatedAt),
	}
}

func messageParams(value chat.Message) map[string]interface{} {
	return map[string]interface{}{
		"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID,
		"conversation_id": value.ConversationID, "turn_id": value.TurnID,
		"role": string(value.Role), "content": value.Content,
		"citations_json": citationsJSON(value.Citations), "tool_call_ids_json": stringSliceJSON(value.ToolCallIDs),
		"task_ids_json": stringSliceJSON(value.TaskIDs), "created_at": optionalMillis(value.CreatedAt),
	}
}

func eventParams(value chat.Event) map[string]interface{} {
	return map[string]interface{}{
		"event_id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID,
		"conversation_id": value.ConversationID, "turn_id": value.TurnID,
		"task_id": value.TaskID, "run_id": value.RunID, "event_type": string(value.Type),
		"data_json": eventDataJSON(value.Data), "created_at": optionalMillis(value.CreatedAt),
	}
}

func turnParams(value chat.Turn) map[string]interface{} {
	return map[string]interface{}{
		"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID,
		"conversation_id": value.ConversationID, "idempotency_key": value.IdempotencyKey,
		"status": string(value.Status), "user_message_id": value.UserMessageID,
		"assistant_message_id": value.AssistantMessageID, "terminal_result": value.TerminalResult,
		"error": value.Error, "cancel_requested": value.CancelRequested,
		"created_at": optionalMillis(value.CreatedAt), "updated_at": optionalMillis(value.UpdatedAt),
		"completed_at": optionalMillis(value.CompletedAt),
	}
}

func conversationFromRecord(record recordReader) chat.Conversation {
	return chat.Conversation{
		ID:              stringValue(recordField(record, "id")),
		TenantID:        stringValue(recordField(record, "tenant_id")),
		UserID:          stringValue(recordField(record, "user_id")),
		ProjectID:       stringValue(recordField(record, "project_id")),
		Title:           stringValue(recordField(record, "title")),
		Status:          chat.ConversationStatus(stringValue(recordField(record, "status"))),
		EventSequence:   recordInt64(recordField(record, "event_sequence")),
		MessageSequence: recordInt64(recordField(record, "message_sequence")),
		CreatedAt:       timeValue(recordField(record, "created_at")),
		UpdatedAt:       timeValue(recordField(record, "updated_at")),
	}
}

func messageFromRecord(record recordReader) chat.Message {
	return chat.Message{
		ID:             stringValue(recordField(record, "id")),
		TenantID:       stringValue(recordField(record, "tenant_id")),
		UserID:         stringValue(recordField(record, "user_id")),
		ConversationID: stringValue(recordField(record, "conversation_id")),
		TurnID:         stringValue(recordField(record, "turn_id")),
		Sequence:       recordInt64(recordField(record, "sequence")),
		Role:           chat.Role(stringValue(recordField(record, "role"))),
		Content:        stringValue(recordField(record, "content")),
		Citations:      parseCitations(recordField(record, "citations_json")),
		ToolCallIDs:    parseStringSliceJSON(recordField(record, "tool_call_ids_json")),
		TaskIDs:        parseStringSliceJSON(recordField(record, "task_ids_json")),
		CreatedAt:      timeValue(recordField(record, "created_at")),
	}
}

func eventFromRecord(record recordReader, tenantID, userID string) chat.Event {
	return chat.Event{
		ID:             stringValue(recordField(record, "event_id")),
		TenantID:       tenantID,
		UserID:         userID,
		ConversationID: stringValue(recordField(record, "conversation_id")),
		TurnID:         stringValue(recordField(record, "turn_id")),
		TaskID:         stringValue(recordField(record, "task_id")),
		RunID:          stringValue(recordField(record, "run_id")),
		Sequence:       recordInt64(recordField(record, "sequence")),
		Type:           chat.EventType(stringValue(recordField(record, "event_type"))),
		Data:           parseEventData(recordField(record, "data_json")),
		CreatedAt:      timeValue(recordField(record, "created_at")),
	}
}

func turnFromRecord(record recordReader) chat.Turn {
	return chat.Turn{
		ID:                 stringValue(recordField(record, "id")),
		TenantID:           stringValue(recordField(record, "tenant_id")),
		UserID:             stringValue(recordField(record, "user_id")),
		ConversationID:     stringValue(recordField(record, "conversation_id")),
		IdempotencyKey:     stringValue(recordField(record, "idempotency_key")),
		Status:             chat.TurnStatus(stringValue(recordField(record, "status"))),
		UserMessageID:      stringValue(recordField(record, "user_message_id")),
		AssistantMessageID: stringValue(recordField(record, "assistant_message_id")),
		TerminalResult:     stringValue(recordField(record, "terminal_result")),
		Error:              stringValue(recordField(record, "error")),
		CancelRequested:    boolValue(recordField(record, "cancel_requested")),
		CreatedAt:          timeValue(recordField(record, "created_at")),
		UpdatedAt:          timeValue(recordField(record, "updated_at")),
		CompletedAt:        timeValue(recordField(record, "completed_at")),
	}
}

func citationsJSON(value []chat.Citation) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func stringSliceJSON(value []string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func parseStringSliceJSON(value interface{}) []string {
	encoded := strings.TrimSpace(stringValue(value))
	if encoded == "" || encoded == "<nil>" || encoded == "null" {
		return nil
	}
	var result []string
	if json.Unmarshal([]byte(encoded), &result) != nil {
		return nil
	}
	return result
}

func parseCitations(value interface{}) []chat.Citation {
	encoded := strings.TrimSpace(stringValue(value))
	if encoded == "" || encoded == "<nil>" || encoded == "null" {
		return nil
	}
	var result []chat.Citation
	if json.Unmarshal([]byte(encoded), &result) != nil {
		return nil
	}
	return result
}

func eventDataJSON(value map[string]string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func parseEventData(value interface{}) map[string]string {
	encoded := strings.TrimSpace(stringValue(value))
	if encoded == "" || encoded == "<nil>" || encoded == "null" {
		return nil
	}
	var result map[string]string
	if json.Unmarshal([]byte(encoded), &result) != nil {
		return nil
	}
	return result
}
