package falkor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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
	graph, err := routedGraph(ctx, r.selector, scope, "conversation list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(conversationQuery+" ORDER BY coalesce(c.updated_at, 0) DESC, c.id LIMIT $limit", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": "", "limit": limit,
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
		    m.citations_json = $citations_json
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
	graph, err := routedGraph(ctx, r.selector, scope, "message list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(`
		MATCH (c:Conversation {id: $conversation_id, tenant_id: $tenant_id, user_id: $user_id})-[:CONTAINS]->(m:Message)
		RETURN m.id AS id, m.tenant_id AS tenant_id, m.user_id AS user_id,
		       m.conversation_id AS conversation_id, m.turn_id AS turn_id,
		       m.sequence AS sequence, m.role AS role, m.content AS content,
		       m.citations_json AS citations_json, m.created_at AS created_at
		ORDER BY coalesce(m.sequence, 0), m.id
		LIMIT $limit
	`, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID,
		"conversation_id": conversationID, "limit": limit,
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

func (r *ChatRepository) AppendEvent(ctx context.Context, value chat.Event) (chat.Event, error) {
	graph, err := routedGraph(ctx, r.selector, identity.Scope{TenantID: value.TenantID, UserID: value.UserID}, "event append")
	if err != nil {
		return chat.Event{}, err
	}
	result, err := graph.Query(`
		MATCH (c:Conversation {id: $conversation_id, tenant_id: $tenant_id, user_id: $user_id})
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
		"citations_json": citationsJSON(value.Citations), "created_at": optionalMillis(value.CreatedAt),
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

func citationsJSON(value []chat.Citation) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
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
