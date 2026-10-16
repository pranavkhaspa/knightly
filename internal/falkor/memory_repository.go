package falkor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pranavkhaspa/knightly/internal/memory"
)

// MemoryRepository is the FalkorDB implementation of memory.Repository. All
// queries in this file are fixed application operations; callers provide only
// typed parameters.
type MemoryRepository struct {
	client *Client
}

var _ memory.Repository = (*MemoryRepository)(nil)

func NewMemoryRepository(client *Client) *MemoryRepository {
	return &MemoryRepository{client: client}
}

func (r *MemoryRepository) Save(ctx context.Context, item memory.Memory) error {
	if strings.TrimSpace(item.TenantID) == "" || strings.TrimSpace(item.UserID) == "" {
		return fmt.Errorf("memory tenant and user IDs are required")
	}
	if item.SupersedesID != "" {
		return r.Supersede(ctx, item.SupersedesID, item)
	}
	if err := r.ready(ctx); err != nil {
		return err
	}
	result, err := r.client.user.Query(`
		MERGE (m:Memory {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET m.created_at = $created_at
		SET m.tenant_id = $tenant_id,
		    m.session_id = $session_id,
		    m.task_id = $task_id,
		    m.project_id = $project_id,
		    m.memory_type = $memory_type,
		    m.content = $content,
		    m.summary = $summary,
		    m.evidence_ids = $evidence_ids,
		    m.metadata_json = $metadata_json,
		    m.updated_at = $updated_at
		RETURN m.id AS id
	`, memoryParams(item), r.client.options())
	if err != nil {
		return fmt.Errorf("save memory in graph: %w", err)
	}
	if err := requireQueryRow(result, "save memory"); err != nil {
		return err
	}
	return r.saveEvidenceLinks(ctx, item)
}

func (r *MemoryRepository) Supersede(ctx context.Context, previousID string, replacement memory.Memory) error {
	if strings.TrimSpace(previousID) == "" {
		return fmt.Errorf("previous memory ID is required")
	}
	if strings.TrimSpace(replacement.TenantID) == "" || strings.TrimSpace(replacement.UserID) == "" {
		return fmt.Errorf("memory tenant and user IDs are required")
	}
	if err := r.ready(ctx); err != nil {
		return err
	}
	params := memoryParams(replacement)
	params["previous_id"] = previousID
	result, err := r.client.user.Query(`
		MATCH (previous:Memory {id: $previous_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (m:Memory {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET m.created_at = $created_at
		SET m.tenant_id = $tenant_id,
		    m.session_id = $session_id,
		    m.task_id = $task_id,
		    m.project_id = $project_id,
		    m.memory_type = $memory_type,
		    m.content = $content,
		    m.summary = $summary,
		    m.evidence_ids = $evidence_ids,
		    m.metadata_json = $metadata_json,
		    m.supersedes_id = $previous_id,
		    m.updated_at = $updated_at
		MERGE (m)-[:SUPERSEDES]->(previous)
		RETURN m.id AS id
	`, params, r.client.options())
	if err != nil {
		return fmt.Errorf("supersede memory in graph: %w", err)
	}
	if err := requireQueryRow(result, "supersede memory"); err != nil {
		return fmt.Errorf("%w: previous memory %q may not exist", err, previousID)
	}
	return r.saveEvidenceLinks(ctx, replacement)
}

func (r *MemoryRepository) Get(ctx context.Context, userID, id string) (memory.Memory, error) {
	if err := r.ready(ctx); err != nil {
		return memory.Memory{}, err
	}
	result, err := r.client.user.ROQuery(`
		MATCH (u:User {id: $user_id})
		MATCH (m:Memory {id: $id, user_id: $user_id})
		WHERE m.tenant_id = u.tenant_id
		RETURN m.id AS id, m.tenant_id AS tenant_id, m.user_id AS user_id,
		       m.session_id AS session_id, m.task_id AS task_id,
		       m.project_id AS project_id, m.memory_type AS memory_type,
		       m.content AS content, m.summary AS summary,
		       m.evidence_ids AS evidence_ids, m.metadata_json AS metadata_json,
		       m.supersedes_id AS supersedes_id, m.created_at AS created_at,
		       m.updated_at AS updated_at
		LIMIT 1
	`, map[string]interface{}{"id": id, "user_id": userID}, r.client.options())
	if err != nil {
		return memory.Memory{}, fmt.Errorf("get memory from graph: %w", err)
	}
	if err := requireQueryRow(result, "get memory"); err != nil {
		return memory.Memory{}, fmt.Errorf("%w: %s", memory.ErrMemoryNotFound, err)
	}
	return memoryFromRecord(result.Record())
}

func (r *MemoryRepository) Retrieve(ctx context.Context, query memory.RetrievalQuery) ([]memory.Memory, error) {
	if err := r.ready(ctx); err != nil {
		return nil, err
	}
	types := make([]string, 0, len(query.Types))
	for _, memoryType := range query.Types {
		types = append(types, string(memoryType))
	}
	result, err := r.client.user.ROQuery(`
		MATCH (m:Memory)
		WHERE m.user_id = $user_id
		  AND ($tenant_id = "" OR m.tenant_id = $tenant_id)
		  AND ($session_id = "" OR m.session_id = $session_id)
		  AND ($task_id = "" OR m.task_id = $task_id)
		  AND ($project_id = "" OR m.project_id = $project_id)
		  AND (size($types) = 0 OR m.memory_type IN $types)
		  AND ($include_superseded OR NOT (m)<-[:SUPERSEDES]-(:Memory))
		RETURN m.id AS id, m.tenant_id AS tenant_id, m.user_id AS user_id,
		       m.session_id AS session_id, m.task_id AS task_id,
		       m.project_id AS project_id, m.memory_type AS memory_type,
		       m.content AS content, m.summary AS summary,
		       m.evidence_ids AS evidence_ids, m.metadata_json AS metadata_json,
		       m.supersedes_id AS supersedes_id, m.created_at AS created_at,
		       m.updated_at AS updated_at
		ORDER BY CASE
		           WHEN $task_id <> "" AND m.task_id = $task_id THEN 0
		           WHEN $session_id <> "" AND m.session_id = $session_id THEN 1
		           WHEN $project_id <> "" AND m.project_id = $project_id THEN 2
		           WHEN m.memory_type IN ["operational", "episodic"] THEN 3
		           ELSE 4
		         END, m.updated_at DESC
		LIMIT $limit
	`, map[string]interface{}{
		"tenant_id":          query.TenantID,
		"user_id":            query.UserID,
		"session_id":         query.SessionID,
		"task_id":            query.TaskID,
		"project_id":         query.ProjectID,
		"types":              types,
		"include_superseded": query.IncludeSuperseded,
		"limit":              query.Limit,
	}, r.client.options())
	if err != nil {
		return nil, fmt.Errorf("retrieve memories from graph: %w", err)
	}
	items := make([]memory.Memory, 0, query.Limit)
	for result.Next() {
		item, parseErr := memoryFromRecord(result.Record())
		if parseErr != nil {
			return nil, parseErr
		}
		items = append(items, item)
	}
	return items, nil
}

func (r *MemoryRepository) saveEvidenceLinks(ctx context.Context, item memory.Memory) error {
	if len(item.EvidenceIDs) == 0 {
		return nil
	}
	if err := r.ready(ctx); err != nil {
		return err
	}
	_, err := r.client.user.Query(`
		UNWIND $evidence_ids AS evidence_id
		MATCH (m:Memory {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		MATCH (e:Evidence {id: evidence_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (m)-[:DERIVED_FROM]->(e)
	`, map[string]interface{}{
		"evidence_ids": item.EvidenceIDs,
		"id":           item.ID,
		"tenant_id":    item.TenantID,
		"user_id":      item.UserID,
	}, r.client.options())
	if err != nil {
		return fmt.Errorf("link memory evidence: %w", err)
	}
	return nil
}

func (r *MemoryRepository) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.client == nil || r.client.user == nil {
		return fmt.Errorf("memory graph repository is not configured")
	}
	return nil
}

func memoryParams(item memory.Memory) map[string]interface{} {
	return map[string]interface{}{
		"id":            item.ID,
		"tenant_id":     item.TenantID,
		"user_id":       item.UserID,
		"session_id":    item.SessionID,
		"task_id":       item.TaskID,
		"project_id":    item.ProjectID,
		"memory_type":   string(item.Type),
		"content":       item.Content,
		"summary":       item.Summary,
		"evidence_ids":  item.EvidenceIDs,
		"metadata_json": jsonMap(item.Metadata),
		"created_at":    optionalMillis(item.CreatedAt),
		"updated_at":    optionalMillis(item.UpdatedAt),
	}
}

func memoryFromRecord(record recordReader) (memory.Memory, error) {
	return memory.Memory{
		ID:           stringValue(recordField(record, "id")),
		TenantID:     stringValue(recordField(record, "tenant_id")),
		UserID:       stringValue(recordField(record, "user_id")),
		SessionID:    stringValue(recordField(record, "session_id")),
		TaskID:       stringValue(recordField(record, "task_id")),
		ProjectID:    stringValue(recordField(record, "project_id")),
		Type:         memory.Type(stringValue(recordField(record, "memory_type"))),
		Content:      stringValue(recordField(record, "content")),
		Summary:      stringValue(recordField(record, "summary")),
		EvidenceIDs:  stringSliceValue(recordField(record, "evidence_ids")),
		Metadata:     parseStringMap(recordField(record, "metadata_json")),
		SupersedesID: stringValue(recordField(record, "supersedes_id")),
		CreatedAt:    timeValue(recordField(record, "created_at")),
		UpdatedAt:    timeValue(recordField(record, "updated_at")),
	}, nil
}

func recordField(record recordReader, key string) interface{} {
	value, _ := record.Get(key)
	return value
}
