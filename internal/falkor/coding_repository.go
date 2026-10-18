package falkor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pranavkhaspa/knightly/internal/coding"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

// CodingRepository persists coding runs, workspaces, and bounded artifacts in
// FalkorDB. It deliberately does not copy product state to a local database.
type CodingRepository struct{ client *Client }

func NewCodingRepository(client *Client) *CodingRepository { return &CodingRepository{client: client} }

func (r *CodingRepository) SaveWorkspace(ctx context.Context, workspace coding.Workspace, tenantID, userID string) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(workspace.TaskID) == "" || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(userID) == "" {
		return fmt.Errorf("workspace task and scope IDs are required")
	}
	result, err := r.client.user.Query(`
		MATCH (t:Task {id: $task_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (w:Workspace {id: $workspace_id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET w.created_at = timestamp()
		SET w.task_id = $task_id, w.repository_path = $repository_path,
		    w.path = $path, w.base_sha = $base_sha, w.branch = $branch,
		    w.cleanup_state = $cleanup_state, w.updated_at = timestamp()
		MERGE (t)-[:HAS_WORKSPACE]->(w)
		RETURN w.id AS id
	`, map[string]interface{}{
		"task_id": workspace.TaskID, "tenant_id": tenantID, "user_id": userID,
		"workspace_id": workspace.TaskID, "repository_path": workspace.RepositoryPath,
		"path": workspace.Path, "base_sha": workspace.BaseSHA, "branch": workspace.Branch,
		"cleanup_state": workspace.CleanupState,
	}, r.client.options())
	if err != nil {
		return fmt.Errorf("save coding workspace: %w", err)
	}
	return requireQueryRow(result, "save coding workspace")
}

func (r *CodingRepository) SaveRun(ctx context.Context, run coding.RunRecord) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(run.ID) == "" || strings.TrimSpace(run.TaskID) == "" || strings.TrimSpace(run.TenantID) == "" || strings.TrimSpace(run.UserID) == "" {
		return fmt.Errorf("coding run IDs and scope are required")
	}
	result, err := r.client.user.Query(`
		MATCH (t:Task {id: $task_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (r:AgentRun {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET r.started_at = $started_at
		SET r.task_id = $task_id, r.project_id = $project_id, r.provider = $provider,
		    r.provider_session_id = $provider_session_id, r.event_cursor = $event_cursor, r.status = $status,
		    r.workspace_id = $workspace_id, r.error = $error, r.updated_at = $updated_at
		MERGE (t)-[:RUNS_AS]->(r)
		WITH t, r
		OPTIONAL MATCH (w:Workspace {id: $workspace_id, tenant_id: $tenant_id, user_id: $user_id})
		FOREACH (_ IN CASE WHEN w IS NULL THEN [] ELSE [1] END | MERGE (r)-[:USES]->(w))
		RETURN r.id AS id
	`, map[string]interface{}{
		"id": run.ID, "task_id": run.TaskID, "tenant_id": run.TenantID, "user_id": run.UserID,
		"project_id": run.ProjectID, "provider": run.Provider, "provider_session_id": run.ProviderSessionID, "event_cursor": run.EventCursor,
		"status": run.Status, "workspace_id": run.WorkspaceID, "error": run.Error,
		"started_at": optionalMillis(run.StartedAt), "updated_at": optionalMillis(run.UpdatedAt),
	}, r.client.options())
	if err != nil {
		return fmt.Errorf("save coding run: %w", err)
	}
	return requireQueryRow(result, "save coding run")
}

func (r *CodingRepository) SaveArtifact(ctx context.Context, artifact coding.Artifact) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(artifact.ID) == "" || strings.TrimSpace(artifact.TaskID) == "" || strings.TrimSpace(artifact.TenantID) == "" || strings.TrimSpace(artifact.UserID) == "" || strings.TrimSpace(artifact.Kind) == "" {
		return fmt.Errorf("artifact IDs, kind, and scope are required")
	}
	if len(artifact.Content) > coding.DefaultMaxDiffBytes+coding.DefaultMaxOutputBytes {
		return fmt.Errorf("artifact content exceeds persistence limit")
	}
	metadata, _ := json.Marshal(artifact.Metadata)
	result, err := r.client.user.Query(`
		MATCH (t:Task {id: $task_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (a:Artifact {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET a.created_at = $created_at
		SET a.task_id = $task_id, a.project_id = $project_id, a.turn_id = $turn_id, a.run_id = $run_id,
		    a.kind = $kind, a.content = $content, a.metadata_json = $metadata_json
		MERGE (t)-[:PRODUCES]->(a)
		WITH a
		OPTIONAL MATCH (turn:Turn {id: $turn_id, tenant_id: $tenant_id, user_id: $user_id})
		FOREACH (_ IN CASE WHEN turn IS NULL THEN [] ELSE [1] END | MERGE (turn)-[:PRODUCES]->(a))
		WITH a
		OPTIONAL MATCH (r:AgentRun {id: $run_id, tenant_id: $tenant_id, user_id: $user_id})
		FOREACH (_ IN CASE WHEN r IS NULL THEN [] ELSE [1] END | MERGE (r)-[:PRODUCES]->(a))
		RETURN a.id AS id
	`, map[string]interface{}{
		"id": artifact.ID, "task_id": artifact.TaskID, "tenant_id": artifact.TenantID, "user_id": artifact.UserID,
		"project_id": artifact.ProjectID, "turn_id": artifact.TurnID, "run_id": artifact.RunID, "kind": artifact.Kind,
		"content": artifact.Content, "metadata_json": string(metadata), "created_at": optionalMillis(artifact.CreatedAt),
	}, r.client.options())
	if err != nil {
		return fmt.Errorf("save coding artifact: %w", err)
	}
	return requireQueryRow(result, "save coding artifact")
}

func (r *CodingRepository) GetArtifact(ctx context.Context, tenantID, userID, artifactID string) (coding.Artifact, error) {
	if err := r.ready(ctx); err != nil {
		return coding.Artifact{}, err
	}
	if err := (identity.Scope{TenantID: tenantID, UserID: userID}).Validate(); err != nil {
		return coding.Artifact{}, err
	}
	result, err := r.client.user.ROQuery(`
		MATCH (a:Artifact {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		RETURN a.id AS id, a.tenant_id AS tenant_id, a.user_id AS user_id,
		       a.project_id AS project_id, a.task_id AS task_id, a.turn_id AS turn_id, a.run_id AS run_id,
		       a.kind AS kind, a.content AS content, a.metadata_json AS metadata_json,
		       a.created_at AS created_at
		LIMIT 1
	`, map[string]interface{}{"id": artifactID, "tenant_id": tenantID, "user_id": userID}, r.client.options())
	if err != nil {
		return coding.Artifact{}, fmt.Errorf("get coding artifact: %w", err)
	}
	if err := requireQueryRow(result, "get coding artifact"); err != nil {
		return coding.Artifact{}, fmt.Errorf("%w: %s", coding.ErrArtifactNotFound, artifactID)
	}
	record := result.Record()
	artifact := coding.Artifact{
		ID: stringValue(recordField(record, "id")), TenantID: stringValue(recordField(record, "tenant_id")),
		UserID: stringValue(recordField(record, "user_id")), ProjectID: stringValue(recordField(record, "project_id")),
		TaskID: stringValue(recordField(record, "task_id")), TurnID: stringValue(recordField(record, "turn_id")), RunID: stringValue(recordField(record, "run_id")),
		Kind: stringValue(recordField(record, "kind")), Content: stringValue(recordField(record, "content")),
		CreatedAt: timeValue(recordField(record, "created_at")),
	}
	if encoded := stringValue(recordField(record, "metadata_json")); encoded != "" {
		_ = json.Unmarshal([]byte(encoded), &artifact.Metadata)
	}
	return artifact, nil
}

func (r *CodingRepository) ready(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.client == nil || r.client.user == nil {
		return fmt.Errorf("coding graph repository is not configured")
	}
	return nil
}
