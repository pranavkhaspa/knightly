package coding

import (
	"context"
	"errors"
	"time"
)

var ErrArtifactNotFound = errors.New("coding artifact not found")

type RunRecord struct {
	ID                string    `json:"id"`
	TenantID          string    `json:"tenant_id"`
	UserID            string    `json:"user_id"`
	ProjectID         string    `json:"project_id,omitempty"`
	TaskID            string    `json:"task_id"`
	Provider          string    `json:"provider"`
	ProviderSessionID string    `json:"provider_session_id,omitempty"`
	EventCursor       string    `json:"event_cursor,omitempty"`
	Status            string    `json:"status"`
	WorkspaceID       string    `json:"workspace_id"`
	Error             string    `json:"error,omitempty"`
	StartedAt         time.Time `json:"started_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type Artifact struct {
	ID        string            `json:"id"`
	TenantID  string            `json:"tenant_id"`
	UserID    string            `json:"user_id"`
	ProjectID string            `json:"project_id,omitempty"`
	TaskID    string            `json:"task_id"`
	TurnID    string            `json:"turn_id,omitempty"`
	RunID     string            `json:"run_id,omitempty"`
	Kind      string            `json:"kind"`
	Content   string            `json:"content,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

type RecordRepository interface {
	SaveWorkspace(context.Context, Workspace, string, string) error
	SaveRun(context.Context, RunRecord) error
	SaveArtifact(context.Context, Artifact) error
	GetArtifact(context.Context, string, string, string) (Artifact, error)
}
