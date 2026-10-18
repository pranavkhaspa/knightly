package coding

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultMaxPromptBytes  = 64 * 1024
	DefaultMaxContextBytes = 64 * 1024
	DefaultMaxDiffBytes    = 2 * 1024 * 1024
	DefaultMaxOutputBytes  = 256 * 1024
)

var (
	ErrInvalidRequest      = errors.New("invalid coding-agent request")
	ErrProviderUnavailable = errors.New("coding-agent provider unavailable")
	ErrWorkspaceNotFound   = errors.New("coding workspace not found")
	ErrWorkspaceConflict   = errors.New("coding workspace already exists")
	ErrWorkspaceBoundary   = errors.New("coding workspace path is outside its configured root")
	ErrUnsupported         = errors.New("coding-agent operation is unsupported")
)

type Capabilities struct {
	Events      bool `json:"events"`
	Cancel      bool `json:"cancel"`
	Resume      bool `json:"resume"`
	Diff        bool `json:"diff"`
	TestResults bool `json:"test_results"`
	Permissions bool `json:"permissions"`
}

// RunRequest is the bounded task envelope sent to a coding provider. Workspace
// paths are selected by NightShift, never by untrusted prompt text.
type RunRequest struct {
	TaskID            string        `json:"task_id"`
	ProjectID         string        `json:"project_id,omitempty"`
	Title             string        `json:"title"`
	Prompt            string        `json:"prompt"`
	ExternalContext   string        `json:"external_context,omitempty"`
	WorkspacePath     string        `json:"workspace_path"`
	RepositoryPath    string        `json:"repository_path"`
	BaseSHA           string        `json:"base_sha"`
	ProviderSessionID string        `json:"provider_session_id,omitempty"`
	Timeout           time.Duration `json:"-"`
}

type RunHandle struct {
	TaskID            string `json:"task_id"`
	ProjectID         string `json:"project_id,omitempty"`
	Provider          string `json:"provider"`
	ProviderSessionID string `json:"provider_session_id"`
	WorkspacePath     string `json:"workspace_path"`
	EventCursor       string `json:"event_cursor,omitempty"`
	TerminalObserved  bool   `json:"-"`
}

type AgentEvent struct {
	ID        string            `json:"event_id,omitempty"`
	Type      string            `json:"event_type"`
	Sequence  string            `json:"sequence,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	TaskID    string            `json:"task_id"`
	RunID     string            `json:"run_id,omitempty"`
	Data      map[string]string `json:"data,omitempty"`
	Terminal  bool              `json:"terminal,omitempty"`
	Error     string            `json:"error,omitempty"`
}

type FileDiff struct {
	Path      string `json:"path"`
	Patch     string `json:"patch,omitempty"`
	Additions int    `json:"additions,omitempty"`
	Deletions int    `json:"deletions,omitempty"`
	Status    string `json:"status,omitempty"`
}

// UnmarshalJSON accepts both the v2 API's `file` field and older adapters' `path`
// field, normalizing both to Path for NightShift's artifact contract.
func (f *FileDiff) UnmarshalJSON(data []byte) error {
	var wire struct {
		Path      string `json:"path"`
		File      string `json:"file"`
		Patch     string `json:"patch"`
		Additions int    `json:"additions"`
		Deletions int    `json:"deletions"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	path := wire.Path
	if path == "" {
		path = wire.File
	}
	*f = FileDiff{Path: path, Patch: wire.Patch, Additions: wire.Additions, Deletions: wire.Deletions, Status: wire.Status}
	return nil
}

type DiffResult struct {
	TaskID    string     `json:"task_id"`
	SessionID string     `json:"session_id"`
	Files     []FileDiff `json:"files"`
	Truncated bool       `json:"truncated,omitempty"`
}

type TestResult struct {
	TaskID     string    `json:"task_id"`
	Passed     bool      `json:"passed"`
	ExitCode   int       `json:"exit_code"`
	Command    []string  `json:"command"`
	Output     string    `json:"output,omitempty"`
	Truncated  bool      `json:"truncated,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

// AgentAdapter is the provider-neutral boundary used by NightShift.
type AgentAdapter interface {
	Name() string
	Capabilities(context.Context) (Capabilities, error)
	Start(context.Context, RunRequest) (RunHandle, error)
	Events(context.Context, RunHandle) (<-chan AgentEvent, error)
	Cancel(context.Context, RunHandle) error
	Resume(context.Context, string, RunRequest) (RunHandle, error)
	Diff(context.Context, RunHandle) (DiffResult, error)
	Tests(context.Context, RunHandle) (TestResult, error)
}

func validateRunRequest(req RunRequest) error {
	if strings.TrimSpace(req.TaskID) == "" || strings.TrimSpace(req.Title) == "" {
		return fmt.Errorf("%w: task ID and title are required", ErrInvalidRequest)
	}
	if strings.TrimSpace(req.Prompt) == "" || len(req.Prompt) > DefaultMaxPromptBytes {
		return fmt.Errorf("%w: prompt must be non-empty and at most %d bytes", ErrInvalidRequest, DefaultMaxPromptBytes)
	}
	if len(req.ExternalContext) > DefaultMaxContextBytes {
		return fmt.Errorf("%w: external context exceeds %d bytes", ErrInvalidRequest, DefaultMaxContextBytes)
	}
	if strings.TrimSpace(req.WorkspacePath) == "" || strings.TrimSpace(req.RepositoryPath) == "" {
		return fmt.Errorf("%w: repository and workspace paths are required", ErrInvalidRequest)
	}
	workspace, err := filepath.Abs(req.WorkspacePath)
	if err != nil {
		return fmt.Errorf("%w: invalid workspace path", ErrInvalidRequest)
	}
	repository, err := filepath.Abs(req.RepositoryPath)
	if err != nil {
		return fmt.Errorf("%w: invalid repository path", ErrInvalidRequest)
	}
	if workspace == repository {
		return fmt.Errorf("%w: workspace must not be the original checkout", ErrInvalidRequest)
	}
	if req.Timeout < 0 {
		return fmt.Errorf("%w: timeout cannot be negative", ErrInvalidRequest)
	}
	return nil
}
