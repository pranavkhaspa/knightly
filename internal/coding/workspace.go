package coding

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var (
	taskIDPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	commitSHAPattern = regexp.MustCompile(`^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$`)
)

type Workspace struct {
	TaskID         string `json:"task_id"`
	RepositoryPath string `json:"repository_path"`
	Path           string `json:"path"`
	BaseSHA        string `json:"base_sha"`
	Branch         string `json:"branch,omitempty"`
	CleanupState   string `json:"cleanup_state"`
}

type WorkspaceManagerConfig struct {
	Root                string
	AllowedRepositories []string
}

// WorkspaceManager creates detached Git worktrees below a dedicated root.
// Repositories must be explicitly allowlisted at construction time.
type WorkspaceManager struct {
	root         string
	allowedRepos map[string]struct{}
	mu           sync.Mutex
	active       map[string]Workspace
}

func NewWorkspaceManager(cfg WorkspaceManagerConfig) (*WorkspaceManager, error) {
	if strings.TrimSpace(cfg.Root) == "" {
		return nil, fmt.Errorf("%w: workspace root is required", ErrInvalidRequest)
	}
	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace root: %w", err)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create workspace root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace root symlinks: %w", err)
	}
	allowed := make(map[string]struct{}, len(cfg.AllowedRepositories))
	for _, value := range cfg.AllowedRepositories {
		if strings.TrimSpace(value) == "" {
			continue
		}
		repo, e := filepath.Abs(value)
		if e != nil {
			return nil, fmt.Errorf("resolve repository: %w", e)
		}
		repo, e = filepath.EvalSymlinks(repo)
		if e != nil {
			return nil, fmt.Errorf("resolve repository %q: %w", value, e)
		}
		info, e := os.Stat(repo)
		if e != nil || !info.IsDir() {
			return nil, fmt.Errorf("%w: allowlisted repository is not a directory: %s", ErrInvalidRequest, repo)
		}
		if pathWithin(root, repo) || pathWithin(repo, root) {
			return nil, fmt.Errorf("%w: workspace root and repository must not contain one another", ErrInvalidRequest)
		}
		allowed[repo] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, fmt.Errorf("%w: at least one allowed repository is required", ErrInvalidRequest)
	}
	return &WorkspaceManager{root: root, allowedRepos: allowed, active: make(map[string]Workspace)}, nil
}

// HeadSHA returns the current commit for an explicitly allowlisted repository.
// It is used when a queued coding task did not pin a base revision at creation.
func (m *WorkspaceManager) HeadSHA(ctx context.Context, repositoryPath string) (string, error) {
	if m == nil {
		return "", fmt.Errorf("%w: workspace manager is nil", ErrInvalidRequest)
	}
	repo, err := filepath.Abs(repositoryPath)
	if err != nil {
		return "", fmt.Errorf("%w: invalid repository path", ErrInvalidRequest)
	}
	repo, err = filepath.EvalSymlinks(repo)
	if err != nil {
		return "", fmt.Errorf("%w: repository does not exist: %v", ErrInvalidRequest, err)
	}
	if _, ok := m.allowedRepos[repo]; !ok {
		return "", fmt.Errorf("%w: repository is not allowlisted", ErrInvalidRequest)
	}
	sha, err := git(ctx, repo, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	sha = strings.TrimSpace(sha)
	if !commitSHAPattern.MatchString(sha) {
		return "", fmt.Errorf("%w: repository HEAD did not resolve to a commit SHA", ErrInvalidRequest)
	}
	return sha, nil
}

func (m *WorkspaceManager) Create(ctx context.Context, taskID, repositoryPath, baseSHA string) (Workspace, error) {
	if m == nil {
		return Workspace{}, fmt.Errorf("%w: workspace manager is nil", ErrInvalidRequest)
	}
	taskID, baseSHA = strings.TrimSpace(taskID), strings.TrimSpace(baseSHA)
	if !taskIDPattern.MatchString(taskID) || !commitSHAPattern.MatchString(baseSHA) {
		return Workspace{}, fmt.Errorf("%w: task ID and base SHA are required", ErrInvalidRequest)
	}
	repo, err := filepath.Abs(repositoryPath)
	if err != nil {
		return Workspace{}, fmt.Errorf("%w: invalid repository path", ErrInvalidRequest)
	}
	repo, err = filepath.EvalSymlinks(repo)
	if err != nil {
		return Workspace{}, fmt.Errorf("%w: repository does not exist: %v", ErrInvalidRequest, err)
	}
	if _, ok := m.allowedRepos[repo]; !ok {
		return Workspace{}, fmt.Errorf("%w: repository is not allowlisted", ErrInvalidRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.active[taskID]; exists {
		return Workspace{}, fmt.Errorf("%w: task %s", ErrWorkspaceConflict, taskID)
	}
	path := filepath.Join(m.root, taskID)
	if !pathWithin(m.root, path) || path == m.root {
		return Workspace{}, fmt.Errorf("%w: derived workspace path is invalid", ErrWorkspaceBoundary)
	}
	if _, e := os.Lstat(path); e == nil {
		return Workspace{}, fmt.Errorf("%w: path already exists", ErrWorkspaceConflict)
	} else if !errors.Is(e, os.ErrNotExist) {
		return Workspace{}, fmt.Errorf("inspect workspace path: %w", e)
	}
	verifiedSHA, err := git(ctx, repo, "rev-parse", "--verify", baseSHA+"^{commit}")
	if err != nil {
		return Workspace{}, fmt.Errorf("verify base commit: %w", err)
	}
	verifiedSHA = strings.TrimSpace(verifiedSHA)
	if verifiedSHA == "" {
		return Workspace{}, fmt.Errorf("%w: base SHA resolved to empty commit", ErrInvalidRequest)
	}
	if _, err := git(ctx, repo, "worktree", "add", "--detach", path, verifiedSHA); err != nil {
		return Workspace{}, fmt.Errorf("create Git worktree: %w", err)
	}
	workspace := Workspace{TaskID: taskID, RepositoryPath: repo, Path: path, BaseSHA: verifiedSHA, CleanupState: "active"}
	m.active[taskID] = workspace
	return workspace, nil
}

// Attach restores a previously persisted workspace after a NightShift restart.
// It verifies the path is beneath the configured root, belongs to an allowlisted
// repository, and is a Git worktree before trusting it.
func (m *WorkspaceManager) Attach(workspace Workspace) (Workspace, error) {
	if m == nil {
		return Workspace{}, fmt.Errorf("%w: workspace manager is nil", ErrInvalidRequest)
	}
	if !taskIDPattern.MatchString(workspace.TaskID) || workspace.Path == "" || workspace.RepositoryPath == "" || workspace.BaseSHA == "" {
		return Workspace{}, fmt.Errorf("%w: persisted workspace metadata is incomplete", ErrInvalidRequest)
	}
	repo, err := filepath.EvalSymlinks(workspace.RepositoryPath)
	if err != nil {
		return Workspace{}, fmt.Errorf("%w: persisted repository is unavailable", ErrInvalidRequest)
	}
	if _, ok := m.allowedRepos[repo]; !ok {
		return Workspace{}, fmt.Errorf("%w: persisted repository is not allowlisted", ErrInvalidRequest)
	}
	path, err := filepath.Abs(workspace.Path)
	if err != nil {
		return Workspace{}, fmt.Errorf("%w: invalid persisted workspace path", ErrInvalidRequest)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil || !pathWithin(m.root, path) {
		return Workspace{}, fmt.Errorf("%w: persisted workspace is missing or outside workspace root", ErrWorkspaceBoundary)
	}
	top, err := git(context.Background(), path, "rev-parse", "--show-toplevel")
	if err != nil || filepath.Clean(strings.TrimSpace(top)) != filepath.Clean(path) {
		return Workspace{}, fmt.Errorf("%w: persisted path is not the expected Git worktree", ErrWorkspaceBoundary)
	}
	workspace.Path, workspace.RepositoryPath = path, repo
	if workspace.CleanupState == "" {
		workspace.CleanupState = "active"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.active[workspace.TaskID]; ok && !samePath(existing.Path, workspace.Path) {
		return Workspace{}, fmt.Errorf("%w: task is already attached to another workspace", ErrWorkspaceConflict)
	}
	m.active[workspace.TaskID] = workspace
	return workspace, nil
}

func (m *WorkspaceManager) Get(taskID string) (Workspace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	workspace, ok := m.active[taskID]
	if !ok {
		return Workspace{}, fmt.Errorf("%w: %s", ErrWorkspaceNotFound, taskID)
	}
	return workspace, nil
}

// Cleanup is only safe after the caller has durably saved task state/artifacts.
func (m *WorkspaceManager) Cleanup(ctx context.Context, taskID string) error {
	if m == nil {
		return fmt.Errorf("%w: workspace manager is nil", ErrInvalidRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	workspace, ok := m.active[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrWorkspaceNotFound, taskID)
	}
	canonical, err := filepath.Abs(workspace.Path)
	if err != nil || !pathWithin(m.root, canonical) || canonical == m.root {
		return fmt.Errorf("%w: refusing to remove path outside workspace root", ErrWorkspaceBoundary)
	}
	if _, err := os.Lstat(canonical); errors.Is(err, os.ErrNotExist) {
		delete(m.active, taskID)
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect workspace before cleanup: %w", err)
	}
	if _, err := git(ctx, workspace.RepositoryPath, "worktree", "remove", "--force", canonical); err != nil {
		return fmt.Errorf("remove Git worktree: %w", err)
	}
	if _, err := git(ctx, workspace.RepositoryPath, "worktree", "prune"); err != nil {
		return fmt.Errorf("prune Git worktrees: %w", err)
	}
	delete(m.active, taskID)
	return nil
}

func (m *WorkspaceManager) UpdateCleanupState(taskID, state string) (Workspace, error) {
	if strings.TrimSpace(state) == "" {
		return Workspace{}, fmt.Errorf("%w: cleanup state is required", ErrInvalidRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	workspace, ok := m.active[taskID]
	if !ok {
		return Workspace{}, fmt.Errorf("%w: %s", ErrWorkspaceNotFound, taskID)
	}
	workspace.CleanupState = state
	m.active[taskID] = workspace
	return workspace, nil
}

func (m *WorkspaceManager) List() []Workspace {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Workspace, 0, len(m.active))
	for _, workspace := range m.active {
		out = append(out, workspace)
	}
	return out
}

func git(ctx context.Context, repository string, args ...string) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("%w: context is required", ErrInvalidRequest)
	}
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if len(message) > 2048 {
			message = message[:2048]
		}
		return "", fmt.Errorf("git %s failed: %w: %s", strings.Join(args, " "), err, message)
	}
	return string(output), nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == "." || relative == ".." {
		return false
	}
	return !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}
