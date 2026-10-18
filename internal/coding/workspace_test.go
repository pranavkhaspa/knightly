package coding

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func initTestRepo(t *testing.T) (string, string) {
	t.Helper()
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0755); err != nil {
		t.Fatal(err)
	}
	commands := [][]string{{"init", "-q"}, {"config", "user.email", "test@example.com"}, {"config", "user.name", "NightShift Test"}}
	for _, args := range commands {
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module example.com/nightshift-testrepo\n\ngo 1.23\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "main.go"), []byte("package main\nfunc main() {}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "main.go", "go.mod"}, {"commit", "-qm", "initial"}} {
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	cmd := exec.Command("git", "-C", repo, "rev-parse", "HEAD")
	sha, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return repo, strings.TrimSpace(string(sha))
}

func TestWorkspaceManagerCreatesIsolatedWorktreeAndCleansUp(t *testing.T) {
	repo, sha := initTestRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	manager, err := NewWorkspaceManager(WorkspaceManagerConfig{Root: root, AllowedRepositories: []string{repo}})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := manager.Create(context.Background(), "task-1", repo, sha)
	if err != nil {
		t.Fatal(err)
	}
	if workspace.BaseSHA != sha {
		t.Fatalf("base SHA = %s, want %s", workspace.BaseSHA, sha)
	}
	if workspace.Path == repo {
		t.Fatal("workspace must be separate from source repository")
	}
	if _, err := os.Stat(filepath.Join(workspace.Path, "main.go")); err != nil {
		t.Fatalf("expected source file in worktree: %v", err)
	}
	if _, err := manager.Create(context.Background(), "task-1", repo, sha); err == nil {
		t.Fatal("expected duplicate task workspace to fail")
	}
	if err := manager.Cleanup(context.Background(), "task-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workspace.Path); !os.IsNotExist(err) {
		t.Fatalf("workspace still exists after cleanup: %v", err)
	}
}

func TestWorkspaceManagerRejectsUnapprovedRepositoryAndUnsafeTaskID(t *testing.T) {
	repo, sha := initTestRepo(t)
	other, _ := initTestRepo(t)
	manager, err := NewWorkspaceManager(WorkspaceManagerConfig{Root: filepath.Join(t.TempDir(), "workspaces"), AllowedRepositories: []string{repo}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(context.Background(), "task-1", other, sha); err == nil {
		t.Fatal("expected unapproved repository to be rejected")
	}
	if _, err := manager.Create(context.Background(), "../escape", repo, sha); err == nil {
		t.Fatal("expected unsafe task ID to be rejected")
	}
}

func TestValidateRunRequestRejectsOriginalCheckout(t *testing.T) {
	req := RunRequest{TaskID: "task-1", Title: "Fix bug", Prompt: "Fix the bug", RepositoryPath: "/tmp/project", WorkspacePath: "/tmp/project"}
	if err := validateRunRequest(req); err == nil {
		t.Fatal("expected original checkout to be rejected")
	}
}

func TestCommandTestRunnerRunsConfiguredArgvInsideWorkspace(t *testing.T) {
	repo, sha := initTestRepo(t)
	manager, err := NewWorkspaceManager(WorkspaceManagerConfig{Root: filepath.Join(t.TempDir(), "workspaces"), AllowedRepositories: []string{repo}})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := manager.Create(context.Background(), "task-test", repo, sha)
	if err != nil {
		t.Fatal(err)
	}
	runner := &CommandTestRunner{Workspaces: manager, CommandsByProject: map[string][]string{"project-1": {"git", "status", "--short"}}, MaxOutputBytes: 128}
	result, err := runner.Run(context.Background(), RunHandle{TaskID: "task-test", ProjectID: "project-1", WorkspacePath: workspace.Path})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passed || result.ExitCode != 0 {
		t.Fatalf("expected configured test command to pass: %#v", result)
	}
	if len(result.Command) != 3 || result.Command[0] != "git" {
		t.Fatalf("unexpected command: %#v", result.Command)
	}
	if err := manager.Cleanup(context.Background(), "task-test"); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceManagerCanReattachPersistedWorktreeAfterRestart(t *testing.T) {
	repo, sha := initTestRepo(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	first, err := NewWorkspaceManager(WorkspaceManagerConfig{Root: root, AllowedRepositories: []string{repo}})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := first.Create(context.Background(), "task-restore", repo, sha)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWorkspaceManager(WorkspaceManagerConfig{Root: root, AllowedRepositories: []string{repo}})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := second.Attach(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Path != workspace.Path || restored.BaseSHA != sha {
		t.Fatalf("restored workspace mismatch: %#v", restored)
	}
	if err := second.Cleanup(context.Background(), "task-restore"); err != nil {
		t.Fatal(err)
	}
}
