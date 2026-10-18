package coding

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// CommandTestRunner runs only preconfigured argument vectors. It never invokes
// a shell and never accepts a test command from the coding-agent prompt.
type CommandTestRunner struct {
	Workspaces        *WorkspaceManager
	CommandsByProject map[string][]string
	Timeout           time.Duration
	MaxOutputBytes    int
}

func (r *CommandTestRunner) Run(ctx context.Context, handle RunHandle) (TestResult, error) {
	if ctx == nil {
		return TestResult{}, fmt.Errorf("%w: context is required", ErrInvalidRequest)
	}
	if r == nil || r.Workspaces == nil {
		return TestResult{}, fmt.Errorf("%w: workspace manager is required", ErrInvalidRequest)
	}
	command := r.CommandsByProject[handle.ProjectID]
	if len(command) == 0 {
		return TestResult{}, fmt.Errorf("%w: no test command configured for project %q", ErrUnsupported, handle.ProjectID)
	}
	workspace, err := r.Workspaces.Get(handle.TaskID)
	if err != nil {
		return TestResult{}, err
	}
	if !samePath(workspace.Path, handle.WorkspacePath) {
		return TestResult{}, fmt.Errorf("%w: run handle workspace does not match task workspace", ErrWorkspaceBoundary)
	}
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	maxOutput := r.MaxOutputBytes
	if maxOutput <= 0 {
		maxOutput = DefaultMaxOutputBytes
	}
	if maxOutput > 4*1024*1024 {
		maxOutput = 4 * 1024 * 1024
	}
	started := time.Now().UTC()
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, command[0], command[1:]...)
	cmd.Dir = workspace.Path
	var output boundedBuffer
	output.limit = maxOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	err = cmd.Run()
	finished := time.Now().UTC()
	result := TestResult{TaskID: handle.TaskID, Command: append([]string(nil), command...), Output: output.String(), Truncated: output.truncated, StartedAt: started, FinishedAt: finished}
	if err == nil {
		result.Passed = true
		result.ExitCode = 0
		return result, nil
	}
	if runCtx.Err() != nil {
		result.Output = appendBounded(result.Output, "\ncommand interrupted: "+runCtx.Err().Error(), maxOutput)
		return result, fmt.Errorf("test command did not complete: %w", runCtx.Err())
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		result.ExitCode = exitErr.ExitCode()
	} else {
		result.ExitCode = -1
	}
	// A non-zero test exit is a result, not an infrastructure error. The caller
	// must persist and display it as a failed test run.
	return result, nil
}

type boundedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	original := len(p)
	remaining := b.limit - b.Len()
	if remaining > 0 {
		if len(p) > remaining {
			_, _ = b.Buffer.Write(p[:remaining])
			b.truncated = true
		} else {
			_, _ = b.Buffer.Write(p)
		}
	} else if len(p) > 0 {
		b.truncated = true
	}
	return original, nil
}
func appendBounded(current, addition string, limit int) string {
	if len(current) >= limit {
		return current
	}
	remaining := limit - len(current)
	if len(addition) > remaining {
		addition = addition[:remaining]
	}
	return strings.TrimSpace(current + addition)
}
