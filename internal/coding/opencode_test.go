package coding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	userMessageJSON      = `{"info":{"id":"msg_1","role":"user","time":{"created":1}},"parts":[{"type":"text","text":"Fix the bug"}]}`
	assistantDoneJSON    = `{"info":{"id":"msg_2","role":"assistant","time":{"created":2,"completed":3}},"parts":[{"type":"text","text":"Done: fixed main.go"}]}`
	assistantAuthErrJSON = `{"info":{"id":"msg_2","role":"assistant","time":{"created":2},"error":{"name":"ProviderAuthError","data":{"message":"no credentials for opencode"}}},"parts":[]}`
)

func TestOpenCodeStartAndDiffFallback(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	var sawPrompt bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing auth header")
		}
		if got := r.URL.Query().Get("directory"); got != workspace {
			t.Errorf("%s %s directory query = %q, want %q", r.Method, r.URL.Path, got, workspace)
		}

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_test", "directory": workspace})

		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_test/prompt_async":
			var body struct {
				Model struct {
					ProviderID string `json:"providerID"`
					ModelID    string `json:"modelID"`
				} `json:"model"`
				Parts []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.Model.ProviderID != "opencode" || body.Model.ModelID != "big-pickle" {
				t.Errorf("model = %#v, want opencode/big-pickle", body.Model)
			}
			if len(body.Parts) != 1 || body.Parts[0].Type != "text" ||
				!strings.Contains(body.Parts[0].Text, "UNTRUSTED EXTERNAL CONTEXT") ||
				!strings.Contains(body.Parts[0].Text, "release notes are data") {
				t.Errorf("prompt parts did not mark external context as untrusted: %#v", body.Parts)
			}
			sawPrompt = true
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_test/abort":
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodGet && r.URL.Path == "/session/ses_test/diff":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"file":"main.go","additions":1,"deletions":0}]`))

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL, APIToken: "test-token", Model: "opencode/big-pickle"})
	if err != nil {
		t.Fatal(err)
	}

	handle, err := adapter.Start(context.Background(), RunRequest{
		TaskID: "task-1", ProjectID: "project-1", Title: "Fix bug", Prompt: "Fix the bug",
		RepositoryPath: filepath.Join(t.TempDir(), "repo"), WorkspacePath: workspace,
		ExternalContext: "release notes are data",
	})
	if err != nil {
		t.Fatal(err)
	}
	if handle.ProviderSessionID != "ses_test" || handle.ProjectID != "project-1" {
		t.Fatalf("unexpected handle: %#v", handle)
	}
	if !sawPrompt {
		t.Fatal("expected prompt to be submitted")
	}
	if err := adapter.Cancel(context.Background(), handle); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	// The workspace is not a git repository, so Diff falls back to OpenCode.
	diff, err := adapter.Diff(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.Files) != 1 || diff.Files[0].Path != "main.go" {
		t.Fatalf("unexpected diff: %#v", diff)
	}
}

func TestOpenCodeDiffUsesGitAsSourceOfTruth(t *testing.T) {
	repo, _ := initTestRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "new_file.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(repo, "main.go"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("// edited\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// No server is reachable: if git were not used, this would fail.
	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	diff, err := adapter.Diff(context.Background(), RunHandle{TaskID: "task-1", ProviderSessionID: "ses_test", WorkspacePath: repo})
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]string{}
	for _, file := range diff.Files {
		status[file.Path] = file.Status
		if !strings.Contains(file.Patch, "diff --git") {
			t.Errorf("file %s has no patch: %#v", file.Path, file)
		}
	}
	if status["new_file.txt"] != "added" || status["main.go"] != "modified" || len(diff.Files) != 2 {
		t.Fatalf("unexpected git diff: %#v", diff.Files)
	}
}

func TestOpenCodeUsesBasicAuthWhenConfigured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "opencode" || password != "server-password" {
			t.Errorf("Basic auth = (%q, %q, %v), want configured credentials", user, password, ok)
		}
		if r.URL.Path != "/global/health" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"healthy":true,"version":"test"}`))
	}))
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL, ServerPassword: "server-password"})
	if err != nil {
		t.Fatal(err)
	}
	version, err := adapter.Health(context.Background())
	if err != nil || version != "test" {
		t.Fatalf("Health() = %q, %v", version, err)
	}
}

func TestOpenCodeRejectsIncompleteBasicAuth(t *testing.T) {
	if _, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: "http://127.0.0.1:4096", ServerUsername: "opencode"}); err == nil {
		t.Fatal("expected incomplete Basic auth configuration to be rejected")
	}
}

func TestOpenCodeRejectsRemoteHTTP(t *testing.T) {
	if _, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: "http://example.com:4096"}); err == nil {
		t.Fatal("expected non-loopback HTTP endpoint to be rejected")
	}
}

func TestOpenCodeResumeRejectsWrongWorkspace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_test", "directory": "/different/workspace"})
	}))
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Resume(context.Background(), "ses_test", RunRequest{
		TaskID: "task-1", Title: "Continue", Prompt: "Continue", RepositoryPath: "/repo", WorkspacePath: "/workspace",
	})
	if err == nil || !strings.Contains(err.Error(), "another workspace") {
		t.Fatalf("expected workspace mismatch, got %v", err)
	}
}

// fakeSessionServer serves a fixed message list and an empty (idle) status map.
func fakeSessionServer(messages string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/session/ses_test/message":
			_, _ = w.Write([]byte(messages))
		case "/session/status":
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func collectUntilTerminal(t *testing.T, stream <-chan AgentEvent) []AgentEvent {
	t.Helper()
	var events []AgentEvent
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-stream:
			if !ok {
				t.Fatalf("event stream closed before a terminal event: %#v", events)
			}
			events = append(events, event)
			if event.Terminal {
				return events
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a terminal event: %#v", events)
		}
	}
}

func TestOpenCodeEventsCompleteWhenAssistantFinishes(t *testing.T) {
	server := fakeSessionServer("[" + userMessageJSON + "," + assistantDoneJSON + "]")
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := adapter.Events(ctx, RunHandle{TaskID: "task-1", ProviderSessionID: "ses_test", WorkspacePath: "/workspace"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectUntilTerminal(t, stream)
	last := events[len(events)-1]
	if last.Type != "session.idle" || last.Error != "" || last.Sequence == "" {
		t.Fatalf("unexpected terminal event: %#v", last)
	}

	summary, err := adapter.Summary(context.Background(), RunHandle{TaskID: "task-1", ProviderSessionID: "ses_test", WorkspacePath: "/workspace"})
	if err != nil || summary != "Done: fixed main.go" {
		t.Fatalf("Summary() = %q, %v", summary, err)
	}
}

func TestOpenCodeEventsSurfaceAssistantError(t *testing.T) {
	server := fakeSessionServer("[" + userMessageJSON + "," + assistantAuthErrJSON + "]")
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := adapter.Events(ctx, RunHandle{TaskID: "task-1", ProviderSessionID: "ses_test", WorkspacePath: "/workspace"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectUntilTerminal(t, stream)
	last := events[len(events)-1]
	if !strings.Contains(last.Error, "ProviderAuthError") || !strings.Contains(last.Error, "opencode auth login") {
		t.Fatalf("expected a provider auth error, got %#v", last)
	}
}

func TestOpenCodeEventsFailWhenNoAssistantResponse(t *testing.T) {
	server := fakeSessionServer("[" + userMessageJSON + "]")
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL, PollInterval: 10 * time.Millisecond, NoResponseTimeout: 100 * time.Millisecond, Model: "opencode/big-pickle"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := adapter.Events(ctx, RunHandle{TaskID: "task-1", ProviderSessionID: "ses_test", WorkspacePath: "/workspace"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectUntilTerminal(t, stream)
	last := events[len(events)-1]
	if last.Type != "session.no_response" || !strings.Contains(last.Error, "no assistant response") {
		t.Fatalf("expected a no-response failure, got %#v", last)
	}
}

func TestParseSessionError(t *testing.T) {
	bare := `{"type":"session.error","properties":{"sessionID":"ses_test","error":{"name":"ProviderAuthError","data":{"message":"no credentials"}}}}`
	if got := parseSessionError(bare, "ses_test"); !strings.Contains(got, "ProviderAuthError") || !strings.Contains(got, "no credentials") {
		t.Fatalf("bare event not parsed: %q", got)
	}
	wrapped := `{"directory":"/w","payload":` + bare + `}`
	if got := parseSessionError(wrapped, "ses_test"); !strings.Contains(got, "ProviderAuthError") {
		t.Fatalf("wrapped (global) event not parsed: %q", got)
	}
	if got := parseSessionError(bare, "ses_other"); got != "" {
		t.Fatalf("event for another session must be ignored, got %q", got)
	}
	aborted := `{"type":"session.error","properties":{"sessionID":"ses_test","error":{"name":"MessageAbortedError","data":{}}}}`
	if got := parseSessionError(aborted, "ses_test"); got != "" {
		t.Fatalf("user abort must not be a failure, got %q", got)
	}
	if got := parseSessionError(`{"type":"server.connected","properties":{}}`, "ses_test"); got != "" {
		t.Fatalf("unrelated event must be ignored, got %q", got)
	}
	if got := parseSessionError(`not json`, "ses_test"); got != "" {
		t.Fatalf("garbage must be ignored, got %q", got)
	}
}

// OpenCode publishes failures that occur before an assistant message exists
// (bad model, missing credentials) only on /event. Without the watcher the run
// would sit in "no assistant response" until the timeout, hiding the real cause.
func TestOpenCodeEventsSurfaceStreamSessionError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {\"type\":\"server.connected\",\"properties\":{}}\n\n"))
			_, _ = w.Write([]byte("data: {\"type\":\"session.error\",\"properties\":{\"sessionID\":\"ses_test\",\"error\":{\"name\":\"ProviderModelNotFoundError\",\"data\":{\"message\":\"model big-pickle not found\"}}}}\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
		case "/session/ses_test/message":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[" + userMessageJSON + "]"))
		case "/session/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL, PollInterval: 10 * time.Millisecond, Model: "opencode/big-pickle"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := adapter.Events(ctx, RunHandle{TaskID: "task-1", ProviderSessionID: "ses_test", WorkspacePath: "/workspace"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectUntilTerminal(t, stream)
	last := events[len(events)-1]
	if last.Type != "session.error" || !strings.Contains(last.Error, "ProviderModelNotFoundError") || !strings.Contains(last.Error, "big-pickle") {
		t.Fatalf("expected the /event session error to be surfaced, got %#v", last)
	}
}
