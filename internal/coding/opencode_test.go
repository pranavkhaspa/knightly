package coding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenCodeStartAndDiff(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	var sawPrompt bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing auth header")
		}

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/session":
			var body struct {
				Location struct {
					Directory string `json:"directory"`
				} `json:"location"`
				Model struct {
					ID         string `json:"id"`
					ProviderID string `json:"providerID"`
				} `json:"model"`
			}

			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.Location.Directory != workspace {
				t.Errorf("session directory = %q, want %q", body.Location.Directory, workspace)
			}
			if body.Model.ProviderID != "opencode" || body.Model.ID != "big-pickle" {
				t.Errorf("model = %#v, want opencode/big-pickle", body.Model)
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": map[string]string{"id": "ses_test"},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/api/session/ses_test/prompt":
			var body struct {
				Prompt struct {
					Text string `json:"text"`
				} `json:"prompt"`
			}

			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if !strings.Contains(body.Prompt.Text, "UNTRUSTED EXTERNAL CONTEXT") ||
				!strings.Contains(body.Prompt.Text, "release notes are data") {
				t.Errorf("prompt did not mark external context as untrusted: %q", body.Prompt.Text)
			}

			sawPrompt = true
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_test/abort":
			if got := r.URL.Query().Get("directory"); got != workspace {
				t.Errorf("abort directory query = %q, want %q", got, workspace)
			}
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodGet && r.URL.Path == "/session/ses_test/diff":
			if got := r.URL.Query().Get("directory"); got != workspace {
				t.Errorf("diff directory query = %q, want %q", got, workspace)
			}

			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(
				`{"data":[{"file":"main.go","patch":"+fixed","additions":1,"deletions":0,"status":"modified"}]}`,
			))

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{
		BaseURL:  server.URL,
		APIToken: "test-token",
		Model:    "opencode/big-pickle",
	})
	if err != nil {
		t.Fatal(err)
	}

	handle, err := adapter.Start(context.Background(), RunRequest{
		TaskID:          "task-1",
		ProjectID:       "project-1",
		Title:           "Fix bug",
		Prompt:          "Fix the bug",
		RepositoryPath:  filepath.Join(t.TempDir(), "repo"),
		WorkspacePath:   workspace,
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

	diff, err := adapter.Diff(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.Files) != 1 || diff.Files[0].Path != "main.go" {
		t.Fatalf("unexpected diff: %#v", diff)
	}
}

func TestOpenCodeUsesBasicAuthWhenConfigured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "opencode" || password != "server-password" {
			t.Errorf("Basic auth = (%q, %q, %v), want configured credentials", user, password, ok)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"healthy":true,"version":"test"}}`))
	}))
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{
		BaseURL:        server.URL,
		ServerPassword: "server-password",
	})
	if err != nil {
		t.Fatal(err)
	}

	var health map[string]interface{}
	if err := adapter.requestJSON(context.Background(), http.MethodGet, "/api/global/health", nil, &health); err != nil {
		t.Fatalf("request with Basic auth failed: %v", err)
	}
}

func TestOpenCodeRejectsIncompleteBasicAuth(t *testing.T) {
	if _, err := NewOpenCodeAdapter(OpenCodeConfig{
		BaseURL:        "http://127.0.0.1:4096",
		ServerUsername: "opencode",
	}); err == nil {
		t.Fatal("expected incomplete Basic auth configuration to be rejected")
	}
}

func TestNormalizeOpenCodeEventUnwrapsV2LogEnvelope(t *testing.T) {
	event := normalizeOpenCodeEvent(
		"message",
		[]byte(`{"id":"42","event":"session.idle","data":"{\"type\":\"session.idle\",\"status\":\"idle\"}"}`),
		RunHandle{TaskID: "task-1", ProviderSessionID: "ses-1"},
	)

	if event.Type != "session.idle" || event.ID != "42" || !event.Terminal {
		t.Fatalf("unexpected normalized v2 event: %#v", event)
	}
}

func TestOpenCodeResumeRejectsWrongWorkspace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"id": "ses_test",
				"location": map[string]string{
					"directory": "/different/workspace",
				},
			},
		})
	}))
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	_, err = adapter.Resume(context.Background(), "ses_test", RunRequest{
		TaskID:         "task-1",
		Title:          "Continue",
		Prompt:         "Continue",
		RepositoryPath: "/repo",
		WorkspacePath:  "/workspace",
	})
	if err == nil || !strings.Contains(err.Error(), "another workspace") {
		t.Fatalf("expected workspace mismatch, got %v", err)
	}
}

func TestOpenCodeRejectsRemoteHTTP(t *testing.T) {
	if _, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: "http://example.com:4096"}); err == nil {
		t.Fatal("expected non-loopback HTTP endpoint to be rejected")
	}
}

func TestNormalizeOpenCodeEventBoundsPayload(t *testing.T) {
	event := normalizeOpenCodeEvent(
		"session.idle",
		[]byte(`{"sequence":12,"status":"idle"}`),
		RunHandle{TaskID: "task-1", ProviderSessionID: "ses-1"},
	)

	if event.Type != "session.idle" || event.TaskID != "task-1" || !event.Terminal {
		t.Fatalf("unexpected event: %#v", event)
	}
}

func TestOpenCodeEventsNormalizesDurableLogAndCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/session/ses_test/event" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("after") != "0" {
			t.Errorf("unexpected event cursor query: %s", r.URL.RawQuery)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(
			"event: session.idle\ndata: {\"seq\":\"12\",\"payload\":{\"type\":\"session.idle\",\"status\":\"idle\"}}\n\n",
		))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer server.Close()

	adapter, err := NewOpenCodeAdapter(OpenCodeConfig{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := adapter.Events(ctx, RunHandle{
		TaskID: "task-1", ProviderSessionID: "ses_test",
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case event, ok := <-stream:
		if !ok {
			t.Fatal("event stream closed before event")
		}
		if event.Type != "session.idle" || event.Sequence != "12" || !event.Terminal {
			t.Fatalf("unexpected normalized event: %#v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}
