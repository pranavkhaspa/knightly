package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

func TestClientListsIssuesWithGetOnly(t *testing.T) {
	var mu sync.Mutex
	var methods, tokens []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		tokens = append(tokens, r.Header.Get("Authorization"))
		mu.Unlock()
		if r.URL.Path != "/repos/owner/repo/issues" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]interface{}{
			{"id": 1, "number": 1, "title": "Real issue", "body": "text", "state": "open", "html_url": "https://example/1", "user": map[string]string{"login": "octocat"}},
			{"id": 2, "number": 2, "title": "Looks like a PR", "state": "open", "pull_request": map[string]string{"url": "https://example/pr"}},
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL, Token: "token-1", Allowlist: testAllowlist(t, "owner/repo")})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	issues, err := client.ListIssues(context.Background(), testScope, ListQuery{Repository: "Owner/Repo"})
	if err != nil {
		t.Fatalf("ListIssues() error = %v", err)
	}
	if len(issues) != 1 || issues[0].Title != "Real issue" || issues[0].Origin != UntrustedOrigin {
		t.Fatalf("issues = %+v", issues)
	}
	for _, method := range methods {
		if method != http.MethodGet {
			t.Fatalf("client used %s, want GET only", method)
		}
	}
	if tokens[0] != "Bearer token-1" {
		t.Fatalf("authorization = %q", tokens[0])
	}
}

func TestClientRefusesUnlistedRepositoryWithoutRequest(t *testing.T) {
	var called bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.NotFound(w, r)
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{BaseURL: server.URL, Allowlist: testAllowlist(t, "owner/repo")})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if _, err := client.GetRepository(context.Background(), testScope, "owner/other"); !errors.Is(err, ErrRepositoryNotAllowed) {
		t.Fatalf("expected ErrRepositoryNotAllowed, got %v", err)
	}
	if called {
		t.Fatal("client contacted the provider for a non-allowlisted repository")
	}
}

func TestClientRequiresScope(t *testing.T) {
	client, err := NewClient(ClientConfig{BaseURL: "https://api.github.com", Allowlist: testAllowlist(t, "owner/repo")})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if _, err := client.ListReleases(context.Background(), identity.Scope{}, ListQuery{}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("expected ErrScopeRequired, got %v", err)
	}
}

func TestHTTPWriterRejectsForceAndMergeBeforeRequest(t *testing.T) {
	var called bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	writer, err := NewHTTPWriter(ClientConfig{Token: "token", BaseURL: server.URL, Allowlist: testAllowlist(t, "owner/repo")})
	if err != nil {
		t.Fatalf("NewHTTPWriter() error = %v", err)
	}
	if _, err := writer.PushBranch(context.Background(), testScope, WriteRequest{Repository: "owner/repo", Branch: "main", SHA: "abc", Force: true}); !errors.Is(err, ErrBlockedOperation) {
		t.Fatalf("expected ErrBlockedOperation for force push, got %v", err)
	}
	if _, err := writer.OpenPullRequest(context.Background(), testScope, WriteRequest{Repository: "owner/repo", Branch: "f", Merge: true}); !errors.Is(err, ErrBlockedOperation) {
		t.Fatalf("expected ErrBlockedOperation for merge, got %v", err)
	}
	if called {
		t.Fatal("writer contacted the provider for a blocked operation")
	}
}

func TestHTTPWriterCreatesIssueWithPost(t *testing.T) {
	var method string
	var payload map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		_ = json.NewDecoder(r.Body).Decode(&payload)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"number": 7, "html_url": "https://example/7"})
	}))
	defer server.Close()

	writer, err := NewHTTPWriter(ClientConfig{Token: "token", BaseURL: server.URL, Allowlist: testAllowlist(t, "owner/repo")})
	if err != nil {
		t.Fatalf("NewHTTPWriter() error = %v", err)
	}
	result, err := writer.CreateIssue(context.Background(), testScope, WriteRequest{Repository: "owner/repo", Title: "Fix", Body: "details"})
	if err != nil {
		t.Fatalf("CreateIssue() error = %v", err)
	}
	if method != http.MethodPost || payload["title"] != "Fix" {
		t.Fatalf("method = %s, payload = %+v", method, payload)
	}
	if result.Reference != "owner/repo#7" {
		t.Fatalf("reference = %q", result.Reference)
	}
}
