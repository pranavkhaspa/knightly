package github

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// WriteRequest is a bounded, approved GitHub write. Force and Merge are never
// honored by any writer: they are rejected before any request is made.
type WriteRequest struct {
	Repository string
	Title      string
	Body       string
	Branch     string
	Base       string
	Path       string
	Content    string
	Message    string
	SHA        string
	Force      bool
	Merge      bool
}

// WriteResult is the bounded outcome of an approved write.
type WriteResult struct {
	Reference string `json:"reference"`
	URL       string `json:"url,omitempty"`
	SHA       string `json:"sha,omitempty"`
	Summary   string `json:"summary,omitempty"`
}

// Writer performs approved GitHub writes. Implementations must not force-push
// or merge.
type Writer interface {
	CreateIssue(context.Context, identity.Scope, WriteRequest) (WriteResult, error)
	CreateBranch(context.Context, identity.Scope, WriteRequest) (WriteResult, error)
	CreateCommit(context.Context, identity.Scope, WriteRequest) (WriteResult, error)
	PushBranch(context.Context, identity.Scope, WriteRequest) (WriteResult, error)
	OpenPullRequest(context.Context, identity.Scope, WriteRequest) (WriteResult, error)
}

// HTTPWriter performs approved writes against the GitHub REST API. It issues no
// force push (the `force` field is always false) and never merges.
type HTTPWriter struct {
	baseURL   *url.URL
	token     string
	allowlist *Allowlist
	client    *http.Client
	maxBody   int64
}

var _ Writer = (*HTTPWriter)(nil)

func NewHTTPWriter(cfg ClientConfig) (*HTTPWriter, error) {
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, fmt.Errorf("%w: a write token is required", ErrInvalidObject)
	}
	base, err := validateEndpoint(cfg.BaseURL, defaultGitHubAPIBase)
	if err != nil {
		return nil, err
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	maxBody := cfg.MaxBodySize
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	return &HTTPWriter{baseURL: base, token: strings.TrimSpace(cfg.Token), allowlist: cfg.Allowlist, client: client, maxBody: maxBody}, nil
}

func (w *HTTPWriter) CreateIssue(ctx context.Context, scope identity.Scope, request WriteRequest) (WriteResult, error) {
	owner, name, err := w.authorize(scope, request)
	if err != nil {
		return WriteResult{}, err
	}
	payload := map[string]interface{}{"title": boundRun(request.Title, MaxTitle)}
	if body := strings.TrimSpace(request.Body); body != "" {
		payload["body"] = bound(body, MaxBody)
	}
	body, err := w.send(ctx, http.MethodPost, "/repos/"+owner+"/"+name+"/issues", payload)
	if err != nil {
		return WriteResult{}, err
	}
	var raw struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	_ = json.Unmarshal(body, &raw)
	return WriteResult{Reference: fmt.Sprintf("%s#%d", request.Repository, raw.Number), URL: bound(raw.HTMLURL, MaxURLLength), Summary: "issue created"}, nil
}

func (w *HTTPWriter) CreateBranch(ctx context.Context, scope identity.Scope, request WriteRequest) (WriteResult, error) {
	owner, name, err := w.authorize(scope, request)
	if err != nil {
		return WriteResult{}, err
	}
	branch := bound(request.Branch, MaxRefLength)
	source := bound(request.SHA, MaxSHALength)
	if branch == "" || source == "" {
		return WriteResult{}, fmt.Errorf("%w: branch and source SHA are required", ErrInvalidObject)
	}
	payload := map[string]interface{}{"ref": "refs/heads/" + branch, "sha": source}
	body, err := w.send(ctx, http.MethodPost, "/repos/"+owner+"/"+name+"/git/refs", payload)
	if err != nil {
		return WriteResult{}, err
	}
	var raw struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	_ = json.Unmarshal(body, &raw)
	return WriteResult{Reference: raw.Ref, SHA: bound(raw.Object.SHA, MaxSHALength), Summary: "branch created"}, nil
}

func (w *HTTPWriter) CreateCommit(ctx context.Context, scope identity.Scope, request WriteRequest) (WriteResult, error) {
	owner, name, err := w.authorize(scope, request)
	if err != nil {
		return WriteResult{}, err
	}
	path := strings.TrimPrefix(strings.TrimSpace(request.Path), "/")
	branch := bound(request.Branch, MaxRefLength)
	if path == "" || branch == "" || request.Content == "" {
		return WriteResult{}, fmt.Errorf("%w: path, branch, and content are required", ErrInvalidObject)
	}
	payload := map[string]interface{}{
		"message": boundRun(request.Message, MaxTitle),
		"content": encodeContent(request.Content),
		"branch":  branch,
	}
	body, err := w.send(ctx, http.MethodPut, "/repos/"+owner+"/"+name+"/contents/"+escapePath(path), payload)
	if err != nil {
		return WriteResult{}, err
	}
	var raw struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	_ = json.Unmarshal(body, &raw)
	return WriteResult{Reference: bound(raw.Commit.SHA, MaxSHALength), SHA: bound(raw.Commit.SHA, MaxSHALength), Summary: "commit created"}, nil
}

func (w *HTTPWriter) PushBranch(ctx context.Context, scope identity.Scope, request WriteRequest) (WriteResult, error) {
	owner, name, err := w.authorize(scope, request)
	if err != nil {
		return WriteResult{}, err
	}
	branch := bound(request.Branch, MaxRefLength)
	sha := bound(request.SHA, MaxSHALength)
	if branch == "" || sha == "" {
		return WriteResult{}, fmt.Errorf("%w: branch and commit SHA are required", ErrInvalidObject)
	}
	payload := map[string]interface{}{"sha": sha, "force": false}
	body, err := w.send(ctx, http.MethodPatch, "/repos/"+owner+"/"+name+"/git/refs/heads/"+url.PathEscape(branch), payload)
	if err != nil {
		return WriteResult{}, err
	}
	var raw struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	_ = json.Unmarshal(body, &raw)
	return WriteResult{Reference: raw.Ref, SHA: bound(raw.Object.SHA, MaxSHALength), Summary: "branch fast-forwarded"}, nil
}

func (w *HTTPWriter) OpenPullRequest(ctx context.Context, scope identity.Scope, request WriteRequest) (WriteResult, error) {
	owner, name, err := w.authorize(scope, request)
	if err != nil {
		return WriteResult{}, err
	}
	head := bound(request.Branch, MaxRefLength)
	base := bound(request.Base, MaxRefLength)
	if head == "" {
		return WriteResult{}, fmt.Errorf("%w: head branch is required", ErrInvalidObject)
	}
	if base == "" {
		base = "main"
	}
	payload := map[string]interface{}{
		"title": boundRun(request.Title, MaxTitle), "head": head, "base": base,
	}
	if body := strings.TrimSpace(request.Body); body != "" {
		payload["body"] = bound(body, MaxBody)
	}
	body, err := w.send(ctx, http.MethodPost, "/repos/"+owner+"/"+name+"/pulls", payload)
	if err != nil {
		return WriteResult{}, err
	}
	var raw struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	_ = json.Unmarshal(body, &raw)
	return WriteResult{Reference: fmt.Sprintf("%s#%d", request.Repository, raw.Number), URL: bound(raw.HTMLURL, MaxURLLength), Summary: "pull request opened"}, nil
}

func (w *HTTPWriter) authorize(scope identity.Scope, request WriteRequest) (string, string, error) {
	if err := scope.Validate(); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if request.Force {
		return "", "", fmt.Errorf("%w: force push is not permitted", ErrBlockedOperation)
	}
	if request.Merge {
		return "", "", fmt.Errorf("%w: auto-merge is not permitted", ErrBlockedOperation)
	}
	normalized := NormalizeRepository(request.Repository)
	if !w.allowlist.Allows(normalized) {
		return "", "", fmt.Errorf("%w: %s", ErrRepositoryNotAllowed, normalized)
	}
	parts := strings.SplitN(normalized, "/", 2)
	return url.PathEscape(parts[0]), url.PathEscape(parts[1]), nil
}

func (w *HTTPWriter) send(ctx context.Context, method, path string, payload interface{}) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode GitHub payload: %w", err)
	}
	endpoint := *w.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawQuery = ""
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("create GitHub write request: %w", err)
	}
	request.Header.Set("Accept", gitHubAcceptHeader)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", gitHubUserAgent)
	request.Header.Set("Authorization", "Bearer "+w.token)
	response, err := w.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: transport error", ErrProviderRequest)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, w.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read response", ErrProviderRequest)
	}
	if int64(len(body)) > w.maxBody {
		return nil, fmt.Errorf("%w: response is too large", ErrProviderRequest)
	}
	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return nil, ErrProviderUnauthorized
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return nil, fmt.Errorf("%w: provider returned HTTP %d", ErrProviderRequest, response.StatusCode)
	}
	return body, nil
}

func encodeContent(value string) string {
	return base64.StdEncoding.EncodeToString([]byte(value))
}

func escapePath(path string) string {
	parts := strings.Split(path, "/")
	for index, part := range parts {
		parts[index] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
