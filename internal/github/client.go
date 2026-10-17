package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	defaultGitHubAPIBase = "https://api.github.com"
	gitHubUserAgent      = "NightShift/0.1 (+https://github.com/pranavkhaspa/knightly)"
	gitHubAcceptHeader   = "application/vnd.github+json"
)

// ClientConfig configures the read-only GitHub REST client.
type ClientConfig struct {
	BaseURL     string
	Token       string
	Allowlist   *Allowlist
	Client      *http.Client
	MaxBodySize int64
}

// Client reads GitHub over HTTPS using GET requests only. Every repository
// scoped call is checked against the allowlist before any request is made.
type Client struct {
	baseURL   *url.URL
	token     string
	allowlist *Allowlist
	client    *http.Client
	maxBody   int64
}

var _ Reader = (*Client)(nil)

func NewClient(cfg ClientConfig) (*Client, error) {
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
	return &Client{
		baseURL: base, token: strings.TrimSpace(cfg.Token), allowlist: cfg.Allowlist,
		client: client, maxBody: maxBody,
	}, nil
}

func (c *Client) GetRepository(ctx context.Context, scope identity.Scope, fullName string) (Repository, error) {
	if err := c.checkScope(scope); err != nil {
		return Repository{}, err
	}
	owner, name, err := c.repoParts(fullName)
	if err != nil {
		return Repository{}, err
	}
	body, err := c.get(ctx, c.endpoint("/repos/"+owner+"/"+name), nil)
	if err != nil {
		return Repository{}, err
	}
	var raw apiRepository
	if err := json.Unmarshal(body, &raw); err != nil {
		return Repository{}, fmt.Errorf("%w: decode repository", ErrProviderRequest)
	}
	return NormalizeRepositoryMetadata(Repository{
		ID: raw.ID, FullName: raw.FullName, Description: raw.Description,
		DefaultBranch: raw.DefaultBranch, HTMLURL: raw.HTMLURL, Private: raw.Private,
		Archived: raw.Archived, OpenIssues: raw.OpenIssues, Stars: raw.Stargazers,
		PushedAt: parseTime(raw.PushedAt), UpdatedAt: parseTime(raw.UpdatedAt),
	}), nil
}

func (c *Client) ListIssues(ctx context.Context, scope identity.Scope, query ListQuery) ([]Issue, error) {
	if err := c.checkScope(scope); err != nil {
		return nil, err
	}
	query, owner, name, err := c.prepareRepoQuery(query)
	if err != nil {
		return nil, err
	}
	body, err := c.get(ctx, c.endpoint("/repos/"+owner+"/"+name+"/issues"), issueParams(query))
	if err != nil {
		return nil, err
	}
	var raws []apiIssue
	if err := json.Unmarshal(body, &raws); err != nil {
		return nil, fmt.Errorf("%w: decode issues", ErrProviderRequest)
	}
	issues := make([]Issue, 0, len(raws))
	for _, raw := range raws {
		if raw.PullRequest != nil {
			continue
		}
		issues = append(issues, NormalizeIssue(Issue{
			ID: raw.ID, Number: raw.Number, Repository: query.Repository, Title: raw.Title,
			Body: raw.Body, State: raw.State, HTMLURL: raw.HTMLURL, Author: Actor{Login: raw.User.Login},
			Labels: toLabels(raw.Labels), Comments: raw.Comments,
			CreatedAt: parseTime(raw.CreatedAt), UpdatedAt: parseTime(raw.UpdatedAt),
		}))
	}
	if len(issues) > query.Limit {
		issues = issues[:query.Limit]
	}
	return issues, nil
}

func (c *Client) ListPullRequests(ctx context.Context, scope identity.Scope, query ListQuery) ([]PullRequest, error) {
	if err := c.checkScope(scope); err != nil {
		return nil, err
	}
	query, owner, name, err := c.prepareRepoQuery(query)
	if err != nil {
		return nil, err
	}
	body, err := c.get(ctx, c.endpoint("/repos/"+owner+"/"+name+"/pulls"), issueParams(query))
	if err != nil {
		return nil, err
	}
	var raws []apiPullRequest
	if err := json.Unmarshal(body, &raws); err != nil {
		return nil, fmt.Errorf("%w: decode pull requests", ErrProviderRequest)
	}
	values := make([]PullRequest, 0, len(raws))
	for _, raw := range raws {
		values = append(values, NormalizePullRequest(PullRequest{
			ID: raw.ID, Number: raw.Number, Repository: query.Repository, Title: raw.Title,
			Body: raw.Body, State: raw.State, HTMLURL: raw.HTMLURL, Author: Actor{Login: raw.User.Login},
			Head: raw.Head.Ref, Base: raw.Base.Ref, Draft: raw.Draft,
			CreatedAt: parseTime(raw.CreatedAt), UpdatedAt: parseTime(raw.UpdatedAt),
		}))
	}
	if len(values) > query.Limit {
		values = values[:query.Limit]
	}
	return values, nil
}

func (c *Client) ListChecks(ctx context.Context, scope identity.Scope, query ListQuery) ([]CheckRun, error) {
	if err := c.checkScope(scope); err != nil {
		return nil, err
	}
	query, owner, name, err := c.prepareRepoQuery(query)
	if err != nil {
		return nil, err
	}
	query.HeadSHA = bound(query.HeadSHA, MaxSHALength)
	params := url.Values{}
	params.Set("per_page", strconv.Itoa(query.Limit))
	if query.HeadSHA != "" {
		params.Set("filter", "latest")
	} else {
		return nil, fmt.Errorf("%w: head SHA is required for checks", ErrInvalidQuery)
	}
	body, err := c.get(ctx, c.endpoint("/repos/"+owner+"/"+name+"/commits/"+url.PathEscape(query.HeadSHA)+"/check-runs"), params)
	if err != nil {
		return nil, err
	}
	var raw struct {
		CheckRuns []apiCheckRun `json:"check_runs"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("%w: decode check runs", ErrProviderRequest)
	}
	values := make([]CheckRun, 0, len(raw.CheckRuns))
	for _, run := range raw.CheckRuns {
		values = append(values, NormalizeCheckRun(CheckRun{
			ID: run.ID, Repository: query.Repository, Name: run.Name, Status: run.Status,
			Conclusion: run.Conclusion, HeadSHA: run.HeadSHA, HTMLURL: run.HTMLURL,
			StartedAt: parseTime(run.StartedAt), CompletedAt: parseTime(run.CompletedAt),
		}))
	}
	if len(values) > query.Limit {
		values = values[:query.Limit]
	}
	return values, nil
}

func (c *Client) ListReleases(ctx context.Context, scope identity.Scope, query ListQuery) ([]Release, error) {
	if err := c.checkScope(scope); err != nil {
		return nil, err
	}
	query, owner, name, err := c.prepareRepoQuery(query)
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Set("per_page", strconv.Itoa(query.Limit))
	params.Set("page", strconv.Itoa(query.Page+1))
	body, err := c.get(ctx, c.endpoint("/repos/"+owner+"/"+name+"/releases"), params)
	if err != nil {
		return nil, err
	}
	var raws []apiRelease
	if err := json.Unmarshal(body, &raws); err != nil {
		return nil, fmt.Errorf("%w: decode releases", ErrProviderRequest)
	}
	values := make([]Release, 0, len(raws))
	for _, raw := range raws {
		values = append(values, NormalizeRelease(Release{
			ID: raw.ID, Repository: query.Repository, TagName: raw.TagName, Name: raw.Name,
			HTMLURL: raw.HTMLURL, Draft: raw.Draft, Prerelease: raw.Prerelease,
			PublishedAt: parseTime(raw.PublishedAt),
		}))
	}
	if len(values) > query.Limit {
		values = values[:query.Limit]
	}
	return values, nil
}

func (c *Client) checkScope(scope identity.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	return nil
}

func (c *Client) prepareRepoQuery(query ListQuery) (ListQuery, string, string, error) {
	query, err := NormalizeListQuery(query)
	if err != nil {
		return ListQuery{}, "", "", err
	}
	owner, name, err := c.repoParts(query.Repository)
	if err != nil {
		return ListQuery{}, "", "", err
	}
	return query, owner, name, nil
}

func (c *Client) repoParts(fullName string) (string, string, error) {
	normalized := NormalizeRepository(fullName)
	if !c.allowlist.Allows(normalized) {
		return "", "", fmt.Errorf("%w: %s", ErrRepositoryNotAllowed, normalized)
	}
	parts := strings.SplitN(normalized, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("%w: repository must be owner/name", ErrInvalidQuery)
	}
	return url.PathEscape(parts[0]), url.PathEscape(parts[1]), nil
}

func (c *Client) endpoint(path string) *url.URL {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawQuery = ""
	endpoint.Fragment = ""
	return &endpoint
}

func (c *Client) get(ctx context.Context, endpoint *url.URL, params url.Values) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if params != nil {
		endpoint.RawQuery = params.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create GitHub request: %w", err)
	}
	request.Header.Set("Accept", gitHubAcceptHeader)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", gitHubUserAgent)
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: transport error", ErrProviderRequest)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read response", ErrProviderRequest)
	}
	if int64(len(body)) > c.maxBody {
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

func issueParams(query ListQuery) url.Values {
	params := url.Values{}
	params.Set("per_page", strconv.Itoa(query.Limit))
	params.Set("page", strconv.Itoa(query.Page+1))
	if query.State != "" {
		params.Set("state", query.State)
	}
	return params
}

func toLabels(labels []apiLabel) []Label {
	if len(labels) == 0 {
		return nil
	}
	result := make([]Label, 0, len(labels))
	for _, label := range labels {
		result = append(result, Label{Name: label.Name})
	}
	return result
}

func validateEndpoint(raw, fallback string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		raw = fallback
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil {
		return nil, fmt.Errorf("%w: endpoint must be an absolute http(s) URL", ErrInvalidQuery)
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) {
		return nil, fmt.Errorf("%w: endpoint must use HTTPS for non-loopback hosts", ErrInvalidQuery)
	}
	return parsed, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type apiRepository struct {
	ID            int64  `json:"id"`
	FullName      string `json:"full_name"`
	Description   string `json:"description"`
	DefaultBranch string `json:"default_branch"`
	HTMLURL       string `json:"html_url"`
	Private       bool   `json:"private"`
	Archived      bool   `json:"archived"`
	OpenIssues    int    `json:"open_issues_count"`
	Stargazers    int    `json:"stargazers_count"`
	PushedAt      string `json:"pushed_at"`
	UpdatedAt     string `json:"updated_at"`
}

type apiLabel struct {
	Name string `json:"name"`
}

type apiUser struct {
	Login string `json:"login"`
}

type apiIssue struct {
	ID          int64      `json:"id"`
	Number      int        `json:"number"`
	Title       string     `json:"title"`
	Body        string     `json:"body"`
	State       string     `json:"state"`
	HTMLURL     string     `json:"html_url"`
	User        apiUser    `json:"user"`
	Labels      []apiLabel `json:"labels"`
	Comments    int        `json:"comments"`
	CreatedAt   string     `json:"created_at"`
	UpdatedAt   string     `json:"updated_at"`
	PullRequest *struct{}  `json:"pull_request"`
}

type apiPullRequest struct {
	ID      int64   `json:"id"`
	Number  int     `json:"number"`
	Title   string  `json:"title"`
	Body    string  `json:"body"`
	State   string  `json:"state"`
	HTMLURL string  `json:"html_url"`
	User    apiUser `json:"user"`
	Head    struct {
		Ref string `json:"ref"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
	Draft     bool   `json:"draft"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type apiCheckRun struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Conclusion  string `json:"conclusion"`
	HeadSHA     string `json:"head_sha"`
	HTMLURL     string `json:"html_url"`
	StartedAt   string `json:"started_at"`
	CompletedAt string `json:"completed_at"`
}

type apiRelease struct {
	ID          int64  `json:"id"`
	TagName     string `json:"tag_name"`
	Name        string `json:"name"`
	HTMLURL     string `json:"html_url"`
	Draft       bool   `json:"draft"`
	Prerelease  bool   `json:"prerelease"`
	PublishedAt string `json:"published_at"`
}
