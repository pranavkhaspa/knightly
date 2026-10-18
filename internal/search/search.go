// Package search contains provider-neutral web search types and adapters.
package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	DefaultMaxQueryLength  = 512
	DefaultMaxPage         = 50
	DefaultMaxResults      = 10
	DefaultMaxResponseSize = int64(2 << 20)
	DefaultRequestTimeout  = 10 * time.Second
	HardMaxQueryLength     = 4096
	HardMaxPage            = 100
	HardMaxResults         = 100
	HardMaxResponseSize    = int64(8 << 20)
	MaxResultTitle         = 512
	MaxResultURL           = 2048
	MaxResultSnippet       = 4096
	MaxResultSource        = 128
	MaxResultCategory      = 64
)

var (
	ErrInvalidConfig    = errors.New("invalid SearXNG configuration")
	ErrInvalidRequest   = errors.New("invalid search request")
	ErrInvalidResponse  = errors.New("invalid SearXNG response")
	ErrInvalidURL       = errors.New("invalid search URL")
	ErrHTTPStatus       = errors.New("SearXNG returned non-2xx status")
	ErrResponseTooLarge = errors.New("SearXNG response exceeds configured limit")
)

type Provider interface {
	Search(context.Context, SearchRequest) (SearchResponse, error)
}

type SearchRequest struct {
	Query string
	Page  int
	Limit int
}

type SearchResult struct {
	Title    string  `json:"title"`
	URL      string  `json:"url"`
	Snippet  string  `json:"snippet,omitempty"`
	Source   string  `json:"source,omitempty"`
	Category string  `json:"category,omitempty"`
	Score    float64 `json:"score,omitempty"`
}

type SearchResponse struct {
	Query   string         `json:"query"`
	Total   int            `json:"total"`
	Results []SearchResult `json:"results"`
}

type Limits struct {
	MaxQueryLength  int
	MaxPage         int
	MaxResults      int
	MaxResponseSize int64
}

type SearXNGConfig struct {
	BaseURL string
	Client  *http.Client
	Timeout time.Duration
	Limits  Limits
}

type SearXNGProvider struct {
	baseURL *url.URL
	client  *http.Client
	timeout time.Duration
	limits  Limits
}

var _ Provider = (*SearXNGProvider)(nil)

func NewSearXNGProvider(config SearXNGConfig) (*SearXNGProvider, error) {
	baseURL, err := validateBaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	limits, err := normalizeLimits(config.Limits)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if config.Timeout < 0 {
		return nil, fmt.Errorf("%w: request timeout must not be negative", ErrInvalidConfig)
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultRequestTimeout
	}
	if config.Client == nil {
		config.Client = http.DefaultClient
	}
	return &SearXNGProvider{baseURL: baseURL, client: config.Client, timeout: config.Timeout, limits: limits}, nil
}

func (p *SearXNGProvider) Search(ctx context.Context, request SearchRequest) (SearchResponse, error) {
	if p == nil || p.baseURL == nil || p.client == nil {
		return SearchResponse{}, fmt.Errorf("%w: provider is not initialized", ErrInvalidConfig)
	}
	if ctx == nil {
		return SearchResponse{}, fmt.Errorf("%w: context is required", ErrInvalidRequest)
	}
	request, err := p.normalizeRequest(request)
	if err != nil {
		return SearchResponse{}, err
	}
	endpoint := *p.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/search"
	endpoint.RawPath = ""
	query := endpoint.Query()
	query.Set("q", request.Query)
	query.Set("format", "json")
	query.Set("pageno", strconv.Itoa(request.Page))
	endpoint.RawQuery = query.Encode()

	requestContext, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestContext, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return SearchResponse{}, fmt.Errorf("create SearXNG request: %w", err)
	}
	response, err := p.client.Do(httpRequest)
	if err != nil {
		return SearchResponse{}, fmt.Errorf("SearXNG request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return SearchResponse{}, fmt.Errorf("%w: HTTP %d", ErrHTTPStatus, response.StatusCode)
	}
	if err := validateJSONContentType(response.Header.Get("Content-Type")); err != nil {
		return SearchResponse{}, err
	}
	if response.ContentLength > p.limits.MaxResponseSize {
		return SearchResponse{}, fmt.Errorf("%w: limit is %d bytes", ErrResponseTooLarge, p.limits.MaxResponseSize)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, p.limits.MaxResponseSize+1))
	if err != nil {
		return SearchResponse{}, fmt.Errorf("read SearXNG response: %w", err)
	}
	if int64(len(body)) > p.limits.MaxResponseSize {
		return SearchResponse{}, fmt.Errorf("%w: limit is %d bytes", ErrResponseTooLarge, p.limits.MaxResponseSize)
	}
	return decodeResponse(body, request)
}

func (p *SearXNGProvider) normalizeRequest(request SearchRequest) (SearchRequest, error) {
	request.Query = strings.TrimSpace(request.Query)
	if request.Query == "" {
		return SearchRequest{}, fmt.Errorf("%w: query is required", ErrInvalidRequest)
	}
	if utf8.RuneCountInString(request.Query) > p.limits.MaxQueryLength {
		return SearchRequest{}, fmt.Errorf("%w: query must be at most %d characters", ErrInvalidRequest, p.limits.MaxQueryLength)
	}
	if request.Page == 0 {
		request.Page = 1
	}
	if request.Page < 1 || request.Page > p.limits.MaxPage {
		return SearchRequest{}, fmt.Errorf("%w: page must be between 1 and %d", ErrInvalidRequest, p.limits.MaxPage)
	}
	if request.Limit == 0 {
		request.Limit = min(DefaultMaxResults, p.limits.MaxResults)
	}
	if request.Limit < 1 || request.Limit > p.limits.MaxResults {
		return SearchRequest{}, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidRequest, p.limits.MaxResults)
	}
	return request, nil
}

type searxResponse struct {
	Query           string        `json:"query"`
	NumberOfResults int           `json:"number_of_results"`
	Results         []searxResult `json:"results"`
}

type searxResult struct {
	Title    string  `json:"title"`
	URL      string  `json:"url"`
	Content  string  `json:"content"`
	Engine   string  `json:"engine"`
	Category string  `json:"category"`
	Score    float64 `json:"score"`
}

func decodeResponse(body []byte, request SearchRequest) (SearchResponse, error) {
	var decoded searxResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return SearchResponse{}, fmt.Errorf("%w: decode JSON: %v", ErrInvalidResponse, err)
	}
	if decoded.Results == nil {
		return SearchResponse{}, fmt.Errorf("%w: results must be an array", ErrInvalidResponse)
	}
	results := make([]SearchResult, 0, min(len(decoded.Results), request.Limit))
	for index, result := range decoded.Results {
		resultURL, err := validateResultURL(result.URL)
		if err != nil {
			return SearchResponse{}, fmt.Errorf("%w: result %d: %v", ErrInvalidResponse, index, err)
		}
		if len(results) == request.Limit {
			break
		}
		results = append(results, SearchResult{
			Title: bounded(result.Title, MaxResultTitle), URL: resultURL,
			Snippet: bounded(result.Content, MaxResultSnippet), Source: bounded(result.Engine, MaxResultSource),
			Category: bounded(result.Category, MaxResultCategory), Score: result.Score,
		})
	}
	query := strings.TrimSpace(decoded.Query)
	if query == "" {
		query = request.Query
	}
	return SearchResponse{Query: bounded(query, DefaultMaxQueryLength), Total: max(decoded.NumberOfResults, 0), Results: results}, nil
}

func normalizeLimits(limits Limits) (Limits, error) {
	if limits.MaxQueryLength == 0 {
		limits.MaxQueryLength = DefaultMaxQueryLength
	}
	if limits.MaxPage == 0 {
		limits.MaxPage = DefaultMaxPage
	}
	if limits.MaxResults == 0 {
		limits.MaxResults = DefaultMaxResults
	}
	if limits.MaxResponseSize == 0 {
		limits.MaxResponseSize = DefaultMaxResponseSize
	}
	if limits.MaxQueryLength < 1 || limits.MaxQueryLength > HardMaxQueryLength {
		return Limits{}, fmt.Errorf("max query length must be between 1 and %d", HardMaxQueryLength)
	}
	if limits.MaxPage < 1 || limits.MaxPage > HardMaxPage {
		return Limits{}, fmt.Errorf("max page must be between 1 and %d", HardMaxPage)
	}
	if limits.MaxResults < 1 || limits.MaxResults > HardMaxResults {
		return Limits{}, fmt.Errorf("max results must be between 1 and %d", HardMaxResults)
	}
	if limits.MaxResponseSize < 1 || limits.MaxResponseSize > HardMaxResponseSize {
		return Limits{}, fmt.Errorf("max response size must be between 1 and %d bytes", HardMaxResponseSize)
	}
	return limits, nil
}

func validateBaseURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.ContainsAny(raw, "\r\n") {
		return nil, errors.New("base URL is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return nil, errors.New("base URL must be an http(s) origin without user info, query, or fragment")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return parsed, nil
}

func validateResultURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > MaxResultURL || strings.ContainsAny(raw, "\r\n") {
		return "", fmt.Errorf("%w: result URL is empty or too long", ErrInvalidURL)
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.Opaque != "" {
		return "", fmt.Errorf("%w: result URL must be an absolute http or https URL", ErrInvalidURL)
	}
	return parsed.String(), nil
}

func validateJSONContentType(value string) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil || (mediaType != "application/json" && !strings.HasSuffix(mediaType, "+json")) {
		return fmt.Errorf("%w: content type %q is not JSON", ErrInvalidResponse, value)
	}
	return nil
}

func bounded(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return string(runes)
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
func max(value, floor int) int {
	if value < floor {
		return floor
	}
	return value
}
