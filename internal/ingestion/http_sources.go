package ingestion

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
)

const (
	defaultHTTPBodyLimit = 2 << 20
	defaultUserAgent     = "NightShift/0.1 (+https://github.com/pranavkhaspa/knightly)"
	defaultHTTPRetries   = 2
	defaultRetryBackoff  = time.Second
	defaultRedirectLimit = 3
)

// RSSSource is a bounded RSS/Atom adapter. It returns structured records and
// never hands raw response bodies to the rest of the application.
type RSSSource struct {
	Source       SourceConfig
	Client       *http.Client
	BodyLimit    int64
	UserAgent    string
	MaxRetries   int
	RetryBackoff time.Duration
	MaxRedirects int
}

var _ SourceAdapter = RSSSource{}

func (s RSSSource) Config() SourceConfig { return s.Source }

func (s RSSSource) Fetch(ctx context.Context, maxItems int) ([]RawItem, error) {
	result, err := s.FetchWithCursor(ctx, SourceCursor{SourceID: s.Source.ID}, maxItems)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (s RSSSource) FetchWithCursor(ctx context.Context, cursor SourceCursor, maxItems int) (FetchResult, error) {
	maxItems = boundedFetchLimit(maxItems)
	if maxItems == 0 {
		return FetchResult{Cursor: cursor}, nil
	}
	body, nextCursor, notModified, err := s.fetch(ctx, cursor, []string{"application/rss+xml", "application/atom+xml", "application/xml", "text/xml"})
	if err != nil {
		return FetchResult{}, err
	}
	if notModified {
		return FetchResult{Cursor: nextCursor, NotModified: true}, nil
	}
	var feed xmlFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return FetchResult{}, fmt.Errorf("decode RSS/Atom feed: %w", err)
	}
	if maxItems < 0 {
		maxItems = 0
	}
	items := make([]RawItem, 0, maxItems)
	for _, item := range feed.Channel.Items {
		if len(items) == maxItems {
			break
		}
		items = append(items, RawItem{
			ExternalID:   firstNonEmpty(item.GUID, item.Link),
			URL:          firstLink(item.Link, item.GUID),
			Title:        item.Title,
			Summary:      item.Description,
			PublishedAt:  parseFeedTime(item.PubDate),
			Technologies: append([]string(nil), s.Source.TechnologyHints...),
			Interests:    append([]string(nil), s.Source.InterestHints...),
		})
	}
	for _, entry := range feed.Entries {
		if len(items) == maxItems {
			break
		}
		items = append(items, RawItem{
			ExternalID:   entry.ID,
			URL:          entry.Link.Href,
			Title:        entry.Title,
			Summary:      firstNonEmpty(entry.Summary, entry.Content),
			PublishedAt:  parseFeedTime(firstNonEmpty(entry.Published, entry.Updated)),
			Technologies: append([]string(nil), s.Source.TechnologyHints...),
			Interests:    append([]string(nil), s.Source.InterestHints...),
		})
	}
	return FetchResult{Items: items, Cursor: nextCursor}, nil
}

func (s RSSSource) fetch(ctx context.Context, cursor SourceCursor, contentTypes []string) ([]byte, SourceCursor, bool, error) {
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	client = boundedRedirectClient(client, s.MaxRedirects)
	userAgent := s.UserAgent
	if userAgent == "" {
		userAgent = defaultUserAgent
	}
	retries := s.MaxRetries
	if retries == 0 {
		retries = defaultHTTPRetries
	}
	if retries < 0 {
		retries = 0
	}
	if retries > 3 {
		retries = 3
	}
	backoff := s.RetryBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, cursor, false, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Source.URL, nil)
		if err != nil {
			return nil, cursor, false, err
		}
		request.Header.Set("User-Agent", userAgent)
		if cursor.ETag != "" {
			request.Header.Set("If-None-Match", cursor.ETag)
		}
		if cursor.LastModified != "" {
			request.Header.Set("If-Modified-Since", cursor.LastModified)
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			if errors.Is(requestErr, context.Canceled) || errors.Is(requestErr, context.DeadlineExceeded) {
				return nil, cursor, false, requestErr
			}
			lastErr = requestErr
			if attempt < retries {
				if err := waitRetry(ctx, retryDelay(backoff, attempt, "")); err != nil {
					return nil, cursor, false, err
				}
				continue
			}
			return nil, cursor, false, lastErr
		}
		nextCursor := cursor
		nextCursor.SourceID = s.Source.ID
		if value := response.Header.Get("ETag"); value != "" {
			nextCursor.ETag = value
		}
		if value := response.Header.Get("Last-Modified"); value != "" {
			nextCursor.LastModified = value
		}
		if response.StatusCode == http.StatusNotModified {
			response.Body.Close()
			return nil, nextCursor, true, nil
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			status := response.StatusCode
			retryAfter := response.Header.Get("Retry-After")
			response.Body.Close()
			lastErr = fmt.Errorf("source returned HTTP %d", status)
			if attempt < retries && retryableHTTPStatus(status) {
				if err := waitRetry(ctx, retryDelay(backoff, attempt, retryAfter)); err != nil {
					return nil, nextCursor, false, err
				}
				cursor = nextCursor
				continue
			}
			return nil, nextCursor, false, lastErr
		}
		mediaType, _, contentTypeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if contentTypeErr != nil || !allowedContentType(mediaType, contentTypes) {
			response.Body.Close()
			return nil, nextCursor, false, fmt.Errorf("unsupported source content type %q", response.Header.Get("Content-Type"))
		}
		limit := s.BodyLimit
		if limit <= 0 || limit > defaultHTTPBodyLimit {
			limit = defaultHTTPBodyLimit
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
		response.Body.Close()
		if readErr != nil {
			return nil, nextCursor, false, readErr
		}
		if int64(len(body)) > limit {
			return nil, nextCursor, false, fmt.Errorf("source response exceeds %d bytes", limit)
		}
		return body, nextCursor, false, nil
	}
	return nil, cursor, false, lastErr
}

func boundedRedirectClient(client *http.Client, configured int) *http.Client {
	clone := *client
	limit := configured
	if limit <= 0 || limit > 10 {
		limit = defaultRedirectLimit
	}
	previous := client.CheckRedirect
	clone.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= limit {
			return fmt.Errorf("source redirect limit exceeded (%d)", limit)
		}
		if previous != nil {
			return previous(request, via)
		}
		return nil
	}
	return &clone
}

func retryableHTTPStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func retryDelay(base time.Duration, attempt int, retryAfter string) time.Duration {
	if value := parseRetryAfter(retryAfter); value > 0 {
		return value
	}
	delay := base
	for index := 0; index < attempt; index++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := time.ParseDuration(value + "s"); err == nil && seconds > 0 {
		if seconds > 30*time.Second {
			return 30 * time.Second
		}
		return seconds
	}
	if when, err := http.ParseTime(value); err == nil {
		delay := time.Until(when)
		if delay > 0 {
			if delay > 30*time.Second {
				return 30 * time.Second
			}
			return delay
		}
	}
	return 0
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// GitHubReleaseSource reads the public releases API without authentication.
// The source URL should be an endpoint such as
// https://api.github.com/repos/FalkorDB/FalkorDB/releases.
type GitHubReleaseSource struct {
	Source       SourceConfig
	Client       *http.Client
	BodyLimit    int64
	UserAgent    string
	MaxRetries   int
	RetryBackoff time.Duration
	MaxRedirects int
}

var _ SourceAdapter = GitHubReleaseSource{}

func (s GitHubReleaseSource) Config() SourceConfig { return s.Source }

func (s GitHubReleaseSource) Fetch(ctx context.Context, maxItems int) ([]RawItem, error) {
	result, err := s.FetchWithCursor(ctx, SourceCursor{SourceID: s.Source.ID}, maxItems)
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (s GitHubReleaseSource) FetchWithCursor(ctx context.Context, cursor SourceCursor, maxItems int) (FetchResult, error) {
	maxItems = boundedFetchLimit(maxItems)
	if maxItems == 0 {
		return FetchResult{Cursor: cursor}, nil
	}
	rss := RSSSource{
		Source: s.Source, Client: s.Client, BodyLimit: s.BodyLimit, UserAgent: s.UserAgent,
		MaxRetries: s.MaxRetries, RetryBackoff: s.RetryBackoff, MaxRedirects: s.MaxRedirects,
	}
	body, nextCursor, notModified, err := rss.fetch(ctx, cursor, []string{"application/json", "application/vnd.github+json"})
	if err != nil {
		return FetchResult{}, err
	}
	if notModified {
		return FetchResult{Cursor: nextCursor, NotModified: true}, nil
	}
	var releases []githubRelease
	if err := json.Unmarshal(body, &releases); err != nil {
		return FetchResult{}, fmt.Errorf("decode GitHub releases: %w", err)
	}
	if maxItems > len(releases) {
		maxItems = len(releases)
	}
	items := make([]RawItem, 0, maxItems)
	for _, release := range releases[:maxItems] {
		externalID := release.TagName
		if release.ID != 0 {
			externalID = fmt.Sprint(release.ID)
		}
		items = append(items, RawItem{
			ExternalID:   externalID,
			URL:          release.HTMLURL,
			Title:        firstNonEmpty(release.Name, release.TagName),
			Summary:      release.Body,
			PublishedAt:  parseFeedTime(release.PublishedAt),
			Technologies: append([]string(nil), s.Source.TechnologyHints...),
			Interests:    append([]string(nil), s.Source.InterestHints...),
		})
	}
	return FetchResult{Items: items, Cursor: nextCursor}, nil
}

func boundedFetchLimit(maxItems int) int {
	if maxItems <= 0 {
		return 0
	}
	if maxItems > hardMaxItems {
		return hardMaxItems
	}
	return maxItems
}

type xmlFeed struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
	Entries []atomEntry `xml:"entry"`
}

type rssItem struct {
	GUID        string `xml:"guid"`
	Link        string `xml:"link"`
	Title       string `xml:"title"`
	Description string `xml:"description"`
	PubDate     string `xml:"pubDate"`
}

type atomEntry struct {
	ID        string   `xml:"id"`
	Title     string   `xml:"title"`
	Link      atomLink `xml:"link"`
	Summary   string   `xml:"summary"`
	Content   string   `xml:"content"`
	Published string   `xml:"published"`
	Updated   string   `xml:"updated"`
}

type atomLink struct {
	Href string `xml:"href,attr"`
}

type githubRelease struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	TagName     string `json:"tag_name"`
	HTMLURL     string `json:"html_url"`
	Body        string `json:"body"`
	PublishedAt string `json:"published_at"`
}

func firstLink(values ...string) string { return firstNonEmpty(values...) }

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func allowedContentType(mediaType string, allowed []string) bool {
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	for _, candidate := range allowed {
		if mediaType == candidate {
			return true
		}
	}
	return false
}

func parseFeedTime(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}
