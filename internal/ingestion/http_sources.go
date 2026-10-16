package ingestion

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultHTTPBodyLimit = 2 << 20
	defaultUserAgent     = "NightShift/0.1 (+https://github.com/pranavkhaspa/knightly)"
)

// RSSSource is a bounded RSS/Atom adapter. It returns structured records and
// never hands raw response bodies to the rest of the application.
type RSSSource struct {
	Source    SourceConfig
	Client    *http.Client
	BodyLimit int64
	UserAgent string
}

func (s RSSSource) Config() SourceConfig { return s.Source }

func (s RSSSource) Fetch(ctx context.Context, maxItems int) ([]RawItem, error) {
	body, err := s.fetch(ctx)
	if err != nil {
		return nil, err
	}
	var feed xmlFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return nil, fmt.Errorf("decode RSS/Atom feed: %w", err)
	}
	items := make([]RawItem, 0, maxItems)
	for _, item := range feed.Channel.Items {
		if len(items) == maxItems {
			break
		}
		items = append(items, RawItem{
			ExternalID:   item.GUID,
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
	return items, nil
}

func (s RSSSource) fetch(ctx context.Context) ([]byte, error) {
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Source.URL, nil)
	if err != nil {
		return nil, err
	}
	userAgent := s.UserAgent
	if userAgent == "" {
		userAgent = defaultUserAgent
	}
	request.Header.Set("User-Agent", userAgent)
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("source returned HTTP %d", response.StatusCode)
	}
	limit := s.BodyLimit
	if limit <= 0 || limit > defaultHTTPBodyLimit {
		limit = defaultHTTPBodyLimit
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("source response exceeds %d bytes", limit)
	}
	return body, nil
}

// GitHubReleaseSource reads the public releases API without authentication.
// The source URL should be an endpoint such as
// https://api.github.com/repos/FalkorDB/FalkorDB/releases.
type GitHubReleaseSource struct {
	Source    SourceConfig
	Client    *http.Client
	BodyLimit int64
	UserAgent string
}

func (s GitHubReleaseSource) Config() SourceConfig { return s.Source }

func (s GitHubReleaseSource) Fetch(ctx context.Context, maxItems int) ([]RawItem, error) {
	rss := RSSSource{Source: s.Source, Client: s.Client, BodyLimit: s.BodyLimit, UserAgent: s.UserAgent}
	body, err := rss.fetch(ctx)
	if err != nil {
		return nil, err
	}
	var releases []githubRelease
	if err := json.Unmarshal(body, &releases); err != nil {
		return nil, fmt.Errorf("decode GitHub releases: %w", err)
	}
	if maxItems < 0 {
		maxItems = 0
	}
	if maxItems > len(releases) {
		maxItems = len(releases)
	}
	items := make([]RawItem, 0, maxItems)
	for _, release := range releases[:maxItems] {
		items = append(items, RawItem{
			ExternalID:   fmt.Sprint(release.ID),
			URL:          release.HTMLURL,
			Title:        firstNonEmpty(release.Name, release.TagName),
			Summary:      release.Body,
			PublishedAt:  parseFeedTime(release.PublishedAt),
			Technologies: append([]string(nil), s.Source.TechnologyHints...),
			Interests:    append([]string(nil), s.Source.InterestHints...),
		})
	}
	return items, nil
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
