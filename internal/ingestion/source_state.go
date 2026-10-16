package ingestion

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	DefaultSourcePollInterval = 15 * time.Minute
	DefaultSourceMaxItems     = defaultMaxItems
)

// SourceCursor is durable HTTP and collection state. It intentionally contains
// no credentials or response payloads; FalkorDB stores it alongside the source
// so a process restart can resume conditional requests.
type SourceCursor struct {
	SourceID      string
	ETag          string
	LastModified  string
	LastAttemptAt time.Time
	LastSuccessAt time.Time
	LastRunID     string
}

type FetchResult struct {
	Items       []RawItem
	Cursor      SourceCursor
	NotModified bool
}

// SourceAdapter is the typed boundary between configured sources and the
// collection service. Fetch keeps the original simple FeedSource contract;
// FetchWithCursor is used by scheduled collection for conditional HTTP calls.
type SourceAdapter interface {
	FeedSource
	FetchWithCursor(context.Context, SourceCursor, int) (FetchResult, error)
}

type SourceAdapterFactory func(SourceConfig) SourceAdapter

// AdapterRegistry resolves persisted source kinds without allowing a source to
// select an arbitrary implementation or URL handler.
type AdapterRegistry struct {
	factories map[string]SourceAdapterFactory
}

func NewAdapterRegistry() *AdapterRegistry {
	return &AdapterRegistry{factories: make(map[string]SourceAdapterFactory)}
}

func (r *AdapterRegistry) Register(kind string, factory SourceAdapterFactory) error {
	if r == nil {
		return errors.New("source adapter registry is required")
	}
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind == "" || factory == nil {
		return errors.New("source adapter kind and factory are required")
	}
	if r.factories == nil {
		r.factories = make(map[string]SourceAdapterFactory)
	}
	r.factories[kind] = factory
	return nil
}

func (r *AdapterRegistry) Resolve(source SourceConfig) (SourceAdapter, error) {
	if r == nil {
		return nil, errors.New("source adapter registry is required")
	}
	kind := strings.ToLower(strings.TrimSpace(source.Kind))
	factory := r.factories[kind]
	if factory == nil {
		return nil, fmt.Errorf("no adapter registered for source kind %q", source.Kind)
	}
	adapter := factory(source)
	if adapter == nil {
		return nil, fmt.Errorf("adapter factory for source kind %q returned nil", source.Kind)
	}
	return adapter, nil
}

// NewHTTPAdapterRegistry registers the MVP's public HTTP source kinds. The
// client is shared so callers can configure connection pooling and deadlines.
func NewHTTPAdapterRegistry(client *http.Client, bodyLimit int64, userAgent string) *AdapterRegistry {
	registry := NewAdapterRegistry()
	_ = registry.Register("rss", func(source SourceConfig) SourceAdapter {
		return RSSSource{Source: source, Client: client, BodyLimit: bodyLimit, UserAgent: userAgent}
	})
	_ = registry.Register("atom", func(source SourceConfig) SourceAdapter {
		return RSSSource{Source: source, Client: client, BodyLimit: bodyLimit, UserAgent: userAgent}
	})
	_ = registry.Register("github", func(source SourceConfig) SourceAdapter {
		return GitHubReleaseSource{Source: source, Client: client, BodyLimit: bodyLimit, UserAgent: userAgent}
	})
	_ = registry.Register("github_release", func(source SourceConfig) SourceAdapter {
		return GitHubReleaseSource{Source: source, Client: client, BodyLimit: bodyLimit, UserAgent: userAgent}
	})
	return registry
}

type SourceRunStatus string

const (
	SourceRunRunning   SourceRunStatus = "running"
	SourceRunSucceeded SourceRunStatus = "succeeded"
	SourceRunFailed    SourceRunStatus = "failed"
)

type SourceRun struct {
	ID              string
	SourceID        string
	StartedAt       time.Time
	FinishedAt      time.Time
	Status          SourceRunStatus
	Error           string
	RawCount        int
	NormalizedCount int
	DuplicateCount  int
	NotModified     bool
}

// SourceRepository is the durable source boundary used by Collector. The
// graph repository also implements GraphWriter, so normalized items and source
// execution state are acknowledged by the same FalkorDB authority.
type SourceRepository interface {
	GraphWriter
	ListSources(context.Context) ([]SourceConfig, error)
	GetSourceCursor(context.Context, string) (SourceCursor, bool, error)
	SaveSourceCursor(context.Context, SourceCursor) error
	BeginSourceRun(context.Context, SourceRun) error
	CompleteSourceRun(context.Context, SourceRun) error
}
