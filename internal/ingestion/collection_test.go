package ingestion

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/coordination"
)

type collectionRepository struct {
	mu      sync.Mutex
	sources []SourceConfig
	cursors map[string]SourceCursor
	runs    map[string]SourceRun
	items   []Item
	upserts []SourceConfig
}

func newCollectionRepository(sources ...SourceConfig) *collectionRepository {
	return &collectionRepository{sources: sources, cursors: make(map[string]SourceCursor), runs: make(map[string]SourceRun)}
}

func (r *collectionRepository) UpsertSource(_ context.Context, source SourceConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upserts = append(r.upserts, source)
	return nil
}

func (r *collectionRepository) UpsertItem(_ context.Context, item Item) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, item)
	return nil
}

func (r *collectionRepository) ListSources(context.Context) ([]SourceConfig, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]SourceConfig(nil), r.sources...), nil
}

func (r *collectionRepository) GetSourceCursor(_ context.Context, sourceID string) (SourceCursor, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cursor, ok := r.cursors[sourceID]
	return cursor, ok, nil
}

func (r *collectionRepository) SaveSourceCursor(_ context.Context, cursor SourceCursor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cursors[cursor.SourceID] = cursor
	return nil
}

func (r *collectionRepository) BeginSourceRun(_ context.Context, run SourceRun) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs[run.ID] = run
	return nil
}

func (r *collectionRepository) CompleteSourceRun(_ context.Context, run SourceRun) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs[run.ID] = run
	return nil
}

func TestCollectorUsesDurableCursorAndPerSourceInterval(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("If-None-Match") == `"feed-v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml")
		w.Header().Set("ETag", `"feed-v1"`)
		_, _ = w.Write([]byte(`<rss><channel><item><guid>one</guid><link>https://example.com/one</link><title>One</title></item></channel></rss>`))
	}))
	defer server.Close()

	configured := SourceConfig{
		ID:           "source_feed",
		Name:         "Example feed",
		Kind:         "rss",
		URL:          server.URL,
		Enabled:      true,
		PollInterval: time.Hour,
		MaxItems:     5,
	}
	repository := newCollectionRepository(configured)
	pipeline := NewPipeline(repository)
	now := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	pipeline.Now = func() time.Time { return now }
	collector, err := NewCollector(repository, pipeline, NewHTTPAdapterRegistry(nil, 0, "test-agent"))
	if err != nil {
		t.Fatal(err)
	}
	collector.Now = func() time.Time { return now }

	first, err := collector.RunNow(context.Background())
	if err != nil {
		t.Fatalf("first RunNow() error = %v", err)
	}
	if first.Succeeded != 1 || len(first.Items) != 1 || len(repository.items) != 1 || repository.cursors[configured.ID].ETag != `"feed-v1"` {
		t.Fatalf("first collection = %+v, items=%d cursor=%+v", first, len(repository.items), repository.cursors[configured.ID])
	}

	second, err := collector.Run(context.Background())
	if err != nil {
		t.Fatalf("interval Run() error = %v", err)
	}
	if second.Skipped != 1 || requests != 1 {
		t.Fatalf("interval collection = %+v, requests=%d; want one skipped and no HTTP request", second, requests)
	}

	third, err := collector.RunNow(context.Background())
	if err != nil {
		t.Fatalf("conditional RunNow() error = %v", err)
	}
	if third.Succeeded != 1 || len(third.Runs) != 1 || !third.Runs[0].NotModified || requests != 2 {
		t.Fatalf("conditional collection = %+v, requests=%d", third, requests)
	}
}

func TestCollectorRecordsOneSourceFailureAndContinues(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(`<rss><channel><item><guid>one</guid><link>https://example.com/one</link><title>One</title></item></channel></rss>`))
	}))
	defer server.Close()
	repository := newCollectionRepository(
		SourceConfig{ID: "source_bad", Name: "Unsupported", Kind: "unknown", URL: server.URL, Enabled: true},
		SourceConfig{ID: "source_good", Name: "Working", Kind: "rss", URL: server.URL, Enabled: true},
	)
	collector, err := NewCollector(repository, NewPipeline(repository), NewHTTPAdapterRegistry(nil, 0, "test-agent"))
	if err != nil {
		t.Fatal(err)
	}

	summary, err := collector.RunNow(context.Background())
	if err != nil {
		t.Fatalf("RunNow() error = %v", err)
	}
	if summary.Succeeded != 1 || summary.Failed != 1 || summary.Errors["source_bad"] == "" {
		t.Fatalf("summary = %+v, want one failed and one successful source", summary)
	}
}

type sourceLeaseHarness struct {
	mu       sync.Mutex
	active   bool
	acquired []coordination.LeaseRequest
	released []string
}

func (h *sourceLeaseHarness) AcquireLease(_ context.Context, request coordination.LeaseRequest) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active {
		return false, nil
	}
	h.active = true
	h.acquired = append(h.acquired, request)
	return true, nil
}

func (h *sourceLeaseHarness) ReleaseLease(_ context.Context, _, token string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active = false
	h.released = append(h.released, token)
	return nil
}

func TestCollectorUsesScopedSourceLeaseAndReleasesIt(t *testing.T) {
	source := SourceConfig{
		ID:       "source_fixture",
		TenantID: "tenant-a",
		UserID:   "user-a",
		Name:     "Fixture",
		Kind:     SourceKindFixture,
		URL:      "https://example.invalid/feed",
		Enabled:  true,
	}
	repository := newCollectionRepository(source)
	collector, err := NewCollector(repository, NewPipeline(repository), NewHTTPAdapterRegistry(nil, 0, "test-agent"))
	if err != nil {
		t.Fatal(err)
	}
	lease := &sourceLeaseHarness{}
	collector.Coordinator = lease
	collector.LeaseOwner = "instance-a"
	collector.Now = func() time.Time { return time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC) }

	summary, err := collector.RunNow(context.Background())
	if err != nil {
		t.Fatalf("RunNow() error = %v", err)
	}
	if summary.Succeeded != 1 || len(lease.acquired) != 1 || len(lease.released) != 1 {
		t.Fatalf("summary=%+v acquired=%+v released=%+v", summary, lease.acquired, lease.released)
	}
	request := lease.acquired[0]
	if request.Key != "source:tenant-a:user-a:source_fixture" || request.Owner != "instance-a" || request.Lifetime <= 0 {
		t.Fatalf("source lease request = %+v", request)
	}
	if len(summary.Runs) != 1 || summary.Runs[0].TenantID != "tenant-a" || summary.Runs[0].UserID != "user-a" {
		t.Fatalf("source run scope = %+v", summary.Runs)
	}
}

func TestCollectorSkipsSourceWhenLeaseIsHeld(t *testing.T) {
	source := SourceConfig{ID: "source_held", TenantID: "tenant-a", UserID: "user-a", Name: "Fixture", Kind: SourceKindFixture, URL: "https://example.invalid/feed", Enabled: true}
	repository := newCollectionRepository(source)
	collector, err := NewCollector(repository, NewPipeline(repository), NewHTTPAdapterRegistry(nil, 0, "test-agent"))
	if err != nil {
		t.Fatal(err)
	}
	lease := &sourceLeaseHarness{active: true}
	collector.Coordinator = lease
	collector.LeaseOwner = "instance-b"

	summary, err := collector.RunNow(context.Background())
	if err != nil {
		t.Fatalf("RunNow() error = %v", err)
	}
	if summary.Skipped != 1 || summary.Succeeded != 0 || len(summary.Runs) != 0 || len(repository.runs) != 0 {
		t.Fatalf("held-source summary=%+v runs=%+v", summary, repository.runs)
	}
}
