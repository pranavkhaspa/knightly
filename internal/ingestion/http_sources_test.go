package ingestion

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRSSSourceParsesBoundedAtomFeed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/atom+xml")
		_, _ = w.Write([]byte(`<feed><entry><id>one</id><title>FalkorDB release</title><link href="https://example.com/release"/><summary>Graph paths</summary><updated>2026-10-16T10:00:00Z</updated></entry><entry><id>two</id><title>Second</title><link href="https://example.com/second"/></entry></feed>`))
	}))
	defer server.Close()

	source := RSSSource{Source: SourceConfig{URL: server.URL, TechnologyHints: []string{"FalkorDB"}}}
	items, err := source.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(items) != 1 || items[0].URL != "https://example.com/release" || len(items[0].Technologies) != 1 {
		t.Fatalf("unexpected parsed Atom items: %#v", items)
	}
}

func TestGitHubReleaseSourceParsesPublicJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":42,"name":"v1","tag_name":"v1.0.0","html_url":"https://github.com/example/release","body":"Graph release","published_at":"2026-10-16T10:00:00Z"}]`))
	}))
	defer server.Close()

	source := GitHubReleaseSource{Source: SourceConfig{URL: server.URL, InterestHints: []string{"Graph Agents"}}}
	items, err := source.Fetch(context.Background(), 5)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(items) != 1 || items[0].ExternalID != "42" || items[0].Title != "v1" {
		t.Fatalf("unexpected GitHub release items: %#v", items)
	}
}
