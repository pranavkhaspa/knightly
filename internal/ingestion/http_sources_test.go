package ingestion

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

func TestRSSSourcePersistsAndReusesConditionalRequestCursor(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 2 {
			if got := r.Header.Get("If-None-Match"); got != `"feed-v1"` {
				t.Errorf("If-None-Match = %q, want feed ETag", got)
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml; charset=utf-8")
		w.Header().Set("ETag", `"feed-v1"`)
		w.Header().Set("Last-Modified", "Fri, 16 Oct 2026 10:00:00 GMT")
		_, _ = w.Write([]byte(`<rss><channel><item><guid>one</guid><link>https://example.com/one</link><title>One</title></item></channel></rss>`))
	}))
	defer server.Close()

	source := RSSSource{Source: SourceConfig{ID: "feed", URL: server.URL}}
	first, err := source.FetchWithCursor(context.Background(), SourceCursor{SourceID: "feed"}, 5)
	if err != nil {
		t.Fatalf("first FetchWithCursor() error = %v", err)
	}
	if len(first.Items) != 1 || first.Cursor.ETag != `"feed-v1"` {
		t.Fatalf("first fetch = %#v, want item and ETag cursor", first)
	}
	second, err := source.FetchWithCursor(context.Background(), first.Cursor, 5)
	if err != nil {
		t.Fatalf("second FetchWithCursor() error = %v", err)
	}
	if !second.NotModified || len(second.Items) != 0 || second.Cursor.ETag != first.Cursor.ETag {
		t.Fatalf("second fetch = %#v, want not-modified cursor result", second)
	}
}

func TestRSSSourceRejectsUnsupportedContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html>not a feed</html>`))
	}))
	defer server.Close()

	_, err := (RSSSource{Source: SourceConfig{URL: server.URL}}).Fetch(context.Background(), 1)
	if err == nil {
		t.Fatal("Fetch() error = nil, want unsupported content type error")
	}
}
