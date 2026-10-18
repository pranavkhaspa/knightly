package ingestion

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
	if err == nil || !strings.Contains(err.Error(), "unsupported source content type") {
		t.Fatalf("Fetch() error = %v, want unsupported content type error", err)
	}
}

func TestRSSSourceBoundsItemLimitAndRejectsOversizedBodies(t *testing.T) {
	called := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.Header().Set("Content-Type", "application/atom+xml")
		_, _ = w.Write([]byte(`<feed><entry><id>one</id><title>Bounded</title><link href="https://example.com/bounded"/></entry></feed>`))
	}))
	defer server.Close()

	source := RSSSource{Source: SourceConfig{URL: server.URL}}
	items, err := source.Fetch(context.Background(), 1<<30)
	if err != nil {
		t.Fatalf("Fetch() with oversized limit error = %v", err)
	}
	if len(items) != 1 || called != 1 {
		t.Fatalf("oversized limit returned %d items after %d requests; want one item and one request", len(items), called)
	}
	items, err = source.Fetch(context.Background(), 0)
	if err != nil {
		t.Fatalf("Fetch() with zero limit error = %v", err)
	}
	if len(items) != 0 || called != 1 {
		t.Fatalf("zero limit returned %d items after %d requests; want no items and no request", len(items), called)
	}
}

func TestRSSSourceReturnsBoundedFailureForBadStatusMalformedFeedAndBody(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		bodyLimit int64
		content   string
		want      string
	}{
		{name: "http status", status: http.StatusBadGateway, body: "upstream unavailable", content: "application/rss+xml", want: "source returned HTTP 502"},
		{name: "malformed feed", status: http.StatusOK, body: "not xml", content: "application/rss+xml", want: "decode RSS/Atom feed"},
		{name: "body limit", status: http.StatusOK, body: strings.Repeat("x", 64), bodyLimit: 32, content: "application/rss+xml", want: "source response exceeds 32 bytes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", test.content)
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			source := RSSSource{Source: SourceConfig{URL: server.URL}, BodyLimit: test.bodyLimit}
			_, err := source.Fetch(context.Background(), 1)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Fetch() error = %v, want message containing %q", err, test.want)
			}
		})
	}
}

func TestRSSSourceHonorsCanceledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (RSSSource{Source: SourceConfig{URL: server.URL}}).Fetch(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Fetch() error = %v, want context.Canceled", err)
	}
}

func TestRSSSourceRetriesTemporaryHTTPFailures(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(`<rss><channel><item><guid>retry-ok</guid><link>https://example.com/retry-ok</link><title>Recovered</title></item></channel></rss>`))
	}))
	defer server.Close()

	source := RSSSource{
		Source:       SourceConfig{ID: "retry", URL: server.URL},
		Client:       server.Client(),
		MaxRetries:   1,
		RetryBackoff: time.Millisecond,
	}
	items, err := source.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(items) != 1 || requests.Load() != 2 {
		t.Fatalf("items=%d requests=%d, want one item after two requests", len(items), requests.Load())
	}
}

func TestRSSSourceBoundsRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/next", http.StatusFound)
			return
		}
		http.Redirect(w, r, "/final", http.StatusFound)
	}))
	defer server.Close()

	source := RSSSource{
		Source:       SourceConfig{URL: server.URL + "/start"},
		Client:       server.Client(),
		MaxRetries:   -1,
		MaxRedirects: 1,
	}
	_, err := source.Fetch(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "redirect limit") {
		t.Fatalf("Fetch() error = %v, want bounded redirect error", err)
	}
}
