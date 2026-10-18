package search

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSearXNGProviderNormalizesAndBoundsResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" || r.URL.Query().Get("format") != "json" || r.URL.Query().Get("pageno") != "2" {
			t.Fatalf("request = %s", r.URL.String())
		}
		if r.URL.Query().Get("q") != "graph + go" {
			t.Fatalf("query = %q", r.URL.Query().Get("q"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"query": " graph + go ", "number_of_results": 2,
			"results": []map[string]interface{}{
				{"title": " First ", "url": "https://example.com/one", "content": " Snippet ", "engine": "google", "score": 0.75},
				{"title": "Second", "url": "https://example.com/two"},
			},
		})
	}))
	defer server.Close()

	provider, err := NewSearXNGProvider(SearXNGConfig{BaseURL: server.URL, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	response, err := provider.Search(context.Background(), SearchRequest{Query: " graph + go ", Page: 2, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if response.Query != "graph + go" || response.Total != 2 || len(response.Results) != 1 || response.Results[0].Title != "First" {
		t.Fatalf("response = %+v", response)
	}
}

func TestSearXNGProviderRejectsInvalidRequestsAndResponses(t *testing.T) {
	provider, err := NewSearXNGProvider(SearXNGConfig{BaseURL: "https://example.com", Limits: Limits{MaxQueryLength: 4, MaxPage: 2, MaxResults: 2}})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []SearchRequest{{}, {Query: "five!"}, {Query: "ok", Page: 3}, {Query: "ok", Limit: 3}} {
		if _, err := provider.Search(context.Background(), request); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("request %+v error = %v", request, err)
		}
	}

	tests := []struct {
		name        string
		body        string
		contentType string
		status      int
		want        error
	}{
		{name: "malformed", body: `{`, contentType: "application/json", want: ErrInvalidResponse},
		{name: "missing results", body: `{"query":"x"}`, contentType: "application/json", want: ErrInvalidResponse},
		{name: "bad URL", body: `{"results":[{"url":"javascript:alert(1)"}]}`, contentType: "application/json", want: ErrInvalidResponse},
		{name: "wrong type", body: `<html>`, contentType: "text/html", want: ErrInvalidResponse},
		{name: "upstream", body: `unavailable`, status: http.StatusBadGateway, want: ErrHTTPStatus},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.contentType != "" {
					w.Header().Set("Content-Type", test.contentType)
				}
				if test.status != 0 {
					w.WriteHeader(test.status)
				}
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			provider, err := NewSearXNGProvider(SearXNGConfig{BaseURL: server.URL, Client: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.Search(context.Background(), SearchRequest{Query: "test", Limit: 1})
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestNewSearXNGProviderRequiresHTTPSForRemoteInstances(t *testing.T) {
	if _, err := NewSearXNGProvider(SearXNGConfig{BaseURL: "http://searx.example.com"}); !errors.Is(err, ErrInsecureBaseURL) {
		t.Fatalf("remote http error = %v", err)
	}
	if _, err := NewSearXNGProvider(SearXNGConfig{BaseURL: "https://searx.example.com"}); err != nil {
		t.Fatalf("remote https error = %v", err)
	}
	if _, err := NewSearXNGProvider(SearXNGConfig{BaseURL: "http://127.0.0.1:8888"}); err != nil {
		t.Fatalf("loopback http error = %v", err)
	}
	if _, err := NewSearXNGProvider(SearXNGConfig{BaseURL: "http://searx.example.com", AllowInsecureHTTP: true}); err != nil {
		t.Fatalf("explicit development http error = %v", err)
	}
}

func TestSearXNGProviderBoundsResponseAndTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	provider, err := NewSearXNGProvider(SearXNGConfig{BaseURL: server.URL, Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Search(context.Background(), SearchRequest{Query: "test", Limit: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error = %v", err)
	}

	large := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(strings.Repeat("x", 33)))
	}))
	defer large.Close()
	provider, err = NewSearXNGProvider(SearXNGConfig{BaseURL: large.URL, Limits: Limits{MaxResponseSize: 32}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Search(context.Background(), SearchRequest{Query: "test", Limit: 1})
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("large response error = %v", err)
	}
}

func TestSearXNGProviderRejectsRemoteInsecureHTTP(t *testing.T) {
	if _, err := NewSearXNGProvider(SearXNGConfig{BaseURL: "http://search.example.com"}); err == nil {
		t.Fatal("remote HTTP SearXNG URL was accepted without an explicit development override")
	}
	if _, err := NewSearXNGProvider(SearXNGConfig{BaseURL: "http://search.example.com", AllowInsecureHTTP: true}); err != nil {
		t.Fatalf("explicit insecure HTTP override rejected: %v", err)
	}
}
