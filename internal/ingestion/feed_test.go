package ingestion

import (
	"strings"
	"testing"
	"time"
)

func TestCanonicalizeURLRemovesTrackingButKeepsMeaningfulQuery(t *testing.T) {
	got, err := CanonicalizeURL("HTTPS://Example.COM:443/articles/?b=2&utm_source=nightshift&a=1#section")
	if err != nil {
		t.Fatalf("CanonicalizeURL() error = %v", err)
	}
	want := "https://example.com/articles?a=1&b=2"
	if got != want {
		t.Fatalf("CanonicalizeURL() = %q, want %q", got, want)
	}
}

func TestNormalizeBoundsAndDeduplicatesByCanonicalURL(t *testing.T) {
	normalizer := NewNormalizer(Limits{MaxItems: 3, MaxTitleBytes: 12, MaxSummarySize: 16, MaxTerms: 2})
	source := SourceConfig{
		ID:      "source_fixture",
		Name:    " Fixture feed ",
		Kind:    "fixture",
		URL:     "https://example.invalid/feed/",
		Enabled: true,
	}
	fetchedAt := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	items, stats, err := normalizer.Normalize(source, []RawItem{
		{
			URL:          "https://Example.invalid/item/?utm_campaign=x",
			Title:        "  A very long title  ",
			Summary:      "  A summary with more bytes than allowed  ",
			Technologies: []string{"FalkorDB", "Graph", "Graph"},
			PublishedAt:  fetchedAt,
		},
		{
			URL:   "https://example.invalid/item/",
			Title: "same canonical item",
		},
		{
			URL:   "https://example.invalid/second",
			Title: "second item",
		},
		{
			URL:   "https://example.invalid/ignored-by-bound",
			Title: "not read",
		},
	}, fetchedAt)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if len(items) != 2 || stats.Accepted != 2 || stats.DroppedDuplicate != 1 || stats.Seen != 3 {
		t.Fatalf("unexpected normalization result: items=%d stats=%+v", len(items), stats)
	}
	if items[0].CanonicalURL != "https://example.invalid/item" {
		t.Fatalf("unexpected canonical item URL: %q", items[0].CanonicalURL)
	}
	if len(items[0].Title) > 12 || len(items[0].Summary) > 16 {
		t.Fatalf("normalizer exceeded bounds: title=%d summary=%d", len(items[0].Title), len(items[0].Summary))
	}
	if got := strings.Join(items[0].Technologies, ","); got != "falkordb,graph" {
		t.Fatalf("unexpected normalized technologies: %q", got)
	}
	if !items[0].FetchedAt.Equal(fetchedAt) {
		t.Fatalf("FetchedAt = %v, want %v", items[0].FetchedAt, fetchedAt)
	}
}

func TestNormalizeRejectsCredentialsAndUnsupportedSchemes(t *testing.T) {
	for _, raw := range []string{
		"ftp://example.invalid/item",
		"https://user:password@example.invalid/item",
	} {
		if _, err := CanonicalizeURL(raw); err == nil {
			t.Fatalf("CanonicalizeURL(%q) accepted an unsafe URL", raw)
		}
	}
}
