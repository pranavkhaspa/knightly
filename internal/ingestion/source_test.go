package ingestion

import (
	"context"
	"testing"
)

func TestFixtureSourceIsDeterministicAndBounded(t *testing.T) {
	source := DefaultFixtureSource()
	first, err := source.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	second, err := source.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("second Fetch() error = %v", err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("Fetch() lengths = %d and %d, want 1", len(first), len(second))
	}
	if first[0].URL != second[0].URL || first[0].Title != second[0].Title {
		t.Fatal("fixture source returned different data for identical runs")
	}
	first[0].Title = "mutated"
	third, err := source.Fetch(context.Background(), 1)
	if err != nil {
		t.Fatalf("third Fetch() error = %v", err)
	}
	if third[0].Title == "mutated" {
		t.Fatal("fixture source exposed mutable internal data")
	}
}

func TestPersistedSourceKindsResolveIncludingFixture(t *testing.T) {
	registry := NewHTTPAdapterRegistry(nil, 0, "test-agent")
	for _, kind := range []string{SourceKindRSS, SourceKindAtom, SourceKindGitHub, SourceKindGitHubRelease, SourceKindFixture, "feed"} {
		adapter, err := registry.Resolve(SourceConfig{ID: "source_" + kind, Kind: kind, Name: "source", URL: "https://example.invalid/feed"})
		if err != nil {
			t.Fatalf("resolve %q: %v", kind, err)
		}
		if adapter == nil {
			t.Fatalf("resolve %q returned nil adapter", kind)
		}
	}
	if DefaultFixtureSource().Config().Kind != SourceKindFixture {
		t.Fatalf("default fixture kind = %q, want %q", DefaultFixtureSource().Config().Kind, SourceKindFixture)
	}
}
