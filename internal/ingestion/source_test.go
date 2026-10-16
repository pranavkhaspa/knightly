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
