package ingestion

import (
	"context"
	"errors"
	"time"
)

// FeedSource is the narrow source boundary used by the ingestion pipeline.
// Implementations must honor maxItems and return structured items rather than
// exposing an unbounded response body.
type FeedSource interface {
	Config() SourceConfig
	Fetch(ctx context.Context, maxItems int) ([]RawItem, error)
}

// FixtureSource is a deterministic source adapter for demos and unit tests. It
// returns a copy of its data, so a pipeline cannot mutate future runs.
type FixtureSource struct {
	config SourceConfig
	items  []RawItem
}

func NewFixtureSource(config SourceConfig, items []RawItem) *FixtureSource {
	return &FixtureSource{config: config, items: cloneRawItems(items)}
}

func (s *FixtureSource) Config() SourceConfig {
	return s.config
}

func (s *FixtureSource) Fetch(ctx context.Context, maxItems int) ([]RawItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if maxItems <= 0 {
		return []RawItem{}, nil
	}
	if maxItems > len(s.items) {
		maxItems = len(s.items)
	}
	items := cloneRawItems(s.items[:maxItems])
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// FetchWithCursor lets the deterministic fixture participate in the same
// scheduled collector contract as conditional HTTP adapters. The fixture has
// no remote cursor semantics, so each due run returns its bounded items.
func (s *FixtureSource) FetchWithCursor(ctx context.Context, cursor SourceCursor, maxItems int) (FetchResult, error) {
	items, err := s.Fetch(ctx, maxItems)
	if err != nil {
		return FetchResult{}, err
	}
	cursor.SourceID = s.config.ID
	return FetchResult{Items: items, Cursor: cursor}, nil
}

// DefaultFixtureSource provides a stable synthetic signal for the seeded
// NightShift project. Its URL is intentionally non-routable and contains no
// credentials or user data.
func DefaultFixtureSource() *FixtureSource {
	return NewFixtureSource(SourceConfig{
		ID:         "source_fixture_nightshift",
		Name:       "NightShift deterministic fixture",
		Kind:       "fixture",
		URL:        "https://example.invalid/nightshift/feed",
		Enabled:    true,
		TrustScore: 1,
	}, []RawItem{
		{
			ExternalID:   "fixture-falkordb-paths",
			URL:          "https://example.invalid/nightshift/falkordb-paths?utm_source=fixture",
			Title:        "FalkorDB graph paths make agent recommendations explainable",
			Summary:      "A synthetic release describing graph-backed evidence paths for developer agents.",
			PublishedAt:  time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC),
			Technologies: []string{"FalkorDB", "Graph Database"},
			Interests:    []string{"Graph Agents"},
		},
		{
			ExternalID:  "fixture-unrelated",
			URL:         "https://example.invalid/nightshift/unrelated",
			Title:       "A synthetic unrelated signal",
			Summary:     "This item has no project technology relationship.",
			PublishedAt: time.Date(2026, 10, 16, 7, 0, 0, 0, time.UTC),
		},
	})
}

func cloneRawItems(items []RawItem) []RawItem {
	cloned := make([]RawItem, len(items))
	copy(cloned, items)
	for index := range cloned {
		cloned[index].Technologies = append([]string(nil), items[index].Technologies...)
		cloned[index].Interests = append([]string(nil), items[index].Interests...)
	}
	return cloned
}

var errNilSource = errors.New("source adapter is required")
