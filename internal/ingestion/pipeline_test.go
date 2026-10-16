package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeGraphWriter struct {
	sources []SourceConfig
	items   []Item
}

func (f *fakeGraphWriter) UpsertSource(_ context.Context, source SourceConfig) error {
	f.sources = append(f.sources, source)
	return nil
}

func (f *fakeGraphWriter) UpsertItem(_ context.Context, item Item) error {
	f.items = append(f.items, item)
	return nil
}

func TestPipelineWritesNormalizedFixtureToGraphWriter(t *testing.T) {
	writer := &fakeGraphWriter{}
	pipeline := NewPipeline(writer)
	pipeline.Normalizer = NewNormalizer(Limits{MaxItems: 10})
	pipeline.Now = func() time.Time { return time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC) }

	result, err := pipeline.Run(context.Background(), DefaultFixtureSource())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(writer.sources) != 1 || len(writer.items) != 2 {
		t.Fatalf("graph writes = %d sources, %d items; want 1 source, 2 items", len(writer.sources), len(writer.items))
	}
	if result.Normalized != 2 || result.Duplicates != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if writer.items[0].SourceID != writer.sources[0].ID {
		t.Fatalf("item source ID %q does not match source %q", writer.items[0].SourceID, writer.sources[0].ID)
	}
}

func TestPipelineRecoversAfterBoundedSourceFailure(t *testing.T) {
	writer := &fakeGraphWriter{}
	pipeline := NewPipeline(writer)
	temporary := errors.New("source timed out")
	source := failingSource{
		config: SourceConfig{
			ID:      "source_timeout",
			Name:    "Temporary source",
			Kind:    "feed",
			URL:     "https://example.com/feed",
			Enabled: true,
		},
		err: temporary,
	}

	result, err := pipeline.Run(context.Background(), source)
	if !errors.Is(err, temporary) {
		t.Fatalf("failed source error = %v, want wrapped timeout", err)
	}
	if result.Source.ID != source.config.ID || len(writer.sources) != 0 || len(writer.items) != 0 {
		t.Fatalf("failed source wrote partial state: result=%+v sources=%d items=%d", result, len(writer.sources), len(writer.items))
	}

	recovered, err := pipeline.Run(context.Background(), DefaultFixtureSource())
	if err != nil {
		t.Fatalf("run after source failure: %v", err)
	}
	if recovered.Normalized != 2 || len(writer.sources) != 1 || len(writer.items) != 2 {
		t.Fatalf("pipeline did not recover: result=%+v sources=%d items=%d", recovered, len(writer.sources), len(writer.items))
	}
}

type failingSource struct {
	config SourceConfig
	err    error
}

func (s failingSource) Config() SourceConfig { return s.config }

func (s failingSource) Fetch(context.Context, int) ([]RawItem, error) {
	return nil, s.err
}
