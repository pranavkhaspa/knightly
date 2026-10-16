package ingestion

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// GraphWriter is implemented by the FalkorDB repository. The ingestion
// package depends on this interface so normalizer and source tests never need
// a live graph server.
type GraphWriter interface {
	UpsertSource(ctx context.Context, source SourceConfig) error
	UpsertItem(ctx context.Context, item Item) error
}

type Pipeline struct {
	Writer     GraphWriter
	Normalizer Normalizer
	Now        func() time.Time
}

type RunResult struct {
	Source         SourceConfig
	RawCount       int
	Normalized     int
	DroppedInvalid int
	Duplicates     int
	Items          []Item
}

func NewPipeline(writer GraphWriter) *Pipeline {
	return &Pipeline{
		Writer:     writer,
		Normalizer: NewNormalizer(DefaultLimits()),
		Now:        func() time.Time { return time.Now().UTC() },
	}
}

func (p *Pipeline) Run(ctx context.Context, source FeedSource) (RunResult, error) {
	if p == nil || p.Writer == nil {
		return RunResult{}, errors.New("graph writer is required")
	}
	if source == nil {
		return RunResult{}, errNilSource
	}
	if err := ctx.Err(); err != nil {
		return RunResult{}, err
	}

	normalizer := p.Normalizer
	if normalizer.Limits == (Limits{}) {
		normalizer = NewNormalizer(DefaultLimits())
	}
	sourceConfig, err := normalizer.NormalizeSource(source.Config())
	if err != nil {
		return RunResult{}, err
	}
	if !sourceConfig.Enabled {
		return RunResult{}, fmt.Errorf("source %q is disabled", sourceConfig.ID)
	}

	limits := normalizer.limits()
	raw, err := source.Fetch(ctx, limits.MaxItems)
	if err != nil {
		return RunResult{Source: sourceConfig}, fmt.Errorf("fetch source %q: %w", sourceConfig.ID, err)
	}
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now().UTC()
	}
	items, stats, err := normalizer.Normalize(sourceConfig, raw, now)
	if err != nil {
		return RunResult{Source: sourceConfig, RawCount: len(raw)}, err
	}

	if err := p.Writer.UpsertSource(ctx, sourceConfig); err != nil {
		return RunResult{Source: sourceConfig, RawCount: len(raw)}, fmt.Errorf("upsert source %q: %w", sourceConfig.ID, err)
	}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return RunResult{Source: sourceConfig, RawCount: len(raw), Normalized: len(items), Items: items}, err
		}
		if err := p.Writer.UpsertItem(ctx, item); err != nil {
			return RunResult{Source: sourceConfig, RawCount: len(raw), Normalized: len(items), Items: items}, fmt.Errorf("upsert item %q: %w", item.ID, err)
		}
	}

	return RunResult{
		Source:         sourceConfig,
		RawCount:       len(raw),
		Normalized:     stats.Accepted,
		DroppedInvalid: stats.DroppedInvalid,
		Duplicates:     stats.DroppedDuplicate,
		Items:          items,
	}, nil
}
