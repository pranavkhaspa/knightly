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
	maxItems := limits.MaxItems
	if sourceConfig.MaxItems > 0 && sourceConfig.MaxItems < maxItems {
		maxItems = sourceConfig.MaxItems
	}
	raw, err := source.Fetch(ctx, maxItems)
	if err != nil {
		return RunResult{Source: sourceConfig}, fmt.Errorf("fetch source %q: %w", sourceConfig.ID, err)
	}
	return p.runFetched(ctx, normalizer, sourceConfig, raw)
}

// RunFetched persists already-fetched adapter output. Collection uses this
// method so a conditional request is made exactly once and its cursor is only
// acknowledged after all normalized graph writes succeed.
func (p *Pipeline) RunFetched(ctx context.Context, source SourceConfig, raw []RawItem) (RunResult, error) {
	if p == nil || p.Writer == nil {
		return RunResult{}, errors.New("graph writer is required")
	}
	if err := ctx.Err(); err != nil {
		return RunResult{}, err
	}
	normalizer := p.Normalizer
	if normalizer.Limits == (Limits{}) {
		normalizer = NewNormalizer(DefaultLimits())
	}
	sourceConfig, err := normalizer.NormalizeSource(source)
	if err != nil {
		return RunResult{}, err
	}
	if !sourceConfig.Enabled {
		return RunResult{}, fmt.Errorf("source %q is disabled", sourceConfig.ID)
	}
	return p.runFetched(ctx, normalizer, sourceConfig, raw)
}

func (p *Pipeline) runFetched(ctx context.Context, normalizer Normalizer, sourceConfig SourceConfig, raw []RawItem) (RunResult, error) {
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
