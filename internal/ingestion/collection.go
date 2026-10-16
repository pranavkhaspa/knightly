package ingestion

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

type AdapterResolver interface {
	Resolve(SourceConfig) (SourceAdapter, error)
}

type CollectOptions struct {
	// Force ignores each source's next-due time. It is useful for an explicit
	// operator run while normal scheduled runs should use source intervals.
	Force bool
}

type CollectionSummary struct {
	StartedAt  time.Time
	FinishedAt time.Time
	Sources    int
	Skipped    int
	Succeeded  int
	Failed     int
	Runs       []SourceRun
	Errors     map[string]string
}

type Collector struct {
	Repository SourceRepository
	Pipeline   *Pipeline
	Adapters   AdapterResolver
	Now        func() time.Time
	sequence   uint64
}

// CollectionRunner adapts Collector to scheduler.Runner without coupling the
// ingestion package to the scheduler package. Its optional RunManual method
// preserves the distinction between scheduled and explicit collection.
type CollectionRunner struct {
	Collector *Collector
}

func NewCollectionRunner(collector *Collector) CollectionRunner {
	return CollectionRunner{Collector: collector}
}

func (r CollectionRunner) Run(ctx context.Context) error {
	if r.Collector == nil {
		return errors.New("collection runner requires a collector")
	}
	_, err := r.Collector.Run(ctx)
	return err
}

func (r CollectionRunner) RunManual(ctx context.Context) error {
	if r.Collector == nil {
		return errors.New("collection runner requires a collector")
	}
	_, err := r.Collector.RunNow(ctx)
	return err
}

func NewCollector(repository SourceRepository, pipeline *Pipeline, adapters AdapterResolver) (*Collector, error) {
	if repository == nil {
		return nil, errors.New("source repository is required")
	}
	if pipeline == nil {
		return nil, errors.New("ingestion pipeline is required")
	}
	if adapters == nil {
		return nil, errors.New("source adapter resolver is required")
	}
	return &Collector{
		Repository: repository,
		Pipeline:   pipeline,
		Adapters:   adapters,
		Now:        func() time.Time { return time.Now().UTC() },
	}, nil
}

func (c *Collector) Run(ctx context.Context) (CollectionSummary, error) {
	return c.Collect(ctx, CollectOptions{})
}

func (c *Collector) RunNow(ctx context.Context) (CollectionSummary, error) {
	return c.Collect(ctx, CollectOptions{Force: true})
}

func (c *Collector) Collect(ctx context.Context, options CollectOptions) (CollectionSummary, error) {
	if c == nil || c.Repository == nil || c.Pipeline == nil || c.Adapters == nil {
		return CollectionSummary{}, errors.New("collector dependencies are required")
	}
	if err := ctx.Err(); err != nil {
		return CollectionSummary{}, err
	}
	now := c.now()
	summary := CollectionSummary{StartedAt: now, Errors: make(map[string]string)}
	sources, err := c.Repository.ListSources(ctx)
	if err != nil {
		return summary, fmt.Errorf("list source configuration: %w", err)
	}
	sort.Slice(sources, func(left, right int) bool { return sources[left].ID < sources[right].ID })
	summary.Sources = len(sources)
	for _, configured := range sources {
		if err := ctx.Err(); err != nil {
			summary.FinishedAt = c.now()
			return summary, err
		}
		if !configured.Enabled {
			summary.Skipped++
			continue
		}
		source, normalizeErr := NewNormalizer(DefaultLimits()).NormalizeSource(configured)
		if normalizeErr != nil {
			c.recordFailure(&summary, configured.ID, normalizeErr)
			continue
		}
		cursor, _, cursorErr := c.Repository.GetSourceCursor(ctx, source.ID)
		if cursorErr != nil {
			c.recordFailure(&summary, source.ID, fmt.Errorf("load source cursor: %w", cursorErr))
			continue
		}
		if !options.Force && !sourceDue(source, cursor, now) {
			summary.Skipped++
			continue
		}

		run, runErr := c.collectSource(ctx, source, cursor)
		summary.Runs = append(summary.Runs, run)
		if runErr != nil {
			c.recordFailure(&summary, source.ID, runErr)
			if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
				summary.FinishedAt = c.now()
				return summary, runErr
			}
			continue
		}
		summary.Succeeded++
	}
	summary.FinishedAt = c.now()
	return summary, nil
}

func (c *Collector) collectSource(ctx context.Context, source SourceConfig, cursor SourceCursor) (SourceRun, error) {
	startedAt := c.now()
	run := SourceRun{
		ID:        StableID("source_run", fmt.Sprintf("%s\x00%s\x00%d", source.ID, startedAt.Format(time.RFC3339Nano), atomic.AddUint64(&c.sequence, 1))),
		SourceID:  source.ID,
		StartedAt: startedAt,
		Status:    SourceRunRunning,
	}
	if err := c.Repository.BeginSourceRun(ctx, run); err != nil {
		return run, fmt.Errorf("begin source run %q: %w", source.ID, err)
	}

	cursor.SourceID = source.ID
	cursor.LastAttemptAt = startedAt
	if err := c.Repository.SaveSourceCursor(ctx, cursor); err != nil {
		return c.failRun(ctx, run, fmt.Errorf("save source attempt cursor: %w", err))
	}

	adapter, err := c.Adapters.Resolve(source)
	if err != nil {
		return c.failRun(ctx, run, err)
	}
	maxItems := source.MaxItems
	if maxItems <= 0 || maxItems > hardMaxItems {
		maxItems = DefaultSourceMaxItems
	}
	fetched, err := adapter.FetchWithCursor(ctx, cursor, maxItems)
	if err != nil {
		return c.failRun(ctx, run, fmt.Errorf("fetch source %q: %w", source.ID, err))
	}
	fetched.Cursor.SourceID = source.ID
	fetched.Cursor.LastAttemptAt = startedAt
	fetched.Cursor.LastSuccessAt = c.now()
	fetched.Cursor.LastRunID = run.ID
	run.NotModified = fetched.NotModified
	run.RawCount = len(fetched.Items)
	if !fetched.NotModified {
		result, pipelineErr := c.Pipeline.RunFetched(ctx, source, fetched.Items)
		if pipelineErr != nil {
			return c.failRun(ctx, run, pipelineErr)
		}
		run.NormalizedCount = result.Normalized
		run.DuplicateCount = result.Duplicates
	}
	if err := c.Repository.SaveSourceCursor(ctx, fetched.Cursor); err != nil {
		return c.failRun(ctx, run, fmt.Errorf("save source cursor: %w", err))
	}
	run.Status = SourceRunSucceeded
	run.FinishedAt = c.now()
	if err := c.Repository.CompleteSourceRun(ctx, run); err != nil {
		return run, fmt.Errorf("complete source run %q: %w", source.ID, err)
	}
	return run, nil
}

func (c *Collector) failRun(ctx context.Context, run SourceRun, runErr error) (SourceRun, error) {
	run.Status = SourceRunFailed
	run.Error = runErr.Error()
	run.FinishedAt = c.now()
	// A cancelled collection still records its terminal run state. Repository
	// methods use the context for preflight checks, so detach only this final
	// acknowledgement from the cancelled work context.
	if err := c.Repository.CompleteSourceRun(context.WithoutCancel(ctx), run); err != nil {
		return run, errors.Join(runErr, fmt.Errorf("record failed source run: %w", err))
	}
	return run, runErr
}

func (c *Collector) recordFailure(summary *CollectionSummary, sourceID string, err error) {
	summary.Failed++
	if summary.Errors == nil {
		summary.Errors = make(map[string]string)
	}
	summary.Errors[sourceID] = err.Error()
}

func (c *Collector) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func sourceDue(source SourceConfig, cursor SourceCursor, now time.Time) bool {
	if cursor.LastSuccessAt.IsZero() {
		return true
	}
	interval := source.PollInterval
	if interval <= 0 {
		interval = DefaultSourcePollInterval
	}
	return !now.Before(cursor.LastSuccessAt.Add(interval))
}
