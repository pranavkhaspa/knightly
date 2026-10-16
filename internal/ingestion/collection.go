package ingestion

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pranavkhaspa/knightly/internal/coordination"
)

type AdapterResolver interface {
	Resolve(SourceConfig) (SourceAdapter, error)
}

// SourceLeaseCoordinator is the small coordination hook needed to make a
// collector safe when multiple daemon instances share a graph. It is optional
// for single-process callers and deliberately narrower than the idempotency
// manager because collection only needs a source lease.
type SourceLeaseCoordinator interface {
	AcquireLease(context.Context, coordination.LeaseRequest) (bool, error)
	ReleaseLease(context.Context, string, string) error
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
	Items      []Item
}

type Collector struct {
	Repository    SourceRepository
	Pipeline      *Pipeline
	Adapters      AdapterResolver
	Coordinator   SourceLeaseCoordinator
	LeaseOwner    string
	LeaseLifetime time.Duration
	Now           func() time.Time
	sequence      uint64
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
		Repository:    repository,
		Pipeline:      pipeline,
		Adapters:      adapters,
		LeaseLifetime: 10 * time.Minute,
		Now:           func() time.Time { return time.Now().UTC() },
	}, nil
}

func (c *Collector) Run(ctx context.Context) (CollectionSummary, error) {
	return c.Collect(ctx, CollectOptions{})
}

func (c *Collector) RunNow(ctx context.Context) (CollectionSummary, error) {
	return c.Collect(ctx, CollectOptions{Force: true})
}

// RecoverStaleRuns delegates crash recovery to a graph-backed source
// repository when it supports the optional recovery contract.
func (c *Collector) RecoverStaleRuns(ctx context.Context, tenantID, userID string, before time.Time) (int, error) {
	if c == nil || c.Repository == nil {
		return 0, errors.New("collector repository is required")
	}
	recovery, ok := c.Repository.(StaleRunRecovery)
	if !ok {
		return 0, errors.New("source run recovery is not supported")
	}
	if tenantID == "" || userID == "" || before.IsZero() {
		return 0, errors.New("tenant, user, and stale cutoff are required")
	}
	return recovery.RecoverStaleRuns(ctx, tenantID, userID, before)
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

		run, items, runErr := c.collectSource(ctx, source, cursor)
		summary.Runs = append(summary.Runs, run)
		c.appendItems(&summary, items)
		if runErr != nil {
			if errors.Is(runErr, coordination.ErrLeaseUnavailable) {
				summary.Skipped++
				continue
			}
			summary.Runs = append(summary.Runs, run)
			c.recordFailure(&summary, source.ID, runErr)
			if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
				summary.FinishedAt = c.now()
				return summary, runErr
			}
			continue
		}
		summary.Runs = append(summary.Runs, run)
		summary.Succeeded++
	}
	summary.FinishedAt = c.now()
	return summary, nil
}

func (c *Collector) collectSource(ctx context.Context, source SourceConfig, cursor SourceCursor) (SourceRun, []Item, error) {
	startedAt := c.now()
	run = SourceRun{
		ID:        StableID("source_run", fmt.Sprintf("%s\x00%s\x00%d", source.ID, startedAt.Format(time.RFC3339Nano), atomic.AddUint64(&c.sequence, 1))),
		TenantID:  source.TenantID,
		UserID:    source.UserID,
		SourceID:  source.ID,
		StartedAt: startedAt,
		Status:    SourceRunRunning,
	}
	releaseLease, leaseErr := c.acquireSourceLease(ctx, source, run)
	if leaseErr != nil {
		return run, leaseErr
	}
	if releaseLease != nil {
		defer func() {
			if releaseErr := releaseLease(); releaseErr != nil {
				if err == nil {
					err = fmt.Errorf("release source lease %q: %w", source.ID, releaseErr)
				} else {
					err = errors.Join(err, fmt.Errorf("release source lease %q: %w", source.ID, releaseErr))
				}
			}
		}()
	}
	if err := c.Repository.BeginSourceRun(ctx, run); err != nil {
		return run, nil, fmt.Errorf("begin source run %q: %w", source.ID, err)
	}

	cursor.SourceID = source.ID
	cursor.TenantID = source.TenantID
	cursor.UserID = source.UserID
	cursor.LastAttemptAt = startedAt
	if err := c.Repository.SaveSourceCursor(ctx, cursor); err != nil {
		failed, err := c.failRun(ctx, run, fmt.Errorf("save source attempt cursor: %w", err))
		return failed, nil, err
	}

	adapter, err := c.Adapters.Resolve(source)
	if err != nil {
		failed, err := c.failRun(ctx, run, err)
		return failed, nil, err
	}
	maxItems := source.MaxItems
	if maxItems <= 0 || maxItems > hardMaxItems {
		maxItems = DefaultSourceMaxItems
	}
	fetched, err := adapter.FetchWithCursor(ctx, cursor, maxItems)
	if err != nil {
		failed, err := c.failRun(ctx, run, fmt.Errorf("fetch source %q: %w", source.ID, err))
		return failed, nil, err
	}
	fetched.Cursor.SourceID = source.ID
	fetched.Cursor.TenantID = source.TenantID
	fetched.Cursor.UserID = source.UserID
	fetched.Cursor.LastAttemptAt = startedAt
	fetched.Cursor.LastSuccessAt = c.now()
	fetched.Cursor.LastRunID = run.ID
	run.NotModified = fetched.NotModified
	run.RawCount = len(fetched.Items)
	if !fetched.NotModified {
		result, pipelineErr := c.Pipeline.RunFetched(ctx, source, fetched.Items)
		if pipelineErr != nil {
			failed, err := c.failRun(ctx, run, pipelineErr)
			return failed, nil, err
		}
		run.NormalizedCount = result.Normalized
		run.DuplicateCount = result.Duplicates
		items := result.Items
		if err := c.Repository.SaveSourceCursor(ctx, fetched.Cursor); err != nil {
			failed, err := c.failRun(ctx, run, fmt.Errorf("save source cursor: %w", err))
			return failed, items, err
		}
		run.Status = SourceRunSucceeded
		run.FinishedAt = c.now()
		if err := c.Repository.CompleteSourceRun(ctx, run); err != nil {
			return run, items, fmt.Errorf("complete source run %q: %w", source.ID, err)
		}
		return run, items, nil
	}
	if err := c.Repository.SaveSourceCursor(ctx, fetched.Cursor); err != nil {
		failed, err := c.failRun(ctx, run, fmt.Errorf("save source cursor: %w", err))
		return failed, nil, err
	}
	run.Status = SourceRunSucceeded
	run.FinishedAt = c.now()
	if err := c.Repository.CompleteSourceRun(ctx, run); err != nil {
		return run, nil, fmt.Errorf("complete source run %q: %w", source.ID, err)
	}
	return run, nil, nil
}

func (c *Collector) appendItems(summary *CollectionSummary, items []Item) {
	if len(items) == 0 {
		return
	}
	summary.Items = append(summary.Items, items...)
}

func (c *Collector) acquireSourceLease(ctx context.Context, source SourceConfig, run SourceRun) (func() error, error) {
	if c.Coordinator == nil {
		return nil, nil
	}
	if c.LeaseOwner == "" {
		return nil, errors.New("source lease owner is required when coordination is enabled")
	}
	lifetime := c.LeaseLifetime
	if lifetime <= 0 {
		lifetime = 10 * time.Minute
	}
	key := "source:" + source.TenantID + ":" + source.UserID + ":" + source.ID
	acquired, err := c.Coordinator.AcquireLease(ctx, coordination.LeaseRequest{
		Key:      key,
		Owner:    c.LeaseOwner,
		Token:    run.ID,
		Now:      run.StartedAt,
		Lifetime: lifetime,
	})
	if err != nil {
		return nil, fmt.Errorf("acquire source lease %q: %w", source.ID, err)
	}
	if !acquired {
		return nil, coordination.ErrLeaseUnavailable
	}
	return func() error {
		return c.Coordinator.ReleaseLease(context.Background(), key, run.ID)
	}, nil
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
