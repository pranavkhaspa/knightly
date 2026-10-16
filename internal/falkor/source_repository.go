package falkor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/ingestion"
)

const (
	SourceListQuery = `
MATCH (source:Source {tenant_id: $tenant_id, user_id: $user_id})
RETURN source.id AS id, source.tenant_id AS tenant_id, source.user_id AS user_id,
       source.name AS name, source.kind AS kind,
       source.url AS url, source.enabled AS enabled,
       source.trust_score AS trust_score,
       source.poll_interval_seconds AS poll_interval_seconds,
       source.max_items AS max_items,
       source.technology_hints AS technology_hints,
       source.interest_hints AS interest_hints
ORDER BY source.id
LIMIT 1000
`

	SourceCursorQuery = `
MATCH (source:Source {id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
      -[:HAS_CURSOR]->(cursor:SourceCursor {source_id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
RETURN cursor.source_id AS source_id, cursor.tenant_id AS tenant_id,
       cursor.user_id AS user_id, cursor.etag AS etag,
       cursor.last_modified AS last_modified,
       cursor.last_attempt_at AS last_attempt_at,
       cursor.last_success_at AS last_success_at,
       cursor.last_run_id AS last_run_id
LIMIT 1
`

	SourceCursorSaveQuery = `
MATCH (source:Source {id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
MERGE (cursor:SourceCursor {source_id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
SET cursor.etag = $etag,
    cursor.last_modified = $last_modified,
    cursor.last_attempt_at = $last_attempt_at,
    cursor.last_success_at = $last_success_at,
    cursor.last_run_id = $last_run_id,
    cursor.updated_at = $updated_at
MERGE (source)-[:HAS_CURSOR]->(cursor)
RETURN cursor.source_id AS source_id
`

	SourceRunBeginQuery = `
MATCH (source:Source {id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
MERGE (run:SourceRun {id: $run_id, source_id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
ON CREATE SET run.created_at = $started_at
SET run.status = $status,
    run.started_at = $started_at,
    run.finished_at = NULL,
    run.error = "",
    run.raw_count = 0,
    run.normalized_count = 0,
    run.duplicate_count = 0,
    run.not_modified = false,
    run.updated_at = $started_at
MERGE (source)-[:HAS_RUN]->(run)
RETURN run.id AS id
`

	SourceRunCompleteQuery = `
MATCH (run:SourceRun {id: $run_id, source_id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
SET run.status = $status,
    run.finished_at = $finished_at,
    run.error = $error,
    run.raw_count = $raw_count,
    run.normalized_count = $normalized_count,
    run.duplicate_count = $duplicate_count,
    run.not_modified = $not_modified,
    run.updated_at = $finished_at
RETURN run.id AS id
`

	SourceDeleteQuery = `
MATCH (source:Source {id: $source_id})
OPTIONAL MATCH (source)-[:HAS_CURSOR|HAS_RUN]->(state)
DETACH DELETE source, state
`
)

type SourceRepository struct {
	client *Client
	scope  identity.Scope
}

var _ ingestion.SourceRepository = (*SourceRepository)(nil)

func NewSourceRepository(client *Client, scopes ...identity.Scope) *SourceRepository {
	repository := &SourceRepository{client: client}
	if len(scopes) > 0 {
		repository.scope = scopes[0]
	}
	return repository
}

func (r *SourceRepository) UpsertSource(ctx context.Context, source ingestion.SourceConfig) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	var err error
	if source, err = r.applyScope(source); err != nil {
		return err
	}
	return r.client.UpsertSource(ctx, source)
}

func (r *SourceRepository) UpsertItem(ctx context.Context, item ingestion.Item) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if r.scope.TenantID != "" || r.scope.UserID != "" {
		if item.TenantID == "" {
			item.TenantID = r.scope.TenantID
		}
		if item.UserID == "" {
			item.UserID = r.scope.UserID
		}
		if item.TenantID != r.scope.TenantID || item.UserID != r.scope.UserID {
			return errors.New("item is outside the source repository scope")
		}
	}
	return r.client.UpsertItem(ctx, item)
}

func (r *SourceRepository) ListSources(ctx context.Context) ([]ingestion.SourceConfig, error) {
	if err := r.ready(ctx); err != nil {
		return nil, err
	}
	if err := r.scope.Validate(); err != nil {
		return nil, fmt.Errorf("source repository scope: %w", err)
	}
	result, err := r.client.user.ROQuery(SourceListQuery, map[string]interface{}{
		"tenant_id": r.scope.TenantID,
		"user_id":   r.scope.UserID,
	}, r.client.options())
	if err != nil {
		return nil, fmt.Errorf("list sources from graph: %w", err)
	}
	sources := make([]ingestion.SourceConfig, 0)
	for result.Next() {
		source, parseErr := sourceFromRecord(result.Record())
		if parseErr != nil {
			return nil, parseErr
		}
		sources = append(sources, source)
	}
	return sources, nil
}

// DeleteSource removes a configured source and its durable cursor/run state.
// It is intentionally an optional repository capability so the collector seam
// remains focused on reads and collection acknowledgements.
func (r *SourceRepository) DeleteSource(ctx context.Context, sourceID string) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if sourceID == "" {
		return errors.New("source ID is required")
	}
	if _, err := r.client.user.Query(SourceDeleteQuery, map[string]interface{}{"source_id": sourceID}, r.client.options()); err != nil {
		return fmt.Errorf("delete source from graph: %w", err)
	}
	return nil
}

func (r *SourceRepository) GetSourceCursor(ctx context.Context, sourceID string) (ingestion.SourceCursor, bool, error) {
	if err := r.ready(ctx); err != nil {
		return ingestion.SourceCursor{}, false, err
	}
	if sourceID == "" {
		return ingestion.SourceCursor{}, false, errors.New("source ID is required")
	}
	if err := r.scope.Validate(); err != nil {
		return ingestion.SourceCursor{}, false, fmt.Errorf("source repository scope: %w", err)
	}
	result, err := r.client.user.ROQuery(SourceCursorQuery, map[string]interface{}{
		"source_id": sourceID,
		"tenant_id": r.scope.TenantID,
		"user_id":   r.scope.UserID,
	}, r.client.options())
	if err != nil {
		return ingestion.SourceCursor{}, false, fmt.Errorf("get source cursor from graph: %w", err)
	}
	if !result.Next() {
		return ingestion.SourceCursor{SourceID: sourceID, TenantID: r.scope.TenantID, UserID: r.scope.UserID}, false, nil
	}
	cursor, _, parseErr := cursorFromRecord(result.Record())
	return cursor, true, parseErr
}

func (r *SourceRepository) SaveSourceCursor(ctx context.Context, cursor ingestion.SourceCursor) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if cursor.SourceID == "" {
		return errors.New("source ID is required")
	}
	if err := r.scope.Validate(); err != nil {
		return fmt.Errorf("source repository scope: %w", err)
	}
	if cursor.TenantID == "" {
		cursor.TenantID = r.scope.TenantID
	}
	if cursor.UserID == "" {
		cursor.UserID = r.scope.UserID
	}
	if cursor.TenantID != r.scope.TenantID || cursor.UserID != r.scope.UserID {
		return errors.New("source cursor is outside the source repository scope")
	}
	result, err := r.client.user.Query(SourceCursorSaveQuery, map[string]interface{}{
		"source_id":       cursor.SourceID,
		"tenant_id":       cursor.TenantID,
		"user_id":         cursor.UserID,
		"etag":            cursor.ETag,
		"last_modified":   cursor.LastModified,
		"last_attempt_at": optionalMillis(cursor.LastAttemptAt),
		"last_success_at": optionalMillis(cursor.LastSuccessAt),
		"last_run_id":     cursor.LastRunID,
		"updated_at":      time.Now().UTC().UnixMilli(),
	}, r.client.options())
	if err != nil {
		return fmt.Errorf("save source cursor in graph: %w", err)
	}
	return requireQueryRow(result, "save source cursor")
}

func (r *SourceRepository) BeginSourceRun(ctx context.Context, run ingestion.SourceRun) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if run.ID == "" || run.SourceID == "" {
		return errors.New("source run ID and source ID are required")
	}
	if err := r.scope.Validate(); err != nil {
		return fmt.Errorf("source repository scope: %w", err)
	}
	if run.TenantID == "" {
		run.TenantID = r.scope.TenantID
	}
	if run.UserID == "" {
		run.UserID = r.scope.UserID
	}
	if run.TenantID != r.scope.TenantID || run.UserID != r.scope.UserID {
		return errors.New("source run is outside the source repository scope")
	}
	result, err := r.client.user.Query(SourceRunBeginQuery, map[string]interface{}{
		"run_id":     run.ID,
		"source_id":  run.SourceID,
		"tenant_id":  run.TenantID,
		"user_id":    run.UserID,
		"status":     string(ingestion.SourceRunRunning),
		"started_at": optionalMillis(run.StartedAt),
	}, r.client.options())
	if err != nil {
		return fmt.Errorf("begin source run in graph: %w", err)
	}
	return requireQueryRow(result, "begin source run")
}

func (r *SourceRepository) CompleteSourceRun(ctx context.Context, run ingestion.SourceRun) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	if run.ID == "" || run.SourceID == "" {
		return errors.New("source run ID and source ID are required")
	}
	if err := r.scope.Validate(); err != nil {
		return fmt.Errorf("source repository scope: %w", err)
	}
	if run.TenantID == "" {
		run.TenantID = r.scope.TenantID
	}
	if run.UserID == "" {
		run.UserID = r.scope.UserID
	}
	if run.TenantID != r.scope.TenantID || run.UserID != r.scope.UserID {
		return errors.New("source run is outside the source repository scope")
	}
	result, err := r.client.user.Query(SourceRunCompleteQuery, map[string]interface{}{
		"run_id":           run.ID,
		"source_id":        run.SourceID,
		"tenant_id":        run.TenantID,
		"user_id":          run.UserID,
		"status":           string(run.Status),
		"finished_at":      optionalMillis(run.FinishedAt),
		"error":            run.Error,
		"raw_count":        run.RawCount,
		"normalized_count": run.NormalizedCount,
		"duplicate_count":  run.DuplicateCount,
		"not_modified":     run.NotModified,
	}, r.client.options())
	if err != nil {
		return fmt.Errorf("complete source run in graph: %w", err)
	}
	return requireQueryRow(result, "complete source run")
}

// RecoverStaleRuns closes source runs left in running state by a crashed
// collector. A recovered run is failed rather than replayed automatically;
// the next normal due check can safely start a fresh run.
func (r *SourceRepository) RecoverStaleRuns(ctx context.Context, tenantID, userID string, before time.Time) (int, error) {
	if err := r.ready(ctx); err != nil {
		return 0, err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(userID) == "" || before.IsZero() {
		return 0, errors.New("tenant, user, and stale cutoff are required")
	}
	result, err := r.client.user.Query(`
		MATCH (run:SourceRun {tenant_id: $tenant_id, user_id: $user_id, status: "running"})
		WHERE coalesce(run.started_at, run.updated_at, 0) < $cutoff
		SET run.status = $status, run.error = "recovered stale source run",
		    run.finished_at = $recovered_at, run.updated_at = $recovered_at
		RETURN count(run) AS recovered
	`, map[string]interface{}{
		"tenant_id":    tenantID,
		"user_id":      userID,
		"status":       string(ingestion.SourceRunFailed),
		"cutoff":       before.UTC().UnixMilli(),
		"recovered_at": time.Now().UTC().UnixMilli(),
	}, r.client.options())
	if err != nil {
		return 0, fmt.Errorf("recover stale source runs: %w", err)
	}
	if !result.Next() {
		return 0, nil
	}
	return int(recordInt64(recordValue(result.Record(), "recovered"))), nil
}

func (r *SourceRepository) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.client == nil || r.client.user == nil {
		return errors.New("source graph repository is not configured")
	}
	return nil
}

func sourceFromRecord(record recordReader) (ingestion.SourceConfig, error) {
	return ingestion.SourceConfig{
		ID:              stringValue(recordField(record, "id")),
		TenantID:        stringValue(recordField(record, "tenant_id")),
		UserID:          stringValue(recordField(record, "user_id")),
		Name:            stringValue(recordField(record, "name")),
		Kind:            stringValue(recordField(record, "kind")),
		URL:             stringValue(recordField(record, "url")),
		Enabled:         boolValue(recordField(record, "enabled")),
		TrustScore:      float64Value(recordField(record, "trust_score")),
		PollInterval:    time.Duration(int64Value(recordField(record, "poll_interval_seconds"))) * time.Second,
		MaxItems:        int(int64Value(recordField(record, "max_items"))),
		TechnologyHints: stringSliceValue(recordField(record, "technology_hints")),
		InterestHints:   stringSliceValue(recordField(record, "interest_hints")),
	}, nil
}

func cursorFromRecord(record recordReader) (ingestion.SourceCursor, bool, error) {
	return ingestion.SourceCursor{
		SourceID:      stringValue(recordField(record, "source_id")),
		TenantID:      stringValue(recordField(record, "tenant_id")),
		UserID:        stringValue(recordField(record, "user_id")),
		ETag:          stringValue(recordField(record, "etag")),
		LastModified:  stringValue(recordField(record, "last_modified")),
		LastAttemptAt: timeValue(recordField(record, "last_attempt_at")),
		LastSuccessAt: timeValue(recordField(record, "last_success_at")),
		LastRunID:     stringValue(recordField(record, "last_run_id")),
	}, true, nil
}

func (r *SourceRepository) applyScope(source ingestion.SourceConfig) (ingestion.SourceConfig, error) {
	if r.scope.TenantID == "" && r.scope.UserID == "" {
		return source, nil
	}
	if err := r.scope.Validate(); err != nil {
		return ingestion.SourceConfig{}, fmt.Errorf("source repository scope: %w", err)
	}
	if source.TenantID == "" {
		source.TenantID = r.scope.TenantID
	}
	if source.UserID == "" {
		source.UserID = r.scope.UserID
	}
	if source.TenantID != r.scope.TenantID || source.UserID != r.scope.UserID {
		return ingestion.SourceConfig{}, errors.New("source is outside the source repository scope")
	}
	return source, nil
}
