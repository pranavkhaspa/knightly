package falkor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pranavkhaspa/knightly/internal/ingestion"
)

const (
	SourceListQuery = `
MATCH (source:Source)
RETURN source.id AS id, source.name AS name, source.kind AS kind,
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
MATCH (source:Source {id: $source_id})-[:HAS_CURSOR]->(cursor:SourceCursor {source_id: $source_id})
RETURN cursor.source_id AS source_id, cursor.etag AS etag,
       cursor.last_modified AS last_modified,
       cursor.last_attempt_at AS last_attempt_at,
       cursor.last_success_at AS last_success_at,
       cursor.last_run_id AS last_run_id
LIMIT 1
`

	SourceCursorSaveQuery = `
MATCH (source:Source {id: $source_id})
MERGE (cursor:SourceCursor {source_id: $source_id})
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
MATCH (source:Source {id: $source_id})
MERGE (run:SourceRun {id: $run_id, source_id: $source_id})
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
MATCH (run:SourceRun {id: $run_id, source_id: $source_id})
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
}

var _ ingestion.SourceRepository = (*SourceRepository)(nil)

func NewSourceRepository(client *Client) *SourceRepository {
	return &SourceRepository{client: client}
}

func (r *SourceRepository) UpsertSource(ctx context.Context, source ingestion.SourceConfig) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	return r.client.UpsertSource(ctx, source)
}

func (r *SourceRepository) UpsertItem(ctx context.Context, item ingestion.Item) error {
	if err := r.ready(ctx); err != nil {
		return err
	}
	return r.client.UpsertItem(ctx, item)
}

func (r *SourceRepository) ListSources(ctx context.Context) ([]ingestion.SourceConfig, error) {
	if err := r.ready(ctx); err != nil {
		return nil, err
	}
	result, err := r.client.user.ROQuery(SourceListQuery, nil, r.client.options())
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
	result, err := r.client.user.ROQuery(SourceCursorQuery, map[string]interface{}{"source_id": sourceID}, r.client.options())
	if err != nil {
		return ingestion.SourceCursor{}, false, fmt.Errorf("get source cursor from graph: %w", err)
	}
	if !result.Next() {
		return ingestion.SourceCursor{SourceID: sourceID}, false, nil
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
	result, err := r.client.user.Query(SourceCursorSaveQuery, map[string]interface{}{
		"source_id":       cursor.SourceID,
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
	result, err := r.client.user.Query(SourceRunBeginQuery, map[string]interface{}{
		"run_id":     run.ID,
		"source_id":  run.SourceID,
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
	result, err := r.client.user.Query(SourceRunCompleteQuery, map[string]interface{}{
		"run_id":           run.ID,
		"source_id":        run.SourceID,
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
		ETag:          stringValue(recordField(record, "etag")),
		LastModified:  stringValue(recordField(record, "last_modified")),
		LastAttemptAt: timeValue(recordField(record, "last_attempt_at")),
		LastSuccessAt: timeValue(recordField(record, "last_success_at")),
		LastRunID:     stringValue(recordField(record, "last_run_id")),
	}, true, nil
}

func optionalMillis(value time.Time) interface{} {
	if value.IsZero() {
		return nil
	}
	return value.UTC().UnixMilli()
}
