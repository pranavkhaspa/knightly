package falkor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pranavkhaspa/knightly/internal/briefing"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

// BriefingRepository keeps daily reports and inbox state in the same routed
// user graph as conversations and tasks. JSON is used only for bounded report
// collections; every addressable inbox item remains a graph node.
type BriefingRepository struct {
	selector UserGraphSelector
}

var _ briefing.Repository = (*BriefingRepository)(nil)

func NewBriefingRepository(selector UserGraphSelector) *BriefingRepository {
	return &BriefingRepository{selector: selector}
}

func (r *BriefingRepository) SaveDaily(ctx context.Context, value briefing.Briefing) (briefing.Briefing, error) {
	scope := identity.Scope{TenantID: value.TenantID, UserID: value.UserID}
	graph, err := routedGraph(ctx, r.selector, scope, "briefing save")
	if err != nil {
		return briefing.Briefing{}, err
	}
	// An idempotent replay returns the first durable report. The service checks
	// the request hash and turns a changed replay into a conflict.
	if existing, getErr := r.GetBriefing(ctx, scope, value.ProjectID, value.Date); getErr == nil {
		// Reconcile every bounded item before returning an existing report. This
		// makes a retry repair a partial item write after a transient graph error.
		for _, item := range value.Items {
			if err := r.saveInboxItem(ctx, existing, item); err != nil {
				return briefing.Briefing{}, err
			}
		}
		return r.GetBriefing(ctx, scope, value.ProjectID, value.Date)
	}
	result, err := graph.Query(`
		MERGE (b:Briefing {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET b.created_at = $created_at
		SET b.tenant_id = $tenant_id, b.user_id = $user_id,
		    b.project_id = $project_id, b.date = $date, b.timezone = $timezone,
		    b.quiet_start = $quiet_start, b.quiet_end = $quiet_end,
		    b.quiet_timezone = $quiet_timezone, b.idempotency_key = $idempotency_key,
		    b.request_hash = $request_hash, b.items_json = $items_json,
		    b.provider_failures_json = $provider_failures_json, b.updated_at = $updated_at
		RETURN b.id AS id
	`, briefingParams(value), queryOptionsFor(r.selector))
	if err != nil {
		return briefing.Briefing{}, fmt.Errorf("save briefing in graph: %w", err)
	}
	if err := requireQueryRow(result, "save briefing"); err != nil {
		return briefing.Briefing{}, err
	}
	for _, item := range value.Items {
		if err := r.saveInboxItem(ctx, value, item); err != nil {
			return briefing.Briefing{}, err
		}
	}
	stored, err := r.GetBriefing(ctx, scope, value.ProjectID, value.Date)
	if err != nil {
		return briefing.Briefing{}, err
	}
	return stored, nil
}

func (r *BriefingRepository) saveInboxItem(ctx context.Context, value briefing.Briefing, item briefing.InboxItem) error {
	graph, err := routedGraph(ctx, r.selector, identity.Scope{TenantID: value.TenantID, UserID: value.UserID}, "inbox save")
	if err != nil {
		return err
	}
	_, err = graph.Query(`
		MATCH (b:Briefing {id: $briefing_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (i:InboxItem {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET i.created_at = $created_at
		SET i.briefing_id = $briefing_id, i.project_id = $project_id, i.date = $date,
		    i.provider = $provider, i.provider_item_id = $provider_item_id,
		    i.kind = $kind, i.title = $title, i.summary = $summary, i.url = $url,
		    i.content_hash = $content_hash, i.published_at = $published_at,
		    i.state = $state, i.read_at = $read_at, i.archived_at = $archived_at,
		    i.updated_at = $updated_at
		MERGE (b)-[:CREATES]->(i)
		RETURN i.id AS id
	`, inboxParams(item), queryOptionsFor(r.selector))
	if err != nil {
		return fmt.Errorf("save inbox item in graph: %w", err)
	}
	return nil
}

func (r *BriefingRepository) GetBriefing(ctx context.Context, scope identity.Scope, projectID string, date briefing.Date) (briefing.Briefing, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "briefing get")
	if err != nil {
		return briefing.Briefing{}, err
	}
	result, err := graph.ROQuery(briefingQuery+" AND b.project_id = $project_id AND b.date = $date"+briefingReturn+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "project_id": projectID, "date": string(date),
	}, queryOptionsFor(r.selector))
	if err != nil {
		return briefing.Briefing{}, fmt.Errorf("get briefing from graph: %w", err)
	}
	if err := requireQueryRow(result, "get briefing"); err != nil {
		return briefing.Briefing{}, fmt.Errorf("%w: %s", briefing.ErrBriefingNotFound, err)
	}
	value := briefingFromRecord(result.Record())
	return r.attachInboxItems(ctx, scope, value)
}

func (r *BriefingRepository) ListBriefings(ctx context.Context, query briefing.BriefingListQuery) ([]briefing.Briefing, error) {
	graph, err := routedGraph(ctx, r.selector, query.Scope, "briefing list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(briefingQuery+` AND ($project_id = "" OR b.project_id = $project_id)
		AND ($from_date = "" OR b.date >= $from_date) AND ($to_date = "" OR b.date <= $to_date)
		`+briefingReturn+` ORDER BY b.date DESC, b.id LIMIT $limit`, map[string]interface{}{
		"tenant_id": query.Scope.TenantID, "user_id": query.Scope.UserID, "project_id": query.ProjectID,
		"from_date": string(query.From), "to_date": string(query.To), "limit": query.Limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list briefings from graph: %w", err)
	}
	values := make([]briefing.Briefing, 0, query.Limit)
	for result.Next() {
		value := briefingFromRecord(result.Record())
		if query.IncludeItems {
			value, err = r.attachInboxItems(ctx, query.Scope, value)
			if err != nil {
				return nil, err
			}
		}
		values = append(values, value)
	}
	return values, nil
}

func (r *BriefingRepository) ListInboxItems(ctx context.Context, query briefing.InboxListQuery) ([]briefing.InboxItem, error) {
	graph, err := routedGraph(ctx, r.selector, query.Scope, "inbox list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(`
		MATCH (i:InboxItem {tenant_id: $tenant_id, user_id: $user_id})
		WHERE ($project_id = "" OR i.project_id = $project_id)
		  AND ($briefing_id = "" OR i.briefing_id = $briefing_id)
		  AND ($date = "" OR i.date = $date)
		  AND ($include_archived OR coalesce(i.state, "unread") <> "archived")
		  AND (NOT $unread_only OR coalesce(i.state, "unread") = "unread")
		RETURN i.id AS id, i.briefing_id AS briefing_id, i.tenant_id AS tenant_id,
		       i.user_id AS user_id, i.project_id AS project_id, i.date AS date,
		       i.provider AS provider, i.provider_item_id AS provider_item_id,
		       i.kind AS kind, i.title AS title, i.summary AS summary, i.url AS url,
		       i.content_hash AS content_hash, i.published_at AS published_at,
		       i.state AS state, i.read_at AS read_at, i.archived_at AS archived_at,
		       i.created_at AS created_at, i.updated_at AS updated_at
		ORDER BY coalesce(i.created_at, 0) DESC, i.id LIMIT $limit
	`, map[string]interface{}{
		"tenant_id": query.Scope.TenantID, "user_id": query.Scope.UserID, "project_id": query.ProjectID,
		"briefing_id": query.BriefingID, "date": string(query.Date), "include_archived": query.IncludeArchived,
		"unread_only": query.UnreadOnly, "limit": query.Limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list inbox from graph: %w", err)
	}
	values := make([]briefing.InboxItem, 0, query.Limit)
	for result.Next() {
		values = append(values, inboxFromRecord(result.Record()))
	}
	return values, nil
}

func (r *BriefingRepository) GetInboxItem(ctx context.Context, scope identity.Scope, id string) (briefing.InboxItem, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "inbox get")
	if err != nil {
		return briefing.InboxItem{}, err
	}
	result, err := graph.ROQuery(inboxQuery+" AND i.id = $id"+inboxReturn+" LIMIT 1", map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": id,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return briefing.InboxItem{}, fmt.Errorf("get inbox item from graph: %w", err)
	}
	if err := requireQueryRow(result, "get inbox item"); err != nil {
		return briefing.InboxItem{}, fmt.Errorf("%w: %s", briefing.ErrInboxItemNotFound, err)
	}
	return inboxFromRecord(result.Record()), nil
}

func (r *BriefingRepository) MarkInboxItemRead(ctx context.Context, scope identity.Scope, id string, at time.Time) (briefing.InboxItem, error) {
	return r.updateInboxState(ctx, scope, id, at, "read")
}

func (r *BriefingRepository) ArchiveInboxItem(ctx context.Context, scope identity.Scope, id string, at time.Time) (briefing.InboxItem, error) {
	return r.updateInboxState(ctx, scope, id, at, "archived")
}

func (r *BriefingRepository) updateInboxState(ctx context.Context, scope identity.Scope, id string, at time.Time, state string) (briefing.InboxItem, error) {
	graph, err := routedGraph(ctx, r.selector, scope, "inbox state update")
	if err != nil {
		return briefing.InboxItem{}, err
	}
	result, err := graph.Query(inboxQuery+` WITH i
		SET i.state = $state,
		    i.read_at = CASE WHEN $state = "read" AND i.read_at IS NULL THEN $at ELSE i.read_at END,
		    i.archived_at = CASE WHEN $state = "archived" AND i.archived_at IS NULL THEN $at ELSE i.archived_at END,
		    i.updated_at = $at
		RETURN i.id AS id, i.briefing_id AS briefing_id, i.tenant_id AS tenant_id,
		       i.user_id AS user_id, i.project_id AS project_id, i.date AS date,
		       i.provider AS provider, i.provider_item_id AS provider_item_id,
		       i.kind AS kind, i.title AS title, i.summary AS summary, i.url AS url,
		       i.content_hash AS content_hash, i.published_at AS published_at,
		       i.state AS state, i.read_at AS read_at, i.archived_at AS archived_at,
		       i.created_at AS created_at, i.updated_at AS updated_at`, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "id": id, "state": state, "at": at.UTC().UnixMilli(),
	}, queryOptionsFor(r.selector))
	if err != nil {
		return briefing.InboxItem{}, fmt.Errorf("update inbox item in graph: %w", err)
	}
	if err := requireQueryRow(result, "update inbox item"); err != nil {
		return briefing.InboxItem{}, fmt.Errorf("%w: %s", briefing.ErrInboxItemNotFound, err)
	}
	return inboxFromRecord(result.Record()), nil
}

const briefingQuery = `
	MATCH (b:Briefing {tenant_id: $tenant_id, user_id: $user_id})
	WHERE true
	`

const briefingReturn = `
	RETURN b.id AS id, b.tenant_id AS tenant_id, b.user_id AS user_id,
	       b.project_id AS project_id, b.date AS date, b.timezone AS timezone,
	       b.quiet_start AS quiet_start, b.quiet_end AS quiet_end,
	       b.quiet_timezone AS quiet_timezone, b.idempotency_key AS idempotency_key,
	       b.request_hash AS request_hash, b.items_json AS items_json,
	       b.provider_failures_json AS provider_failures_json,
	       b.created_at AS created_at, b.updated_at AS updated_at
`

const inboxQuery = `
	MATCH (i:InboxItem {tenant_id: $tenant_id, user_id: $user_id})
	WHERE true
`

const inboxReturn = `
	RETURN i.id AS id, i.briefing_id AS briefing_id, i.tenant_id AS tenant_id,
	       i.user_id AS user_id, i.project_id AS project_id, i.date AS date,
	       i.provider AS provider, i.provider_item_id AS provider_item_id,
	       i.kind AS kind, i.title AS title, i.summary AS summary, i.url AS url,
	       i.content_hash AS content_hash, i.published_at AS published_at,
	       i.state AS state, i.read_at AS read_at, i.archived_at AS archived_at,
	       i.created_at AS created_at, i.updated_at AS updated_at
`

func briefingParams(value briefing.Briefing) map[string]interface{} {
	return map[string]interface{}{
		"id": value.ID, "tenant_id": value.TenantID, "user_id": value.UserID, "project_id": value.ProjectID,
		"date": string(value.Date), "timezone": value.Timezone, "quiet_start": value.QuietHours.Start,
		"quiet_end": value.QuietHours.End, "quiet_timezone": value.QuietHours.Timezone,
		"idempotency_key": value.IdempotencyKey, "request_hash": value.RequestHash,
		"items_json": marshalJSON(value.Items), "provider_failures_json": marshalJSON(value.ProviderFailures),
		"created_at": optionalMillis(value.CreatedAt), "updated_at": optionalMillis(value.UpdatedAt),
	}
}

func inboxParams(value briefing.InboxItem) map[string]interface{} {
	return map[string]interface{}{
		"id": value.ID, "briefing_id": value.BriefingID, "tenant_id": value.TenantID, "user_id": value.UserID,
		"project_id": value.ProjectID, "date": string(value.Date), "provider": value.Provider,
		"provider_item_id": value.ProviderItemID, "kind": value.Kind, "title": value.Title, "summary": value.Summary,
		"url": value.URL, "content_hash": value.ContentHash, "published_at": optionalMillis(value.PublishedAt),
		"state": string(value.State), "read_at": optionalMillis(value.ReadAt), "archived_at": optionalMillis(value.ArchivedAt),
		"created_at": optionalMillis(value.CreatedAt), "updated_at": optionalMillis(value.UpdatedAt),
	}
}

func (r *BriefingRepository) attachInboxItems(ctx context.Context, scope identity.Scope, value briefing.Briefing) (briefing.Briefing, error) {
	items, err := r.ListInboxItems(ctx, briefing.InboxListQuery{Scope: scope, BriefingID: value.ID, ProjectID: value.ProjectID, IncludeArchived: true, Limit: briefing.MaxListLimit})
	if err != nil {
		return briefing.Briefing{}, err
	}
	value.Items = items
	return value, nil
}

func briefingFromRecord(record recordReader) briefing.Briefing {
	value := briefing.Briefing{
		ID: stringValue(recordField(record, "id")), TenantID: stringValue(recordField(record, "tenant_id")),
		UserID: stringValue(recordField(record, "user_id")), ProjectID: stringValue(recordField(record, "project_id")),
		Date: briefing.Date(stringValue(recordField(record, "date"))), Timezone: stringValue(recordField(record, "timezone")),
		QuietHours:     briefing.QuietHours{Start: stringValue(recordField(record, "quiet_start")), End: stringValue(recordField(record, "quiet_end")), Timezone: stringValue(recordField(record, "quiet_timezone"))},
		IdempotencyKey: stringValue(recordField(record, "idempotency_key")), RequestHash: stringValue(recordField(record, "request_hash")),
		CreatedAt: timeValue(recordField(record, "created_at")), UpdatedAt: timeValue(recordField(record, "updated_at")),
	}
	_ = json.Unmarshal([]byte(stringValue(recordField(record, "items_json"))), &value.Items)
	_ = json.Unmarshal([]byte(stringValue(recordField(record, "provider_failures_json"))), &value.ProviderFailures)
	return value
}

func inboxFromRecord(record recordReader) briefing.InboxItem {
	return briefing.InboxItem{
		ID: stringValue(recordField(record, "id")), BriefingID: stringValue(recordField(record, "briefing_id")),
		TenantID: stringValue(recordField(record, "tenant_id")), UserID: stringValue(recordField(record, "user_id")),
		ProjectID: stringValue(recordField(record, "project_id")), Date: briefing.Date(stringValue(recordField(record, "date"))),
		Provider: stringValue(recordField(record, "provider")), ProviderItemID: stringValue(recordField(record, "provider_item_id")),
		Kind: stringValue(recordField(record, "kind")), Title: stringValue(recordField(record, "title")),
		Summary: stringValue(recordField(record, "summary")), URL: stringValue(recordField(record, "url")),
		ContentHash: stringValue(recordField(record, "content_hash")), PublishedAt: timeValue(recordField(record, "published_at")),
		State: briefing.InboxState(stringValue(recordField(record, "state"))), ReadAt: timeValue(recordField(record, "read_at")),
		ArchivedAt: timeValue(recordField(record, "archived_at")), CreatedAt: timeValue(recordField(record, "created_at")),
		UpdatedAt: timeValue(recordField(record, "updated_at")),
	}
}

func marshalJSON(value interface{}) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
