package falkor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pranavkhaspa/knightly/internal/calendar"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

const CalendarAccountListQuery = `
	MATCH (a:ProviderAccount {tenant_id: $tenant_id, user_id: $user_id, provider: $provider})
	RETURN a.id AS id, a.tenant_id AS tenant_id, a.user_id AS user_id,
	       a.provider AS provider, a.calendar_id AS calendar_id, a.email_address AS email_address,
	       a.scopes AS scopes, a.status AS status,
	       a.connected_at AS connected_at, a.updated_at AS updated_at, a.last_sync_at AS last_sync_at
	ORDER BY a.connected_at DESC
	LIMIT $limit
`

const CalendarAccountGetQuery = `
	MATCH (a:ProviderAccount {id: $account_id, tenant_id: $tenant_id, user_id: $user_id})
	RETURN a.id AS id, a.tenant_id AS tenant_id, a.user_id AS user_id,
	       a.provider AS provider, a.calendar_id AS calendar_id, a.email_address AS email_address,
	       a.scopes AS scopes, a.status AS status,
	       a.connected_at AS connected_at, a.updated_at AS updated_at, a.last_sync_at AS last_sync_at
	LIMIT 1
`

const CalendarAccountSaveQuery = `
	MERGE (a:ProviderAccount {id: $account_id, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET a.created_at = $updated_at
	SET a.provider = $provider,
	    a.calendar_id = $calendar_id,
	    a.email_address = $email_address,
	    a.scopes = $scopes,
	    a.status = $status,
	    a.connected_at = $connected_at,
	    a.last_sync_at = $last_sync_at,
	    a.updated_at = $updated_at
	RETURN a.id AS id
`

const CalendarEventSaveQuery = `
	UNWIND $rows AS row
	MERGE (a:ProviderAccount {id: row.account_id, tenant_id: $tenant_id, user_id: $user_id})
	MERGE (e:CalendarEvent {id: row.id, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET e.created_at = $observed_at
	SET e.account_id = row.account_id,
	    e.calendar_id = row.calendar_id,
	    e.summary = row.summary,
	    e.description_excerpt = row.description_excerpt,
	    e.location = row.location,
	    e.start_at = row.start_at,
	    e.end_at = row.end_at,
	    e.all_day = row.all_day,
	    e.status = row.status,
	    e.attendees_json = row.attendees_json,
	    e.recurring_event_id = row.recurring_event_id,
	    e.html_link = row.html_link,
	    e.event_updated_at = row.event_updated_at,
	    e.untrusted_origin = row.origin,
	    e.content_hash = row.content_hash,
	    e.observed_at = $observed_at,
	    e.updated_at = $observed_at
	MERGE (a)-[:HAS_EVENT]->(e)
	MERGE (s:CalendarSource {id: row.calendar_id, tenant_id: $tenant_id, user_id: $user_id})
	MERGE (e)-[:IN_CALENDAR]->(s)
	RETURN count(e) AS stored
`

const CalendarEventListQuery = `
	MATCH (a:ProviderAccount {tenant_id: $tenant_id, user_id: $user_id})-[:HAS_EVENT]->(e:CalendarEvent)
	WHERE $account_id = "" OR e.account_id = $account_id
	RETURN e.id AS id, e.account_id AS account_id, e.calendar_id AS calendar_id,
	       e.summary AS summary, e.description_excerpt AS description_excerpt, e.location AS location,
	       e.start_at AS start_at, e.end_at AS end_at, e.all_day AS all_day, e.status AS status,
	       e.attendees_json AS attendees_json, e.recurring_event_id AS recurring_event_id,
	       e.html_link AS html_link, e.event_updated_at AS event_updated_at,
	       e.untrusted_origin AS untrusted_origin
	ORDER BY e.start_at ASC
	LIMIT $limit
`

const CalendarEventGetQuery = `
	MATCH (e:CalendarEvent {id: $event_id, tenant_id: $tenant_id, user_id: $user_id})
	RETURN e.id AS id, e.account_id AS account_id, e.calendar_id AS calendar_id,
	       e.summary AS summary, e.description_excerpt AS description_excerpt, e.location AS location,
	       e.start_at AS start_at, e.end_at AS end_at, e.all_day AS all_day, e.status AS status,
	       e.attendees_json AS attendees_json, e.recurring_event_id AS recurring_event_id,
	       e.html_link AS html_link, e.event_updated_at AS event_updated_at,
	       e.untrusted_origin AS untrusted_origin
	LIMIT 1
`

// CalendarAccountRepository persists bounded provider-account metadata.
type CalendarAccountRepository struct {
	selector UserGraphSelector
}

var _ calendar.AccountStore = (*CalendarAccountRepository)(nil)

func NewCalendarAccountRepository(selector UserGraphSelector) *CalendarAccountRepository {
	return &CalendarAccountRepository{selector: selector}
}

func (r *CalendarAccountRepository) SaveAccount(ctx context.Context, scope identity.Scope, account calendar.Account) (calendar.Account, error) {
	if err := scope.Validate(); err != nil {
		return calendar.Account{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "calendar account save")
	if err != nil {
		return calendar.Account{}, err
	}
	account.TenantID = scope.TenantID
	account.UserID = scope.UserID
	if account.UpdatedAt.IsZero() {
		account.UpdatedAt = time.Now().UTC()
	}
	if _, err := graph.Query(CalendarAccountSaveQuery, map[string]interface{}{
		"account_id": account.ID, "tenant_id": scope.TenantID, "user_id": scope.UserID,
		"provider": account.Provider, "calendar_id": account.CalendarID, "email_address": account.EmailAddress,
		"scopes": account.Scopes, "status": string(account.Status),
		"connected_at": optionalMillis(account.ConnectedAt), "last_sync_at": optionalMillis(account.LastSyncAt),
		"updated_at": optionalMillis(account.UpdatedAt),
	}, queryOptionsFor(r.selector)); err != nil {
		return calendar.Account{}, fmt.Errorf("save calendar account in graph: %w", err)
	}
	return account, nil
}

func (r *CalendarAccountRepository) GetAccount(ctx context.Context, scope identity.Scope, accountID string) (calendar.Account, error) {
	if err := scope.Validate(); err != nil {
		return calendar.Account{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "calendar account get")
	if err != nil {
		return calendar.Account{}, err
	}
	result, err := graph.ROQuery(CalendarAccountGetQuery, map[string]interface{}{
		"account_id": accountID, "tenant_id": scope.TenantID, "user_id": scope.UserID,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return calendar.Account{}, fmt.Errorf("get calendar account from graph: %w", err)
	}
	if result == nil || !result.Next() {
		return calendar.Account{}, fmt.Errorf("%w: %s", calendar.ErrAccountNotFound, accountID)
	}
	return calendarAccountFromRecord(result.Record()), nil
}

func (r *CalendarAccountRepository) ListAccounts(ctx context.Context, scope identity.Scope, limit int) ([]calendar.Account, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "calendar account list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(CalendarAccountListQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "provider": calendar.ProviderGoogleCalendar, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list calendar accounts from graph: %w", err)
	}
	accounts := make([]calendar.Account, 0)
	for result != nil && result.Next() {
		accounts = append(accounts, calendarAccountFromRecord(result.Record()))
	}
	return accounts, nil
}

// CalendarEventRepository persists bounded normalized event records.
type CalendarEventRepository struct {
	selector UserGraphSelector
	now      func() time.Time
}

var _ calendar.EventStore = (*CalendarEventRepository)(nil)

func NewCalendarEventRepository(selector UserGraphSelector) *CalendarEventRepository {
	return &CalendarEventRepository{selector: selector, now: func() time.Time { return time.Now().UTC() }}
}

func (r *CalendarEventRepository) SaveEvents(ctx context.Context, scope identity.Scope, events []calendar.Event) ([]calendar.Event, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}
	graph, err := routedGraph(ctx, r.selector, scope, "calendar event save")
	if err != nil {
		return nil, err
	}
	observedAt := r.now().UTC()
	normalized := make([]calendar.Event, 0, len(events))
	rows := make([]map[string]interface{}, 0, len(events))
	for _, event := range events {
		value, err := calendar.NormalizeEvent(event, time.UTC)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, value)
		rows = append(rows, map[string]interface{}{
			"id": value.ID, "account_id": value.AccountID, "calendar_id": value.CalendarID,
			"summary": value.Summary, "description_excerpt": value.Description, "location": value.Location,
			"start_at": optionalMillis(value.Start), "end_at": optionalMillis(value.End), "all_day": value.AllDay,
			"status": value.Status, "attendees_json": encodeAttendees(value.Attendees),
			"recurring_event_id": value.RecurringEventID, "html_link": value.HTMLLink,
			"event_updated_at": optionalMillis(value.Updated), "origin": value.Origin,
			"content_hash": calendarContentHash(value),
		})
	}
	if _, err := graph.Query(CalendarEventSaveQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "rows": rows, "observed_at": optionalMillis(observedAt),
	}, queryOptionsFor(r.selector)); err != nil {
		return nil, fmt.Errorf("save calendar events in graph: %w", err)
	}
	return normalized, nil
}

func (r *CalendarEventRepository) ListEvents(ctx context.Context, scope identity.Scope, accountID string, limit int) ([]calendar.Event, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "calendar event list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(CalendarEventListQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "account_id": accountID, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list calendar events from graph: %w", err)
	}
	events := make([]calendar.Event, 0)
	for result != nil && result.Next() {
		events = append(events, calendarEventFromRecord(result.Record()))
	}
	return events, nil
}

func (r *CalendarEventRepository) GetEvent(ctx context.Context, scope identity.Scope, eventID string) (calendar.Event, error) {
	if err := scope.Validate(); err != nil {
		return calendar.Event{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "calendar event get")
	if err != nil {
		return calendar.Event{}, err
	}
	result, err := graph.ROQuery(CalendarEventGetQuery, map[string]interface{}{
		"event_id": eventID, "tenant_id": scope.TenantID, "user_id": scope.UserID,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return calendar.Event{}, fmt.Errorf("get calendar event from graph: %w", err)
	}
	if result == nil || !result.Next() {
		return calendar.Event{}, fmt.Errorf("%w: %s", calendar.ErrEventNotFound, eventID)
	}
	return calendarEventFromRecord(result.Record()), nil
}

func calendarAccountFromRecord(record recordReader) calendar.Account {
	return calendar.Account{
		ID:           stringValue(recordField(record, "id")),
		TenantID:     stringValue(recordField(record, "tenant_id")),
		UserID:       stringValue(recordField(record, "user_id")),
		Provider:     stringValue(recordField(record, "provider")),
		CalendarID:   stringValue(recordField(record, "calendar_id")),
		EmailAddress: stringValue(recordField(record, "email_address")),
		Scopes:       stringSliceValue(recordField(record, "scopes")),
		Status:       calendar.AccountStatus(stringValue(recordField(record, "status"))),
		ConnectedAt:  timeValue(recordField(record, "connected_at")),
		UpdatedAt:    timeValue(recordField(record, "updated_at")),
		LastSyncAt:   timeValue(recordField(record, "last_sync_at")),
	}
}

func calendarEventFromRecord(record recordReader) calendar.Event {
	event := calendar.Event{
		ID:               stringValue(recordField(record, "id")),
		AccountID:        stringValue(recordField(record, "account_id")),
		CalendarID:       stringValue(recordField(record, "calendar_id")),
		Summary:          stringValue(recordField(record, "summary")),
		Description:      stringValue(recordField(record, "description_excerpt")),
		Location:         stringValue(recordField(record, "location")),
		Start:            timeValue(recordField(record, "start_at")),
		End:              timeValue(recordField(record, "end_at")),
		AllDay:           boolValue(recordField(record, "all_day")),
		Status:           stringValue(recordField(record, "status")),
		Attendees:        decodeAttendees(stringValue(recordField(record, "attendees_json"))),
		RecurringEventID: stringValue(recordField(record, "recurring_event_id")),
		Updated:          timeValue(recordField(record, "event_updated_at")),
		Origin:           stringValue(recordField(record, "untrusted_origin")),
	}
	if event.Origin == "" {
		event.Origin = calendar.UntrustedOrigin
	}
	return event
}

func calendarContentHash(value calendar.Event) string {
	sum := sha256.Sum256([]byte(value.Summary + "\x00" + value.Location + "\x00" + value.Description + "\x00" + value.Status))
	return hex.EncodeToString(sum[:])
}

func encodeAttendees(attendees []calendar.Attendee) string {
	if len(attendees) == 0 {
		return ""
	}
	encoded, err := json.Marshal(attendees)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func decodeAttendees(encoded string) []calendar.Attendee {
	if encoded == "" || encoded == "<nil>" {
		return nil
	}
	var attendees []calendar.Attendee
	if err := json.Unmarshal([]byte(encoded), &attendees); err != nil {
		return nil
	}
	return attendees
}
