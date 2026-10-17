package calendar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/googleauth"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

// CalendarReadOnlyScope is the only Google scope used in phase P0. It cannot
// create, edit, or delete events or respond to invitations.
const CalendarReadOnlyScope = "https://www.googleapis.com/auth/calendar.readonly"

const (
	googleCalendarBaseURL = "https://www.googleapis.com"
	tokenRefreshSkew      = time.Minute
)

var (
	ErrCalendarConfig       = errors.New("invalid Google Calendar reader configuration")
	ErrProviderRequest      = errors.New("Google Calendar API request failed")
	ErrProviderUnauthorized = errors.New("Google Calendar API rejected the access token")
	ErrTokenExpired         = errors.New("calendar access token is expired and cannot be refreshed")
	ErrScopeMissing         = errors.New("granted OAuth scope is missing calendar.readonly")
)

// GoogleConfig configures a read-only Google Calendar reader bound to one account.
type GoogleConfig struct {
	BaseURL     string
	Scope       identity.Scope
	Account     Account
	Location    *time.Location
	OAuth       *googleauth.Client
	Credentials googleauth.Store
	Client      *http.Client
	Clock       func() time.Time
	MaxBodySize int64
}

// GoogleCalendarReader reads events through the Calendar REST API using only
// GET requests. It has no method that mutates a calendar.
type GoogleCalendarReader struct {
	baseURL     *url.URL
	scope       identity.Scope
	account     Account
	location    *time.Location
	oauth       *googleauth.Client
	credentials googleauth.Store
	client      *http.Client
	now         func() time.Time
	maxBody     int64
	tokens      googleauth.TokenSet
}

var _ CalendarReader = (*GoogleCalendarReader)(nil)

func NewGoogleCalendarReader(config GoogleConfig) (*GoogleCalendarReader, error) {
	if config.OAuth == nil {
		return nil, fmt.Errorf("%w: OAuth client is required", ErrCalendarConfig)
	}
	account := config.Account
	account.ID = strings.TrimSpace(account.ID)
	if account.ID == "" {
		return nil, fmt.Errorf("%w: account ID is required", ErrCalendarConfig)
	}
	if err := config.Scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCalendarConfig, err)
	}
	baseURL, err := googleauth.ValidateEndpoint(config.BaseURL, googleCalendarBaseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCalendarConfig, err)
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	now := config.Clock
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	location := config.Location
	if location == nil {
		location = time.UTC
	}
	reader := &GoogleCalendarReader{
		baseURL: baseURL, scope: config.Scope, account: account, location: location,
		oauth: config.OAuth, credentials: config.Credentials, client: client, now: now, maxBody: config.MaxBodySize,
	}
	if reader.maxBody <= 0 {
		reader.maxBody = 1 << 20
	}
	if config.Credentials != nil {
		tokens, loadErr := config.Credentials.Load(context.Background(), config.Scope, account.ID)
		if loadErr != nil && !errors.Is(loadErr, googleauth.ErrCredentialNotFound) {
			return nil, fmt.Errorf("load calendar credentials: %w", loadErr)
		}
		reader.tokens = tokens
	}
	return reader, nil
}

func (r *GoogleCalendarReader) ListEvents(ctx context.Context, scope identity.Scope, query ListQuery) (EventPage, error) {
	if err := r.checkScope(scope); err != nil {
		return EventPage{}, err
	}
	query, err := NormalizeListQuery(query)
	if err != nil {
		return EventPage{}, err
	}
	timeMin, timeMax := query.TimeMin, query.TimeMax
	switch query.Window {
	case WindowToday:
		timeMin, timeMax = DayWindow(r.now(), r.location)
	case WindowUpcoming:
		if timeMin.IsZero() {
			timeMin = r.now().In(r.location)
		}
	}
	endpoint := r.endpoint("/calendar/v3/calendars/" + url.PathEscape(query.CalendarID) + "/events")
	params := endpoint.Query()
	params.Set("singleEvents", "true")
	params.Set("orderBy", "startTime")
	params.Set("maxResults", strconv.Itoa(query.Limit))
	if !timeMin.IsZero() {
		params.Set("timeMin", timeMin.UTC().Format(time.RFC3339))
	}
	if !timeMax.IsZero() {
		params.Set("timeMax", timeMax.UTC().Format(time.RFC3339))
	}
	if query.PageToken != "" {
		params.Set("pageToken", query.PageToken)
	}
	endpoint.RawQuery = params.Encode()

	body, err := r.get(ctx, endpoint)
	if err != nil {
		return EventPage{}, err
	}
	var listed googleEventList
	if err := json.Unmarshal(body, &listed); err != nil {
		return EventPage{}, fmt.Errorf("%w: decode event list", ErrProviderRequest)
	}
	events := make([]Event, 0, min(len(listed.Items), query.Limit))
	for _, raw := range listed.Items {
		if len(events) == query.Limit {
			break
		}
		event, eventErr := r.toEvent(query.CalendarID, raw)
		if eventErr != nil {
			return EventPage{}, eventErr
		}
		events = append(events, event)
	}
	return EventPage{Events: events, NextPageToken: bound(listed.NextPageToken, MaxPageTokenSize)}, nil
}

func (r *GoogleCalendarReader) GetEvent(ctx context.Context, scope identity.Scope, calendarID, eventID string) (Event, error) {
	if err := r.checkScope(scope); err != nil {
		return Event{}, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || len(eventID) > MaxEventIDSize {
		return Event{}, fmt.Errorf("%w: event ID is required and bounded", ErrInvalidEvent)
	}
	if strings.TrimSpace(calendarID) == "" {
		calendarID = r.account.CalendarID
	}
	if calendarID == "" {
		calendarID = "primary"
	}
	body, err := r.get(ctx, r.endpoint("/calendar/v3/calendars/"+url.PathEscape(calendarID)+"/events/"+url.PathEscape(eventID)))
	if err != nil {
		return Event{}, err
	}
	var raw googleEvent
	if err := json.Unmarshal(body, &raw); err != nil {
		return Event{}, fmt.Errorf("%w: decode event %q", ErrProviderRequest, eventID)
	}
	return r.toEvent(calendarID, raw)
}

func (r *GoogleCalendarReader) checkScope(scope identity.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if scope != r.scope {
		return fmt.Errorf("%w: reader is bound to a different scope", ErrScopeRequired)
	}
	return nil
}

func (r *GoogleCalendarReader) toEvent(calendarID string, raw googleEvent) (Event, error) {
	start, allDay := parseEventTime(raw.Start, r.location)
	end, _ := parseEventTime(raw.End, r.location)
	event := Event{
		ID:               raw.ID,
		AccountID:        r.account.ID,
		CalendarID:       calendarID,
		Summary:          raw.Summary,
		Description:      raw.Description,
		Location:         raw.Location,
		Start:            start,
		End:              end,
		AllDay:           allDay,
		Status:           raw.Status,
		RecurringEventID: raw.RecurringEventID,
		HTMLLink:         raw.HTMLLink,
		Updated:          parseRFC3339(raw.Updated),
		Attendees:        toAttendees(raw.Attendees),
	}
	return NormalizeEvent(event, r.location)
}

func (r *GoogleCalendarReader) endpoint(path string) *url.URL {
	endpoint := *r.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawPath = ""
	endpoint.RawQuery = ""
	endpoint.Fragment = ""
	return &endpoint
}

func (r *GoogleCalendarReader) get(ctx context.Context, endpoint *url.URL) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.authorize(ctx); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Google Calendar request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+r.tokens.AccessToken)
	request.Header.Set("Accept", "application/json")
	response, err := r.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: transport error", ErrProviderRequest)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, r.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read response", ErrProviderRequest)
	}
	if int64(len(body)) > r.maxBody {
		return nil, fmt.Errorf("%w: response is too large", ErrProviderRequest)
	}
	switch {
	case response.StatusCode == http.StatusUnauthorized:
		return nil, ErrProviderUnauthorized
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return nil, fmt.Errorf("%w: provider returned HTTP %d", ErrProviderRequest, response.StatusCode)
	}
	return body, nil
}

func (r *GoogleCalendarReader) authorize(ctx context.Context) error {
	if r.tokens.Valid(r.now(), tokenRefreshSkew) {
		return nil
	}
	if strings.TrimSpace(r.tokens.RefreshToken) == "" {
		return ErrTokenExpired
	}
	refreshed, err := r.oauth.Refresh(ctx, r.tokens)
	if err != nil {
		return err
	}
	r.tokens = refreshed
	if r.credentials != nil {
		if err := r.credentials.Save(ctx, r.scope, r.account.ID, refreshed); err != nil {
			return fmt.Errorf("persist refreshed calendar credentials: %w", err)
		}
	}
	return nil
}

type googleEventList struct {
	Items         []googleEvent `json:"items"`
	NextPageToken string        `json:"nextPageToken"`
}

type googleEvent struct {
	ID               string           `json:"id"`
	Summary          string           `json:"summary"`
	Description      string           `json:"description"`
	Location         string           `json:"location"`
	Status           string           `json:"status"`
	HTMLLink         string           `json:"htmlLink"`
	Updated          string           `json:"updated"`
	RecurringEventID string           `json:"recurringEventId"`
	Start            googleEventTime  `json:"start"`
	End              googleEventTime  `json:"end"`
	Attendees        []googleAttendee `json:"attendees"`
}

type googleEventTime struct {
	DateTime string `json:"dateTime"`
	Date     string `json:"date"`
	TimeZone string `json:"timeZone"`
}

type googleAttendee struct {
	DisplayName    string `json:"displayName"`
	Email          string `json:"email"`
	ResponseStatus string `json:"responseStatus"`
	Organizer      bool   `json:"organizer"`
}

func parseEventTime(value googleEventTime, location *time.Location) (time.Time, bool) {
	if strings.TrimSpace(value.DateTime) != "" {
		if parsed, err := time.Parse(time.RFC3339, value.DateTime); err == nil {
			return parsed, false
		}
	}
	if strings.TrimSpace(value.Date) != "" {
		if parsed, err := time.ParseInLocation("2006-01-02", value.Date, location); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func parseRFC3339(value string) time.Time {
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value)); err == nil {
		return parsed.UTC()
	}
	return time.Time{}
}

func toAttendees(attendees []googleAttendee) []Attendee {
	if len(attendees) == 0 {
		return nil
	}
	result := make([]Attendee, 0, len(attendees))
	for _, attendee := range attendees {
		result = append(result, Attendee{
			Name: attendee.DisplayName, Email: attendee.Email,
			ResponseStatus: attendee.ResponseStatus, Organizer: attendee.Organizer,
		})
	}
	return result
}
