// Package calendar contains provider-neutral, read-only calendar types. The P0
// reader boundary is intentionally incapable of creating, editing, or
// responding to events: CalendarReader exposes only read operations and every
// provider adapter uses the read-only OAuth scope.
package calendar

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/untrusted"
)

const (
	DefaultListLimit = 25
	MaxListLimit     = 100
	MaxQueryLength   = 512
	MaxPageTokenSize = 1024
	MaxEventIDSize   = 512
	MaxCalendarID    = 256
	MaxSummary       = 1024
	MaxDescription   = 4096
	MaxLocation      = 512
	MaxAttendees     = 100
	MaxAttendeeName  = 256
	MaxAttendeeEmail = 320
	MaxStatusLength  = 64
)

// UntrustedOrigin marks event-derived text as untrusted external input.
const UntrustedOrigin = string(untrusted.Calendar)

var (
	ErrInvalidQuery    = errors.New("invalid calendar query")
	ErrInvalidEvent    = errors.New("invalid calendar event")
	ErrAccountNotFound = errors.New("calendar account not found")
	ErrEventNotFound   = errors.New("calendar event not found")
	ErrScopeRequired   = errors.New("trusted calendar scope is required")
)

// Attendee is a bounded event attendee.
type Attendee struct {
	Name           string `json:"name,omitempty"`
	Email          string `json:"email"`
	ResponseStatus string `json:"response_status,omitempty"`
	Organizer      bool   `json:"organizer,omitempty"`
}

// Event is a bounded, normalized calendar event.
type Event struct {
	ID               string     `json:"id"`
	AccountID        string     `json:"account_id"`
	CalendarID       string     `json:"calendar_id"`
	Summary          string     `json:"summary,omitempty"`
	Description      string     `json:"description_excerpt,omitempty"`
	Location         string     `json:"location,omitempty"`
	Start            time.Time  `json:"start"`
	End              time.Time  `json:"end"`
	AllDay           bool       `json:"all_day"`
	Status           string     `json:"status,omitempty"`
	Attendees        []Attendee `json:"attendees,omitempty"`
	RecurringEventID string     `json:"recurring_event_id,omitempty"`
	HTMLLink         string     `json:"html_link,omitempty"`
	Updated          time.Time  `json:"updated,omitempty"`
	// Origin marks event text as untrusted external content.
	Origin string `json:"origin"`
}

// Window describes the queryable time window.
type Window string

const (
	WindowToday    Window = "today"
	WindowUpcoming Window = "upcoming"
)

// ListQuery is a bounded event-list request.
type ListQuery struct {
	CalendarID string
	Window     Window
	TimeMin    time.Time
	TimeMax    time.Time
	PageToken  string
	Limit      int
}

// EventPage is a bounded page of events with an opaque continuation token.
type EventPage struct {
	Events        []Event `json:"events"`
	NextPageToken string  `json:"next_page_token,omitempty"`
}

// CalendarReader is the read-only provider boundary. It has no method that
// mutates a calendar.
type CalendarReader interface {
	ListEvents(context.Context, identity.Scope, ListQuery) (EventPage, error)
	GetEvent(context.Context, identity.Scope, string, string) (Event, error)
}

// NormalizeListQuery validates and bounds a list request.
func NormalizeListQuery(query ListQuery) (ListQuery, error) {
	query.CalendarID = strings.TrimSpace(query.CalendarID)
	if query.CalendarID == "" {
		query.CalendarID = "primary"
	}
	if len(query.CalendarID) > MaxCalendarID {
		return ListQuery{}, fmt.Errorf("%w: calendar ID is too long", ErrInvalidQuery)
	}
	query.PageToken = strings.TrimSpace(query.PageToken)
	if len(query.PageToken) > MaxPageTokenSize {
		return ListQuery{}, fmt.Errorf("%w: page token is too long", ErrInvalidQuery)
	}
	switch query.Window {
	case "", WindowUpcoming, WindowToday:
	default:
		return ListQuery{}, fmt.Errorf("%w: unknown window %q", ErrInvalidQuery, query.Window)
	}
	if query.Limit == 0 {
		query.Limit = DefaultListLimit
	}
	if query.Limit < 1 || query.Limit > MaxListLimit {
		return ListQuery{}, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxListLimit)
	}
	if !query.TimeMax.IsZero() && !query.TimeMin.IsZero() && query.TimeMax.Before(query.TimeMin) {
		return ListQuery{}, fmt.Errorf("%w: time_max must not be before time_min", ErrInvalidQuery)
	}
	return query, nil
}

// NormalizeEvent bounds every field, normalizes start/end into the user's
// timezone, and marks the event text as untrusted.
func NormalizeEvent(event Event, location *time.Location) (Event, error) {
	event.ID = strings.TrimSpace(event.ID)
	if event.ID == "" || len(event.ID) > MaxEventIDSize {
		return Event{}, fmt.Errorf("%w: event ID is required and bounded", ErrInvalidEvent)
	}
	if location == nil {
		location = time.UTC
	}
	event.CalendarID = bound(event.CalendarID, MaxCalendarID)
	if event.CalendarID == "" {
		event.CalendarID = "primary"
	}
	event.Summary = sanitizeText(event.Summary, MaxSummary)
	event.Location = sanitizeText(event.Location, MaxLocation)
	event.Status = bound(event.Status, MaxStatusLength)
	event.RecurringEventID = bound(event.RecurringEventID, MaxEventIDSize)
	event.HTMLLink = bound(event.HTMLLink, MaxEventIDSize)
	event.Attendees = normalizeAttendees(event.Attendees)
	event.Start = normalizeTime(event.Start, location)
	event.End = normalizeTime(event.End, location)
	if !event.End.IsZero() && !event.Start.IsZero() && event.End.Before(event.Start) {
		return Event{}, fmt.Errorf("%w: event end is before start", ErrInvalidEvent)
	}
	event.Origin = UntrustedOrigin
	description := event.Description
	if content, err := untrusted.New(untrusted.Calendar, description); err == nil {
		description = content.Bounded()
	} else {
		description = bound(description, MaxDescription)
	}
	event.Description = description
	return event, nil
}

func normalizeAttendees(attendees []Attendee) []Attendee {
	if len(attendees) == 0 {
		return nil
	}
	if len(attendees) > MaxAttendees {
		attendees = attendees[:MaxAttendees]
	}
	result := make([]Attendee, 0, len(attendees))
	for _, attendee := range attendees {
		result = append(result, Attendee{
			Name:           bound(attendee.Name, MaxAttendeeName),
			Email:          bound(attendee.Email, MaxAttendeeEmail),
			ResponseStatus: bound(attendee.ResponseStatus, MaxStatusLength),
			Organizer:      attendee.Organizer,
		})
	}
	return result
}

func normalizeTime(value time.Time, location *time.Location) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.In(location)
}

// sanitizeText removes control characters that could smuggle instructions or
// log injection through event fields.
func sanitizeText(value string, limit int) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\t", " ")
	return bound(value, limit)
}

func bound(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return string(runes)
}

// LoadLocation resolves the user's configured timezone, defaulting to UTC.
func LoadLocation(name string) *time.Location {
	name = strings.TrimSpace(name)
	if name == "" {
		return time.UTC
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return time.UTC
	}
	return location
}

// DayWindow returns the [start, end) instants of the given day in location.
func DayWindow(now time.Time, location *time.Location) (time.Time, time.Time) {
	if location == nil {
		location = time.UTC
	}
	local := now.In(location)
	start := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	return start, start.AddDate(0, 0, 1)
}
