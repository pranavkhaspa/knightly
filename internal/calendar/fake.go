package calendar

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// FakeReader is a deterministic in-memory CalendarReader for tests and demos.
// It performs no network access and never mutates a calendar.
type FakeReader struct {
	events   []Event
	location *time.Location
	now      func() time.Time
}

var _ CalendarReader = (*FakeReader)(nil)

func NewFakeReader(location *time.Location, now func() time.Time, events ...Event) *FakeReader {
	if location == nil {
		location = time.UTC
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	normalized := make([]Event, 0, len(events))
	for _, event := range events {
		if value, err := NormalizeEvent(event, location); err == nil {
			normalized = append(normalized, value)
		}
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Start.Before(normalized[j].Start) })
	return &FakeReader{events: normalized, location: location, now: now}
}

func (r *FakeReader) ListEvents(ctx context.Context, scope identity.Scope, query ListQuery) (EventPage, error) {
	if err := ctx.Err(); err != nil {
		return EventPage{}, err
	}
	if err := scope.Validate(); err != nil {
		return EventPage{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	query, err := NormalizeListQuery(query)
	if err != nil {
		return EventPage{}, err
	}
	timeMin, timeMax := r.windowBounds(query)
	filtered := make([]Event, 0, len(r.events))
	for _, event := range r.events {
		if !matchesCalendar(event, query.CalendarID) {
			continue
		}
		if !timeMin.IsZero() && event.End.Before(timeMin) {
			continue
		}
		if !timeMax.IsZero() && !event.Start.Before(timeMax) {
			continue
		}
		filtered = append(filtered, event)
	}
	offset := 0
	if query.PageToken != "" {
		parsed, parseErr := strconv.Atoi(query.PageToken)
		if parseErr != nil || parsed < 0 {
			return EventPage{}, fmt.Errorf("%w: invalid page token", ErrInvalidQuery)
		}
		offset = parsed
	}
	if offset >= len(filtered) {
		return EventPage{Events: []Event{}}, nil
	}
	end := offset + query.Limit
	next := ""
	if end < len(filtered) {
		next = strconv.Itoa(end)
	} else {
		end = len(filtered)
	}
	return EventPage{Events: append([]Event(nil), filtered[offset:end]...), NextPageToken: next}, nil
}

func (r *FakeReader) GetEvent(ctx context.Context, scope identity.Scope, calendarID, eventID string) (Event, error) {
	if err := ctx.Err(); err != nil {
		return Event{}, err
	}
	if err := scope.Validate(); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	for _, event := range r.events {
		if event.ID == strings.TrimSpace(eventID) {
			return event, nil
		}
	}
	return Event{}, fmt.Errorf("%w: %s", ErrEventNotFound, eventID)
}

func (r *FakeReader) windowBounds(query ListQuery) (time.Time, time.Time) {
	timeMin, timeMax := query.TimeMin, query.TimeMax
	switch query.Window {
	case WindowToday:
		start, end := DayWindow(r.now(), r.location)
		return start, end
	case WindowUpcoming:
		if timeMin.IsZero() {
			timeMin = r.now().In(r.location)
		}
	}
	return timeMin, timeMax
}

func matchesCalendar(event Event, calendarID string) bool {
	return calendarID == "" || calendarID == "primary" || event.CalendarID == calendarID
}
