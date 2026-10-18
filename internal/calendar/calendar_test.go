package calendar

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

var testScope = identity.Scope{TenantID: "tenant-a", UserID: "user-a"}

func TestNormalizeEventBoundsFieldsAndMarksUntrusted(t *testing.T) {
	event, err := NormalizeEvent(Event{
		ID:          " evt-1 ",
		Summary:     strings.Repeat("s", MaxSummary+50),
		Description: "line one\nline two\twith tabs",
		Start:       time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 1, 2, 16, 0, 0, 0, time.UTC),
	}, time.UTC)
	if err != nil {
		t.Fatalf("NormalizeEvent() error = %v", err)
	}
	if event.ID != "evt-1" {
		t.Fatalf("id = %q", event.ID)
	}
	if len([]rune(event.Summary)) != MaxSummary {
		t.Fatalf("summary length = %d", len([]rune(event.Summary)))
	}
	if len([]rune(event.Description)) > MaxDescription {
		t.Fatalf("description length = %d", len([]rune(event.Description)))
	}
	if !strings.Contains(event.Description, "line one") {
		t.Fatalf("description lost content: %q", event.Description)
	}
	if event.CalendarID != "primary" {
		t.Fatalf("calendar id default = %q", event.CalendarID)
	}
	if event.Origin != UntrustedOrigin {
		t.Fatalf("origin = %q", event.Origin)
	}
}

func TestNormalizeEventRejectsMissingIDAndReversedWindow(t *testing.T) {
	if _, err := NormalizeEvent(Event{}, time.UTC); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("missing id error = %v", err)
	}
	_, err := NormalizeEvent(Event{
		ID:    "evt-1",
		Start: time.Date(2026, 1, 2, 16, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC),
	}, time.UTC)
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("reversed window error = %v", err)
	}
}

func TestNormalizeListQueryDefaultsAndRejectsBadInput(t *testing.T) {
	query, err := NormalizeListQuery(ListQuery{})
	if err != nil {
		t.Fatalf("NormalizeListQuery() error = %v", err)
	}
	if query.CalendarID != "primary" || query.Limit != DefaultListLimit {
		t.Fatalf("query = %+v", query)
	}
	if _, err := NormalizeListQuery(ListQuery{Limit: MaxListLimit + 1}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("limit error = %v", err)
	}
	if _, err := NormalizeListQuery(ListQuery{Window: "someday"}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("window error = %v", err)
	}
}

func TestDayWindowCoversConfiguredTimezoneDay(t *testing.T) {
	location := LoadLocation("America/New_York")
	now := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	start, end := DayWindow(now, location)
	if start.Location() != location || end.Sub(start) != 24*time.Hour {
		t.Fatalf("window = %v..%v", start, end)
	}
	if start.Hour() != 0 || start.Minute() != 0 {
		t.Fatalf("window did not start at local midnight: %v", start)
	}
}

func TestLoadLocationFallsBackToUTC(t *testing.T) {
	if LoadLocation("Not/AZone") != time.UTC || LoadLocation("") != time.UTC {
		t.Fatal("LoadLocation did not fall back to UTC")
	}
}

func TestFakeReaderFiltersByWindowAndPaginates(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	reader := NewFakeReader(time.UTC, func() time.Time { return now },
		Event{ID: "a", Start: now.Add(1 * time.Hour), End: now.Add(2 * time.Hour)},
		Event{ID: "b", Start: now.Add(3 * time.Hour), End: now.Add(4 * time.Hour)},
		Event{ID: "c", Start: now.Add(48 * time.Hour), End: now.Add(49 * time.Hour)},
	)
	page, err := reader.ListEvents(context.Background(), testScope, ListQuery{Window: WindowToday, Limit: 1})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(page.Events) != 1 || page.Events[0].ID != "a" || page.NextPageToken == "" {
		t.Fatalf("page = %+v", page)
	}
	next, err := reader.ListEvents(context.Background(), testScope, ListQuery{Window: WindowToday, Limit: 5, PageToken: page.NextPageToken})
	if err != nil {
		t.Fatalf("ListEvents(page 2) error = %v", err)
	}
	if len(next.Events) != 1 || next.Events[0].ID != "b" {
		t.Fatalf("page 2 = %+v", next)
	}
	upcoming, err := reader.ListEvents(context.Background(), testScope, ListQuery{Window: WindowUpcoming, Limit: 10})
	if err != nil || len(upcoming.Events) != 3 {
		t.Fatalf("upcoming = %+v err=%v", upcoming, err)
	}
}

func TestFakeReaderRequiresScope(t *testing.T) {
	reader := NewFakeReader(time.UTC, nil)
	if _, err := reader.ListEvents(context.Background(), identity.Scope{}, ListQuery{}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("scope error = %v", err)
	}
}
