package falkor

import (
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/calendar"
)

func TestCalendarAccountRecordMapping(t *testing.T) {
	account := calendarAccountFromRecord(sourceRecord{
		"id":            "gcal_account_1",
		"tenant_id":     "tenant-a",
		"user_id":       "user-a",
		"provider":      "gcal",
		"calendar_id":   "primary",
		"email_address": "user@example.com",
		"scopes":        []interface{}{calendar.CalendarReadOnlyScope},
		"status":        "connected",
		"connected_at":  int64(1_700_000_000_000),
		"updated_at":    int64(1_700_000_000_000),
	})
	if account.ID != "gcal_account_1" || account.Provider != calendar.ProviderGoogleCalendar || account.Status != calendar.AccountConnected {
		t.Fatalf("account = %+v", account)
	}
	if len(account.Scopes) != 1 || !strings.Contains(account.Scopes[0], "calendar.readonly") {
		t.Fatalf("scopes = %+v", account.Scopes)
	}
	if account.ConnectedAt.IsZero() {
		t.Fatal("connected_at was not parsed")
	}
}

func TestCalendarEventRecordMapping(t *testing.T) {
	event := calendarEventFromRecord(sourceRecord{
		"id":                  "evt-1",
		"account_id":          "gcal_account_1",
		"calendar_id":         "primary",
		"summary":             "Standup",
		"description_excerpt": "daily sync",
		"location":            "Room 1",
		"start_at":            int64(1_700_000_000_000),
		"end_at":              int64(1_700_000_360_000),
		"all_day":             false,
		"status":              "confirmed",
		"attendees_json":      `[{"email":"a@example.com","name":"Ada","organizer":true}]`,
		"untrusted_origin":    "calendar",
	})
	if event.ID != "evt-1" || event.CalendarID != "primary" || event.Summary != "Standup" {
		t.Fatalf("event = %+v", event)
	}
	if len(event.Attendees) != 1 || !event.Attendees[0].Organizer {
		t.Fatalf("attendees = %+v", event.Attendees)
	}
	if event.Origin != calendar.UntrustedOrigin {
		t.Fatalf("origin = %q", event.Origin)
	}
	if !event.Start.Equal(time.UnixMilli(1_700_000_000_000).UTC()) {
		t.Fatalf("start = %v", event.Start)
	}
}

func TestCalendarEventDefaultsOriginWhenMissing(t *testing.T) {
	event := calendarEventFromRecord(sourceRecord{"id": "evt-2", "start_at": int64(1_700_000_000_000)})
	if event.Origin != calendar.UntrustedOrigin {
		t.Fatalf("origin default = %q", event.Origin)
	}
}

func TestCalendarRepositoryQueriesKeepStateInFalkorDB(t *testing.T) {
	combined := CalendarAccountListQuery + CalendarAccountGetQuery + CalendarAccountSaveQuery +
		CalendarEventSaveQuery + CalendarEventListQuery + CalendarEventGetQuery
	for _, fragment := range []string{
		"ProviderAccount", "CalendarEvent", "CalendarSource", "HAS_EVENT", "IN_CALENDAR",
		"tenant_id", "user_id", "content_hash", "attendees_json", "untrusted_origin",
	} {
		if !strings.Contains(combined, fragment) {
			t.Errorf("calendar repository queries missing %q", fragment)
		}
	}
	for name, query := range map[string]string{"event save": CalendarEventSaveQuery, "account save": CalendarAccountSaveQuery} {
		if !strings.Contains(query, "MERGE") {
			t.Errorf("%s query is not idempotent", name)
		}
	}
}

func TestCalendarAttendeeEncodingRoundTrip(t *testing.T) {
	attendees := []calendar.Attendee{{Name: "Ada", Email: "a@example.com", ResponseStatus: "accepted", Organizer: true}}
	decoded := decodeAttendees(encodeAttendees(attendees))
	if len(decoded) != 1 || decoded[0].Email != "a@example.com" || !decoded[0].Organizer {
		t.Fatalf("decoded = %+v", decoded)
	}
	if decodeAttendees("") != nil {
		t.Error("empty attendee JSON should decode to nil")
	}
}

func TestCalendarContentHashTracksChanges(t *testing.T) {
	base := calendar.Event{ID: "evt-1", Summary: "Standup", Location: "Room 1"}
	if calendarContentHash(base) == calendarContentHash(calendar.Event{ID: "evt-1", Summary: "Standup", Location: "Room 2"}) {
		t.Error("content hash did not change with the location")
	}
}
