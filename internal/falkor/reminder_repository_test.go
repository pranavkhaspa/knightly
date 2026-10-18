package falkor

import (
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/reminder"
)

func TestReminderRecordMapping(t *testing.T) {
	value := reminderFromRecord(sourceRecord{
		"id":               "rem-1",
		"tenant_id":        "tenant-a",
		"user_id":          "user-a",
		"title":            "Standup",
		"body":             "daily",
		"due_at":           int64(1_700_000_000_000),
		"timezone":         "America/New_York",
		"recurrence_json":  `{"interval":1,"unit":"day"}`,
		"quiet_hours_json": `{"start":"22:00","end":"07:00"}`,
		"status":           "scheduled",
		"idempotency_key":  "key-1",
		"attempts":         int64(2),
		"created_at":       int64(1_700_000_000_000),
		"updated_at":       int64(1_700_000_000_000),
	})
	if value.ID != "rem-1" || value.Status != reminder.StatusScheduled || value.Attempts != 2 {
		t.Fatalf("value = %+v", value)
	}
	if value.Recurrence.Unit != reminder.UnitDay || value.Recurrence.Interval != 1 {
		t.Fatalf("recurrence = %+v", value.Recurrence)
	}
	if value.QuietHours.Start != "22:00" || value.QuietHours.End != "07:00" {
		t.Fatalf("quiet hours = %+v", value.QuietHours)
	}
	if !value.DueAt.Equal(time.UnixMilli(1_700_000_000_000).UTC()) {
		t.Fatalf("due_at = %v", value.DueAt)
	}
}

func TestReminderEncodingRoundTrip(t *testing.T) {
	recurrence := reminder.Recurrence{Interval: 2, Unit: reminder.UnitWeek}
	if decoded := decodeRecurrence(encodeRecurrence(recurrence)); decoded != recurrence {
		t.Fatalf("recurrence = %+v", decoded)
	}
	if decodeRecurrence("") != (reminder.Recurrence{}) {
		t.Fatal("empty recurrence should decode to zero value")
	}
	quiet := reminder.QuietHours{Start: "22:00", End: "07:00"}
	if decoded := decodeQuietHours(encodeQuietHours(quiet)); decoded != quiet {
		t.Fatalf("quiet hours = %+v", decoded)
	}
	if decodeQuietHours("") != (reminder.QuietHours{}) {
		t.Fatal("empty quiet hours should decode to zero value")
	}
}

func TestInboxItemRecordMapping(t *testing.T) {
	item := inboxItemFromRecord(sourceRecord{
		"id": "inbox_key-1@2026-01-02T09:00:00Z", "tenant_id": "tenant-a", "user_id": "user-a",
		"kind": "reminder", "title": "Standup", "body": "daily", "reminder_id": "rem-1",
		"due_at": int64(1_700_000_000_000), "idempotency_key": "key-1@2026-01-02T09:00:00Z",
		"created_at": int64(1_700_000_000_000),
	})
	if item.ReminderID != "rem-1" || item.Kind != reminder.InboxKindReminder {
		t.Fatalf("item = %+v", item)
	}
}

func TestReminderRepositoryQueriesKeepStateInFalkorDB(t *testing.T) {
	combined := ReminderSaveQuery + ReminderGetQuery + ReminderListQuery + ReminderClaimDueQuery +
		InboxItemSaveQuery + InboxItemListQuery
	for _, fragment := range []string{
		"Reminder", "InboxItem", "DELIVERED_AS", "tenant_id", "user_id",
		"lease_until", "idempotency_key", "quiet_hours_json", "recurrence_json",
	} {
		if !strings.Contains(combined, fragment) {
			t.Errorf("reminder repository queries missing %q", fragment)
		}
	}
	if !strings.Contains(ReminderClaimDueQuery, "lease_until = $lease_until") {
		t.Error("claim query does not take an atomic lease")
	}
	if !strings.Contains(ReminderSaveQuery, "MERGE") || !strings.Contains(InboxItemSaveQuery, "MERGE") {
		t.Error("save queries must be idempotent")
	}
}
