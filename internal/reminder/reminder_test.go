package reminder

import (
	"errors"
	"testing"
	"time"
)

func TestNormalizeReminderAppliesDefaultsAndBounds(t *testing.T) {
	value, err := NormalizeReminder(Reminder{
		ID: " rem-1 ", Title: "  Standup  ", Body: string(make([]byte, 1)),
		DueAt: time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC),
	}, "Europe/London")
	if err != nil {
		t.Fatalf("NormalizeReminder() error = %v", err)
	}
	if value.ID != "rem-1" || value.Title != "Standup" {
		t.Fatalf("value = %+v", value)
	}
	if value.Timezone != "Europe/London" || value.Status != StatusScheduled {
		t.Fatalf("defaults = %+v", value)
	}
	if value.IdempotencyKey != "rem-1" {
		t.Fatalf("idempotency key = %q", value.IdempotencyKey)
	}
	if _, err := NormalizeReminder(Reminder{ID: "rem-1", DueAt: time.Now()}, "UTC"); !errors.Is(err, ErrInvalidReminder) {
		t.Fatalf("missing title error = %v", err)
	}
	if _, err := NormalizeReminder(Reminder{ID: "", Title: "x", DueAt: time.Now()}, "UTC"); !errors.Is(err, ErrInvalidReminder) {
		t.Fatalf("missing id error = %v", err)
	}
}

func TestNormalizeReminderValidatesRecurrenceAndQuietHours(t *testing.T) {
	base := Reminder{ID: "rem-1", Title: "x", DueAt: time.Now().UTC()}
	bad := base
	bad.Recurrence = Recurrence{Interval: 1, Unit: "fortnight"}
	if _, err := NormalizeReminder(bad, "UTC"); !errors.Is(err, ErrInvalidReminder) {
		t.Fatalf("unit error = %v", err)
	}
	badQuiet := base
	badQuiet.QuietHours = QuietHours{Start: "22:00"}
	if _, err := NormalizeReminder(badQuiet, "UTC"); !errors.Is(err, ErrInvalidReminder) {
		t.Fatalf("quiet hours error = %v", err)
	}
}

func TestRecurrenceNextAdvancesPastAnchor(t *testing.T) {
	anchor := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	next, ok := Recurrence{Interval: 1, Unit: UnitDay}.next(anchor)
	if !ok || !next.Equal(anchor.AddDate(0, 0, 1)) {
		t.Fatalf("daily next = %v ok=%v", next, ok)
	}
	weekly, ok := Recurrence{Interval: 2, Unit: UnitWeek}.next(anchor)
	if !ok || !weekly.Equal(anchor.AddDate(0, 0, 14)) {
		t.Fatalf("weekly next = %v", weekly)
	}
	if _, ok := (Recurrence{}).next(anchor); ok {
		t.Fatal("one-shot recurrence should not advance")
	}
}

func TestQuietHoursActiveAndResume(t *testing.T) {
	window := QuietHours{Start: "09:00", End: "17:00"}
	noon := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	if !window.active(noon, time.UTC) {
		t.Fatal("12:00 should be inside 09:00-17:00")
	}
	resume := window.resumeAt(noon, time.UTC)
	if resume.Hour() != 17 || resume.Day() != 2 {
		t.Fatalf("resume = %v", resume)
	}
	overnight := QuietHours{Start: "22:00", End: "07:00"}
	late := time.Date(2026, 1, 2, 23, 0, 0, 0, time.UTC)
	if !overnight.active(late, time.UTC) {
		t.Fatal("23:00 should be inside overnight quiet hours")
	}
	if overnight.resumeAt(late, time.UTC).Day() != 3 {
		t.Fatalf("overnight resume = %v", overnight.resumeAt(late, time.UTC))
	}
	if (QuietHours{}).active(noon, time.UTC) {
		t.Fatal("empty quiet hours must be disabled")
	}
}

func TestOccurrenceKeyStablePerOccurrence(t *testing.T) {
	value := Reminder{ID: "rem-1", IdempotencyKey: "key-1"}
	occurrence := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	first := OccurrenceKey(value, occurrence)
	second := OccurrenceKey(value, occurrence)
	if first != second {
		t.Fatalf("keys differ: %q vs %q", first, second)
	}
	if first == OccurrenceKey(value, occurrence.Add(time.Hour)) {
		t.Fatal("keys must differ across occurrences")
	}
}
