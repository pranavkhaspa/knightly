// Package reminder contains provider-neutral, graph-backed reminders. Reminders
// are user-authored and trusted, but delivery to any sink that creates an
// external side effect is approval-gated. The inbox sink is always safe and is
// delivered first.
package reminder

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultListLimit    = 50
	MaxListLimit        = 200
	MaxClaimBatch       = 50
	MaxIDLength         = 128
	MaxTitle            = 256
	MaxBody             = 2048
	MaxSourceRef        = 256
	MaxTimezoneLength   = 64
	MaxIdempotencyKey   = 256
	MaxInboxItemsPerRun = 200
)

var (
	ErrInvalidReminder  = errors.New("invalid reminder")
	ErrReminderNotFound = errors.New("reminder not found")
	ErrScopeRequired    = errors.New("trusted reminder scope is required")
	ErrApprovalRequired = errors.New("reminder delivery requires explicit approval")
	ErrNotPending       = errors.New("reminder is not awaiting approval")
)

// Status is the durable lifecycle state of a reminder.
type Status string

const (
	StatusScheduled       Status = "scheduled"
	StatusPendingApproval Status = "pending_approval"
	StatusDelivered       Status = "delivered"
	StatusCancelled       Status = "cancelled"
)

func (s Status) Valid() bool {
	switch s {
	case StatusScheduled, StatusPendingApproval, StatusDelivered, StatusCancelled:
		return true
	default:
		return false
	}
}

// Unit is the recurrence granularity. An empty unit makes the reminder one-shot.
type Unit string

const (
	UnitMinute Unit = "minute"
	UnitHour   Unit = "hour"
	UnitDay    Unit = "day"
	UnitWeek   Unit = "week"
)

// Recurrence describes a fixed-interval repeat. Interval defaults to 1.
type Recurrence struct {
	Interval int  `json:"interval,omitempty"`
	Unit     Unit `json:"unit,omitempty"`
}

func (r Recurrence) recurring() bool { return r.Unit != "" }

func (r Recurrence) validate() error {
	if !r.recurring() {
		return nil
	}
	switch r.Unit {
	case UnitMinute, UnitHour, UnitDay, UnitWeek:
	default:
		return fmt.Errorf("%w: unknown recurrence unit %q", ErrInvalidReminder, r.Unit)
	}
	if r.Interval < 1 || r.Interval > 10000 {
		return fmt.Errorf("%w: recurrence interval must be between 1 and 10000", ErrInvalidReminder)
	}
	return nil
}

// next returns the first occurrence strictly after "after".
func (r Recurrence) next(after time.Time) (time.Time, bool) {
	if !r.recurring() {
		return time.Time{}, false
	}
	interval := r.Interval
	if interval < 1 {
		interval = 1
	}
	candidate := after
	for step := 0; step < 100000; step++ {
		switch r.Unit {
		case UnitMinute:
			candidate = candidate.Add(time.Duration(interval) * time.Minute)
		case UnitHour:
			candidate = candidate.Add(time.Duration(interval) * time.Hour)
		case UnitDay:
			candidate = candidate.AddDate(0, 0, interval)
		case UnitWeek:
			candidate = candidate.AddDate(0, 0, 7*interval)
		default:
			return time.Time{}, false
		}
		if candidate.After(after) {
			return candidate, true
		}
	}
	return time.Time{}, false
}

// QuietHours is a daily "HH:MM".."HH:MM" interval in the reminder's timezone. A
// window whose start is after its end crosses midnight. An empty window is
// disabled.
type QuietHours struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
}

func (q QuietHours) enabled() bool { return q.Start != "" && q.End != "" }

func (q QuietHours) validate() error {
	if q.Start == "" && q.End == "" {
		return nil
	}
	if q.Start == "" || q.End == "" {
		return fmt.Errorf("%w: quiet hours require both a start and an end", ErrInvalidReminder)
	}
	if _, ok := parseClock(q.Start); !ok {
		return fmt.Errorf("%w: quiet-hour start must be HH:MM", ErrInvalidReminder)
	}
	if _, ok := parseClock(q.End); !ok {
		return fmt.Errorf("%w: quiet-hour end must be HH:MM", ErrInvalidReminder)
	}
	return nil
}

func (q QuietHours) active(now time.Time, location *time.Location) bool {
	if !q.enabled() {
		return false
	}
	startMinutes, _ := parseClock(q.Start)
	endMinutes, _ := parseClock(q.End)
	if startMinutes == endMinutes {
		return false
	}
	if location != nil {
		now = now.In(location)
	}
	position := now.Hour()*60 + now.Minute()
	if startMinutes < endMinutes {
		return position >= startMinutes && position < endMinutes
	}
	return position >= startMinutes || position < endMinutes
}

// resumeAt returns the instant quiet hours next end, bounded to within one day.
func (q QuietHours) resumeAt(now time.Time, location *time.Location) time.Time {
	if location != nil {
		now = now.In(location)
	}
	endMinutes, _ := parseClock(q.End)
	resume := time.Date(now.Year(), now.Month(), now.Day(), endMinutes/60, endMinutes%60, 0, 0, now.Location())
	if !resume.After(now) {
		resume = resume.AddDate(0, 0, 1)
	}
	return resume.UTC()
}

// Reminder is a bounded, normalized reminder record. It never stores secrets.
type Reminder struct {
	ID             string     `json:"id"`
	TenantID       string     `json:"tenant_id"`
	UserID         string     `json:"user_id"`
	Title          string     `json:"title"`
	Body           string     `json:"body,omitempty"`
	DueAt          time.Time  `json:"due_at"`
	Timezone       string     `json:"timezone,omitempty"`
	Recurrence     Recurrence `json:"recurrence,omitempty"`
	QuietHours     QuietHours `json:"quiet_hours,omitempty"`
	Status         Status     `json:"status"`
	Source         string     `json:"source,omitempty"`
	SourceRef      string     `json:"source_ref,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
	Attempts       int        `json:"attempts"`
	LeaseUntil     time.Time  `json:"lease_until,omitempty"`
	DeliveredAt    time.Time  `json:"delivered_at,omitempty"`
	ApprovedBy     string     `json:"approved_by,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// Delivery is one due occurrence handed to a sink.
type Delivery struct {
	Reminder       Reminder
	Occurrence     time.Time
	IdempotencyKey string
}

// InboxItem is a bounded, inert notification surfaced in the user's inbox.
type InboxItem struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	UserID         string    `json:"user_id"`
	Kind           string    `json:"kind"`
	Title          string    `json:"title"`
	Body           string    `json:"body,omitempty"`
	ReminderID     string    `json:"reminder_id"`
	DueAt          time.Time `json:"due_at"`
	IdempotencyKey string    `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
}

const InboxKindReminder = "reminder"

// OccurrenceKey derives the stable per-occurrence deduplication key.
func OccurrenceKey(value Reminder, occurrence time.Time) string {
	base := value.IdempotencyKey
	if base == "" {
		base = value.ID
	}
	return base + "@" + occurrence.UTC().Format(time.RFC3339)
}

// NormalizeReminder bounds fields, applies defaults, and validates the record.
func NormalizeReminder(value Reminder, defaultTimezone string) (Reminder, error) {
	value.ID = strings.TrimSpace(value.ID)
	if value.ID == "" || len(value.ID) > MaxIDLength {
		return Reminder{}, fmt.Errorf("%w: ID is required and bounded", ErrInvalidReminder)
	}
	value.Title = bound(strings.TrimSpace(value.Title), MaxTitle)
	if value.Title == "" {
		return Reminder{}, fmt.Errorf("%w: title is required", ErrInvalidReminder)
	}
	value.Body = bound(value.Body, MaxBody)
	value.Source = bound(strings.TrimSpace(value.Source), 64)
	value.SourceRef = bound(strings.TrimSpace(value.SourceRef), MaxSourceRef)
	value.ApprovedBy = bound(strings.TrimSpace(value.ApprovedBy), MaxIDLength)
	if value.DueAt.IsZero() {
		return Reminder{}, fmt.Errorf("%w: due time is required", ErrInvalidReminder)
	}
	value.DueAt = value.DueAt.UTC()
	value.Timezone = bound(strings.TrimSpace(value.Timezone), MaxTimezoneLength)
	if value.Timezone == "" {
		value.Timezone = defaultTimezone
	}
	if err := value.Recurrence.validate(); err != nil {
		return Reminder{}, err
	}
	if err := value.QuietHours.validate(); err != nil {
		return Reminder{}, err
	}
	value.IdempotencyKey = bound(strings.TrimSpace(value.IdempotencyKey), MaxIdempotencyKey)
	if value.IdempotencyKey == "" {
		value.IdempotencyKey = value.ID
	}
	if value.Status == "" {
		value.Status = StatusScheduled
	}
	if !value.Status.Valid() {
		return Reminder{}, fmt.Errorf("%w: unknown status %q", ErrInvalidReminder, value.Status)
	}
	if value.Attempts < 0 {
		value.Attempts = 0
	}
	return value, nil
}

// LoadLocation resolves an IANA timezone, falling back to UTC.
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

func parseClock(value string) (minutes int, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(value), ":", 2)
	if len(parts) != 2 {
		return 0, false
	}
	var hour, minute int
	if _, err := fmt.Sscanf(parts[0], "%d", &hour); err != nil {
		return 0, false
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &minute); err != nil {
		return 0, false
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, false
	}
	return hour*60 + minute, true
}

func bound(value string, limit int) string {
	runes := []rune(value)
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return string(runes)
}
