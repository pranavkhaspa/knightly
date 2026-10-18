package reminder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

var testScope = identity.Scope{TenantID: "tenant-a", UserID: "user-a"}

type recordingSink struct {
	name     string
	approval bool
	calls    []string
	fail     error
}

func (s *recordingSink) Name() string           { return s.name }
func (s *recordingSink) RequiresApproval() bool { return s.approval }
func (s *recordingSink) Deliver(_ context.Context, _ identity.Scope, delivery Delivery) error {
	if s.fail != nil {
		return s.fail
	}
	s.calls = append(s.calls, delivery.IdempotencyKey)
	return nil
}

func newTestService(t *testing.T, now time.Time, sinks ...Sink) (*Service, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	service, err := NewService(ServiceDeps{
		Store: store, Inbox: store, Sinks: sinks, Timezone: "UTC",
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service, store
}

func TestTickDeliversDueOneShotOnce(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	service, store := newTestService(t, now)
	created, err := service.Create(context.Background(), testScope, Reminder{
		ID: "rem-1", Title: "Standup", DueAt: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	result, err := service.Tick(context.Background(), testScope)
	if err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	if result.Delivered != 1 {
		t.Fatalf("result = %+v", result)
	}
	value, err := service.Get(context.Background(), testScope, created.ID)
	if err != nil || value.Status != StatusDelivered || value.DeliveredAt.IsZero() {
		t.Fatalf("reminder = %+v err=%v", value, err)
	}
	items, err := store.ListInboxItems(context.Background(), testScope, 10)
	if err != nil || len(items) != 1 || items[0].ReminderID != created.ID {
		t.Fatalf("inbox = %+v err=%v", items, err)
	}
	second, err := service.Tick(context.Background(), testScope)
	if err != nil || second.Claimed != 0 {
		t.Fatalf("second tick = %+v err=%v", second, err)
	}
}

func TestRecurringReminderReschedules(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	service, _ := newTestService(t, now)
	created, err := service.Create(context.Background(), testScope, Reminder{
		ID: "rem-2", Title: "Daily", DueAt: now.Add(-time.Minute),
		Recurrence: Recurrence{Interval: 1, Unit: UnitDay},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.Tick(context.Background(), testScope); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	value, err := service.Get(context.Background(), testScope, created.ID)
	if err != nil || value.Status != StatusScheduled {
		t.Fatalf("value = %+v err=%v", value, err)
	}
	want := now.Add(-time.Minute).AddDate(0, 0, 1)
	if !value.DueAt.Equal(want) {
		t.Fatalf("next due = %v, want %v", value.DueAt, want)
	}
}

func TestTickIsIdempotentAcrossRestart(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	store := NewMemoryStore()
	service, err := NewService(ServiceDeps{Store: store, Inbox: store, Timezone: "UTC", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	created, err := service.Create(context.Background(), testScope, Reminder{ID: "rem-3", Title: "Ping", DueAt: now.Add(-time.Minute)})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.Tick(context.Background(), testScope); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	// Simulate a crash after inbox delivery but before the status was
	// finalized: force the reminder back to scheduled with an expired lease.
	value, _ := store.Get(context.Background(), testScope, created.ID)
	value.Status = StatusScheduled
	value.LeaseUntil = time.Time{}
	value.DeliveredAt = time.Time{}
	if _, err := store.Save(context.Background(), testScope, value); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	// A fresh service instance (post-restart) reconciles the same occurrence.
	restarted, err := NewService(ServiceDeps{Store: store, Inbox: store, Timezone: "UTC", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if _, err := restarted.Tick(context.Background(), testScope); err != nil {
		t.Fatalf("restart Tick() error = %v", err)
	}
	items, err := store.ListInboxItems(context.Background(), testScope, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("inbox after restart = %+v err=%v", items, err)
	}
}

func TestTickDefersDuringQuietHours(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	service, store := newTestService(t, now)
	created, err := service.Create(context.Background(), testScope, Reminder{
		ID: "rem-4", Title: "Focus", DueAt: now.Add(-time.Minute),
		QuietHours: QuietHours{Start: "09:00", End: "17:00"},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	result, err := service.Tick(context.Background(), testScope)
	if err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	if result.Deferred != 1 || result.Delivered != 0 {
		t.Fatalf("result = %+v", result)
	}
	value, err := service.Get(context.Background(), testScope, created.ID)
	if err != nil || value.Status != StatusScheduled || value.DueAt.Hour() != 17 {
		t.Fatalf("value = %+v err=%v", value, err)
	}
	items, err := store.ListInboxItems(context.Background(), testScope, 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("inbox should be empty during quiet hours, got %+v", items)
	}
}

func TestExternalSinkRequiresApproval(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	sink := &recordingSink{name: "webhook", approval: true}
	service, store := newTestService(t, now, sink)
	created, err := service.Create(context.Background(), testScope, Reminder{ID: "rem-5", Title: "Notify", DueAt: now.Add(-time.Minute)})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	result, err := service.Tick(context.Background(), testScope)
	if err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	if result.PendingApproval != 1 || len(sink.calls) != 0 {
		t.Fatalf("result = %+v calls = %v", result, sink.calls)
	}
	value, err := service.Get(context.Background(), testScope, created.ID)
	if err != nil || value.Status != StatusPendingApproval {
		t.Fatalf("value = %+v err=%v", value, err)
	}
	items, err := store.ListInboxItems(context.Background(), testScope, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("inbox item missing: %+v err=%v", items, err)
	}
	approved, err := service.Approve(context.Background(), testScope, created.ID, "user-a")
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	if approved.Status != StatusDelivered || len(sink.calls) != 1 {
		t.Fatalf("approved = %+v calls = %v", approved, sink.calls)
	}
	if approved.ApprovedBy != "user-a" {
		t.Fatalf("approved_by = %q", approved.ApprovedBy)
	}
}

func TestApproveWithoutActorIsRejected(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	sink := &recordingSink{name: "webhook", approval: true}
	service, _ := newTestService(t, now, sink)
	created, err := service.Create(context.Background(), testScope, Reminder{ID: "rem-6", Title: "Notify", DueAt: now.Add(-time.Minute)})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.Tick(context.Background(), testScope); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	if _, err := service.Approve(context.Background(), testScope, created.ID, "  "); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("Approve() error = %v", err)
	}
}

func TestServiceRejectsUntrustedScope(t *testing.T) {
	service, _ := newTestService(t, time.Now().UTC())
	if _, err := service.List(context.Background(), identity.Scope{}, ListQuery{}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("List() error = %v", err)
	}
}

func TestCancelStopsDelivery(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	service, _ := newTestService(t, now)
	created, err := service.Create(context.Background(), testScope, Reminder{ID: "rem-7", Title: "Later", DueAt: now.Add(-time.Minute)})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := service.Cancel(context.Background(), testScope, created.ID); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	result, err := service.Tick(context.Background(), testScope)
	if err != nil || result.Claimed != 0 {
		t.Fatalf("tick after cancel = %+v err=%v", result, err)
	}
}
