package reminder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// Store is the durable reminder boundary. ClaimDue must perform an atomic
// lease so two daemon instances cannot deliver the same occurrence twice.
type Store interface {
	Save(context.Context, identity.Scope, Reminder) (Reminder, error)
	Get(context.Context, identity.Scope, string) (Reminder, error)
	List(context.Context, identity.Scope, ListQuery) ([]Reminder, error)
	ClaimDue(context.Context, identity.Scope, time.Time, time.Time, int) ([]Reminder, error)
}

// InboxStore persists deduplicated inbox items.
type InboxStore interface {
	SaveInboxItem(context.Context, identity.Scope, InboxItem) (InboxItem, error)
	ListInboxItems(context.Context, identity.Scope, int) ([]InboxItem, error)
}

// ListQuery filters a reminder list by status.
type ListQuery struct {
	Status Status
	Limit  int
}

// Sink receives one due occurrence. A sink that creates an external side effect
// must report RequiresApproval and is never delivered by an automatic tick.
type Sink interface {
	Name() string
	RequiresApproval() bool
	Deliver(context.Context, identity.Scope, Delivery) error
}

// ServiceDeps wires the reminder service to its storage and delivery sinks.
type ServiceDeps struct {
	Store         Store
	Inbox         InboxStore
	Sinks         []Sink
	Timezone      string
	Clock         func() time.Time
	LeaseDuration time.Duration
}

// TickResult summarizes one reconciliation pass.
type TickResult struct {
	Claimed         int
	Delivered       int
	Deferred        int
	PendingApproval int
}

// Service orchestrates bounded reminder delivery. Inbox delivery is always
// attempted first; external sinks are approval-gated.
type Service struct {
	store         Store
	inbox         InboxStore
	sinks         []Sink
	timezone      string
	now           func() time.Time
	leaseDuration time.Duration
}

func NewService(deps ServiceDeps) (*Service, error) {
	if deps.Store == nil {
		return nil, errors.New("reminder service requires a store")
	}
	if deps.Inbox == nil {
		return nil, errors.New("reminder service requires an inbox store")
	}
	now := deps.Clock
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	lease := deps.LeaseDuration
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	timezone := strings.TrimSpace(deps.Timezone)
	if timezone == "" {
		timezone = "UTC"
	}
	return &Service{
		store: deps.Store, inbox: deps.Inbox, sinks: append([]Sink(nil), deps.Sinks...),
		timezone: timezone, now: now, leaseDuration: lease,
	}, nil
}

// Create validates and persists a reminder.
func (s *Service) Create(ctx context.Context, scope identity.Scope, value Reminder) (Reminder, error) {
	if err := scope.Validate(); err != nil {
		return Reminder{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	value.TenantID = scope.TenantID
	value.UserID = scope.UserID
	normalized, err := NormalizeReminder(value, s.timezone)
	if err != nil {
		return Reminder{}, err
	}
	if normalized.CreatedAt.IsZero() {
		normalized.CreatedAt = s.now().UTC()
	}
	normalized.UpdatedAt = s.now().UTC()
	if normalized.Status == StatusDelivered || normalized.Status == StatusCancelled {
		return Reminder{}, fmt.Errorf("%w: cannot create a reminder in status %q", ErrInvalidReminder, normalized.Status)
	}
	return s.store.Save(ctx, scope, normalized)
}

func (s *Service) Get(ctx context.Context, scope identity.Scope, id string) (Reminder, error) {
	if err := scope.Validate(); err != nil {
		return Reminder{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	return s.store.Get(ctx, scope, id)
}

func (s *Service) List(ctx context.Context, scope identity.Scope, query ListQuery) ([]Reminder, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if query.Limit < 1 || query.Limit > MaxListLimit {
		query.Limit = DefaultListLimit
	}
	if query.Status != "" && !query.Status.Valid() {
		return nil, fmt.Errorf("%w: unknown status %q", ErrInvalidReminder, query.Status)
	}
	return s.store.List(ctx, scope, query)
}

// Cancel stops future delivery of a reminder.
func (s *Service) Cancel(ctx context.Context, scope identity.Scope, id string) (Reminder, error) {
	if err := scope.Validate(); err != nil {
		return Reminder{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	value, err := s.store.Get(ctx, scope, id)
	if err != nil {
		return Reminder{}, err
	}
	if value.Status == StatusDelivered || value.Status == StatusCancelled {
		return value, nil
	}
	value.Status = StatusCancelled
	value.LeaseUntil = time.Time{}
	value.UpdatedAt = s.now().UTC()
	return s.store.Save(ctx, scope, value)
}

func (s *Service) Inbox(ctx context.Context, scope identity.Scope, limit int) ([]InboxItem, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if limit < 1 || limit > MaxListLimit {
		limit = DefaultListLimit
	}
	return s.inbox.ListInboxItems(ctx, scope, limit)
}

// Tick claims due reminders and delivers their current occurrence. It is safe
// to call repeatedly and after restart: delivery is deduplicated by occurrence
// key and the reminder is advanced to its next occurrence.
func (s *Service) Tick(ctx context.Context, scope identity.Scope) (TickResult, error) {
	if err := scope.Validate(); err != nil {
		return TickResult{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	now := s.now().UTC()
	claimed, err := s.store.ClaimDue(ctx, scope, now, now.Add(s.leaseDuration), MaxClaimBatch)
	if err != nil {
		return TickResult{}, err
	}
	result := TickResult{Claimed: len(claimed)}
	for _, value := range claimed {
		outcome, deliverErr := s.deliverOccurrence(ctx, scope, value, now)
		if deliverErr != nil {
			return result, deliverErr
		}
		switch outcome {
		case StatusDelivered:
			result.Delivered++
		case StatusPendingApproval:
			result.PendingApproval++
		default:
			result.Deferred++
		}
	}
	return result, nil
}

func (s *Service) deliverOccurrence(ctx context.Context, scope identity.Scope, value Reminder, now time.Time) (Status, error) {
	location := LoadLocation(value.Timezone)
	if value.QuietHours.active(now, location) {
		value.DueAt = value.QuietHours.resumeAt(now, location)
		value.LeaseUntil = time.Time{}
		value.UpdatedAt = now
		if _, err := s.store.Save(ctx, scope, value); err != nil {
			return "", err
		}
		return StatusScheduled, nil
	}
	occurrence := value.DueAt
	delivery := Delivery{Reminder: value, Occurrence: occurrence, IdempotencyKey: OccurrenceKey(value, occurrence)}
	if err := s.deliverInbox(ctx, scope, delivery, now); err != nil {
		return "", err
	}
	externalPending := false
	for _, sink := range s.sinks {
		if sink.RequiresApproval() {
			externalPending = true
			continue
		}
		if err := sink.Deliver(ctx, scope, delivery); err != nil {
			return "", fmt.Errorf("deliver reminder %q to %s: %w", value.ID, sink.Name(), err)
		}
	}
	value.Attempts++
	value.DeliveredAt = now
	value.LeaseUntil = time.Time{}
	if externalPending {
		value.Status = StatusPendingApproval
		value.UpdatedAt = now
		saved, err := s.store.Save(ctx, scope, value)
		if err != nil {
			return "", err
		}
		return saved.Status, nil
	}
	return s.finalizeOccurrence(ctx, scope, value, occurrence, now)
}

func (s *Service) finalizeOccurrence(ctx context.Context, scope identity.Scope, value Reminder, occurrence, now time.Time) (Status, error) {
	if next, ok := value.Recurrence.next(occurrence); ok {
		value.DueAt = next
		value.Status = StatusScheduled
		value.DeliveredAt = time.Time{}
	} else {
		value.Status = StatusDelivered
	}
	value.UpdatedAt = now
	saved, err := s.store.Save(ctx, scope, value)
	if err != nil {
		return "", err
	}
	return saved.Status, nil
}

// Approve delivers an occurrence that was held for explicit approval. It is
// idempotent for reminders that are no longer pending.
func (s *Service) Approve(ctx context.Context, scope identity.Scope, id, approvedBy string) (Reminder, error) {
	if err := scope.Validate(); err != nil {
		return Reminder{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	approvedBy = strings.TrimSpace(approvedBy)
	if approvedBy == "" {
		return Reminder{}, fmt.Errorf("%w: approving actor is required", ErrApprovalRequired)
	}
	value, err := s.store.Get(ctx, scope, id)
	if err != nil {
		return Reminder{}, err
	}
	if value.Status != StatusPendingApproval {
		return value, nil
	}
	now := s.now().UTC()
	occurrence := value.DueAt
	delivery := Delivery{Reminder: value, Occurrence: occurrence, IdempotencyKey: OccurrenceKey(value, occurrence)}
	for _, sink := range s.sinks {
		if !sink.RequiresApproval() {
			continue
		}
		if err := sink.Deliver(ctx, scope, delivery); err != nil {
			return Reminder{}, fmt.Errorf("approve reminder %q to %s: %w", value.ID, sink.Name(), err)
		}
	}
	value.ApprovedBy = approvedBy
	if _, err := s.finalizeOccurrence(ctx, scope, value, occurrence, now); err != nil {
		return Reminder{}, err
	}
	return s.store.Get(ctx, scope, id)
}

func (s *Service) deliverInbox(ctx context.Context, scope identity.Scope, delivery Delivery, now time.Time) error {
	key := delivery.IdempotencyKey
	item := InboxItem{
		ID:             "inbox_" + key,
		TenantID:       scope.TenantID,
		UserID:         scope.UserID,
		Kind:           InboxKindReminder,
		Title:          delivery.Reminder.Title,
		Body:           delivery.Reminder.Body,
		ReminderID:     delivery.Reminder.ID,
		DueAt:          delivery.Occurrence.UTC(),
		IdempotencyKey: key,
		CreatedAt:      now,
	}
	item.ID = bound(item.ID, MaxIDLength)
	if _, err := s.inbox.SaveInboxItem(ctx, scope, item); err != nil {
		return fmt.Errorf("deliver reminder %q to inbox: %w", delivery.Reminder.ID, err)
	}
	return nil
}
