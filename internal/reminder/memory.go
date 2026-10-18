package reminder

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// MemoryStore is a deterministic in-memory Store and InboxStore for tests and
// local demos. It performs no external I/O.
type MemoryStore struct {
	mu     sync.Mutex
	values map[string]Reminder
	inbox  map[string]InboxItem
	now    func() time.Time
}

var (
	_ Store      = (*MemoryStore)(nil)
	_ InboxStore = (*MemoryStore)(nil)
)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		values: make(map[string]Reminder),
		inbox:  make(map[string]InboxItem),
		now:    func() time.Time { return time.Now().UTC() },
	}
}

func (m *MemoryStore) key(scope identity.Scope, id string) string {
	return scope.TenantID + "\x00" + scope.UserID + "\x00" + id
}

func (m *MemoryStore) Save(_ context.Context, scope identity.Scope, value Reminder) (Reminder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.values[m.key(scope, value.ID)]
	if ok {
		value.CreatedAt = existing.CreatedAt
	}
	m.values[m.key(scope, value.ID)] = value
	return value, nil
}

func (m *MemoryStore) Get(_ context.Context, scope identity.Scope, id string) (Reminder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[m.key(scope, id)]
	if !ok {
		return Reminder{}, ErrReminderNotFound
	}
	return value, nil
}

func (m *MemoryStore) List(_ context.Context, scope identity.Scope, query ListQuery) ([]Reminder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := make([]Reminder, 0, len(m.values))
	for _, value := range m.values {
		if value.TenantID != scope.TenantID || value.UserID != scope.UserID {
			continue
		}
		if query.Status != "" && value.Status != query.Status {
			continue
		}
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].DueAt.Before(values[j].DueAt) })
	if len(values) > query.Limit {
		values = values[:query.Limit]
	}
	return values, nil
}

// ClaimDue atomically leases due reminders under a single lock.
func (m *MemoryStore) ClaimDue(_ context.Context, scope identity.Scope, now, leaseUntil time.Time, limit int) ([]Reminder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	claimed := make([]Reminder, 0, limit)
	for key, value := range m.values {
		if len(claimed) >= limit {
			break
		}
		if value.TenantID != scope.TenantID || value.UserID != scope.UserID || value.Status != StatusScheduled {
			continue
		}
		if value.DueAt.After(now) {
			continue
		}
		if value.LeaseUntil.After(now) {
			continue
		}
		value.LeaseUntil = leaseUntil
		m.values[key] = value
		claimed = append(claimed, value)
	}
	sort.Slice(claimed, func(i, j int) bool { return claimed[i].DueAt.Before(claimed[j].DueAt) })
	return claimed, nil
}

func (m *MemoryStore) SaveInboxItem(_ context.Context, scope identity.Scope, item InboxItem) (InboxItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.key(scope, item.ID)
	if existing, ok := m.inbox[key]; ok {
		return existing, nil
	}
	m.inbox[key] = item
	return item, nil
}

func (m *MemoryStore) ListInboxItems(_ context.Context, scope identity.Scope, limit int) ([]InboxItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]InboxItem, 0, len(m.inbox))
	for _, item := range m.inbox {
		if item.TenantID != scope.TenantID || item.UserID != scope.UserID {
			continue
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}
