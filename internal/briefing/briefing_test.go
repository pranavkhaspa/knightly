package briefing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type memoryRepository struct {
	briefings map[string]Briefing
	items     map[string]InboxItem
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		briefings: make(map[string]Briefing),
		items:     make(map[string]InboxItem),
	}
}

func (r *memoryRepository) SaveDaily(_ context.Context, value Briefing) (Briefing, error) {
	if existing, ok := r.briefings[value.Key()]; ok {
		return cloneBriefing(existing), nil
	}
	value = cloneBriefing(value)
	r.briefings[value.Key()] = value
	for _, item := range value.Items {
		r.items[item.ID] = cloneInboxItem(item)
	}
	return cloneBriefing(value), nil
}

func (r *memoryRepository) GetBriefing(_ context.Context, scope identity.Scope, projectID string, date Date) (Briefing, error) {
	for _, value := range r.briefings {
		if sameScope(value.TenantID, value.UserID, scope) && value.ProjectID == projectID && value.Date == date {
			return cloneBriefing(value), nil
		}
	}
	return Briefing{}, ErrBriefingNotFound
}

func (r *memoryRepository) ListBriefings(_ context.Context, query BriefingListQuery) ([]Briefing, error) {
	result := make([]Briefing, 0, len(r.briefings))
	for _, value := range r.briefings {
		if !sameScope(value.TenantID, value.UserID, query.Scope) || value.ProjectID != query.ProjectID {
			continue
		}
		if query.From != "" && dateBefore(value.Date, query.From) || query.To != "" && dateBefore(query.To, value.Date) {
			continue
		}
		result = append(result, cloneBriefing(value))
	}
	return result, nil
}

func (r *memoryRepository) ListInboxItems(_ context.Context, query InboxListQuery) ([]InboxItem, error) {
	result := make([]InboxItem, 0, len(r.items))
	for _, value := range r.items {
		if !sameScope(value.TenantID, value.UserID, query.Scope) || value.ProjectID != query.ProjectID {
			continue
		}
		if query.BriefingID != "" && value.BriefingID != query.BriefingID || query.Date != "" && value.Date != query.Date {
			continue
		}
		if !query.IncludeArchived && value.IsArchived() || query.UnreadOnly && value.IsRead() {
			continue
		}
		result = append(result, cloneInboxItem(value))
	}
	return result, nil
}

func (r *memoryRepository) GetInboxItem(_ context.Context, _ identity.Scope, id string) (InboxItem, error) {
	value, ok := r.items[id]
	if !ok {
		return InboxItem{}, ErrInboxItemNotFound
	}
	return cloneInboxItem(value), nil
}

func (r *memoryRepository) MarkInboxItemRead(_ context.Context, _ identity.Scope, id string, at time.Time) (InboxItem, error) {
	value, ok := r.items[id]
	if !ok {
		return InboxItem{}, ErrInboxItemNotFound
	}
	if value.ReadAt.IsZero() {
		value.ReadAt = at
	}
	if value.State != Archived {
		value.State = Read
	}
	value.UpdatedAt = at
	r.items[id] = value
	return cloneInboxItem(value), nil
}

func (r *memoryRepository) ArchiveInboxItem(_ context.Context, _ identity.Scope, id string, at time.Time) (InboxItem, error) {
	value, ok := r.items[id]
	if !ok {
		return InboxItem{}, ErrInboxItemNotFound
	}
	if value.ArchivedAt.IsZero() {
		value.ArchivedAt = at
	}
	value.State = Archived
	value.UpdatedAt = at
	r.items[id] = value
	return cloneInboxItem(value), nil
}

func testBriefing() Briefing {
	return Briefing{
		TenantID:   "tenant-a",
		UserID:     "user-a",
		ProjectID:  "project-a",
		Date:       "2026-10-17",
		Timezone:   "UTC",
		QuietHours: QuietHours{Start: "22:00", End: "06:00", Timezone: "UTC"},
		Items: []InboxItem{{
			Provider:       "github",
			ProviderItemID: "issue-1",
			Kind:           "pull_request",
			Title:          "Review the new provider adapter",
			Summary:        "A bounded summary",
		}},
		ProviderFailures: []ProviderFailure{{
			Provider:  "rss",
			Code:      "timeout",
			Reason:    "provider timed out",
			Retryable: true,
		}},
	}
}

func testService(repository *memoryRepository) *Service {
	service := NewService(repository)
	service.now = func() time.Time { return time.Date(2026, 10, 17, 9, 30, 0, 0, time.UTC) }
	return service
}

func TestSaveDailyIsIdempotentAndPreservesPartialProviderFailure(t *testing.T) {
	repository := newMemoryRepository()
	service := testService(repository)

	first, err := service.SaveDaily(context.Background(), testBriefing())
	if err != nil {
		t.Fatalf("SaveDaily() error = %v", err)
	}
	if first.ID == "" || first.IdempotencyKey != first.Key() || first.RequestHash == "" {
		t.Fatalf("SaveDaily() did not fill durable identity: %#v", first)
	}
	if len(first.Items) != 1 || first.Items[0].BriefingID != first.ID || first.Items[0].State != Unread {
		t.Fatalf("saved item = %#v, want linked unread item", first.Items)
	}
	if len(first.ProviderFailures) != 1 || !first.ProviderFailures[0].Retryable {
		t.Fatalf("provider failures = %#v, want retained partial failure", first.ProviderFailures)
	}

	replay := testBriefing()
	second, err := service.SaveDaily(context.Background(), replay)
	if err != nil {
		t.Fatalf("idempotent SaveDaily() error = %v", err)
	}
	if second.ID != first.ID || second.CreatedAt != first.CreatedAt || second.RequestHash != first.RequestHash {
		t.Fatalf("replay = %#v, want original durable record", second)
	}

	changed := testBriefing()
	changed.Items[0].Title = "A different request for the same day"
	_, err = service.SaveDaily(context.Background(), changed)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed replay error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestSaveDailyValidatesTimezoneQuietHoursAndBounds(t *testing.T) {
	service := testService(newMemoryRepository())

	invalidTimezone := testBriefing()
	invalidTimezone.Timezone = "Not/A-Timezone"
	_, err := service.SaveDaily(context.Background(), invalidTimezone)
	if !errors.Is(err, ErrInvalidBriefing) {
		t.Fatalf("invalid timezone error = %v, want ErrInvalidBriefing", err)
	}

	invalidQuietHours := testBriefing()
	invalidQuietHours.QuietHours.End = ""
	_, err = service.SaveDaily(context.Background(), invalidQuietHours)
	if !errors.Is(err, ErrInvalidBriefing) {
		t.Fatalf("invalid quiet hours error = %v, want ErrInvalidBriefing", err)
	}

	tooManyItems := testBriefing()
	tooManyItems.Items = make([]InboxItem, MaxBriefingItems+1)
	for index := range tooManyItems.Items {
		tooManyItems.Items[index] = InboxItem{
			Provider:       "github",
			ProviderItemID: "issue-" + string(rune('a'+index%26)),
			Kind:           "issue",
			Title:          "bounded item",
		}
	}
	_, err = service.SaveDaily(context.Background(), tooManyItems)
	if !errors.Is(err, ErrInvalidBriefing) {
		t.Fatalf("too many items error = %v, want ErrInvalidBriefing", err)
	}
}

func TestListDetailReadAndArchiveStayWithinScope(t *testing.T) {
	repository := newMemoryRepository()
	service := testService(repository)
	saved, err := service.SaveDaily(context.Background(), testBriefing())
	if err != nil {
		t.Fatalf("SaveDaily() error = %v", err)
	}
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}

	briefings, err := service.ListBriefings(context.Background(), BriefingListQuery{
		Scope: scope, ProjectID: "project-a", From: "2026-10-17", To: "2026-10-17", IncludeItems: true,
	})
	if err != nil {
		t.Fatalf("ListBriefings() error = %v", err)
	}
	if len(briefings) != 1 || len(briefings[0].Items) != 1 {
		t.Fatalf("ListBriefings() = %#v, want one detailed briefing", briefings)
	}

	detail, err := service.GetBriefing(context.Background(), scope, "project-a", "2026-10-17")
	if err != nil {
		t.Fatalf("GetBriefing() error = %v", err)
	}
	if detail.ID != saved.ID {
		t.Fatalf("GetBriefing() ID = %q, want %q", detail.ID, saved.ID)
	}

	itemID := saved.Items[0].ID
	read, err := service.ReadInboxItem(context.Background(), scope, itemID)
	if err != nil {
		t.Fatalf("ReadInboxItem() error = %v", err)
	}
	if read.State != Read || read.ReadAt.IsZero() {
		t.Fatalf("read item = %#v, want read state and timestamp", read)
	}
	firstReadAt := read.ReadAt
	readAgain, err := service.ReadInboxItem(context.Background(), scope, itemID)
	if err != nil {
		t.Fatalf("repeat ReadInboxItem() error = %v", err)
	}
	if readAgain.ReadAt != firstReadAt {
		t.Fatalf("repeat read timestamp = %v, want original %v", readAgain.ReadAt, firstReadAt)
	}

	archived, err := service.ArchiveInboxItem(context.Background(), scope, itemID)
	if err != nil {
		t.Fatalf("ArchiveInboxItem() error = %v", err)
	}
	if archived.State != Archived || archived.ArchivedAt.IsZero() || !archived.IsRead() {
		t.Fatalf("archived item = %#v, want archived and still read", archived)
	}

	activeItems, err := service.ListInboxItems(context.Background(), InboxListQuery{Scope: scope, ProjectID: "project-a"})
	if err != nil {
		t.Fatalf("ListInboxItems(active) error = %v", err)
	}
	if len(activeItems) != 0 {
		t.Fatalf("active items = %#v, want archived item excluded", activeItems)
	}
	allItems, err := service.ListInboxItems(context.Background(), InboxListQuery{Scope: scope, ProjectID: "project-a", IncludeArchived: true})
	if err != nil {
		t.Fatalf("ListInboxItems(all) error = %v", err)
	}
	if len(allItems) != 1 || allItems[0].ID != itemID {
		t.Fatalf("all items = %#v, want archived item detail", allItems)
	}

	_, err = service.GetInboxItem(context.Background(), identity.Scope{TenantID: "tenant-other", UserID: "user-a"}, itemID)
	if err == nil {
		t.Fatal("GetInboxItem() accepted a different tenant scope")
	}
}

func TestListQueriesRejectUnboundedLimitsAndInvalidDateRanges(t *testing.T) {
	service := testService(newMemoryRepository())
	scope := identity.Scope{TenantID: "tenant-a", UserID: "user-a"}

	_, err := service.ListBriefings(context.Background(), BriefingListQuery{Scope: scope, ProjectID: "project-a", Limit: MaxListLimit + 1})
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("unbounded briefing limit error = %v, want ErrInvalidQuery", err)
	}
	_, err = service.ListBriefings(context.Background(), BriefingListQuery{
		Scope: scope, ProjectID: "project-a", From: "2026-10-18", To: "2026-10-17",
	})
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("reverse date range error = %v, want ErrInvalidQuery", err)
	}
	_, err = service.ListInboxItems(context.Background(), InboxListQuery{Scope: scope, ProjectID: "project-a", Limit: MaxListLimit + 1})
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("unbounded inbox limit error = %v, want ErrInvalidQuery", err)
	}
}
