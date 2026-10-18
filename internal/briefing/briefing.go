// Package briefing contains the durable domain contract for one user's daily
// briefing and the inbox items collected into it. It deliberately has no
// transport or persistence implementation; repositories adapt this contract to
// the application's durable store.
package briefing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	MaxTimezone              = 64
	MaxBriefingItems         = 100
	MaxProviderFailures      = 32
	MaxProviderName          = 64
	MaxProviderItemID        = 256
	MaxProviderCode          = 64
	MaxProviderFailureReason = 1024
	MaxInboxKind             = 64
	MaxInboxTitle            = 240
	MaxInboxSummary          = 8000
	MaxInboxURL              = 2048
	DefaultListLimit         = 20
	MaxListLimit             = 100
)

var (
	ErrInvalidBriefing     = errors.New("invalid briefing")
	ErrInvalidInboxItem    = errors.New("invalid inbox item")
	ErrInvalidQuery        = errors.New("invalid briefing query")
	ErrBriefingNotFound    = errors.New("briefing not found")
	ErrInboxItemNotFound   = errors.New("inbox item not found")
	ErrIdempotencyConflict = errors.New("briefing idempotency key conflicts with an existing record")
	ErrInvalidInboxState   = errors.New("invalid inbox item state")
)

// Date is a civil calendar date in ISO-8601 form. It intentionally is not a
// time.Time: a daily briefing belongs to a local date, not to an instant.
type Date string

const dateLayout = "2006-01-02"

func (d Date) Validate() error {
	value := strings.TrimSpace(string(d))
	if value == "" {
		return errors.New("date is required")
	}
	if _, err := time.Parse(dateLayout, value); err != nil || value != string(d) {
		return errors.New("date must use YYYY-MM-DD")
	}
	return nil
}

func (d Date) String() string { return string(d) }

// QuietHours is a daily half-open interval in the briefing timezone. Equal
// start and end values disable quiet hours; a start after end crosses midnight.
type QuietHours struct {
	Start    string `json:"start,omitempty"`
	End      string `json:"end,omitempty"`
	Timezone string `json:"timezone,omitempty"`
}

func (q QuietHours) Enabled() bool {
	return strings.TrimSpace(q.Start) != "" && strings.TrimSpace(q.End) != "" && q.Start != q.End
}

// ProviderFailure records an expected provider-specific failure while allowing
// the rest of the daily briefing to be durably saved.
type ProviderFailure struct {
	Provider   string    `json:"provider"`
	Code       string    `json:"code,omitempty"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	OccurredAt time.Time `json:"occurred_at"`
}

type InboxState string

const (
	Unread   InboxState = "unread"
	Read     InboxState = "read"
	Archived InboxState = "archived"
)

func (s InboxState) Valid() bool {
	switch s {
	case Unread, Read, Archived:
		return true
	default:
		return false
	}
}

// InboxItem is an independently durable item belonging to one daily briefing.
// ReadAt and ArchivedAt preserve the audit instants while State provides the
// bounded queryable state. An archived item may or may not have been read.
type InboxItem struct {
	ID             string     `json:"id"`
	BriefingID     string     `json:"briefing_id"`
	TenantID       string     `json:"tenant_id"`
	UserID         string     `json:"user_id"`
	ProjectID      string     `json:"project_id"`
	Date           Date       `json:"date"`
	Provider       string     `json:"provider"`
	ProviderItemID string     `json:"provider_item_id"`
	Kind           string     `json:"kind"`
	Title          string     `json:"title"`
	Summary        string     `json:"summary,omitempty"`
	URL            string     `json:"url,omitempty"`
	ContentHash    string     `json:"content_hash,omitempty"`
	PublishedAt    time.Time  `json:"published_at,omitempty"`
	State          InboxState `json:"state"`
	ReadAt         time.Time  `json:"read_at,omitempty"`
	ArchivedAt     time.Time  `json:"archived_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (i InboxItem) IsRead() bool     { return !i.ReadAt.IsZero() }
func (i InboxItem) IsArchived() bool { return !i.ArchivedAt.IsZero() }

// Briefing is the durable aggregate for one tenant/user/project/date tuple.
// A non-empty ProviderFailures list is a partial result, not a failed save.
type Briefing struct {
	ID               string            `json:"id"`
	TenantID         string            `json:"tenant_id"`
	UserID           string            `json:"user_id"`
	ProjectID        string            `json:"project_id"`
	Date             Date              `json:"date"`
	Timezone         string            `json:"timezone"`
	QuietHours       QuietHours        `json:"quiet_hours,omitempty"`
	IdempotencyKey   string            `json:"idempotency_key"`
	RequestHash      string            `json:"request_hash"`
	Items            []InboxItem       `json:"items,omitempty"`
	ProviderFailures []ProviderFailure `json:"provider_failures,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

// Key is the idempotency tuple for a daily briefing. Repositories must enforce
// uniqueness on this value atomically and return the existing record on an
// identical replay.
func (b Briefing) Key() string {
	return strings.Join([]string{b.TenantID, b.UserID, b.ProjectID, string(b.Date)}, "\x00")
}

type BriefingListQuery struct {
	Scope        identity.Scope
	ProjectID    string
	From         Date
	To           Date
	Limit        int
	IncludeItems bool
}

type InboxListQuery struct {
	Scope           identity.Scope
	ProjectID       string
	BriefingID      string
	Date            Date
	IncludeArchived bool
	UnreadOnly      bool
	Limit           int
}

// BriefingRepository is the storage seam for durable daily records. SaveDaily
// must atomically create both the briefing and its inbox items, enforcing the
// tenant/user/project/date uniqueness represented by Briefing.Key().
type BriefingRepository interface {
	SaveDaily(context.Context, Briefing) (Briefing, error)
	GetBriefing(context.Context, identity.Scope, string, Date) (Briefing, error)
	ListBriefings(context.Context, BriefingListQuery) ([]Briefing, error)
}

// InboxRepository is the storage seam for independently addressable inbox
// records and their idempotent state transitions.
type InboxRepository interface {
	ListInboxItems(context.Context, InboxListQuery) ([]InboxItem, error)
	GetInboxItem(context.Context, identity.Scope, string) (InboxItem, error)
	MarkInboxItemRead(context.Context, identity.Scope, string, time.Time) (InboxItem, error)
	ArchiveInboxItem(context.Context, identity.Scope, string, time.Time) (InboxItem, error)
}

// Repository is the complete persistence boundary required by Service.
type Repository interface {
	BriefingRepository
	InboxRepository
}

// BriefingService is the application-facing contract. Implementations are
// expected to preserve tenant/user scope on every read and state transition.
type BriefingService interface {
	SaveDaily(context.Context, Briefing) (Briefing, error)
	ListBriefings(context.Context, BriefingListQuery) ([]Briefing, error)
	GetBriefing(context.Context, identity.Scope, string, Date) (Briefing, error)
	ListInboxItems(context.Context, InboxListQuery) ([]InboxItem, error)
	GetInboxItem(context.Context, identity.Scope, string) (InboxItem, error)
	ReadInboxItem(context.Context, identity.Scope, string) (InboxItem, error)
	ArchiveInboxItem(context.Context, identity.Scope, string) (InboxItem, error)
}

type Service struct {
	repository Repository
	now        func() time.Time
}

func NewService(repository Repository) *Service {
	return &Service{
		repository: repository,
		now:        func() time.Time { return time.Now().UTC() },
	}
}

var _ BriefingService = (*Service)(nil)

// SaveDaily validates and durably saves a complete or partial daily briefing.
// Replaying the same tenant/user/project/date and request returns the stored
// record; changing the request for that tuple returns ErrIdempotencyConflict.
func (s *Service) SaveDaily(ctx context.Context, value Briefing) (Briefing, error) {
	if s == nil || s.repository == nil {
		return Briefing{}, fmt.Errorf("%w: repository is required", ErrInvalidBriefing)
	}
	value = s.prepareBriefing(value)
	if err := value.Validate(); err != nil {
		return Briefing{}, err
	}
	stored, err := s.repository.SaveDaily(ctx, value)
	if err != nil {
		return Briefing{}, fmt.Errorf("save daily briefing %q: %w", value.ID, err)
	}
	if err := stored.Validate(); err != nil {
		return Briefing{}, fmt.Errorf("stored daily briefing %q: %w", value.ID, err)
	}
	if stored.Key() != value.Key() {
		return Briefing{}, fmt.Errorf("%w: repository returned a different daily key", ErrInvalidBriefing)
	}
	if stored.RequestHash != value.RequestHash {
		return Briefing{}, fmt.Errorf("%w: key %q", ErrIdempotencyConflict, value.IdempotencyKey)
	}
	return cloneBriefing(stored), nil
}

func (s *Service) ListBriefings(ctx context.Context, query BriefingListQuery) ([]Briefing, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidQuery)
	}
	query = normalizeBriefingQuery(query)
	if err := validateBriefingQuery(query); err != nil {
		return nil, err
	}
	values, err := s.repository.ListBriefings(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list briefings: %w", err)
	}
	if len(values) > query.Limit {
		values = values[:query.Limit]
	}
	result := make([]Briefing, 0, len(values))
	for _, value := range values {
		if err := validateBriefing(value); err != nil {
			return nil, fmt.Errorf("%w: repository returned invalid briefing: %v", ErrInvalidBriefing, err)
		}
		if !sameScope(value.TenantID, value.UserID, query.Scope) || value.ProjectID != query.ProjectID {
			return nil, fmt.Errorf("%w: repository returned a briefing outside the requested scope", ErrInvalidBriefing)
		}
		if query.From != "" && dateBefore(value.Date, query.From) || query.To != "" && dateBefore(query.To, value.Date) {
			return nil, fmt.Errorf("%w: repository returned a briefing outside the requested date range", ErrInvalidBriefing)
		}
		copy := cloneBriefing(value)
		if !query.IncludeItems {
			copy.Items = nil
		}
		result = append(result, copy)
	}
	return result, nil
}

func (s *Service) GetBriefing(ctx context.Context, scope identity.Scope, projectID string, date Date) (Briefing, error) {
	if s == nil || s.repository == nil {
		return Briefing{}, fmt.Errorf("%w: repository is required", ErrBriefingNotFound)
	}
	if err := validateLookup(scope, projectID, date); err != nil {
		return Briefing{}, fmt.Errorf("%w: %v", ErrBriefingNotFound, err)
	}
	value, err := s.repository.GetBriefing(ctx, scope, strings.TrimSpace(projectID), normalizeDate(date))
	if err != nil {
		return Briefing{}, fmt.Errorf("get briefing %q: %w", date, err)
	}
	if err := validateBriefing(value); err != nil {
		return Briefing{}, fmt.Errorf("%w: repository returned invalid briefing: %v", ErrInvalidBriefing, err)
	}
	if !sameScope(value.TenantID, value.UserID, scope) || value.ProjectID != strings.TrimSpace(projectID) || value.Date != normalizeDate(date) {
		return Briefing{}, fmt.Errorf("%w: repository returned a briefing outside the requested scope", ErrInvalidBriefing)
	}
	return cloneBriefing(value), nil
}

func (s *Service) ListInboxItems(ctx context.Context, query InboxListQuery) ([]InboxItem, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidQuery)
	}
	query = normalizeInboxQuery(query)
	if err := validateInboxQuery(query); err != nil {
		return nil, err
	}
	values, err := s.repository.ListInboxItems(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list inbox items: %w", err)
	}
	if len(values) > query.Limit {
		values = values[:query.Limit]
	}
	result := make([]InboxItem, 0, len(values))
	for _, value := range values {
		if err := validateInboxItem(value); err != nil {
			return nil, fmt.Errorf("%w: repository returned invalid inbox item: %v", ErrInvalidInboxItem, err)
		}
		if !sameScope(value.TenantID, value.UserID, query.Scope) || value.ProjectID != query.ProjectID {
			return nil, fmt.Errorf("%w: repository returned an item outside the requested scope", ErrInvalidInboxItem)
		}
		if query.BriefingID != "" && value.BriefingID != query.BriefingID || query.Date != "" && value.Date != query.Date {
			return nil, fmt.Errorf("%w: repository returned an item outside the requested briefing", ErrInvalidInboxItem)
		}
		if !query.IncludeArchived && value.IsArchived() {
			return nil, fmt.Errorf("%w: repository returned an archived item for an active-only query", ErrInvalidInboxItem)
		}
		if query.UnreadOnly && value.IsRead() {
			return nil, fmt.Errorf("%w: repository returned a read item for an unread-only query", ErrInvalidInboxItem)
		}
		result = append(result, cloneInboxItem(value))
	}
	return result, nil
}

func (s *Service) GetInboxItem(ctx context.Context, scope identity.Scope, id string) (InboxItem, error) {
	if s == nil || s.repository == nil {
		return InboxItem{}, fmt.Errorf("%w: repository is required", ErrInboxItemNotFound)
	}
	if err := validateScopeAndID(scope, "inbox item ID", id); err != nil {
		return InboxItem{}, fmt.Errorf("%w: %v", ErrInboxItemNotFound, err)
	}
	value, err := s.repository.GetInboxItem(ctx, scope, strings.TrimSpace(id))
	if err != nil {
		return InboxItem{}, fmt.Errorf("get inbox item %q: %w", id, err)
	}
	if err := validateInboxItem(value); err != nil {
		return InboxItem{}, fmt.Errorf("%w: repository returned invalid inbox item: %v", ErrInvalidInboxItem, err)
	}
	if !sameScope(value.TenantID, value.UserID, scope) || value.ID != strings.TrimSpace(id) {
		return InboxItem{}, fmt.Errorf("%w: repository returned an item outside the requested scope", ErrInvalidInboxItem)
	}
	return cloneInboxItem(value), nil
}

// ReadInboxItem is idempotent. Repeated calls preserve the first ReadAt value
// in the repository while returning the current record.
func (s *Service) ReadInboxItem(ctx context.Context, scope identity.Scope, id string) (InboxItem, error) {
	if s == nil || s.repository == nil {
		return InboxItem{}, fmt.Errorf("%w: repository is required", ErrInvalidInboxState)
	}
	if err := validateScopeAndID(scope, "inbox item ID", id); err != nil {
		return InboxItem{}, fmt.Errorf("%w: %v", ErrInvalidInboxState, err)
	}
	value, err := s.repository.MarkInboxItemRead(ctx, scope, strings.TrimSpace(id), s.currentTime())
	if err != nil {
		return InboxItem{}, fmt.Errorf("read inbox item %q: %w", id, err)
	}
	if err := validateInboxItem(value); err != nil {
		return InboxItem{}, fmt.Errorf("%w: repository returned invalid inbox item: %v", ErrInvalidInboxState, err)
	}
	if !sameScope(value.TenantID, value.UserID, scope) || value.ID != strings.TrimSpace(id) {
		return InboxItem{}, fmt.Errorf("%w: repository returned an item outside the requested scope", ErrInvalidInboxState)
	}
	if !value.IsRead() {
		return InboxItem{}, fmt.Errorf("%w: repository did not mark item as read", ErrInvalidInboxState)
	}
	return cloneInboxItem(value), nil
}

// ArchiveInboxItem is idempotent and does not implicitly mark an item read.
func (s *Service) ArchiveInboxItem(ctx context.Context, scope identity.Scope, id string) (InboxItem, error) {
	if s == nil || s.repository == nil {
		return InboxItem{}, fmt.Errorf("%w: repository is required", ErrInvalidInboxState)
	}
	if err := validateScopeAndID(scope, "inbox item ID", id); err != nil {
		return InboxItem{}, fmt.Errorf("%w: %v", ErrInvalidInboxState, err)
	}
	value, err := s.repository.ArchiveInboxItem(ctx, scope, strings.TrimSpace(id), s.currentTime())
	if err != nil {
		return InboxItem{}, fmt.Errorf("archive inbox item %q: %w", id, err)
	}
	if err := validateInboxItem(value); err != nil {
		return InboxItem{}, fmt.Errorf("%w: repository returned invalid inbox item: %v", ErrInvalidInboxState, err)
	}
	if !sameScope(value.TenantID, value.UserID, scope) || value.ID != strings.TrimSpace(id) {
		return InboxItem{}, fmt.Errorf("%w: repository returned an item outside the requested scope", ErrInvalidInboxState)
	}
	if !value.IsArchived() {
		return InboxItem{}, fmt.Errorf("%w: repository did not archive item", ErrInvalidInboxState)
	}
	return cloneInboxItem(value), nil
}

// Validate checks the durable invariants for a briefing and all embedded
// inbox records. Service callers normally use SaveDaily, which fills derived
// identifiers and timestamps before validation.
func (b Briefing) Validate() error { return validateBriefing(b) }

// Validate checks the durable invariants for one inbox item.
func (i InboxItem) Validate() error { return validateInboxItem(i) }

func (s *Service) prepareBriefing(value Briefing) Briefing {
	value = cloneBriefing(value)
	value.TenantID = identity.NormalizeID(value.TenantID)
	value.UserID = identity.NormalizeID(value.UserID)
	value.ProjectID = identity.NormalizeID(value.ProjectID)
	value.Date = normalizeDate(value.Date)
	value.Timezone = strings.TrimSpace(value.Timezone)
	value.QuietHours = normalizeQuietHours(value.QuietHours)
	if value.ID == "" {
		value.ID = stableBriefingID(value)
	}
	if value.IdempotencyKey == "" {
		value.IdempotencyKey = value.Key()
	}
	now := s.currentTime()
	if value.CreatedAt.IsZero() {
		value.CreatedAt = now
	}
	if value.UpdatedAt.IsZero() {
		value.UpdatedAt = value.CreatedAt
	}
	for index := range value.Items {
		item := &value.Items[index]
		*item = normalizeInboxItem(*item)
		if item.ID == "" {
			item.ID = stableInboxItemID(value, *item)
		}
		if item.BriefingID == "" {
			item.BriefingID = value.ID
		}
		if item.TenantID == "" {
			item.TenantID = value.TenantID
		}
		if item.UserID == "" {
			item.UserID = value.UserID
		}
		if item.ProjectID == "" {
			item.ProjectID = value.ProjectID
		}
		if item.Date == "" {
			item.Date = value.Date
		}
		if item.State == "" {
			item.State = stateFor(item)
		}
		if item.CreatedAt.IsZero() {
			item.CreatedAt = value.CreatedAt
		}
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = item.CreatedAt
		}
	}
	for index := range value.ProviderFailures {
		failure := &value.ProviderFailures[index]
		failure.Provider = strings.TrimSpace(failure.Provider)
		failure.Code = strings.TrimSpace(failure.Code)
		failure.Reason = strings.TrimSpace(failure.Reason)
		if failure.OccurredAt.IsZero() {
			failure.OccurredAt = now
		}
	}
	if value.RequestHash == "" {
		value.RequestHash = fingerprint(value)
	}
	return value
}

func (s *Service) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

func validateBriefing(value Briefing) error {
	if err := identity.ValidateID("briefing ID", value.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBriefing, err)
	}
	if err := (identity.Scope{TenantID: value.TenantID, UserID: value.UserID}).Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBriefing, err)
	}
	if err := identity.ValidateID("project ID", value.ProjectID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBriefing, err)
	}
	if err := value.Date.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBriefing, err)
	}
	if err := validateTimezone(value.Timezone, "timezone"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBriefing, err)
	}
	if err := validateQuietHours(value.QuietHours, value.Timezone); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBriefing, err)
	}
	if value.IdempotencyKey == "" || value.IdempotencyKey != value.Key() {
		return fmt.Errorf("%w: idempotency key must match tenant, user, project, and date", ErrInvalidBriefing)
	}
	if err := validateHash(value.RequestHash); err != nil {
		return fmt.Errorf("%w: request hash: %v", ErrInvalidBriefing, err)
	}
	if len(value.Items) > MaxBriefingItems {
		return fmt.Errorf("%w: a briefing cannot contain more than %d items", ErrInvalidBriefing, MaxBriefingItems)
	}
	if len(value.ProviderFailures) > MaxProviderFailures {
		return fmt.Errorf("%w: a briefing cannot contain more than %d provider failures", ErrInvalidBriefing, MaxProviderFailures)
	}
	seenIDs := make(map[string]struct{}, len(value.Items))
	seenProviderItems := make(map[string]struct{}, len(value.Items))
	for _, item := range value.Items {
		if err := validateInboxItem(item); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidBriefing, err)
		}
		if !sameBriefingIdentity(item, value) {
			return fmt.Errorf("%w: inbox item %q does not belong to the briefing", ErrInvalidBriefing, item.ID)
		}
		if _, exists := seenIDs[item.ID]; exists {
			return fmt.Errorf("%w: duplicate inbox item ID %q", ErrInvalidBriefing, item.ID)
		}
		seenIDs[item.ID] = struct{}{}
		providerKey := item.Provider + "\x00" + item.ProviderItemID
		if _, exists := seenProviderItems[providerKey]; exists {
			return fmt.Errorf("%w: duplicate provider item %q", ErrInvalidBriefing, item.ProviderItemID)
		}
		seenProviderItems[providerKey] = struct{}{}
	}
	for _, failure := range value.ProviderFailures {
		if err := validateProviderFailure(failure); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidBriefing, err)
		}
	}
	return nil
}

func validateInboxItem(value InboxItem) error {
	for label, id := range map[string]string{
		"inbox item ID": value.ID, "briefing ID": value.BriefingID,
		"tenant ID": value.TenantID, "user ID": value.UserID, "project ID": value.ProjectID,
	} {
		if err := identity.ValidateID(label, id); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidInboxItem, err)
		}
	}
	if err := value.Date.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInboxItem, err)
	}
	for label, item := range map[string]string{"provider": value.Provider, "provider item ID": value.ProviderItemID, "kind": value.Kind, "title": value.Title} {
		if strings.TrimSpace(item) == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidInboxItem, label)
		}
	}
	if len([]rune(value.Provider)) > MaxProviderName || len([]rune(value.ProviderItemID)) > MaxProviderItemID {
		return fmt.Errorf("%w: provider identity exceeds its bound", ErrInvalidInboxItem)
	}
	if len([]rune(value.Kind)) > MaxInboxKind || len([]rune(value.Title)) > MaxInboxTitle || len([]rune(value.Summary)) > MaxInboxSummary {
		return fmt.Errorf("%w: kind, title, or summary exceeds its bound", ErrInvalidInboxItem)
	}
	if len([]rune(value.ContentHash)) > 128 {
		return fmt.Errorf("%w: content hash exceeds its bound", ErrInvalidInboxItem)
	}
	if len([]rune(value.URL)) > MaxInboxURL {
		return fmt.Errorf("%w: URL exceeds its bound", ErrInvalidInboxItem)
	}
	if value.URL != "" {
		parsed, err := url.Parse(value.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
			return fmt.Errorf("%w: URL must be an http(s) URL without credentials", ErrInvalidInboxItem)
		}
	}
	if !value.State.Valid() {
		return fmt.Errorf("%w: unknown state %q", ErrInvalidInboxState, value.State)
	}
	if value.State == Unread && !value.ReadAt.IsZero() {
		return fmt.Errorf("%w: unread item cannot have a read timestamp", ErrInvalidInboxState)
	}
	if value.State == Read && value.ReadAt.IsZero() {
		return fmt.Errorf("%w: read item requires a read timestamp", ErrInvalidInboxState)
	}
	if value.State != Archived && !value.ArchivedAt.IsZero() {
		return fmt.Errorf("%w: only archived items may have an archive timestamp", ErrInvalidInboxState)
	}
	if value.State == Archived && value.ArchivedAt.IsZero() {
		return fmt.Errorf("%w: archived item requires an archive timestamp", ErrInvalidInboxState)
	}
	return nil
}

func validateProviderFailure(value ProviderFailure) error {
	if strings.TrimSpace(value.Provider) == "" {
		return errors.New("provider failure provider is required")
	}
	if len([]rune(value.Provider)) > MaxProviderName || len([]rune(value.Code)) > MaxProviderCode || len([]rune(value.Reason)) > MaxProviderFailureReason {
		return errors.New("provider failure exceeds its bound")
	}
	if strings.TrimSpace(value.Reason) == "" {
		return errors.New("provider failure reason is required")
	}
	if value.OccurredAt.IsZero() {
		return errors.New("provider failure occurrence time is required")
	}
	return nil
}

func validateTimezone(value, label string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", label)
	}
	if len([]rune(value)) > MaxTimezone {
		return fmt.Errorf("%s is too long", label)
	}
	if _, err := time.LoadLocation(value); err != nil {
		return fmt.Errorf("%s must be a valid IANA timezone", label)
	}
	return nil
}

func validateQuietHours(value QuietHours, briefingTimezone string) error {
	if (strings.TrimSpace(value.Start) == "") != (strings.TrimSpace(value.End) == "") {
		return errors.New("quiet hours require both start and end")
	}
	if value.Start != "" {
		for label, clock := range map[string]string{"quiet hours start": value.Start, "quiet hours end": value.End} {
			if len(clock) != len("15:04") {
				return fmt.Errorf("%s must use HH:MM", label)
			}
			if _, err := time.Parse("15:04", clock); err != nil {
				return fmt.Errorf("%s must use HH:MM", label)
			}
		}
	}
	if value.Timezone != "" {
		if err := validateTimezone(value.Timezone, "quiet hours timezone"); err != nil {
			return err
		}
		if value.Timezone != briefingTimezone {
			return errors.New("quiet hours timezone must match briefing timezone")
		}
	}
	return nil
}

func validateHash(value string) error {
	if len(value) != sha256.Size*2 {
		return errors.New("must be a SHA-256 hex digest")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return errors.New("must be a SHA-256 hex digest")
	}
	return nil
}

func validateBriefingQuery(query BriefingListQuery) error {
	if err := query.Scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	if err := identity.ValidateID("project ID", query.ProjectID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	if err := validateDateRange(query.From, query.To); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	if query.Limit < 1 || query.Limit > MaxListLimit {
		return fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxListLimit)
	}
	return nil
}

func validateInboxQuery(query InboxListQuery) error {
	if err := query.Scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	if err := identity.ValidateID("project ID", query.ProjectID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	if query.BriefingID != "" {
		if err := identity.ValidateID("briefing ID", query.BriefingID); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidQuery, err)
		}
	}
	if query.Date != "" {
		if err := query.Date.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidQuery, err)
		}
	}
	if query.Limit < 1 || query.Limit > MaxListLimit {
		return fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxListLimit)
	}
	return nil
}

func validateLookup(scope identity.Scope, projectID string, date Date) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := identity.ValidateID("project ID", projectID); err != nil {
		return err
	}
	return date.Validate()
}

func validateScopeAndID(scope identity.Scope, label, id string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	return identity.ValidateID(label, id)
}

func validateDateRange(from, to Date) error {
	if from != "" {
		if err := from.Validate(); err != nil {
			return fmt.Errorf("from date: %v", err)
		}
	}
	if to != "" {
		if err := to.Validate(); err != nil {
			return fmt.Errorf("to date: %v", err)
		}
	}
	if from != "" && to != "" && dateBefore(to, from) {
		return errors.New("from date must not be after to date")
	}
	return nil
}

func normalizeBriefingQuery(query BriefingListQuery) BriefingListQuery {
	query.ProjectID = identity.NormalizeID(query.ProjectID)
	query.From = normalizeDate(query.From)
	query.To = normalizeDate(query.To)
	if query.Limit == 0 {
		query.Limit = DefaultListLimit
	}
	return query
}

func normalizeInboxQuery(query InboxListQuery) InboxListQuery {
	query.ProjectID = identity.NormalizeID(query.ProjectID)
	query.BriefingID = identity.NormalizeID(query.BriefingID)
	query.Date = normalizeDate(query.Date)
	if query.Limit == 0 {
		query.Limit = DefaultListLimit
	}
	return query
}

func normalizeDate(value Date) Date { return Date(strings.TrimSpace(string(value))) }

func normalizeQuietHours(value QuietHours) QuietHours {
	value.Start = strings.TrimSpace(value.Start)
	value.End = strings.TrimSpace(value.End)
	value.Timezone = strings.TrimSpace(value.Timezone)
	return value
}

func normalizeInboxItem(value InboxItem) InboxItem {
	value.ID = identity.NormalizeID(value.ID)
	value.BriefingID = identity.NormalizeID(value.BriefingID)
	value.TenantID = identity.NormalizeID(value.TenantID)
	value.UserID = identity.NormalizeID(value.UserID)
	value.ProjectID = identity.NormalizeID(value.ProjectID)
	value.Date = normalizeDate(value.Date)
	value.Provider = strings.TrimSpace(value.Provider)
	value.ProviderItemID = strings.TrimSpace(value.ProviderItemID)
	value.Kind = strings.TrimSpace(value.Kind)
	value.Title = strings.TrimSpace(value.Title)
	value.Summary = strings.TrimSpace(value.Summary)
	value.URL = strings.TrimSpace(value.URL)
	value.ContentHash = strings.TrimSpace(value.ContentHash)
	return value
}

func stateFor(value *InboxItem) InboxState {
	if !value.ArchivedAt.IsZero() {
		return Archived
	}
	if !value.ReadAt.IsZero() {
		return Read
	}
	return Unread
}

func sameScope(tenantID, userID string, scope identity.Scope) bool {
	return tenantID == scope.TenantID && userID == scope.UserID
}

func sameBriefingIdentity(item InboxItem, value Briefing) bool {
	return item.BriefingID == value.ID && item.TenantID == value.TenantID && item.UserID == value.UserID && item.ProjectID == value.ProjectID && item.Date == value.Date
}

func dateBefore(left, right Date) bool { return string(left) < string(right) }

func stableBriefingID(value Briefing) string {
	sum := sha256.Sum256([]byte("briefing\x00" + value.Key()))
	return "briefing_" + hex.EncodeToString(sum[:])[:24]
}

func stableInboxItemID(briefingValue Briefing, item InboxItem) string {
	key := briefingValue.Key() + "\x00" + item.Provider + "\x00" + item.ProviderItemID
	sum := sha256.Sum256([]byte("inbox_item\x00" + key))
	return "inbox_" + hex.EncodeToString(sum[:])[:24]
}

func fingerprint(value Briefing) string {
	items := make([]fingerprintItem, 0, len(value.Items))
	for _, item := range value.Items {
		items = append(items, fingerprintItem{
			Provider: item.Provider, ProviderItemID: item.ProviderItemID, Kind: item.Kind,
			Title: item.Title, Summary: item.Summary, URL: item.URL, ContentHash: item.ContentHash,
			PublishedAt: item.PublishedAt,
		})
	}
	failures := make([]fingerprintFailure, 0, len(value.ProviderFailures))
	for _, failure := range value.ProviderFailures {
		failures = append(failures, fingerprintFailure{Provider: failure.Provider, Code: failure.Code, Reason: failure.Reason, Retryable: failure.Retryable})
	}
	canonical := struct {
		Key        string               `json:"key"`
		Timezone   string               `json:"timezone"`
		QuietHours QuietHours           `json:"quiet_hours"`
		Items      []fingerprintItem    `json:"items"`
		Failures   []fingerprintFailure `json:"failures"`
	}{Key: value.Key(), Timezone: value.Timezone, QuietHours: value.QuietHours, Items: items, Failures: failures}
	encoded, _ := json.Marshal(canonical)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

type fingerprintItem struct {
	Provider       string    `json:"provider"`
	ProviderItemID string    `json:"provider_item_id"`
	Kind           string    `json:"kind"`
	Title          string    `json:"title"`
	Summary        string    `json:"summary"`
	URL            string    `json:"url"`
	ContentHash    string    `json:"content_hash"`
	PublishedAt    time.Time `json:"published_at"`
}

type fingerprintFailure struct {
	Provider  string `json:"provider"`
	Code      string `json:"code"`
	Reason    string `json:"reason"`
	Retryable bool   `json:"retryable"`
}

func cloneBriefing(value Briefing) Briefing {
	if value.Items != nil {
		items := append([]InboxItem(nil), value.Items...)
		value.Items = make([]InboxItem, len(items))
		for index, item := range items {
			value.Items[index] = cloneInboxItem(item)
		}
	}
	value.ProviderFailures = append([]ProviderFailure(nil), value.ProviderFailures...)
	return value
}

func cloneInboxItem(value InboxItem) InboxItem { return value }
