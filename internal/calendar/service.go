package calendar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/googleauth"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

const ProviderGoogleCalendar = "gcal"

// AccountStatus is the connection state of a provider account.
type AccountStatus string

const (
	AccountConnected    AccountStatus = "connected"
	AccountDisconnected AccountStatus = "disconnected"
)

// Account is bounded provider-account metadata. Secrets are never part of it.
type Account struct {
	ID           string        `json:"id"`
	TenantID     string        `json:"tenant_id"`
	UserID       string        `json:"user_id"`
	Provider     string        `json:"provider"`
	CalendarID   string        `json:"calendar_id"`
	EmailAddress string        `json:"email_address,omitempty"`
	Scopes       []string      `json:"scopes,omitempty"`
	Status       AccountStatus `json:"status"`
	ConnectedAt  time.Time     `json:"connected_at,omitempty"`
	UpdatedAt    time.Time     `json:"updated_at,omitempty"`
	LastSyncAt   time.Time     `json:"last_sync_at,omitempty"`
}

// AccountStore persists bounded provider-account metadata in the graph.
type AccountStore interface {
	SaveAccount(context.Context, identity.Scope, Account) (Account, error)
	GetAccount(context.Context, identity.Scope, string) (Account, error)
	ListAccounts(context.Context, identity.Scope, int) ([]Account, error)
}

// EventStore persists bounded normalized event records.
type EventStore interface {
	SaveEvents(context.Context, identity.Scope, []Event) ([]Event, error)
	ListEvents(context.Context, identity.Scope, string, int) ([]Event, error)
	GetEvent(context.Context, identity.Scope, string) (Event, error)
}

// ReaderFactory builds a read-only reader for a connected account.
type ReaderFactory func(context.Context, identity.Scope, Account) (CalendarReader, error)

// ServiceDeps wires the read-only calendar service to its storage and provider.
type ServiceDeps struct {
	Accounts    AccountStore
	Events      EventStore
	Credentials googleauth.Store
	OAuth       *googleauth.Client
	NewReader   ReaderFactory
	Location    *time.Location
	Clock       func() time.Time
}

// ConnectRequest connects an account by redeeming a code or importing tokens.
type ConnectRequest struct {
	Code         string
	CalendarID   string
	EmailAddress string
	AccessToken  string
	RefreshToken string
	Expiry       time.Time
}

type Service struct {
	accounts    AccountStore
	events      EventStore
	credentials googleauth.Store
	oauth       *googleauth.Client
	newReader   ReaderFactory
	location    *time.Location
	now         func() time.Time
}

func NewService(deps ServiceDeps) (*Service, error) {
	if deps.Accounts == nil || deps.Events == nil || deps.Credentials == nil {
		return nil, errors.New("calendar service requires account, event, and credential stores")
	}
	now := deps.Clock
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	location := deps.Location
	if location == nil {
		location = time.UTC
	}
	return &Service{
		accounts: deps.Accounts, events: deps.Events, credentials: deps.Credentials,
		oauth: deps.OAuth, newReader: deps.NewReader, location: location, now: now,
	}, nil
}

func (s *Service) AuthCodeURL(state string) (string, error) {
	if s.oauth == nil {
		return "", errors.New("calendar OAuth is not configured")
	}
	return s.oauth.AuthCodeURL(state, []string{CalendarReadOnlyScope})
}

func (s *Service) Connect(ctx context.Context, scope identity.Scope, request ConnectRequest) (Account, error) {
	if err := scope.Validate(); err != nil {
		return Account{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	tokens, err := s.obtainTokens(ctx, request)
	if err != nil {
		return Account{}, err
	}
	if tokens.Scope != "" && !googleauth.HasScope(tokens.Scope, CalendarReadOnlyScope) {
		return Account{}, ErrScopeMissing
	}
	calendarID := strings.TrimSpace(request.CalendarID)
	if calendarID == "" {
		calendarID = "primary"
	}
	accountID := AccountID(scope, calendarID, request.EmailAddress)
	if err := s.credentials.Save(ctx, scope, accountID, tokens); err != nil {
		return Account{}, err
	}
	now := s.now().UTC()
	account := Account{
		ID: accountID, TenantID: scope.TenantID, UserID: scope.UserID, Provider: ProviderGoogleCalendar,
		CalendarID: calendarID, EmailAddress: bound(request.EmailAddress, MaxAttendeeEmail),
		Scopes: []string{CalendarReadOnlyScope}, Status: AccountConnected, ConnectedAt: now, UpdatedAt: now,
	}
	return s.accounts.SaveAccount(ctx, scope, account)
}

func (s *Service) obtainTokens(ctx context.Context, request ConnectRequest) (googleauth.TokenSet, error) {
	if code := strings.TrimSpace(request.Code); code != "" {
		if s.oauth == nil {
			return googleauth.TokenSet{}, errors.New("calendar OAuth is not configured")
		}
		return s.oauth.Exchange(ctx, code)
	}
	if strings.TrimSpace(request.RefreshToken) == "" && strings.TrimSpace(request.AccessToken) == "" {
		return googleauth.TokenSet{}, errors.New("an authorization code or tokens are required")
	}
	return googleauth.TokenSet{
		AccessToken: request.AccessToken, RefreshToken: request.RefreshToken,
		Scope: CalendarReadOnlyScope, Expiry: request.Expiry,
	}, nil
}

func (s *Service) Disconnect(ctx context.Context, scope identity.Scope, accountID string) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	account, err := s.accounts.GetAccount(ctx, scope, accountID)
	if err != nil {
		return err
	}
	if err := s.credentials.Delete(ctx, scope, accountID); err != nil && !errors.Is(err, googleauth.ErrCredentialNotFound) {
		return err
	}
	account.Status = AccountDisconnected
	account.Scopes = nil
	account.UpdatedAt = s.now().UTC()
	_, err = s.accounts.SaveAccount(ctx, scope, account)
	return err
}

func (s *Service) Accounts(ctx context.Context, scope identity.Scope, limit int) ([]Account, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if limit < 1 || limit > 50 {
		limit = 20
	}
	return s.accounts.ListAccounts(ctx, scope, limit)
}

func (s *Service) Status(ctx context.Context, scope identity.Scope, accountID string) (Account, error) {
	if err := scope.Validate(); err != nil {
		return Account{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	return s.accounts.GetAccount(ctx, scope, accountID)
}

// ListEvents reads bounded events from the provider and persists them.
func (s *Service) ListEvents(ctx context.Context, scope identity.Scope, accountID string, query ListQuery) (EventPage, error) {
	account, reader, err := s.connectedReader(ctx, scope, accountID)
	if err != nil {
		return EventPage{}, err
	}
	if strings.TrimSpace(query.CalendarID) == "" {
		query.CalendarID = account.CalendarID
	}
	page, err := reader.ListEvents(ctx, scope, query)
	if err != nil {
		return EventPage{}, err
	}
	for index := range page.Events {
		page.Events[index].AccountID = account.ID
	}
	if _, err := s.events.SaveEvents(ctx, scope, page.Events); err != nil {
		return EventPage{}, err
	}
	account.LastSyncAt = s.now().UTC()
	account.UpdatedAt = account.LastSyncAt
	if _, err := s.accounts.SaveAccount(ctx, scope, account); err != nil {
		return EventPage{}, err
	}
	return page, nil
}

func (s *Service) GetEvent(ctx context.Context, scope identity.Scope, accountID, eventID string) (Event, error) {
	account, reader, err := s.connectedReader(ctx, scope, accountID)
	if err != nil {
		return Event{}, err
	}
	event, err := reader.GetEvent(ctx, scope, account.CalendarID, eventID)
	if err != nil {
		return Event{}, err
	}
	event.AccountID = account.ID
	event, err = NormalizeEvent(event, s.location)
	if err != nil {
		return Event{}, err
	}
	if _, err := s.events.SaveEvents(ctx, scope, []Event{event}); err != nil {
		return Event{}, err
	}
	return event, nil
}

func (s *Service) StoredEvents(ctx context.Context, scope identity.Scope, accountID string, limit int) ([]Event, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if limit < 1 || limit > MaxListLimit {
		limit = DefaultListLimit
	}
	return s.events.ListEvents(ctx, scope, accountID, limit)
}

func (s *Service) connectedReader(ctx context.Context, scope identity.Scope, accountID string) (Account, CalendarReader, error) {
	if err := scope.Validate(); err != nil {
		return Account{}, nil, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	account, err := s.accounts.GetAccount(ctx, scope, accountID)
	if err != nil {
		return Account{}, nil, err
	}
	if account.Status != AccountConnected {
		return Account{}, nil, fmt.Errorf("%w: account %q is %s", ErrAccountNotFound, accountID, account.Status)
	}
	if s.newReader == nil {
		return Account{}, nil, errors.New("calendar reader factory is not configured")
	}
	reader, err := s.newReader(ctx, scope, account)
	if err != nil {
		return Account{}, nil, err
	}
	return account, reader, nil
}

// AccountID derives a stable account identifier from scope, calendar, and mailbox.
func AccountID(scope identity.Scope, calendarID, email string) string {
	normalized := strings.ToLower(strings.TrimSpace(email)) + "\x00" + strings.TrimSpace(calendarID)
	sum := sha256.Sum256([]byte(scope.TenantID + "\x00" + scope.UserID + "\x00" + ProviderGoogleCalendar + "\x00" + normalized))
	return "gcal_account_" + hex.EncodeToString(sum[:])[:24]
}
