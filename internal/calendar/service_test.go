package calendar

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/googleauth"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

type memoryAccounts struct {
	values map[string]Account
}

func newMemoryAccounts() *memoryAccounts { return &memoryAccounts{values: map[string]Account{}} }

func (m *memoryAccounts) SaveAccount(_ context.Context, _ identity.Scope, account Account) (Account, error) {
	m.values[account.ID] = account
	return account, nil
}

func (m *memoryAccounts) GetAccount(_ context.Context, _ identity.Scope, id string) (Account, error) {
	account, ok := m.values[id]
	if !ok {
		return Account{}, ErrAccountNotFound
	}
	return account, nil
}

func (m *memoryAccounts) ListAccounts(_ context.Context, _ identity.Scope, _ int) ([]Account, error) {
	accounts := make([]Account, 0, len(m.values))
	for _, account := range m.values {
		accounts = append(accounts, account)
	}
	return accounts, nil
}

type memoryEvents struct {
	values []Event
}

func (m *memoryEvents) SaveEvents(_ context.Context, _ identity.Scope, events []Event) ([]Event, error) {
	m.values = append(m.values, events...)
	return events, nil
}

func (m *memoryEvents) ListEvents(_ context.Context, _ identity.Scope, accountID string, _ int) ([]Event, error) {
	filtered := make([]Event, 0, len(m.values))
	for _, event := range m.values {
		if accountID == "" || event.AccountID == accountID {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

func (m *memoryEvents) GetEvent(_ context.Context, _ identity.Scope, eventID string) (Event, error) {
	for _, event := range m.values {
		if event.ID == eventID {
			return event, nil
		}
	}
	return Event{}, ErrEventNotFound
}

func newTestService(t *testing.T, reader CalendarReader) (*Service, *memoryAccounts, *memoryEvents, googleauth.Store) {
	t.Helper()
	accounts := newMemoryAccounts()
	events := &memoryEvents{}
	store, err := googleauth.NewFileStore(t.TempDir() + "/credentials.json")
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	service, err := NewService(ServiceDeps{
		Accounts: accounts, Events: events, Credentials: store, Location: time.UTC,
		NewReader: func(context.Context, identity.Scope, Account) (CalendarReader, error) { return reader, nil },
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service, accounts, events, store
}

func TestServiceConnectImportsTokensAndStoresAccount(t *testing.T) {
	service, accounts, _, store := newTestService(t, NewFakeReader(time.UTC, nil))
	account, err := service.Connect(context.Background(), testScope, ConnectRequest{
		RefreshToken: "refresh-1", EmailAddress: "user@example.com",
	})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if account.Provider != ProviderGoogleCalendar || account.CalendarID != "primary" || account.Status != AccountConnected {
		t.Fatalf("account = %+v", account)
	}
	if _, ok := accounts.values[account.ID]; !ok {
		t.Fatal("account was not persisted")
	}
	if _, err := store.Load(context.Background(), testScope, account.ID); err != nil {
		t.Fatalf("credential not persisted: %v", err)
	}
}

func TestServiceConnectRejectsMissingGrantedScope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "token-1", "scope": "https://www.googleapis.com/auth/gmail.readonly",
		})
	}))
	defer server.Close()
	oauth, err := googleauth.NewClient(googleauth.Config{
		ClientID: "id", ClientSecret: "secret", RedirectURL: "http://127.0.0.1/callback", TokenURL: server.URL,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	store, err := googleauth.NewFileStore(t.TempDir() + "/credentials.json")
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	service, err := NewService(ServiceDeps{
		Accounts: newMemoryAccounts(), Events: &memoryEvents{}, Credentials: store, OAuth: oauth, Location: time.UTC,
		NewReader: func(context.Context, identity.Scope, Account) (CalendarReader, error) {
			return NewFakeReader(time.UTC, nil), nil
		},
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if _, err := service.Connect(context.Background(), testScope, ConnectRequest{Code: "auth-code"}); !errors.Is(err, ErrScopeMissing) {
		t.Fatalf("Connect() error = %v, want ErrScopeMissing", err)
	}
}

func TestServiceListEventsPersistsAndBounds(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	reader := NewFakeReader(time.UTC, func() time.Time { return now },
		Event{ID: "evt-1", Start: now.Add(time.Hour), End: now.Add(2 * time.Hour)},
	)
	service, _, events, _ := newTestService(t, reader)
	account, err := service.Connect(context.Background(), testScope, ConnectRequest{RefreshToken: "refresh"})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	page, err := service.ListEvents(context.Background(), testScope, account.ID, ListQuery{Window: WindowUpcoming})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(page.Events) != 1 || page.Events[0].AccountID != account.ID {
		t.Fatalf("page = %+v", page)
	}
	if len(events.values) != 1 || events.values[0].ID != "evt-1" {
		t.Fatalf("persisted events = %+v", events.values)
	}
	updated, err := service.Status(context.Background(), testScope, account.ID)
	if err != nil || updated.LastSyncAt.IsZero() {
		t.Fatalf("status after list = %+v err=%v", updated, err)
	}
}

func TestServiceDisconnectRemovesCredentials(t *testing.T) {
	service, _, _, store := newTestService(t, NewFakeReader(time.UTC, nil))
	account, err := service.Connect(context.Background(), testScope, ConnectRequest{RefreshToken: "refresh"})
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if err := service.Disconnect(context.Background(), testScope, account.ID); err != nil {
		t.Fatalf("Disconnect() error = %v", err)
	}
	if _, err := store.Load(context.Background(), testScope, account.ID); !errors.Is(err, googleauth.ErrCredentialNotFound) {
		t.Fatalf("credential still present: %v", err)
	}
	status, err := service.Status(context.Background(), testScope, account.ID)
	if err != nil || status.Status != AccountDisconnected {
		t.Fatalf("status = %+v err=%v", status, err)
	}
	if _, err := service.ListEvents(context.Background(), testScope, account.ID, ListQuery{}); err == nil {
		t.Fatal("ListEvents() succeeded for a disconnected account")
	}
}

func TestServiceRejectsUntrustedScope(t *testing.T) {
	service, _, _, _ := newTestService(t, NewFakeReader(time.UTC, nil))
	if _, err := service.Accounts(context.Background(), identity.Scope{}, 10); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("Accounts() error = %v", err)
	}
}

func TestServiceAuthCodeURLRequiresOAuth(t *testing.T) {
	service, _, _, _ := newTestService(t, NewFakeReader(time.UTC, nil))
	if _, err := service.AuthCodeURL("state-1"); err == nil {
		t.Fatal("AuthCodeURL() succeeded without an OAuth client")
	}
}
