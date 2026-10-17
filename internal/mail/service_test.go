package mail

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type memAccountStore struct {
	data map[string]map[string]Account
}

func newMemAccountStore() *memAccountStore {
	return &memAccountStore{data: map[string]map[string]Account{}}
}

func scopeKey(scope identity.Scope) string { return scope.TenantID + "\x00" + scope.UserID }

func (s *memAccountStore) SaveAccount(_ context.Context, scope identity.Scope, account Account) (Account, error) {
	key := scopeKey(scope)
	if s.data[key] == nil {
		s.data[key] = map[string]Account{}
	}
	s.data[key][account.ID] = account
	return account, nil
}

func (s *memAccountStore) GetAccount(_ context.Context, scope identity.Scope, accountID string) (Account, error) {
	account, ok := s.data[scopeKey(scope)][accountID]
	if !ok {
		return Account{}, ErrAccountNotFound
	}
	return account, nil
}

func (s *memAccountStore) ListAccounts(_ context.Context, scope identity.Scope, limit int) ([]Account, error) {
	accounts := make([]Account, 0, len(s.data[scopeKey(scope)]))
	for _, account := range s.data[scopeKey(scope)] {
		accounts = append(accounts, account)
	}
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].ID < accounts[j].ID })
	if len(accounts) > limit {
		accounts = accounts[:limit]
	}
	return accounts, nil
}

type memMessageStore struct {
	data map[string]map[string]Message
}

func newMemMessageStore() *memMessageStore {
	return &memMessageStore{data: map[string]map[string]Message{}}
}

func (s *memMessageStore) SaveMessages(_ context.Context, scope identity.Scope, messages []Message) ([]Message, error) {
	if s.data[scopeKey(scope)] == nil {
		s.data[scopeKey(scope)] = map[string]Message{}
	}
	for _, message := range messages {
		value, err := NormalizeMessage(message)
		if err != nil {
			return nil, err
		}
		s.data[scopeKey(scope)][value.ID] = value
	}
	return messages, nil
}

func (s *memMessageStore) ListMessages(_ context.Context, scope identity.Scope, accountID string, limit int) ([]Message, error) {
	result := make([]Message, 0)
	for _, message := range s.data[scopeKey(scope)] {
		if accountID == "" || message.AccountID == accountID {
			result = append(result, message)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *memMessageStore) GetMessage(_ context.Context, scope identity.Scope, messageID string) (Message, error) {
	message, ok := s.data[scopeKey(scope)][messageID]
	if !ok {
		return Message{}, ErrMessageNotFound
	}
	return message, nil
}

type fakeProfiler struct {
	*FakeReader
	email string
}

func (f fakeProfiler) Profile(context.Context) (string, error) { return f.email, nil }

func newTestService(t *testing.T, reader MailReader, email string, oauth *OAuthClient) (*Service, *memAccountStore, *memMessageStore, CredentialStore) {
	t.Helper()
	credentials, err := NewFileCredentialStore(filepath.Join(t.TempDir(), "mail.json"))
	if err != nil {
		t.Fatal(err)
	}
	accounts := newMemAccountStore()
	messages := newMemMessageStore()
	service, err := NewService(ServiceDeps{
		Accounts: accounts, Messages: messages, Credentials: credentials, OAuth: oauth,
		NewReader: func(context.Context, identity.Scope, Account) (MailReader, error) {
			if reader == nil {
				return nil, errors.New("no reader")
			}
			if email != "" {
				if fake, ok := reader.(*FakeReader); ok {
					return fakeProfiler{FakeReader: fake, email: email}, nil
				}
			}
			return reader, nil
		},
		Clock: func() time.Time { return time.Unix(1000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	return service, accounts, messages, credentials
}

func TestServiceRequiresStores(t *testing.T) {
	if _, err := NewService(ServiceDeps{}); err == nil {
		t.Fatal("expected missing stores to fail")
	}
}

func TestServiceConnectTokenImportAndIsolation(t *testing.T) {
	service, _, _, credentials := newTestService(t, nil, "", nil)
	account, err := service.Connect(context.Background(), testScope, ConnectRequest{
		EmailAddress: "User@Example.com", AccessToken: "access", RefreshToken: "refresh",
	})
	if err != nil {
		t.Fatal(err)
	}
	if account.Status != AccountConnected || account.Provider != ProviderGmail || account.EmailAddress != "User@Example.com" {
		t.Fatalf("account = %+v", account)
	}
	if len(account.Scopes) != 1 || account.Scopes[0] != GmailReadOnlyScope {
		t.Fatalf("scopes = %+v", account.Scopes)
	}
	if _, err := credentials.Load(context.Background(), testScope, account.ID); err != nil {
		t.Fatalf("credentials not saved: %v", err)
	}
	accounts, err := service.Accounts(context.Background(), testScope, 10)
	if err != nil || len(accounts) != 1 {
		t.Fatalf("accounts = %+v err=%v", accounts, err)
	}
	other, err := service.Accounts(context.Background(), identity.Scope{TenantID: "tenant_x", UserID: "user_x"}, 10)
	if err != nil || len(other) != 0 {
		t.Fatalf("cross-scope accounts = %+v err=%v", other, err)
	}
}

func TestServiceDisconnectClearsCredentials(t *testing.T) {
	service, _, _, credentials := newTestService(t, nil, "", nil)
	account, err := service.Connect(context.Background(), testScope, ConnectRequest{EmailAddress: "user@example.com", RefreshToken: "refresh"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Disconnect(context.Background(), testScope, account.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := credentials.Load(context.Background(), testScope, account.ID); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("credentials were not deleted: %v", err)
	}
	status, err := service.Status(context.Background(), testScope, account.ID)
	if err != nil || status.Status != AccountDisconnected {
		t.Fatalf("status = %+v err=%v", status, err)
	}
	if _, err := service.ListMessages(context.Background(), testScope, account.ID, ListQuery{}); err == nil {
		t.Fatal("disconnected account should not list messages")
	}
}

func TestServiceListAndGetMessagesPersist(t *testing.T) {
	reader := NewFakeReader(
		Message{ID: "m1", AccountID: "ignored", Subject: "Deploy alert", From: Address{Email: "ops@example.com"}},
		Message{ID: "m2", AccountID: "ignored", Subject: "Lunch", From: Address{Email: "friend@example.com"}},
	)
	service, _, messages, _ := newTestService(t, reader, "", nil)
	account, err := service.Connect(context.Background(), testScope, ConnectRequest{EmailAddress: "user@example.com", RefreshToken: "refresh"})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.ListMessages(context.Background(), testScope, account.ID, ListQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 2 {
		t.Fatalf("page = %+v", page)
	}
	stored, err := service.StoredMessages(context.Background(), testScope, account.ID, 10)
	if err != nil || len(stored) != 2 {
		t.Fatalf("stored = %+v err=%v", stored, err)
	}
	for _, message := range stored {
		if message.AccountID != account.ID {
			t.Errorf("stored message account = %q want %q", message.AccountID, account.ID)
		}
	}
	single, err := service.GetMessage(context.Background(), testScope, account.ID, "m1")
	if err != nil || single.ID != "m1" {
		t.Fatalf("get message = %+v err=%v", single, err)
	}
	if _, ok := messages.data[scopeKey(testScope)]["m1"]; !ok {
		t.Fatal("get message did not persist")
	}
}

func TestServiceConnectViaAuthorizationCodeResolvesProfile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at","refresh_token":"rt","token_type":"Bearer","scope":"` + GmailReadOnlyScope + `","expires_in":3600}`))
	}))
	defer server.Close()
	oauth, err := NewOAuthClient(OAuthConfig{
		ClientID: "id", ClientSecret: "secret", RedirectURL: "https://example.com/cb",
		AuthURL: "https://example.com/auth", TokenURL: server.URL + "/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := NewFakeReader(Message{ID: "m1", Subject: "hello"})
	service, _, _, _ := newTestService(t, reader, "resolved@example.com", oauth)
	account, err := service.Connect(context.Background(), testScope, ConnectRequest{Code: "the-code"})
	if err != nil {
		t.Fatal(err)
	}
	if account.EmailAddress != "resolved@example.com" {
		t.Fatalf("resolved email = %q", account.EmailAddress)
	}
}

func TestServiceConnectRejectsBroadScope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at","scope":"https://mail.google.com/","expires_in":3600}`))
	}))
	defer server.Close()
	oauth, err := NewOAuthClient(OAuthConfig{
		ClientID: "id", ClientSecret: "secret", RedirectURL: "https://example.com/cb",
		AuthURL: "https://example.com/auth", TokenURL: server.URL + "/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	service, _, _, _ := newTestService(t, NewFakeReader(), "", oauth)
	if _, err := service.Connect(context.Background(), testScope, ConnectRequest{Code: "code"}); !errors.Is(err, ErrScopeMissing) {
		t.Fatalf("err = %v, want ErrScopeMissing", err)
	}
}

func TestServiceAuthCodeURLRequiresOAuth(t *testing.T) {
	service, _, _, _ := newTestService(t, nil, "", nil)
	if _, err := service.AuthCodeURL("state"); err == nil {
		t.Fatal("expected missing OAuth to fail")
	}
}
