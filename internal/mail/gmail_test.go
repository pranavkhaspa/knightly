package mail

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

type recordedRequest struct {
	Method string
	Path   string
	Query  url.Values
}

func newGmailTestServer(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (*httptest.Server, *[]recordedRequest, *sync.Mutex) {
	t.Helper()
	var (
		mu       sync.Mutex
		requests []recordedRequest
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, recordedRequest{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()})
		mu.Unlock()
		if r.Method != http.MethodGet && r.URL.Path != "/token" {
			t.Errorf("non-read method %s to %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handler(w, r)
	}))
	return server, &requests, &mu
}

func newGmailReader(t *testing.T, server *httptest.Server, credentials CredentialStore, tokens TokenSet) *GmailReader {
	t.Helper()
	oauth, err := NewOAuthClient(OAuthConfig{
		ClientID: "id", ClientSecret: "secret", RedirectURL: "https://example.com/cb",
		AuthURL: "https://example.com/auth", TokenURL: server.URL + "/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewGmailReader(GmailConfig{
		BaseURL: server.URL, Scope: testScope, Account: Account{ID: "acct-1"},
		OAuth: oauth, Credentials: credentials, Clock: func() time.Time { return time.Unix(1000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	reader.tokens = tokens
	return reader
}

func TestGmailReaderListsMetadataUsingOnlyReadRequests(t *testing.T) {
	server, requests, mu := newGmailTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/gmail/v1/users/me/messages":
			if r.URL.Query().Get("q") != "is:unread" || r.URL.Query().Get("maxResults") != "2" {
				t.Errorf("list query = %v", r.URL.Query())
			}
			_, _ = w.Write([]byte(`{"messages":[{"id":"m1","threadId":"t1"},{"id":"m2","threadId":"t2"}],"nextPageToken":"next"}`))
		case r.URL.Path == "/gmail/v1/users/me/messages/m1":
			if r.URL.Query().Get("format") != "metadata" || len(r.URL.Query()["metadataHeaders"]) != 5 {
				t.Errorf("metadata query = %v", r.URL.Query())
			}
			_, _ = w.Write([]byte(`{"id":"m1","threadId":"t1","labelIds":["UNREAD","INBOX"],"snippet":"Build failed","internalDate":"1699999999000",
				"payload":{"headers":[{"name":"From","value":"Ops Team <ops@example.com>"},{"name":"To","value":"user@example.com"},{"name":"Subject","value":"Deploy alert"}],
				"parts":[{"filename":"report.pdf","mimeType":"application/pdf","body":{"size":2048,"attachmentId":"att1"}}]}}`))
		case r.URL.Path == "/gmail/v1/users/me/messages/m2":
			_, _ = w.Write([]byte(`{"id":"m2","threadId":"t2","payload":{"headers":[{"name":"Subject","value":"Lunch"}]}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	defer server.Close()

	reader := newGmailReader(t, server, nil, TokenSet{AccessToken: "valid"})
	page, err := reader.ListMessages(context.Background(), testScope, ListQuery{Query: "is:unread", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 2 || page.NextPageToken != "next" {
		t.Fatalf("page = %+v", page)
	}
	first := page.Messages[0]
	if first.Subject != "Deploy alert" || first.From.Email != "ops@example.com" || first.From.Name != "Ops Team" {
		t.Fatalf("message = %+v", first)
	}
	if !first.Unread || first.Origin != UntrustedOrigin || !first.HasAttachments {
		t.Fatalf("message flags = %+v", first)
	}
	if len(first.Attachments) != 1 || first.Attachments[0].Filename != "report.pdf" || first.Attachments[0].Size != 2048 {
		t.Fatalf("attachments = %+v", first.Attachments)
	}
	if !first.ReceivedAt.Equal(time.UnixMilli(1699999999000).UTC()) {
		t.Errorf("received at = %v", first.ReceivedAt)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, request := range *requests {
		if request.Method != http.MethodGet {
			t.Errorf("reader issued %s %s", request.Method, request.Path)
		}
	}
}

func TestGmailReaderRejectsWrongScope(t *testing.T) {
	server, _, _ := newGmailTestServer(t, func(http.ResponseWriter, *http.Request) {})
	defer server.Close()
	reader := newGmailReader(t, server, nil, TokenSet{AccessToken: "valid"})
	if _, err := reader.ListMessages(context.Background(), identity.Scope{TenantID: "other", UserID: "other"}, ListQuery{}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("err = %v, want ErrScopeRequired", err)
	}
}

func TestGmailReaderMapsProviderErrors(t *testing.T) {
	server, _, _ := newGmailTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	defer server.Close()
	reader := newGmailReader(t, server, nil, TokenSet{AccessToken: "valid"})
	if _, err := reader.GetMessage(context.Background(), testScope, "m1"); !errors.Is(err, ErrProviderRequest) {
		t.Fatalf("err = %v, want ErrProviderRequest", err)
	}
}

func TestGmailReaderMapsUnauthorized(t *testing.T) {
	server, _, _ := newGmailTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	defer server.Close()
	reader := newGmailReader(t, server, nil, TokenSet{AccessToken: "valid"})
	if _, err := reader.ListMessages(context.Background(), testScope, ListQuery{}); !errors.Is(err, ErrProviderUnauthorized) {
		t.Fatalf("err = %v, want ErrProviderUnauthorized", err)
	}
}

func TestGmailReaderRequiresRefreshTokenWhenExpired(t *testing.T) {
	server, _, _ := newGmailTestServer(t, func(http.ResponseWriter, *http.Request) {})
	defer server.Close()
	reader := newGmailReader(t, server, nil, TokenSet{AccessToken: "expired", Expiry: time.Unix(1, 0).UTC()})
	if _, err := reader.ListMessages(context.Background(), testScope, ListQuery{}); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("err = %v, want ErrTokenExpired", err)
	}
}

func TestGmailReaderRefreshesExpiredTokenAndPersists(t *testing.T) {
	server, _, _ := newGmailTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			_, _ = w.Write([]byte(`{"access_token":"fresh","token_type":"Bearer","scope":"` + GmailReadOnlyScope + `","expires_in":3600}`))
			return
		}
		if r.URL.Path == "/gmail/v1/users/me/messages" {
			_, _ = w.Write([]byte(`{"messages":[]}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	defer server.Close()

	store, err := NewFileCredentialStore(t.TempDir() + "/mail.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), testScope, "acct-1", TokenSet{AccessToken: "expired", RefreshToken: "refresh", Expiry: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	reader := newGmailReader(t, server, store, TokenSet{AccessToken: "expired", RefreshToken: "refresh", Expiry: time.Unix(1, 0).UTC()})
	page, err := reader.ListMessages(context.Background(), testScope, ListQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 0 {
		t.Fatalf("page = %+v", page)
	}
	persisted, err := store.Load(context.Background(), testScope, "acct-1")
	if err != nil {
		t.Fatal(err)
	}
	if persisted.AccessToken != "fresh" || persisted.RefreshToken != "refresh" {
		t.Fatalf("persisted = %+v", persisted)
	}
}

func TestParseAddressHelpersBounds(t *testing.T) {
	if address := parseAddress("Solo <solo@example.com>"); address.Name != "Solo" || address.Email != "solo@example.com" {
		t.Errorf("address = %+v", address)
	}
	if address := parseAddress("bare@example.com"); address.Email != "bare@example.com" {
		t.Errorf("bare address = %+v", address)
	}
	list := parseAddressList("a@example.com, B <b@example.com>")
	if len(list) != 2 || list[1].Name != "B" {
		t.Errorf("list = %+v", list)
	}
	if value := internalDate("nope"); !value.IsZero() {
		t.Errorf("internal date = %v", value)
	}
	if !containsLabel([]string{"UNREAD"}, "unread") {
		t.Error("label match should be case-insensitive")
	}
}

func TestGmailReaderRejectsInsecureBaseURL(t *testing.T) {
	oauth, err := NewOAuthClient(OAuthConfig{ClientID: "id", ClientSecret: "secret", RedirectURL: "https://example.com/cb"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewGmailReader(GmailConfig{
		BaseURL: "http://gmail.example.com", Scope: testScope, Account: Account{ID: "acct"},
		OAuth: oauth,
	})
	if err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("err = %v, want HTTPS rejection", err)
	}
}
