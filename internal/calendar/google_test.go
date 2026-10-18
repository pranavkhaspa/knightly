package calendar

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/googleauth"
	"github.com/pranavkhaspa/knightly/internal/identity"
)

func testOAuthClient(t *testing.T, tokenURL string) *googleauth.Client {
	t.Helper()
	client, err := googleauth.NewClient(googleauth.Config{
		ClientID: "client-id", ClientSecret: "client-secret", RedirectURL: "http://127.0.0.1/callback",
		TokenURL: tokenURL,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func seededStore(t *testing.T, tokens googleauth.TokenSet) googleauth.Store {
	t.Helper()
	store, err := googleauth.NewFileStore(t.TempDir() + "/credentials.json")
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	if err := store.Save(context.Background(), testScope, "acct-1", tokens); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	return store
}

func TestGoogleCalendarReaderListsEventsWithGetOnly(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var methods, auth []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		auth = append(auth, r.Header.Get("Authorization"))
		mu.Unlock()
		if r.URL.Path != "/calendar/v3/calendars/primary/events" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("singleEvents") != "true" || r.URL.Query().Get("orderBy") != "startTime" {
			t.Errorf("missing expansion parameters: %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"nextPageToken": "page-2",
			"items": []map[string]interface{}{
				{
					"id": "evt-1", "summary": "Standup", "status": "confirmed",
					"start": map[string]string{"dateTime": "2026-01-02T09:30:00Z"},
					"end":   map[string]string{"dateTime": "2026-01-02T09:45:00Z"},
				},
				{
					"id": "evt-2", "summary": "Holiday",
					"start": map[string]string{"date": "2026-01-05"},
					"end":   map[string]string{"date": "2026-01-06"},
				},
			},
		})
	}))
	defer server.Close()

	store := seededStore(t, googleauth.TokenSet{AccessToken: "token-1", Scope: CalendarReadOnlyScope, Expiry: now.Add(time.Hour)})
	reader, err := NewGoogleCalendarReader(GoogleConfig{
		BaseURL: server.URL, Scope: testScope, Account: Account{ID: "acct-1", CalendarID: "primary"},
		OAuth: testOAuthClient(t, server.URL+"/token"), Credentials: store, Location: time.UTC,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewGoogleCalendarReader() error = %v", err)
	}
	page, err := reader.ListEvents(context.Background(), testScope, ListQuery{Window: WindowUpcoming, Limit: 5})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(page.Events) != 2 || page.NextPageToken != "page-2" {
		t.Fatalf("page = %+v", page)
	}
	if page.Events[0].Summary != "Standup" || page.Events[1].AllDay != true {
		t.Fatalf("events = %+v", page.Events)
	}
	if page.Events[0].Origin != UntrustedOrigin || page.Events[0].AccountID != "acct-1" {
		t.Fatalf("event metadata = %+v", page.Events[0])
	}
	mu.Lock()
	defer mu.Unlock()
	for index, method := range methods {
		if method != http.MethodGet {
			t.Fatalf("reader used %s; the read-only boundary must only issue GET", method)
		}
		if auth[index] != "Bearer token-1" {
			t.Fatalf("authorization = %q", auth[index])
		}
	}
}

func TestGoogleCalendarReaderRejectsForeignScope(t *testing.T) {
	now := time.Now().UTC()
	store := seededStore(t, googleauth.TokenSet{AccessToken: "token-1", Expiry: now.Add(time.Hour)})
	reader, err := NewGoogleCalendarReader(GoogleConfig{
		BaseURL: "http://127.0.0.1:1", Scope: testScope, Account: Account{ID: "acct-1"},
		OAuth: testOAuthClient(t, "https://oauth2.googleapis.com/token"), Credentials: store,
	})
	if err != nil {
		t.Fatalf("NewGoogleCalendarReader() error = %v", err)
	}
	other := identity.Scope{TenantID: "tenant-b", UserID: "user-b"}
	if _, err := reader.ListEvents(context.Background(), other, ListQuery{}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("foreign scope error = %v", err)
	}
}

func TestGoogleCalendarReaderRefreshesExpiredTokenAndPersists(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/token":
			if err := r.ParseForm(); err != nil {
				t.Errorf("ParseForm() error = %v", err)
			}
			if r.Form.Get("grant_type") != "refresh_token" {
				t.Errorf("grant_type = %q", r.Form.Get("grant_type"))
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "token-2", "expires_in": 3600})
		case r.URL.Path == "/calendar/v3/calendars/primary/events":
			if r.Header.Get("Authorization") != "Bearer token-2" {
				t.Errorf("authorization after refresh = %q", r.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"items": []map[string]interface{}{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := seededStore(t, googleauth.TokenSet{
		AccessToken: "token-1", RefreshToken: "refresh-1", Scope: CalendarReadOnlyScope, Expiry: now.Add(-time.Hour),
	})
	reader, err := NewGoogleCalendarReader(GoogleConfig{
		BaseURL: server.URL, Scope: testScope, Account: Account{ID: "acct-1", CalendarID: "primary"},
		OAuth: testOAuthClient(t, server.URL+"/token"), Credentials: store, Location: time.UTC,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewGoogleCalendarReader() error = %v", err)
	}
	if _, err := reader.ListEvents(context.Background(), testScope, ListQuery{Window: WindowUpcoming}); err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	persisted, err := store.Load(context.Background(), testScope, "acct-1")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if persisted.AccessToken != "token-2" || persisted.RefreshToken != "refresh-1" {
		t.Fatalf("persisted tokens = %s", persisted)
	}
}

func TestGoogleCalendarReaderGetEventParsesAttendees(t *testing.T) {
	now := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/events/evt-9") {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"id": "evt-9", "summary": "Review",
			"start": map[string]string{"dateTime": "2026-01-02T10:00:00Z"},
			"end":   map[string]string{"dateTime": "2026-01-02T11:00:00Z"},
			"attendees": []map[string]interface{}{
				{"email": "a@example.com", "displayName": "Ada", "responseStatus": "accepted", "organizer": true},
			},
		})
	}))
	defer server.Close()

	store := seededStore(t, googleauth.TokenSet{AccessToken: "token-1", Expiry: now.Add(time.Hour)})
	reader, err := NewGoogleCalendarReader(GoogleConfig{
		BaseURL: server.URL, Scope: testScope, Account: Account{ID: "acct-1", CalendarID: "primary"},
		OAuth: testOAuthClient(t, server.URL+"/token"), Credentials: store, Location: time.UTC,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewGoogleCalendarReader() error = %v", err)
	}
	event, err := reader.GetEvent(context.Background(), testScope, "primary", "evt-9")
	if err != nil {
		t.Fatalf("GetEvent() error = %v", err)
	}
	if len(event.Attendees) != 1 || event.Attendees[0].Email != "a@example.com" || !event.Attendees[0].Organizer {
		t.Fatalf("attendees = %+v", event.Attendees)
	}
}
