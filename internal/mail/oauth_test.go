package mail

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func testOAuthClient(t *testing.T, tokenHandler http.HandlerFunc) (*OAuthClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(tokenHandler)
	client, err := NewOAuthClient(OAuthConfig{
		ClientID: "client-id", ClientSecret: "client-secret",
		RedirectURL: "http://127.0.0.1:8080/api/v1/mail/accounts/connect/callback",
		AuthURL:     "http://127.0.0.1:9999/auth",
		TokenURL:    server.URL + "/token",
		Clock:       func() time.Time { return time.Unix(1000, 0).UTC() },
	})
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return client, server
}

func TestOAuthClientRequiresSecureEndpoints(t *testing.T) {
	_, err := NewOAuthClient(OAuthConfig{
		ClientID: "id", ClientSecret: "secret", RedirectURL: "https://example.com/cb",
		TokenURL: "http://accounts.example.com/token",
	})
	if !errors.Is(err, ErrInsecureOAuthURL) {
		t.Fatalf("err = %v, want ErrInsecureOAuthURL", err)
	}
}

func TestAuthCodeURLRequestsReadOnlyScopeOffline(t *testing.T) {
	client, server := testOAuthClient(t, func(http.ResponseWriter, *http.Request) {})
	defer server.Close()
	raw, err := client.AuthCodeURL("state-123")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if query.Get("scope") != GmailReadOnlyScope {
		t.Errorf("scope = %q", query.Get("scope"))
	}
	if query.Get("access_type") != "offline" || query.Get("response_type") != "code" || query.Get("state") != "state-123" {
		t.Errorf("query = %v", query)
	}
	if query.Get("client_secret") != "" {
		t.Error("authorization URL must not contain the client secret")
	}
	if _, err := client.AuthCodeURL(""); err == nil {
		t.Error("empty state should fail")
	}
}

func TestOAuthExchangeParsesTokens(t *testing.T) {
	client, server := testOAuthClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if r.PostForm.Get("grant_type") != "authorization_code" || r.PostForm.Get("code") != "the-code" {
			t.Errorf("form = %v", r.PostForm)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at","refresh_token":"rt","token_type":"Bearer","scope":"` + GmailReadOnlyScope + `","expires_in":3600}`))
	})
	defer server.Close()
	tokens, err := client.Exchange(context.Background(), "the-code")
	if err != nil {
		t.Fatal(err)
	}
	if tokens.AccessToken != "at" || tokens.RefreshToken != "rt" || !hasReadOnlyScope(tokens.Scope) {
		t.Fatalf("tokens = %+v", tokens)
	}
	if !tokens.Expiry.Equal(time.Unix(4600, 0).UTC()) {
		t.Errorf("expiry = %v", tokens.Expiry)
	}
}

func TestOAuthRefreshPreservesRefreshToken(t *testing.T) {
	client, server := testOAuthClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.PostForm.Get("grant_type") != "refresh_token" || r.PostForm.Get("refresh_token") != "old-refresh" {
			t.Errorf("form = %v", r.PostForm)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"new-access","token_type":"Bearer","scope":"` + GmailReadOnlyScope + `"}`))
	})
	defer server.Close()
	refreshed, err := client.Refresh(context.Background(), TokenSet{RefreshToken: "old-refresh"})
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.AccessToken != "new-access" || refreshed.RefreshToken != "old-refresh" {
		t.Fatalf("refreshed = %+v", refreshed)
	}
}

func TestOAuthRejectsBroadScope(t *testing.T) {
	client, server := testOAuthClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at","scope":"https://mail.google.com/","expires_in":3600}`))
	})
	defer server.Close()
	if _, err := client.Exchange(context.Background(), "code"); !errors.Is(err, ErrScopeMissing) {
		t.Fatalf("err = %v, want ErrScopeMissing", err)
	}
}

func TestOAuthExchangeErrorsAndBounds(t *testing.T) {
	client, server := testOAuthClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
	})
	defer server.Close()
	if _, err := client.Exchange(context.Background(), "code"); !errors.Is(err, ErrOAuthExchange) {
		t.Fatalf("err = %v, want ErrOAuthExchange", err)
	}

	client, server = testOAuthClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at","padding":"` + strings.Repeat("x", int(maxTokenResponse)+10) + `"}`))
	})
	defer server.Close()
	if _, err := client.Exchange(context.Background(), "code"); !errors.Is(err, ErrOAuthExchange) {
		t.Fatalf("oversized err = %v, want ErrOAuthExchange", err)
	}
}
