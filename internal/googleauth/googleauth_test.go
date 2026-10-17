package googleauth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

var scope = identity.Scope{TenantID: "tenant_demo", UserID: "user_demo"}

func TestFileStoreRoundTripIsolationAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials", "tokens.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	tokens := TokenSet{AccessToken: "access", RefreshToken: "refresh", Scope: "scope/a", Expiry: time.Now().Add(time.Hour)}
	if err := store.Save(context.Background(), scope, "acct", tokens); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background(), scope, "acct")
	if err != nil || loaded.AccessToken != "access" {
		t.Fatalf("loaded = %+v err=%v", loaded, err)
	}
	if _, err := store.Load(context.Background(), identity.Scope{TenantID: "other", UserID: "other"}, "acct"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("cross-scope err = %v", err)
	}
	if runtime.GOOS != "windows" {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("file mode = %v, want 0600", info.Mode().Perm())
		}
	}
}

func TestTokenSetRedactsAndValidates(t *testing.T) {
	tokens := TokenSet{AccessToken: "secret-access", RefreshToken: "secret-refresh"}
	if strings.Contains(tokens.String(), "secret-access") || strings.Contains(tokens.GoString(), "secret-refresh") {
		t.Errorf("TokenSet string leaked material: %q / %q", tokens.String(), tokens.GoString())
	}
	now := time.Unix(1000, 0).UTC()
	if (TokenSet{}).Valid(now, time.Minute) || !(TokenSet{AccessToken: "a", Expiry: now.Add(time.Hour)}).Valid(now, time.Minute) {
		t.Error("token validity is wrong")
	}
}

func newTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	client, err := NewClient(Config{
		ClientID: "id", ClientSecret: "secret", RedirectURL: "http://127.0.0.1:8080/cb",
		AuthURL: "http://127.0.0.1:9999/auth", TokenURL: server.URL + "/token",
		Clock: func() time.Time { return time.Unix(1000, 0).UTC() },
	})
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return client, server
}

func TestClientRequiresSecureEndpoints(t *testing.T) {
	if _, err := NewClient(Config{ClientID: "id", ClientSecret: "secret", RedirectURL: "https://example.com/cb", TokenURL: "http://accounts.example.com/token"}); !errors.Is(err, ErrInsecureEndpoint) {
		t.Fatalf("err = %v, want ErrInsecureEndpoint", err)
	}
}

func TestAuthCodeURLCarriesScopes(t *testing.T) {
	client, server := newTestClient(t, func(http.ResponseWriter, *http.Request) {})
	defer server.Close()
	raw, err := client.AuthCodeURL("state", []string{"scope/a", "scope/b"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(raw)
	if parsed.Query().Get("scope") != "scope/a scope/b" || parsed.Query().Get("access_type") != "offline" {
		t.Fatalf("query = %v", parsed.Query())
	}
	if _, err := client.AuthCodeURL("", []string{"scope/a"}); err == nil {
		t.Error("empty state should fail")
	}
	if _, err := client.AuthCodeURL("state", nil); err == nil {
		t.Error("missing scopes should fail")
	}
}

func TestExchangeAndRefreshBounds(t *testing.T) {
	client, server := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at","refresh_token":"rt","token_type":"Bearer","scope":"scope/a","expires_in":3600}`))
	})
	defer server.Close()
	tokens, err := client.Exchange(context.Background(), "code")
	if err != nil {
		t.Fatal(err)
	}
	if tokens.AccessToken != "at" || tokens.RefreshToken != "rt" || !tokens.Expiry.Equal(time.Unix(4600, 0).UTC()) {
		t.Fatalf("tokens = %+v", tokens)
	}
	client, server = newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"new"}`))
	})
	defer server.Close()
	refreshed, err := client.Refresh(context.Background(), TokenSet{RefreshToken: "old"})
	if err != nil || refreshed.RefreshToken != "old" || refreshed.AccessToken != "new" {
		t.Fatalf("refreshed = %+v err=%v", refreshed, err)
	}
}
