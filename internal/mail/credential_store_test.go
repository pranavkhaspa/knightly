package mail

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

func TestFileCredentialStoreRoundTripAndIsolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials", "mail.json")
	store, err := NewFileCredentialStore(path)
	if err != nil {
		t.Fatal(err)
	}
	tokens := TokenSet{AccessToken: "access-secret", RefreshToken: "refresh-secret", Scope: GmailReadOnlyScope, Expiry: time.Now().Add(time.Hour)}
	if err := store.Save(context.Background(), testScope, "acct-1", tokens); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background(), testScope, "acct-1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AccessToken != tokens.AccessToken || loaded.RefreshToken != tokens.RefreshToken {
		t.Fatalf("loaded tokens = %+v", loaded)
	}
	// Another scope must not see this account's tokens.
	if _, err := store.Load(context.Background(), identity.Scope{TenantID: "tenant_other", UserID: "user_other"}, "acct-1"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("cross-scope load err = %v", err)
	}
	if runtime.GOOS != "windows" {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if info.Mode().Perm() != 0o600 {
			t.Errorf("credential file mode = %v, want 0600", info.Mode().Perm())
		}
		if directory, statErr := os.Stat(filepath.Dir(path)); statErr != nil {
			t.Fatal(statErr)
		} else if directory.Mode().Perm() != 0o700 {
			t.Errorf("credential dir mode = %v, want 0700", directory.Mode().Perm())
		}
	}
}

func TestFileCredentialStoreDelete(t *testing.T) {
	store, err := NewFileCredentialStore(filepath.Join(t.TempDir(), "mail.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), testScope, "acct-1", TokenSet{AccessToken: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(context.Background(), testScope, "acct-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), testScope, "acct-1"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("load after delete err = %v", err)
	}
	if err := store.Delete(context.Background(), testScope, "acct-1"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("second delete err = %v", err)
	}
}

func TestFileCredentialStoreRejectsInvalidTarget(t *testing.T) {
	store, err := NewFileCredentialStore(filepath.Join(t.TempDir(), "mail.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), testScope, "", TokenSet{}); err == nil {
		t.Fatal("expected empty account ID to fail")
	}
	if err := store.Save(context.Background(), identity.Scope{}, "acct", TokenSet{}); err == nil {
		t.Fatal("expected invalid scope to fail")
	}
}

func TestFileCredentialStoreCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mail.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileCredentialStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background(), testScope, "acct"); err == nil || !strings.Contains(err.Error(), "decode provider credential file") {
		t.Fatalf("err = %v", err)
	}
}

func TestTokenSetStringRedactsSecrets(t *testing.T) {
	tokens := TokenSet{AccessToken: "access-secret", RefreshToken: "refresh-secret"}
	for _, value := range []string{tokens.String(), tokens.GoString()} {
		if strings.Contains(value, "access-secret") || strings.Contains(value, "refresh-secret") {
			t.Errorf("redacted value leaked a token: %q", value)
		}
	}
}

func TestTokenSetValid(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	if (TokenSet{}).Valid(now, time.Minute) {
		t.Error("empty token set should be invalid")
	}
	if !(TokenSet{AccessToken: "a", Expiry: now.Add(time.Hour)}).Valid(now, time.Minute) {
		t.Error("unexpired token should be valid")
	}
	if (TokenSet{AccessToken: "a", Expiry: now.Add(time.Second)}).Valid(now, time.Minute) {
		t.Error("token expiring within skew should be invalid")
	}
}
