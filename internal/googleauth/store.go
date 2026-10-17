package googleauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

var ErrCredentialNotFound = errors.New("provider credential not found")

// TokenSet holds OAuth tokens for exactly one provider account. It is stored
// only in a protected local credential file and never in FalkorDB, logs,
// prompts, or artifacts.
type TokenSet struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	TokenType    string    `json:"token_type,omitempty"`
	Scope        string    `json:"scope,omitempty"`
	Expiry       time.Time `json:"expiry,omitempty"`
}

// String never reveals token material so a TokenSet can be logged safely.
func (t TokenSet) String() string {
	return fmt.Sprintf("TokenSet{has_access=%t, has_refresh=%t, scope=%q}", t.AccessToken != "", t.RefreshToken != "", t.Scope)
}

// GoString keeps %#v output redacted as well.
func (t TokenSet) GoString() string { return t.String() }

// Valid reports whether the access token is present and not expired beyond skew.
func (t TokenSet) Valid(now time.Time, skew time.Duration) bool {
	if strings.TrimSpace(t.AccessToken) == "" {
		return false
	}
	if t.Expiry.IsZero() {
		return true
	}
	return now.Add(skew).Before(t.Expiry)
}

// Store persists OAuth tokens outside the graph.
type Store interface {
	Save(context.Context, identity.Scope, string, TokenSet) error
	Load(context.Context, identity.Scope, string) (TokenSet, error)
	Delete(context.Context, identity.Scope, string) error
}

// FileStore stores tokens in a single JSON file with owner-only permissions
// (0600) inside an owner-only directory (0700). Writes are atomic.
type FileStore struct {
	path string
}

var _ Store = (*FileStore)(nil)

func NewFileStore(path string) (*FileStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("provider credential file path is required")
	}
	if strings.ContainsAny(path, "\r\n") {
		return nil, errors.New("provider credential file path must not contain a newline")
	}
	return &FileStore{path: path}, nil
}

type credentialFile struct {
	Accounts map[string]TokenSet `json:"accounts"`
}

func credentialKey(scope identity.Scope, accountID string) string {
	return scope.TenantID + "\x00" + scope.UserID + "\x00" + accountID
}

func (s *FileStore) Save(ctx context.Context, scope identity.Scope, accountID string, tokens TokenSet) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTarget(scope, accountID); err != nil {
		return err
	}
	file, err := s.read()
	if err != nil {
		return err
	}
	if file.Accounts == nil {
		file.Accounts = make(map[string]TokenSet)
	}
	file.Accounts[credentialKey(scope, accountID)] = tokens
	return s.write(file)
}

func (s *FileStore) Load(ctx context.Context, scope identity.Scope, accountID string) (TokenSet, error) {
	if err := ctx.Err(); err != nil {
		return TokenSet{}, err
	}
	if err := validateTarget(scope, accountID); err != nil {
		return TokenSet{}, err
	}
	file, err := s.read()
	if err != nil {
		return TokenSet{}, err
	}
	tokens, ok := file.Accounts[credentialKey(scope, accountID)]
	if !ok {
		return TokenSet{}, fmt.Errorf("%w for account %q", ErrCredentialNotFound, accountID)
	}
	return tokens, nil
}

func (s *FileStore) Delete(ctx context.Context, scope identity.Scope, accountID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTarget(scope, accountID); err != nil {
		return err
	}
	file, err := s.read()
	if err != nil {
		return err
	}
	if _, ok := file.Accounts[credentialKey(scope, accountID)]; !ok {
		return fmt.Errorf("%w for account %q", ErrCredentialNotFound, accountID)
	}
	delete(file.Accounts, credentialKey(scope, accountID))
	return s.write(file)
}

func (s *FileStore) read() (credentialFile, error) {
	encoded, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return credentialFile{Accounts: make(map[string]TokenSet)}, nil
		}
		return credentialFile{}, fmt.Errorf("read provider credential file: %w", err)
	}
	if len(encoded) == 0 {
		return credentialFile{Accounts: make(map[string]TokenSet)}, nil
	}
	var file credentialFile
	if err := json.Unmarshal(encoded, &file); err != nil {
		return credentialFile{}, fmt.Errorf("decode provider credential file: %w", err)
	}
	if file.Accounts == nil {
		file.Accounts = make(map[string]TokenSet)
	}
	return file, nil
}

func (s *FileStore) write(file credentialFile) error {
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create provider credential directory: %w", err)
	}
	encoded, err := json.Marshal(file)
	if err != nil {
		return fmt.Errorf("encode provider credential file: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".provider-credentials-*")
	if err != nil {
		return fmt.Errorf("create temporary provider credential file: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("restrict provider credential file permissions: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write provider credential file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close provider credential file: %w", err)
	}
	if err := os.Rename(temporaryName, s.path); err != nil {
		return fmt.Errorf("replace provider credential file: %w", err)
	}
	return nil
}

func validateTarget(scope identity.Scope, accountID string) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("invalid scope: %v", err)
	}
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return errors.New("provider account ID is required")
	}
	if strings.ContainsAny(accountID, "\x00\r\n") {
		return errors.New("provider account ID contains invalid characters")
	}
	return nil
}
