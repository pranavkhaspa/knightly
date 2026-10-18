package mail

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// AccountStore persists bounded provider-account metadata in the graph.
type AccountStore interface {
	SaveAccount(context.Context, identity.Scope, Account) (Account, error)
	GetAccount(context.Context, identity.Scope, string) (Account, error)
	ListAccounts(context.Context, identity.Scope, int) ([]Account, error)
}

// MessageStore persists bounded normalized mail records. It deduplicates by
// provider message ID and never stores message bodies.
type MessageStore interface {
	SaveMessages(context.Context, identity.Scope, []Message) ([]Message, error)
	ListMessages(context.Context, identity.Scope, string, int) ([]Message, error)
	GetMessage(context.Context, identity.Scope, string) (Message, error)
}

// ReaderFactory builds a read-only reader for a connected account.
type ReaderFactory func(context.Context, identity.Scope, Account) (MailReader, error)

// ServiceDeps wires the read-only mail service to its storage and provider.
type ServiceDeps struct {
	Accounts    AccountStore
	Messages    MessageStore
	Credentials CredentialStore
	OAuth       *OAuthClient
	NewReader   ReaderFactory
	Clock       func() time.Time
}

// ConnectRequest connects an account either by redeeming an authorization code
// or by importing tokens obtained out of band (local P0 setup).
type ConnectRequest struct {
	Code         string
	EmailAddress string
	AccessToken  string
	RefreshToken string
	Expiry       time.Time
}

type Service struct {
	accounts    AccountStore
	messages    MessageStore
	credentials CredentialStore
	oauth       *OAuthClient
	newReader   ReaderFactory
	now         func() time.Time
}

func NewService(deps ServiceDeps) (*Service, error) {
	if deps.Accounts == nil || deps.Messages == nil || deps.Credentials == nil {
		return nil, errors.New("mail service requires account, message, and credential stores")
	}
	now := deps.Clock
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{
		accounts: deps.Accounts, messages: deps.Messages, credentials: deps.Credentials,
		oauth: deps.OAuth, newReader: deps.NewReader, now: now,
	}, nil
}

// AuthCodeURL returns the consent URL for connecting a Gmail account.
func (s *Service) AuthCodeURL(state string) (string, error) {
	if s.oauth == nil {
		return "", errors.New("mail OAuth is not configured")
	}
	return s.oauth.AuthCodeURL(state)
}

func (s *Service) Connect(ctx context.Context, scope identity.Scope, request ConnectRequest) (Account, error) {
	if err := scope.Validate(); err != nil {
		return Account{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	tokens, err := s.obtainTokens(ctx, request)
	if err != nil {
		return Account{}, err
	}
	if tokens.Scope != "" && !hasReadOnlyScope(tokens.Scope) {
		return Account{}, ErrScopeMissing
	}
	email := bound(request.EmailAddress, MaxAddressEmail)
	accountID := AccountID(scope, email)
	if email == "" {
		reader, readerErr := s.reader(ctx, scope, Account{ID: accountID, Provider: ProviderGmail})
		if readerErr != nil {
			return Account{}, readerErr
		}
		if profiler, ok := reader.(interface {
			Profile(context.Context) (string, error)
		}); ok {
			if resolved, profileErr := profiler.Profile(ctx); profileErr == nil {
				email = resolved
				accountID = AccountID(scope, email)
			}
		}
	}
	if err := s.credentials.Save(ctx, scope, accountID, tokens); err != nil {
		return Account{}, err
	}
	now := s.now().UTC()
	account := Account{
		ID: accountID, TenantID: scope.TenantID, UserID: scope.UserID, Provider: ProviderGmail,
		EmailAddress: email, Scopes: []string{GmailReadOnlyScope}, Status: AccountConnected,
		ConnectedAt: now, UpdatedAt: now,
	}
	return s.accounts.SaveAccount(ctx, scope, account)
}

func (s *Service) obtainTokens(ctx context.Context, request ConnectRequest) (TokenSet, error) {
	if code := strings.TrimSpace(request.Code); code != "" {
		if s.oauth == nil {
			return TokenSet{}, errors.New("mail OAuth is not configured")
		}
		return s.oauth.Exchange(ctx, code)
	}
	if strings.TrimSpace(request.RefreshToken) == "" && strings.TrimSpace(request.AccessToken) == "" {
		return TokenSet{}, errors.New("an authorization code or tokens are required")
	}
	return TokenSet{
		AccessToken: request.AccessToken, RefreshToken: request.RefreshToken,
		Scope: GmailReadOnlyScope, Expiry: request.Expiry,
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
	if err := s.credentials.Delete(ctx, scope, accountID); err != nil && !errors.Is(err, ErrCredentialNotFound) {
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

// ListMessages reads bounded messages from the provider and persists them. It
// never mutates mailbox state.
func (s *Service) ListMessages(ctx context.Context, scope identity.Scope, accountID string, query ListQuery) (MessagePage, error) {
	account, reader, err := s.connectedReader(ctx, scope, accountID)
	if err != nil {
		return MessagePage{}, err
	}
	page, err := reader.ListMessages(ctx, scope, query)
	if err != nil {
		return MessagePage{}, err
	}
	for index := range page.Messages {
		page.Messages[index].AccountID = account.ID
	}
	if _, err := s.messages.SaveMessages(ctx, scope, page.Messages); err != nil {
		return MessagePage{}, err
	}
	account.LastSyncAt = s.now().UTC()
	account.UpdatedAt = account.LastSyncAt
	if _, err := s.accounts.SaveAccount(ctx, scope, account); err != nil {
		return MessagePage{}, err
	}
	return page, nil
}

func (s *Service) GetMessage(ctx context.Context, scope identity.Scope, accountID, messageID string) (Message, error) {
	account, reader, err := s.connectedReader(ctx, scope, accountID)
	if err != nil {
		return Message{}, err
	}
	message, err := reader.GetMessage(ctx, scope, messageID)
	if err != nil {
		return Message{}, err
	}
	message.AccountID = account.ID
	message, err = NormalizeMessage(message)
	if err != nil {
		return Message{}, err
	}
	if _, err := s.messages.SaveMessages(ctx, scope, []Message{message}); err != nil {
		return Message{}, err
	}
	return message, nil
}

func (s *Service) StoredMessages(ctx context.Context, scope identity.Scope, accountID string, limit int) ([]Message, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if limit < 1 || limit > MaxListLimit {
		limit = DefaultListLimit
	}
	return s.messages.ListMessages(ctx, scope, accountID, limit)
}

func (s *Service) connectedReader(ctx context.Context, scope identity.Scope, accountID string) (Account, MailReader, error) {
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
	reader, err := s.reader(ctx, scope, account)
	if err != nil {
		return Account{}, nil, err
	}
	return account, reader, nil
}

func (s *Service) reader(ctx context.Context, scope identity.Scope, account Account) (MailReader, error) {
	if s.newReader == nil {
		return nil, errors.New("mail reader factory is not configured")
	}
	return s.newReader(ctx, scope, account)
}

// AccountID derives a stable account identifier from the scope and mailbox.
func AccountID(scope identity.Scope, email string) string {
	normalized := strings.ToLower(strings.TrimSpace(email))
	if normalized == "" {
		normalized = "gmail"
	}
	sum := sha256.Sum256([]byte(scope.TenantID + "\x00" + scope.UserID + "\x00" + ProviderGmail + "\x00" + normalized))
	return "mail_account_" + hex.EncodeToString(sum[:])[:24]
}
