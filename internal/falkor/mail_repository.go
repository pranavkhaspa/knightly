package falkor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/mail"
)

// MailAccountListQuery returns bounded account metadata for one scope.
const MailAccountListQuery = `
	MATCH (a:ProviderAccount {tenant_id: $tenant_id, user_id: $user_id})
	RETURN a.id AS id, a.tenant_id AS tenant_id, a.user_id AS user_id,
	       a.provider AS provider, a.email_address AS email_address,
	       a.scopes AS scopes, a.status AS status,
	       a.connected_at AS connected_at, a.updated_at AS updated_at,
	       a.last_sync_at AS last_sync_at
	ORDER BY a.connected_at DESC
	LIMIT $limit
`

const MailAccountGetQuery = `
	MATCH (a:ProviderAccount {id: $account_id, tenant_id: $tenant_id, user_id: $user_id})
	RETURN a.id AS id, a.tenant_id AS tenant_id, a.user_id AS user_id,
	       a.provider AS provider, a.email_address AS email_address,
	       a.scopes AS scopes, a.status AS status,
	       a.connected_at AS connected_at, a.updated_at AS updated_at,
	       a.last_sync_at AS last_sync_at
	LIMIT 1
`

const MailAccountSaveQuery = `
	MERGE (a:ProviderAccount {id: $account_id, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET a.created_at = $updated_at
	SET a.provider = $provider,
	    a.email_address = $email_address,
	    a.scopes = $scopes,
	    a.status = $status,
	    a.connected_at = $connected_at,
	    a.last_sync_at = $last_sync_at,
	    a.updated_at = $updated_at
	RETURN a.id AS id
`

const MailMessageSaveQuery = `
	UNWIND $rows AS row
	MERGE (a:ProviderAccount {id: row.account_id, tenant_id: $tenant_id, user_id: $user_id})
	MERGE (m:MailMessage {id: row.id, tenant_id: $tenant_id, user_id: $user_id})
	ON CREATE SET m.created_at = $observed_at
	SET m.account_id = row.account_id,
	    m.thread_id = row.thread_id,
	    m.from_name = row.from_name,
	    m.from_email = row.from_email,
	    m.subject = row.subject,
	    m.snippet = row.snippet,
	    m.body_excerpt = row.body_excerpt,
	    m.received_at = row.received_at,
	    m.labels = row.labels,
	    m.has_attachments = row.has_attachments,
	    m.attachments_json = row.attachments_json,
	    m.unread = row.unread,
	    m.untrusted_origin = row.origin,
	    m.content_hash = row.content_hash,
	    m.observed_at = $observed_at,
	    m.updated_at = $observed_at
	MERGE (a)-[:HAS_MESSAGE]->(m)
	RETURN count(m) AS stored
`

const MailThreadLinkQuery = `
	UNWIND $rows AS row
	MATCH (m:MailMessage {id: row.id, tenant_id: $tenant_id, user_id: $user_id})
	MERGE (t:MailThread {id: row.thread_id, tenant_id: $tenant_id, user_id: $user_id})
	MERGE (m)-[:IN_THREAD]->(t)
	RETURN count(t) AS linked
`

const MailMessageListQuery = `
	MATCH (a:ProviderAccount {tenant_id: $tenant_id, user_id: $user_id})-[:HAS_MESSAGE]->(m:MailMessage)
	WHERE $account_id = "" OR m.account_id = $account_id
	RETURN m.id AS id, m.account_id AS account_id, m.thread_id AS thread_id,
	       m.from_name AS from_name, m.from_email AS from_email,
	       m.subject AS subject, m.snippet AS snippet, m.body_excerpt AS body_excerpt,
	       m.received_at AS received_at, m.labels AS labels,
	       m.has_attachments AS has_attachments, m.attachments_json AS attachments_json,
	       m.unread AS unread, m.untrusted_origin AS untrusted_origin
	ORDER BY m.received_at DESC
	LIMIT $limit
`

const MailMessageGetQuery = `
	MATCH (m:MailMessage {id: $message_id, tenant_id: $tenant_id, user_id: $user_id})
	RETURN m.id AS id, m.account_id AS account_id, m.thread_id AS thread_id,
	       m.from_name AS from_name, m.from_email AS from_email,
	       m.subject AS subject, m.snippet AS snippet, m.body_excerpt AS body_excerpt,
	       m.received_at AS received_at, m.labels AS labels,
	       m.has_attachments AS has_attachments, m.attachments_json AS attachments_json,
	       m.unread AS unread, m.untrusted_origin AS untrusted_origin
	LIMIT 1
`

// MailAccountRepository persists bounded provider-account metadata.
type MailAccountRepository struct {
	selector UserGraphSelector
}

var _ mail.AccountStore = (*MailAccountRepository)(nil)

func NewMailAccountRepository(selector UserGraphSelector) *MailAccountRepository {
	return &MailAccountRepository{selector: selector}
}

func (r *MailAccountRepository) SaveAccount(ctx context.Context, scope identity.Scope, account mail.Account) (mail.Account, error) {
	if err := scope.Validate(); err != nil {
		return mail.Account{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "mail account save")
	if err != nil {
		return mail.Account{}, err
	}
	account.TenantID = scope.TenantID
	account.UserID = scope.UserID
	if account.UpdatedAt.IsZero() {
		account.UpdatedAt = time.Now().UTC()
	}
	if _, err := graph.Query(MailAccountSaveQuery, map[string]interface{}{
		"account_id":    account.ID,
		"tenant_id":     scope.TenantID,
		"user_id":       scope.UserID,
		"provider":      account.Provider,
		"email_address": account.EmailAddress,
		"scopes":        account.Scopes,
		"status":        string(account.Status),
		"connected_at":  optionalMillis(account.ConnectedAt),
		"last_sync_at":  optionalMillis(account.LastSyncAt),
		"updated_at":    optionalMillis(account.UpdatedAt),
	}, queryOptionsFor(r.selector)); err != nil {
		return mail.Account{}, fmt.Errorf("save mail account in graph: %w", err)
	}
	return account, nil
}

func (r *MailAccountRepository) GetAccount(ctx context.Context, scope identity.Scope, accountID string) (mail.Account, error) {
	if err := scope.Validate(); err != nil {
		return mail.Account{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "mail account get")
	if err != nil {
		return mail.Account{}, err
	}
	result, err := graph.ROQuery(MailAccountGetQuery, map[string]interface{}{
		"account_id": accountID, "tenant_id": scope.TenantID, "user_id": scope.UserID,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return mail.Account{}, fmt.Errorf("get mail account from graph: %w", err)
	}
	if result == nil || !result.Next() {
		return mail.Account{}, fmt.Errorf("%w: %s", mail.ErrAccountNotFound, accountID)
	}
	return mailAccountFromRecord(result.Record()), nil
}

func (r *MailAccountRepository) ListAccounts(ctx context.Context, scope identity.Scope, limit int) ([]mail.Account, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "mail account list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(MailAccountListQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list mail accounts from graph: %w", err)
	}
	accounts := make([]mail.Account, 0)
	for result != nil && result.Next() {
		accounts = append(accounts, mailAccountFromRecord(result.Record()))
	}
	return accounts, nil
}

// MailMessageRepository persists bounded normalized mail records.
type MailMessageRepository struct {
	selector UserGraphSelector
	now      func() time.Time
}

var _ mail.MessageStore = (*MailMessageRepository)(nil)

func NewMailMessageRepository(selector UserGraphSelector) *MailMessageRepository {
	return &MailMessageRepository{selector: selector, now: func() time.Time { return time.Now().UTC() }}
}

func (r *MailMessageRepository) SaveMessages(ctx context.Context, scope identity.Scope, messages []mail.Message) ([]mail.Message, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		return nil, nil
	}
	graph, err := routedGraph(ctx, r.selector, scope, "mail message save")
	if err != nil {
		return nil, err
	}
	observedAt := r.now().UTC()
	normalized := make([]mail.Message, 0, len(messages))
	rows := make([]map[string]interface{}, 0, len(messages))
	threadRows := make([]map[string]interface{}, 0, len(messages))
	for _, message := range messages {
		value, err := mail.NormalizeMessage(message)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, value)
		rows = append(rows, map[string]interface{}{
			"id": value.ID, "account_id": value.AccountID, "thread_id": value.ThreadID,
			"from_name": value.From.Name, "from_email": value.From.Email,
			"subject": value.Subject, "snippet": value.Snippet, "body_excerpt": value.BodyExcerpt,
			"received_at": optionalMillis(value.ReceivedAt), "labels": value.Labels,
			"has_attachments": value.HasAttachments, "attachments_json": encodeAttachments(value.Attachments),
			"unread": value.Unread, "origin": value.Origin, "content_hash": mailContentHash(value),
		})
		if value.ThreadID != "" {
			threadRows = append(threadRows, map[string]interface{}{"id": value.ID, "thread_id": value.ThreadID})
		}
	}
	if _, err := graph.Query(MailMessageSaveQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID,
		"rows": rows, "observed_at": optionalMillis(observedAt),
	}, queryOptionsFor(r.selector)); err != nil {
		return nil, fmt.Errorf("save mail messages in graph: %w", err)
	}
	if len(threadRows) > 0 {
		if _, err := graph.Query(MailThreadLinkQuery, map[string]interface{}{
			"tenant_id": scope.TenantID, "user_id": scope.UserID, "rows": threadRows,
		}, queryOptionsFor(r.selector)); err != nil {
			return nil, fmt.Errorf("link mail threads in graph: %w", err)
		}
	}
	return normalized, nil
}

func (r *MailMessageRepository) ListMessages(ctx context.Context, scope identity.Scope, accountID string, limit int) ([]mail.Message, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "mail message list")
	if err != nil {
		return nil, err
	}
	result, err := graph.ROQuery(MailMessageListQuery, map[string]interface{}{
		"tenant_id": scope.TenantID, "user_id": scope.UserID, "account_id": accountID, "limit": limit,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("list mail messages from graph: %w", err)
	}
	messages := make([]mail.Message, 0)
	for result != nil && result.Next() {
		messages = append(messages, mailMessageFromRecord(result.Record()))
	}
	return messages, nil
}

func (r *MailMessageRepository) GetMessage(ctx context.Context, scope identity.Scope, messageID string) (mail.Message, error) {
	if err := scope.Validate(); err != nil {
		return mail.Message{}, err
	}
	graph, err := routedGraph(ctx, r.selector, scope, "mail message get")
	if err != nil {
		return mail.Message{}, err
	}
	result, err := graph.ROQuery(MailMessageGetQuery, map[string]interface{}{
		"message_id": messageID, "tenant_id": scope.TenantID, "user_id": scope.UserID,
	}, queryOptionsFor(r.selector))
	if err != nil {
		return mail.Message{}, fmt.Errorf("get mail message from graph: %w", err)
	}
	if result == nil || !result.Next() {
		return mail.Message{}, fmt.Errorf("%w: %s", mail.ErrMessageNotFound, messageID)
	}
	return mailMessageFromRecord(result.Record()), nil
}

func mailAccountFromRecord(record recordReader) mail.Account {
	return mail.Account{
		ID:           stringValue(recordField(record, "id")),
		TenantID:     stringValue(recordField(record, "tenant_id")),
		UserID:       stringValue(recordField(record, "user_id")),
		Provider:     stringValue(recordField(record, "provider")),
		EmailAddress: stringValue(recordField(record, "email_address")),
		Scopes:       stringSliceValue(recordField(record, "scopes")),
		Status:       mail.AccountStatus(stringValue(recordField(record, "status"))),
		ConnectedAt:  timeValue(recordField(record, "connected_at")),
		UpdatedAt:    timeValue(recordField(record, "updated_at")),
		LastSyncAt:   timeValue(recordField(record, "last_sync_at")),
	}
}

func mailMessageFromRecord(record recordReader) mail.Message {
	message := mail.Message{
		ID:             stringValue(recordField(record, "id")),
		AccountID:      stringValue(recordField(record, "account_id")),
		ThreadID:       stringValue(recordField(record, "thread_id")),
		From:           mail.Address{Name: stringValue(recordField(record, "from_name")), Email: stringValue(recordField(record, "from_email"))},
		Subject:        stringValue(recordField(record, "subject")),
		Snippet:        stringValue(recordField(record, "snippet")),
		BodyExcerpt:    stringValue(recordField(record, "body_excerpt")),
		ReceivedAt:     timeValue(recordField(record, "received_at")),
		Labels:         stringSliceValue(recordField(record, "labels")),
		HasAttachments: boolValue(recordField(record, "has_attachments")),
		Attachments:    decodeAttachments(stringValue(recordField(record, "attachments_json"))),
		Unread:         boolValue(recordField(record, "unread")),
		Origin:         stringValue(recordField(record, "untrusted_origin")),
	}
	if message.Origin == "" {
		message.Origin = mail.UntrustedOrigin
	}
	return message
}

func mailContentHash(value mail.Message) string {
	sum := sha256.Sum256([]byte(value.Subject + "\x00" + value.From.Email + "\x00" + value.Snippet + "\x00" + value.BodyExcerpt))
	return hex.EncodeToString(sum[:])
}

func encodeAttachments(attachments []mail.Attachment) string {
	if len(attachments) == 0 {
		return ""
	}
	encoded, err := json.Marshal(attachments)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func decodeAttachments(encoded string) []mail.Attachment {
	if encoded == "" || encoded == "<nil>" {
		return nil
	}
	var attachments []mail.Attachment
	if err := json.Unmarshal([]byte(encoded), &attachments); err != nil {
		return nil
	}
	return attachments
}
