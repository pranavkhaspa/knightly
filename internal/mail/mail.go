// Package mail contains provider-neutral, read-only email types. The phase P0
// reader boundary is intentionally incapable of sending, deleting, labeling,
// archiving, or marking mail read: MailReader exposes only read operations, and
// every provider adapter must use read-only OAuth scopes.
package mail

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/untrusted"
)

const (
	DefaultListLimit  = 20
	MaxListLimit      = 50
	MaxQueryLength    = 512
	MaxPageTokenSize  = 1024
	MaxMessageIDSize  = 256
	MaxSubjectLength  = 512
	MaxAddressName    = 256
	MaxAddressEmail   = 320
	MaxSnippetLength  = 1024
	MaxBodyExcerpt    = 4096
	MaxRecipients     = 50
	MaxAttachments    = 50
	MaxAttachmentName = 255
	MaxMIMETypeSize   = 128
	MaxLabels         = 50
	MaxLabelLength    = 128
	MaxHeaderValue    = 1024
)

// UntrustedOrigin marks email-derived text as hostile external input.
const UntrustedOrigin = string(untrusted.Mail)

var (
	ErrInvalidQuery    = errors.New("invalid mail query")
	ErrInvalidMessage  = errors.New("invalid mail message")
	ErrAccountNotFound = errors.New("mail account not found")
	ErrMessageNotFound = errors.New("mail message not found")
	ErrScopeRequired   = errors.New("trusted mail scope is required")
)

// AccountStatus is the connection state of a provider account.
type AccountStatus string

const (
	AccountConnected    AccountStatus = "connected"
	AccountDisconnected AccountStatus = "disconnected"
	AccountError        AccountStatus = "error"
)

// Account is bounded provider-account metadata. Secrets are never part of it.
type Account struct {
	ID           string        `json:"id"`
	TenantID     string        `json:"tenant_id"`
	UserID       string        `json:"user_id"`
	Provider     string        `json:"provider"`
	EmailAddress string        `json:"email_address,omitempty"`
	Scopes       []string      `json:"scopes,omitempty"`
	Status       AccountStatus `json:"status"`
	ConnectedAt  time.Time     `json:"connected_at,omitempty"`
	UpdatedAt    time.Time     `json:"updated_at,omitempty"`
	LastSyncAt   time.Time     `json:"last_sync_at,omitempty"`
}

// Address is a single normalized email address.
type Address struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email"`
}

// Attachment is bounded attachment metadata. Content is never stored.
type Attachment struct {
	ID       string `json:"id,omitempty"`
	Filename string `json:"filename,omitempty"`
	MIMEType string `json:"mime_type,omitempty"`
	Size     int64  `json:"size,omitempty"`
}

// Message is a bounded, normalized email record.
type Message struct {
	ID             string       `json:"id"`
	AccountID      string       `json:"account_id"`
	ThreadID       string       `json:"thread_id,omitempty"`
	From           Address      `json:"from"`
	To             []Address    `json:"to,omitempty"`
	CC             []Address    `json:"cc,omitempty"`
	Subject        string       `json:"subject,omitempty"`
	Snippet        string       `json:"snippet,omitempty"`
	BodyExcerpt    string       `json:"body_excerpt,omitempty"`
	ReceivedAt     time.Time    `json:"received_at,omitempty"`
	Labels         []string     `json:"labels,omitempty"`
	HasAttachments bool         `json:"has_attachments"`
	Attachments    []Attachment `json:"attachments,omitempty"`
	Unread         bool         `json:"unread"`
	// Origin marks the message body as untrusted external content.
	Origin string `json:"origin"`
}

// ListQuery is a bounded message-list request.
type ListQuery struct {
	// Query is an optional provider search expression (for example a Gmail
	// search string such as "is:unread newer_than:7d").
	Query     string
	PageToken string
	Limit     int
}

// MessagePage is a bounded page of messages with an opaque continuation token.
type MessagePage struct {
	Messages      []Message `json:"messages"`
	NextPageToken string    `json:"next_page_token,omitempty"`
}

// MailReader is the read-only provider boundary. It deliberately has no
// method that mutates mailbox state.
type MailReader interface {
	ListMessages(context.Context, identity.Scope, ListQuery) (MessagePage, error)
	GetMessage(context.Context, identity.Scope, string) (Message, error)
}

// NormalizeListQuery validates and bounds a list request.
func NormalizeListQuery(query ListQuery) (ListQuery, error) {
	query.Query = strings.TrimSpace(query.Query)
	if utf8.RuneCountInString(query.Query) > MaxQueryLength {
		return ListQuery{}, fmt.Errorf("%w: query must be at most %d characters", ErrInvalidQuery, MaxQueryLength)
	}
	query.PageToken = strings.TrimSpace(query.PageToken)
	if len(query.PageToken) > MaxPageTokenSize {
		return ListQuery{}, fmt.Errorf("%w: page token is too long", ErrInvalidQuery)
	}
	if query.Limit == 0 {
		query.Limit = DefaultListLimit
	}
	if query.Limit < 1 || query.Limit > MaxListLimit {
		return ListQuery{}, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxListLimit)
	}
	return query, nil
}

// NormalizeMessage bounds every field and marks the body as untrusted. It
// rejects a message that is missing required identifiers.
func NormalizeMessage(message Message) (Message, error) {
	message.ID = strings.TrimSpace(message.ID)
	if message.ID == "" || len(message.ID) > MaxMessageIDSize {
		return Message{}, fmt.Errorf("%w: message ID is required and bounded", ErrInvalidMessage)
	}
	message.AccountID = strings.TrimSpace(message.AccountID)
	message.ThreadID = bound(message.ThreadID, MaxMessageIDSize)
	message.Subject = bound(message.Subject, MaxSubjectLength)
	message.Snippet = bound(message.Snippet, MaxSnippetLength)
	message.From = normalizeAddress(message.From)
	message.To = normalizeAddresses(message.To)
	message.CC = normalizeAddresses(message.CC)
	message.Labels = normalizeLabels(message.Labels)
	message.Attachments = normalizeAttachments(message.Attachments)
	message.HasAttachments = len(message.Attachments) > 0
	message.Subject = sanitizeHeader(message.Subject)
	message.Origin = UntrustedOrigin
	excerpt := message.BodyExcerpt
	if content, err := untrusted.New(untrusted.Mail, excerpt); err == nil {
		excerpt = content.Bounded()
	} else {
		excerpt = bound(excerpt, MaxBodyExcerpt)
	}
	message.BodyExcerpt = excerpt
	return message, nil
}

func normalizeAddress(address Address) Address {
	return Address{Name: sanitizeHeader(bound(address.Name, MaxAddressName)), Email: bound(address.Email, MaxAddressEmail)}
}

func normalizeAddresses(addresses []Address) []Address {
	if len(addresses) == 0 {
		return nil
	}
	if len(addresses) > MaxRecipients {
		addresses = addresses[:MaxRecipients]
	}
	result := make([]Address, 0, len(addresses))
	for _, address := range addresses {
		result = append(result, normalizeAddress(address))
	}
	return result
}

func normalizeLabels(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	if len(labels) > MaxLabels {
		labels = labels[:MaxLabels]
	}
	result := make([]string, 0, len(labels))
	for _, label := range labels {
		if value := bound(label, MaxLabelLength); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func normalizeAttachments(attachments []Attachment) []Attachment {
	if len(attachments) == 0 {
		return nil
	}
	if len(attachments) > MaxAttachments {
		attachments = attachments[:MaxAttachments]
	}
	result := make([]Attachment, 0, len(attachments))
	for _, attachment := range attachments {
		size := attachment.Size
		if size < 0 {
			size = 0
		}
		result = append(result, Attachment{
			ID:       bound(attachment.ID, MaxMessageIDSize),
			Filename: bound(attachment.Filename, MaxAttachmentName),
			MIMEType: bound(attachment.MIMEType, MaxMIMETypeSize),
			Size:     size,
		})
	}
	return result
}

// sanitizeHeader removes control characters that could smuggle instructions or
// log injection through a header value.
func sanitizeHeader(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\t", " ")
	return bound(strings.TrimSpace(strings.Join(strings.Fields(value), " ")), MaxHeaderValue)
}

func bound(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return string(runes)
}
