package mail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

const (
	gmailDefaultBaseURL = "https://gmail.googleapis.com"
	gmailMaxMetadata    = 50
	tokenRefreshSkew    = time.Minute
)

var (
	ErrGmailConfig          = errors.New("invalid Gmail reader configuration")
	ErrProviderRequest      = errors.New("Gmail API request failed")
	ErrProviderUnauthorized = errors.New("Gmail API rejected the access token")
	ErrTokenExpired         = errors.New("mail access token is expired and cannot be refreshed")
)

// GmailConfig configures a read-only Gmail reader bound to one account.
type GmailConfig struct {
	BaseURL     string
	Scope       identity.Scope
	Account     Account
	OAuth       *OAuthClient
	Credentials CredentialStore
	Client      *http.Client
	Clock       func() time.Time
	MaxBodySize int64
}

// GmailReader reads messages through the Gmail REST API using only GET
// requests. It has no method that mutates mailbox state.
type GmailReader struct {
	baseURL     *url.URL
	scope       identity.Scope
	account     Account
	oauth       *OAuthClient
	credentials CredentialStore
	client      *http.Client
	now         func() time.Time
	maxBody     int64
	tokens      TokenSet
}

var _ MailReader = (*GmailReader)(nil)

func NewGmailReader(config GmailConfig) (*GmailReader, error) {
	if config.OAuth == nil {
		return nil, fmt.Errorf("%w: OAuth client is required", ErrGmailConfig)
	}
	account := config.Account
	account.ID = strings.TrimSpace(account.ID)
	if account.ID == "" {
		return nil, fmt.Errorf("%w: account ID is required", ErrGmailConfig)
	}
	if err := config.Scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGmailConfig, err)
	}
	baseURL, err := validateOAuthEndpoint(config.BaseURL, gmailDefaultBaseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGmailConfig, err)
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	now := config.Clock
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	reader := &GmailReader{
		baseURL: baseURL, scope: config.Scope, account: account, oauth: config.OAuth,
		credentials: config.Credentials, client: client, now: now, maxBody: config.MaxBodySize,
	}
	if reader.maxBody <= 0 {
		reader.maxBody = 1 << 20
	}
	if config.Credentials != nil {
		tokens, loadErr := config.Credentials.Load(context.Background(), config.Scope, account.ID)
		if loadErr != nil && !errors.Is(loadErr, ErrCredentialNotFound) {
			return nil, fmt.Errorf("load mail credentials: %w", loadErr)
		}
		reader.tokens = tokens
	}
	return reader, nil
}

func (r *GmailReader) ListMessages(ctx context.Context, scope identity.Scope, query ListQuery) (MessagePage, error) {
	if err := r.checkScope(scope); err != nil {
		return MessagePage{}, err
	}
	query, err := NormalizeListQuery(query)
	if err != nil {
		return MessagePage{}, err
	}
	endpoint := r.endpoint("/gmail/v1/users/me/messages")
	params := endpoint.Query()
	if query.Query != "" {
		params.Set("q", query.Query)
	}
	params.Set("maxResults", strconv.Itoa(min(query.Limit, gmailMaxMetadata)))
	if query.PageToken != "" {
		params.Set("pageToken", query.PageToken)
	}
	endpoint.RawQuery = params.Encode()

	body, err := r.get(ctx, endpoint)
	if err != nil {
		return MessagePage{}, err
	}
	var listed gmailListResponse
	if err := json.Unmarshal(body, &listed); err != nil {
		return MessagePage{}, fmt.Errorf("%w: decode message list", ErrProviderRequest)
	}
	messages := make([]Message, 0, min(len(listed.Messages), query.Limit))
	for _, reference := range listed.Messages {
		if len(messages) == query.Limit {
			break
		}
		message, fetchErr := r.fetchMetadata(ctx, reference.ID)
		if fetchErr != nil {
			return MessagePage{}, fetchErr
		}
		messages = append(messages, message)
	}
	return MessagePage{Messages: messages, NextPageToken: bound(listed.NextPageToken, MaxPageTokenSize)}, nil
}

func (r *GmailReader) GetMessage(ctx context.Context, scope identity.Scope, messageID string) (Message, error) {
	if err := r.checkScope(scope); err != nil {
		return Message{}, err
	}
	messageID = strings.TrimSpace(messageID)
	if messageID == "" || len(messageID) > MaxMessageIDSize {
		return Message{}, fmt.Errorf("%w: message ID is required and bounded", ErrInvalidMessage)
	}
	return r.fetchMetadata(ctx, messageID)
}

// Profile returns the mailbox address of the authorized user. It is read-only
// and used only to label the connected account.
func (r *GmailReader) Profile(ctx context.Context) (string, error) {
	body, err := r.get(ctx, r.endpoint("/gmail/v1/users/me/profile"))
	if err != nil {
		return "", err
	}
	var profile struct {
		EmailAddress string `json:"emailAddress"`
	}
	if err := json.Unmarshal(body, &profile); err != nil {
		return "", fmt.Errorf("%w: decode profile", ErrProviderRequest)
	}
	return bound(profile.EmailAddress, MaxAddressEmail), nil
}

func (r *GmailReader) checkScope(scope identity.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	if scope != r.scope {
		return fmt.Errorf("%w: reader is bound to a different scope", ErrScopeRequired)
	}
	return nil
}

func (r *GmailReader) fetchMetadata(ctx context.Context, messageID string) (Message, error) {
	endpoint := r.endpoint("/gmail/v1/users/me/messages/" + url.PathEscape(messageID))
	params := endpoint.Query()
	params.Set("format", "metadata")
	for _, header := range []string{"From", "To", "Cc", "Subject", "Date"} {
		params.Add("metadataHeaders", header)
	}
	endpoint.RawQuery = params.Encode()
	body, err := r.get(ctx, endpoint)
	if err != nil {
		return Message{}, err
	}
	var raw gmailMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return Message{}, fmt.Errorf("%w: decode message %q", ErrProviderRequest, messageID)
	}
	return r.toMessage(messageID, raw)
}

func (r *GmailReader) toMessage(messageID string, raw gmailMessage) (Message, error) {
	message := Message{
		ID:         messageID,
		AccountID:  r.account.ID,
		ThreadID:   bound(raw.ThreadID, MaxMessageIDSize),
		Subject:    headerValue(raw.Payload.Headers, "Subject"),
		Snippet:    raw.Snippet,
		From:       parseAddress(headerValue(raw.Payload.Headers, "From")),
		To:         parseAddressList(headerValue(raw.Payload.Headers, "To")),
		CC:         parseAddressList(headerValue(raw.Payload.Headers, "Cc")),
		Labels:     raw.LabelIDs,
		ReceivedAt: internalDate(raw.InternalDate),
		Unread:     containsLabel(raw.LabelIDs, "UNREAD"),
	}
	message.Attachments = collectAttachments(raw.Payload)
	return NormalizeMessage(message)
}

func (r *GmailReader) endpoint(path string) *url.URL {
	endpoint := *r.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	endpoint.RawPath = ""
	endpoint.RawQuery = ""
	endpoint.Fragment = ""
	return &endpoint
}

func (r *GmailReader) get(ctx context.Context, endpoint *url.URL) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.authorize(ctx); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Gmail request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+r.tokens.AccessToken)
	request.Header.Set("Accept", "application/json")
	response, err := r.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: transport error", ErrProviderRequest)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, r.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read response", ErrProviderRequest)
	}
	if int64(len(body)) > r.maxBody {
		return nil, fmt.Errorf("%w: response is too large", ErrProviderRequest)
	}
	switch {
	case response.StatusCode == http.StatusUnauthorized:
		return nil, ErrProviderUnauthorized
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return nil, fmt.Errorf("%w: provider returned HTTP %d", ErrProviderRequest, response.StatusCode)
	}
	return body, nil
}

func (r *GmailReader) authorize(ctx context.Context) error {
	if r.tokens.Valid(r.now(), tokenRefreshSkew) {
		return nil
	}
	if strings.TrimSpace(r.tokens.RefreshToken) == "" {
		return ErrTokenExpired
	}
	refreshed, err := r.oauth.Refresh(ctx, r.tokens)
	if err != nil {
		return err
	}
	r.tokens = refreshed
	if r.credentials != nil {
		if err := r.credentials.Save(ctx, r.scope, r.account.ID, refreshed); err != nil {
			return fmt.Errorf("persist refreshed mail credentials: %w", err)
		}
	}
	return nil
}

type gmailListResponse struct {
	Messages      []gmailReference `json:"messages"`
	NextPageToken string           `json:"nextPageToken"`
}

type gmailReference struct {
	ID       string `json:"id"`
	ThreadID string `json:"threadId"`
}

type gmailMessage struct {
	ID           string       `json:"id"`
	ThreadID     string       `json:"threadId"`
	LabelIDs     []string     `json:"labelIds"`
	Snippet      string       `json:"snippet"`
	InternalDate string       `json:"internalDate"`
	Payload      gmailPayload `json:"payload"`
}

type gmailPayload struct {
	Headers []gmailHeader `json:"headers"`
	Parts   []gmailPart   `json:"parts"`
}

type gmailHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type gmailPart struct {
	PartID   string      `json:"partId"`
	Filename string      `json:"filename"`
	MIMEType string      `json:"mimeType"`
	Body     gmailBody   `json:"body"`
	Parts    []gmailPart `json:"parts"`
}

type gmailBody struct {
	Size         int64  `json:"size"`
	AttachmentID string `json:"attachmentId"`
}

func headerValue(headers []gmailHeader, name string) string {
	for _, header := range headers {
		if strings.EqualFold(header.Name, name) {
			return bound(header.Value, MaxHeaderValue)
		}
	}
	return ""
}

func collectAttachments(payload gmailPayload) []Attachment {
	result := make([]Attachment, 0)
	var walk func(part gmailPart)
	walk = func(part gmailPart) {
		if len(result) >= MaxAttachments {
			return
		}
		if part.Filename != "" {
			result = append(result, Attachment{
				ID:       bound(part.Body.AttachmentID, MaxMessageIDSize),
				Filename: bound(part.Filename, MaxAttachmentName),
				MIMEType: bound(part.MIMEType, MaxMIMETypeSize),
				Size:     part.Body.Size,
			})
		}
		for _, child := range part.Parts {
			if len(result) >= MaxAttachments {
				return
			}
			walk(child)
		}
	}
	for _, part := range payload.Parts {
		walk(part)
	}
	return result
}

func parseAddress(value string) Address {
	value = strings.TrimSpace(value)
	if value == "" {
		return Address{}
	}
	if parsed, err := mail.ParseAddress(value); err == nil {
		return Address{Name: bound(parsed.Name, MaxAddressName), Email: bound(parsed.Address, MaxAddressEmail)}
	}
	if index := strings.LastIndexAny(value, "<>"); index >= 0 {
		value = strings.Trim(strings.TrimSpace(value), "<>")
	}
	return Address{Email: bound(value, MaxAddressEmail)}
}

func parseAddressList(value string) []Address {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parsed, err := mail.ParseAddressList(value)
	if err != nil {
		return []Address{parseAddress(value)}
	}
	result := make([]Address, 0, min(len(parsed), MaxRecipients))
	for _, address := range parsed {
		if len(result) == MaxRecipients {
			break
		}
		result = append(result, Address{Name: bound(address.Name, MaxAddressName), Email: bound(address.Address, MaxAddressEmail)})
	}
	return result
}

func internalDate(value string) time.Time {
	millis, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || millis <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(millis).UTC()
}

func containsLabel(labels []string, target string) bool {
	for _, label := range labels {
		if strings.EqualFold(label, target) {
			return true
		}
	}
	return false
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
