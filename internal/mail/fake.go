package mail

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// FakeReader is a deterministic in-memory MailReader for tests and demos. It
// performs no network access and never mutates mailbox state.
type FakeReader struct {
	messages []Message
	pageSize int
}

var _ MailReader = (*FakeReader)(nil)

func NewFakeReader(messages ...Message) *FakeReader {
	normalized := make([]Message, 0, len(messages))
	for _, message := range messages {
		if value, err := NormalizeMessage(message); err == nil {
			normalized = append(normalized, value)
		}
	}
	return &FakeReader{messages: normalized, pageSize: DefaultListLimit}
}

func (r *FakeReader) ListMessages(ctx context.Context, scope identity.Scope, query ListQuery) (MessagePage, error) {
	if err := ctx.Err(); err != nil {
		return MessagePage{}, err
	}
	if err := scope.Validate(); err != nil {
		return MessagePage{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	query, err := NormalizeListQuery(query)
	if err != nil {
		return MessagePage{}, err
	}
	offset := 0
	if query.PageToken != "" {
		parsed, parseErr := strconv.Atoi(query.PageToken)
		if parseErr != nil || parsed < 0 {
			return MessagePage{}, fmt.Errorf("%w: invalid page token", ErrInvalidQuery)
		}
		offset = parsed
	}
	filtered := make([]Message, 0, len(r.messages))
	for _, message := range r.messages {
		if matchesQuery(message, query.Query) {
			filtered = append(filtered, message)
		}
	}
	if offset >= len(filtered) {
		return MessagePage{Messages: []Message{}}, nil
	}
	end := offset + query.Limit
	next := ""
	if end < len(filtered) {
		next = strconv.Itoa(end)
	} else {
		end = len(filtered)
	}
	page := append([]Message(nil), filtered[offset:end]...)
	return MessagePage{Messages: page, NextPageToken: next}, nil
}

func (r *FakeReader) GetMessage(ctx context.Context, scope identity.Scope, messageID string) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	if err := scope.Validate(); err != nil {
		return Message{}, fmt.Errorf("%w: %v", ErrScopeRequired, err)
	}
	messageID = strings.TrimSpace(messageID)
	for _, message := range r.messages {
		if message.ID == messageID {
			return message, nil
		}
	}
	return Message{}, fmt.Errorf("%w: %s", ErrMessageNotFound, messageID)
}

func matchesQuery(message Message, query string) bool {
	query = strings.TrimSpace(query)
	if query == "" {
		return true
	}
	lower := strings.ToLower(query)
	return strings.Contains(strings.ToLower(message.Subject), lower) ||
		strings.Contains(strings.ToLower(message.From.Email), lower)
}
