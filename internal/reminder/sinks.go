package reminder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

var ErrSinkDisabled = errors.New("reminder sink is not configured")

const (
	webhookTimeout = 10 * time.Second
	webhookMaxBody = int64(64 * 1024)
)

// HTTPWebhookSink posts a bounded delivery payload to an operator-configured
// URL. Because it creates an external side effect it is never delivered by an
// automatic tick: it requires explicit approval. It refuses plain HTTP to any
// non-loopback host.
type HTTPWebhookSink struct {
	url    *url.URL
	client *http.Client
}

var _ Sink = (*HTTPWebhookSink)(nil)

func NewHTTPWebhookSink(rawURL string, client *http.Client) (*HTTPWebhookSink, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, ErrSinkDisabled
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil {
		return nil, fmt.Errorf("%w: webhook URL must be an absolute http(s) URL", ErrInvalidReminder)
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) {
		return nil, fmt.Errorf("%w: webhook URL must use HTTPS for non-loopback hosts", ErrInvalidReminder)
	}
	if client == nil {
		client = &http.Client{Timeout: webhookTimeout}
	}
	return &HTTPWebhookSink{url: parsed, client: client}, nil
}

func (s *HTTPWebhookSink) Name() string { return "webhook" }

func (s *HTTPWebhookSink) RequiresApproval() bool { return true }

func (s *HTTPWebhookSink) Deliver(ctx context.Context, scope identity.Scope, delivery Delivery) error {
	if s == nil || s.url == nil {
		return ErrSinkDisabled
	}
	payload, err := json.Marshal(map[string]interface{}{
		"tenant_id":       scope.TenantID,
		"user_id":         scope.UserID,
		"reminder_id":     delivery.Reminder.ID,
		"title":           delivery.Reminder.Title,
		"body":            delivery.Reminder.Body,
		"occurrence":      delivery.Occurrence.UTC(),
		"idempotency_key": delivery.IdempotencyKey,
	})
	if err != nil {
		return fmt.Errorf("encode webhook payload: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url.String(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", delivery.IdempotencyKey)
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("webhook transport error: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, webhookMaxBody))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", response.StatusCode)
	}
	return nil
}

// WebhookApprovalProposal returns the idempotency key and target to use when
// proposing an approval action for a held webhook delivery.
func WebhookApprovalProposal(delivery Delivery) (target, idempotencyKey string) {
	return "reminder:" + delivery.Reminder.ID, "reminder-webhook:" + delivery.IdempotencyKey
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if parsed := net.ParseIP(host); parsed != nil {
		return parsed.IsLoopback()
	}
	return false
}
