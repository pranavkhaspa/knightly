// Package observability provides structured logging, bounded metrics, and an
// optional non-blocking OTLP exporter for the NightShift daemon. Everything in
// this package is safe to call from a hot path: logging and metric updates never
// block on the network, and no value is emitted before it is redacted.
package observability

import (
	"net/url"
	"regexp"
	"strings"
)

// Placeholder replaces any value that may contain a credential.
const Placeholder = "[REDACTED]"

// sensitiveKeys are attribute or header names whose entire value is a secret.
// Matching is case-insensitive and ignores separators such as '-' and '_'.
var sensitiveKeys = []string{
	"authorization",
	"proxyauthorization",
	"cookie",
	"setcookie",
	"token",
	"accesstoken",
	"refreshtoken",
	"idtoken",
	"bearertoken",
	"clientsecret",
	"secret",
	"password",
	"apikey",
	"privatekey",
	"credential",
	"session",
	"signature",
	"proxysignature",
}

// secretPatterns catch credential material embedded in an otherwise ordinary
// free-text value (a log message, URL, or error string).
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{4,}`),
	regexp.MustCompile(`(?i)\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}`),
	regexp.MustCompile(`(?i)\b(access_token|refresh_token|id_token|client_secret|api[_-]?key|password|session|cookie|secret|token|signature)=([^&\s"']+)`),
	regexp.MustCompile(`(?i)"(access_token|refresh_token|id_token|client_secret|api[_-]?key|password|refresh|secret|token)"\s*:\s*"[^"]*"`),
	regexp.MustCompile(`(?i)\bAIza[0-9A-Za-z_-]{20,}`),
	regexp.MustCompile(`(?i)\bgh[pousr]_[A-Za-z0-9]{20,}`),
	regexp.MustCompile(`(?i)\bxox[baprs]-[A-Za-z0-9-]{10,}`),
}

// Redactor removes credential material from strings, header values, and URLs.
// It is immutable and safe for concurrent use.
type Redactor struct {
	patterns []*regexp.Regexp
	keys     []string
}

// NewRedactor returns a Redactor with the built-in credential rules.
func NewRedactor() *Redactor {
	keys := make([]string, 0, len(sensitiveKeys))
	for _, key := range sensitiveKeys {
		keys = append(keys, normalizeKey(key))
	}
	return &Redactor{patterns: secretPatterns, keys: keys}
}

// SensitiveKey reports whether a header or attribute name is a credential name.
// Matching ignores case and separators and accepts prefixes such as
// "X-Nightshift-Proxy-Signature" or "session_id".
func (r *Redactor) SensitiveKey(key string) bool {
	if r == nil {
		return false
	}
	normalized := normalizeKey(key)
	for _, candidate := range r.keys {
		if strings.Contains(normalized, candidate) {
			return true
		}
	}
	return false
}

// Value redacts a single attribute or header value. A sensitive key always
// yields the placeholder; otherwise free-text credentials are scrubbed.
func (r *Redactor) Value(key, value string) string {
	if r == nil {
		return value
	}
	if value == "" {
		return value
	}
	if r.SensitiveKey(key) {
		return Placeholder
	}
	return r.Scrub(value)
}

// Scrub replaces embedded credential material in free text.
func (r *Redactor) Scrub(value string) string {
	if r == nil || value == "" {
		return value
	}
	for _, pattern := range r.patterns {
		value = pattern.ReplaceAllStringFunc(value, func(match string) string {
			if key, _, ok := splitAssignment(match); ok {
				return key + "=" + Placeholder
			}
			return Placeholder
		})
	}
	return value
}

// ScrubURL removes credential-bearing query values from a URL while keeping the
// path so operators can still locate the request.
func (r *Redactor) ScrubURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return r.Scrub(raw)
	}
	query := parsed.Query()
	changed := false
	for key := range query {
		if r.SensitiveKey(key) {
			query.Set(key, Placeholder)
			changed = true
			continue
		}
		value := query.Get(key)
		if scrubbed := r.Scrub(value); scrubbed != value {
			query.Set(key, scrubbed)
			changed = true
		}
	}
	if changed {
		parsed.RawQuery = query.Encode()
	}
	return parsed.String()
}

func normalizeKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.ReplaceAll(key, "-", "")
	key = strings.ReplaceAll(key, "_", "")
	key = strings.ReplaceAll(key, " ", "")
	return key
}

func splitAssignment(match string) (string, string, bool) {
	if index := strings.IndexAny(match, "=:"); index > 0 {
		key := match[:index]
		if !strings.ContainsAny(key, " \t") {
			return key, match[index+1:], true
		}
	}
	return "", "", false
}
