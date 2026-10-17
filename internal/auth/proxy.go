package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// TrustedProxyConfig configures signature-verified identity headers set by an
// authenticated reverse proxy (for example oauth2-proxy or a TLS terminator).
// The proxy must be the only party that can reach the listener and must sign a
// canonical string over the identity headers with a shared secret.
type TrustedProxyConfig struct {
	Secret          []byte
	TenantHeader    string
	UserHeader      string
	ApproverHeader  string
	SignatureHeader string
	TimestampHeader string
	MaxSkew         time.Duration
	Clock           func() time.Time
}

// TrustedProxyResolver verifies a proxy HMAC over the identity headers. A client
// cannot forge the scope or approver without the shared secret.
type TrustedProxyResolver struct {
	secret          []byte
	tenantHeader    string
	userHeader      string
	approverHeader  string
	signatureHeader string
	timestampHeader string
	maxSkew         time.Duration
	clock           func() time.Time
}

var _ IdentityResolver = (*TrustedProxyResolver)(nil)

func NewTrustedProxyResolver(cfg TrustedProxyConfig) (*TrustedProxyResolver, error) {
	if len(cfg.Secret) == 0 {
		return nil, fmt.Errorf("%w: trusted-proxy secret is required", ErrInvalidConfig)
	}
	cfg.TenantHeader = defaultHeader(cfg.TenantHeader, "X-Nightshift-Tenant")
	cfg.UserHeader = defaultHeader(cfg.UserHeader, "X-Nightshift-User")
	cfg.ApproverHeader = defaultHeader(cfg.ApproverHeader, "X-Nightshift-Approver")
	cfg.SignatureHeader = defaultHeader(cfg.SignatureHeader, "X-Nightshift-Proxy-Signature")
	cfg.TimestampHeader = defaultHeader(cfg.TimestampHeader, "X-Nightshift-Proxy-Timestamp")
	if cfg.MaxSkew <= 0 {
		cfg.MaxSkew = 60 * time.Second
	}
	clock := cfg.Clock
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &TrustedProxyResolver{
		secret: cfg.Secret, tenantHeader: cfg.TenantHeader, userHeader: cfg.UserHeader,
		approverHeader: cfg.ApproverHeader, signatureHeader: cfg.SignatureHeader,
		timestampHeader: cfg.TimestampHeader, maxSkew: cfg.MaxSkew, clock: clock,
	}, nil
}

func (r *TrustedProxyResolver) Resolve(_ context.Context, request *http.Request) (Identity, error) {
	if request == nil {
		return Identity{}, fmt.Errorf("%w: request is required", ErrIdentityRejected)
	}
	tenantID := header(request, r.tenantHeader)
	userID := header(request, r.userHeader)
	approverID := header(request, r.approverHeader)
	signature := header(request, r.signatureHeader)
	timestamp := header(request, r.timestampHeader)
	if tenantID == "" || userID == "" || signature == "" || timestamp == "" {
		return Identity{}, fmt.Errorf("%w: trusted-proxy identity headers are incomplete", ErrIdentityRejected)
	}
	observed, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: invalid proxy timestamp", ErrIdentityRejected)
	}
	at := time.Unix(observed, 0).UTC()
	if skew := r.clock().UTC().Sub(at); skew > r.maxSkew || skew < -r.maxSkew {
		return Identity{}, fmt.Errorf("%w: proxy timestamp is outside the allowed window", ErrIdentityRejected)
	}
	expected := r.sign(tenantID, userID, approverID, timestamp)
	provided, err := hex.DecodeString(strings.TrimSpace(signature))
	if err != nil || !hmac.Equal(expected, provided) {
		return Identity{}, fmt.Errorf("%w: proxy signature is invalid", ErrIdentityRejected)
	}
	return newIdentity(tenantID, userID, approverID)
}

func (r *TrustedProxyResolver) sign(tenantID, userID, approverID, timestamp string) []byte {
	mac := hmac.New(sha256.New, r.secret)
	mac.Write([]byte(strings.Join([]string{tenantID, userID, approverID, timestamp}, "\n")))
	return mac.Sum(nil)
}

// Sign produces the hex signature a trusted proxy must send for these values.
// It is exported so deployment tooling and tests can mirror the contract.
func (r *TrustedProxyResolver) Sign(tenantID, userID, approverID string, at time.Time) (string, string) {
	timestamp := strconv.FormatInt(at.UTC().Unix(), 10)
	return hex.EncodeToString(r.sign(tenantID, userID, approverID, timestamp)), timestamp
}

func defaultHeader(value, fallback string) string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return fallback
}

func header(request *http.Request, name string) string {
	if request == nil {
		return ""
	}
	return strings.TrimSpace(request.Header.Get(name))
}
