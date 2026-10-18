package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

func newProxyResolver(t *testing.T, secret string) *TrustedProxyResolver {
	t.Helper()
	resolver, err := NewTrustedProxyResolver(TrustedProxyConfig{
		Secret: []byte(secret),
		Clock:  func() time.Time { return time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("NewTrustedProxyResolver() error = %v", err)
	}
	return resolver
}

func signedProxyRequest(resolver *TrustedProxyResolver, tenant, user, approver string, at time.Time) *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	request.Header.Set("X-Nightshift-Tenant", tenant)
	request.Header.Set("X-Nightshift-User", user)
	request.Header.Set("X-Nightshift-Approver", approver)
	signature, timestamp := resolver.Sign(tenant, user, approver, at)
	request.Header.Set("X-Nightshift-Proxy-Signature", signature)
	request.Header.Set("X-Nightshift-Proxy-Timestamp", timestamp)
	return request
}

func TestTrustedProxyResolverVerifiesSignature(t *testing.T) {
	resolver := newProxyResolver(t, "shared-secret")
	request := signedProxyRequest(resolver, "tenant-a", "user-a", "approver-a", time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC))
	value, err := resolver.Resolve(context.Background(), request)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if value.Scope != (identity.Scope{TenantID: "tenant-a", UserID: "user-a"}) || value.ApproverID != "approver-a" {
		t.Fatalf("identity = %#v", value)
	}
}

func TestTrustedProxyResolverRejectsTamperingAndStale(t *testing.T) {
	resolver := newProxyResolver(t, "shared-secret")
	valid := signedProxyRequest(resolver, "tenant-a", "user-a", "user-a", time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC))
	if _, err := resolver.Resolve(context.Background(), valid); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	tampered := signedProxyRequest(resolver, "tenant-a", "user-a", "user-a", time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC))
	tampered.Header.Set("X-Nightshift-User", "attacker-user")
	if _, err := resolver.Resolve(context.Background(), tampered); err == nil {
		t.Fatal("tampered user header was accepted")
	}

	unsigned := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	unsigned.Header.Set("X-Nightshift-Tenant", "attacker-tenant")
	unsigned.Header.Set("X-Nightshift-User", "attacker-user")
	if _, err := resolver.Resolve(context.Background(), unsigned); err == nil {
		t.Fatal("client-supplied headers without a signature were accepted")
	}

	stale := signedProxyRequest(resolver, "tenant-a", "user-a", "user-a", time.Date(2026, 10, 17, 11, 0, 0, 0, time.UTC))
	if _, err := resolver.Resolve(context.Background(), stale); err == nil {
		t.Fatal("stale proxy timestamp was accepted")
	}
}

func TestMiddlewareUsesResolverAsAuthority(t *testing.T) {
	resolver := newProxyResolver(t, "shared-secret")
	middleware, err := New(Config{
		Resolver:              resolver,
		RequireAuthentication: true,
		Scope:                 identity.Scope{TenantID: "tenant-install", UserID: "user-install"},
		ApproverID:            "user-install",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	var got Identity
	handler := middleware.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		got, _ = IdentityFromRequest(request)
		writer.WriteHeader(http.StatusNoContent)
	}))

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned request status = %d, want 401", unauthorized.Code)
	}

	request := signedProxyRequest(resolver, "tenant-verified", "user-verified", "user-verified", time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC))
	authorized := httptest.NewRecorder()
	handler.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusNoContent {
		t.Fatalf("signed request status = %d, want 204", authorized.Code)
	}
	if got.Scope != (identity.Scope{TenantID: "tenant-verified", UserID: "user-verified"}) {
		t.Fatalf("resolved identity = %#v", got)
	}
}
