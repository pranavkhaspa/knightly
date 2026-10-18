package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

var oidcClock = time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

func rsaJWKS(t *testing.T, key *rsa.PrivateKey, kid string) JWKS {
	t.Helper()
	exponent := big.NewInt(int64(key.PublicKey.E)).Bytes()
	return JWKS{Keys: []JWK{{
		Kty: "RSA", Kid: kid, Alg: "RS256", Use: "sig",
		N: base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
		E: base64.RawURLEncoding.EncodeToString(exponent),
	}}}
}

func signJWT(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]interface{}) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": kid, "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func baseClaims() map[string]interface{} {
	return map[string]interface{}{
		"iss": "https://issuer.example", "aud": "nightshift",
		"sub": "user-a", "tenant": "tenant-a",
		"exp": float64(oidcClock.Add(time.Hour).Unix()), "nbf": float64(oidcClock.Add(-time.Minute).Unix()),
	}
}

func newOIDCResolver(t *testing.T, jwks JWKS) *OIDCResolver {
	t.Helper()
	resolver, err := NewOIDCResolver(OIDCConfig{
		Issuer: "https://issuer.example", Audience: "nightshift", JWKS: &jwks,
		Clock: func() time.Time { return oidcClock },
	})
	if err != nil {
		t.Fatalf("NewOIDCResolver() error = %v", err)
	}
	return resolver
}

func bearerRequest(token string) *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	return request
}

func TestOIDCResolverVerifiesRS256AndMapsClaims(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	resolver := newOIDCResolver(t, rsaJWKS(t, key, "key-1"))
	token := signJWT(t, key, "key-1", baseClaims())
	value, err := resolver.Resolve(context.Background(), bearerRequest(token))
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if value.Scope != (identity.Scope{TenantID: "tenant-a", UserID: "user-a"}) || value.ApproverID != "user-a" {
		t.Fatalf("identity = %#v", value)
	}
}

func TestOIDCResolverRejectsBadClaimsAndSignatures(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	resolver := newOIDCResolver(t, rsaJWKS(t, key, "key-1"))

	expired := baseClaims()
	expired["exp"] = float64(oidcClock.Add(-2 * time.Hour).Unix())
	if _, err := resolver.Resolve(context.Background(), bearerRequest(signJWT(t, key, "key-1", expired))); err == nil {
		t.Fatal("expired token was accepted")
	}

	wrongAudience := baseClaims()
	wrongAudience["aud"] = "other-service"
	if _, err := resolver.Resolve(context.Background(), bearerRequest(signJWT(t, key, "key-1", wrongAudience))); err == nil {
		t.Fatal("token with wrong audience was accepted")
	}

	wrongIssuer := baseClaims()
	wrongIssuer["iss"] = "https://attacker.example"
	if _, err := resolver.Resolve(context.Background(), bearerRequest(signJWT(t, key, "key-1", wrongIssuer))); err == nil {
		t.Fatal("token with wrong issuer was accepted")
	}

	if _, err := resolver.Resolve(context.Background(), bearerRequest(signJWT(t, other, "key-1", baseClaims()))); err == nil {
		t.Fatal("token signed by an unknown key was accepted")
	}

	if _, err := resolver.Resolve(context.Background(), httptest.NewRequest(http.MethodGet, "/", nil)); err == nil {
		t.Fatal("request without a bearer token was accepted")
	}
}

func TestOIDCResolverFetchesJWKSOverHTTP(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	document := rsaJWKS(t, key, "key-1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(document)
	}))
	defer server.Close()

	resolver, err := NewOIDCResolver(OIDCConfig{
		Issuer: "https://issuer.example", Audience: "nightshift", JWKSURL: server.URL,
		Clock: func() time.Time { return oidcClock },
	})
	if err != nil {
		t.Fatalf("NewOIDCResolver() error = %v", err)
	}
	token := signJWT(t, key, "key-1", baseClaims())
	value, err := resolver.Resolve(context.Background(), bearerRequest(token))
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if value.Scope.UserID != "user-a" {
		t.Fatalf("identity = %#v", value)
	}
}

func TestOIDCResolverVerifiesES256(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	size := (key.Curve.Params().BitSize + 7) / 8
	jwks := JWKS{Keys: []JWK{{
		Kty: "EC", Kid: "ec-1", Alg: "ES256", Crv: "P-256",
		X: base64.RawURLEncoding.EncodeToString(leftPad(key.X.Bytes(), size)),
		Y: base64.RawURLEncoding.EncodeToString(leftPad(key.Y.Bytes(), size)),
	}}}
	resolver, err := NewOIDCResolver(OIDCConfig{
		Issuer: "https://issuer.example", Audience: "nightshift", JWKS: &jwks,
		Clock: func() time.Time { return oidcClock },
	})
	if err != nil {
		t.Fatalf("NewOIDCResolver() error = %v", err)
	}
	header, _ := json.Marshal(map[string]string{"alg": "ES256", "kid": "ec-1", "typ": "JWT"})
	payload, _ := json.Marshal(baseClaims())
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("ecdsa.Sign: %v", err)
	}
	signature := append(leftPad(r.Bytes(), size), leftPad(s.Bytes(), size)...)
	token := signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)

	value, err := resolver.Resolve(context.Background(), bearerRequest(token))
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if value.Scope.TenantID != "tenant-a" {
		t.Fatalf("identity = %#v", value)
	}
}

func leftPad(value []byte, size int) []byte {
	if len(value) >= size {
		return value
	}
	padded := make([]byte, size)
	copy(padded[size-len(value):], value)
	return padded
}
