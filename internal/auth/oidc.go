package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// OIDCConfig configures bearer-token verification against a provider JWKS.
type OIDCConfig struct {
	Issuer        string
	Audience      string
	JWKSURL       string
	TenantClaim   string
	UserClaim     string
	ApproverClaim string
	Leeway        time.Duration
	CacheTTL      time.Duration
	Client        *http.Client
	Clock         func() time.Time
	JWKS          *JWKS
}

// JWKS is a parsed JSON Web Key Set.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// JWK is a single JSON Web Key. RSA (`RSA`) and EC (`EC`) keys are supported.
type JWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// OIDCResolver verifies an RS256/ES256 bearer token and maps verified claims to
// a trusted identity. It never reads tenant or approver values from anywhere but
// the signed token.
type OIDCResolver struct {
	issuer        string
	audience      string
	jwksURL       string
	tenantClaim   string
	userClaim     string
	approverClaim string
	leeway        time.Duration
	cacheTTL      time.Duration
	client        *http.Client
	clock         func() time.Time

	mu        sync.Mutex
	keys      map[string]crypto.PublicKey
	fetchedAt time.Time
	static    map[string]crypto.PublicKey
}

var _ IdentityResolver = (*OIDCResolver)(nil)

func NewOIDCResolver(cfg OIDCConfig) (*OIDCResolver, error) {
	cfg.Issuer = strings.TrimSpace(cfg.Issuer)
	if cfg.Issuer == "" {
		return nil, fmt.Errorf("%w: OIDC issuer is required", ErrInvalidConfig)
	}
	if cfg.JWKS == nil && strings.TrimSpace(cfg.JWKSURL) == "" {
		return nil, fmt.Errorf("%w: a JWKS document or URL is required", ErrInvalidConfig)
	}
	if cfg.TenantClaim == "" {
		cfg.TenantClaim = "tenant"
	}
	if cfg.UserClaim == "" {
		cfg.UserClaim = "sub"
	}
	if cfg.Leeway <= 0 {
		cfg.Leeway = 60 * time.Second
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 15 * time.Minute
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	clock := cfg.Clock
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	resolver := &OIDCResolver{
		issuer: cfg.Issuer, audience: cfg.Audience, jwksURL: strings.TrimSpace(cfg.JWKSURL),
		tenantClaim: cfg.TenantClaim, userClaim: cfg.UserClaim, approverClaim: cfg.ApproverClaim,
		leeway: cfg.Leeway, cacheTTL: cfg.CacheTTL, client: client, clock: clock,
	}
	if cfg.JWKS != nil {
		keys, err := parseJWKS(*cfg.JWKS)
		if err != nil {
			return nil, err
		}
		resolver.static = keys
	}
	return resolver, nil
}

func (r *OIDCResolver) Resolve(ctx context.Context, request *http.Request) (Identity, error) {
	token, ok := bearer(request.Header.Get("Authorization"))
	if !ok {
		return Identity{}, fmt.Errorf("%w: bearer token is required", ErrIdentityRejected)
	}
	header, claims, signingInput, signature, err := splitToken(token)
	if err != nil {
		return Identity{}, err
	}
	algorithm, err := headerAlgorithm(header)
	if err != nil {
		return Identity{}, err
	}
	key, err := r.keyFor(ctx, header.Kid, algorithm)
	if err != nil {
		return Identity{}, err
	}
	if err := verifySignature(algorithm, key, signingInput, signature); err != nil {
		return Identity{}, err
	}
	if err := r.validateClaims(claims); err != nil {
		return Identity{}, err
	}
	tenantID := claimString(claims, r.tenantClaim)
	userID := claimString(claims, r.userClaim)
	approverID := userID
	if r.approverClaim != "" {
		if value := claimString(claims, r.approverClaim); value != "" {
			approverID = value
		}
	}
	if userID == "" {
		return Identity{}, fmt.Errorf("%w: user claim %q is missing", ErrIdentityRejected, r.userClaim)
	}
	if tenantID == "" {
		tenantID = r.issuer
	}
	return newIdentity(tenantID, userID, approverID)
}

func (r *OIDCResolver) keyFor(ctx context.Context, kid, algorithm string) (crypto.PublicKey, error) {
	if r.static != nil {
		if key, ok := selectKey(r.static, kid, algorithm); ok {
			return key, nil
		}
		return nil, fmt.Errorf("%w: no matching key for kid %q", ErrIdentityRejected, kid)
	}
	if key, ok := r.cachedKey(kid, algorithm); ok {
		return key, nil
	}
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	if key, ok := r.cachedKey(kid, algorithm); ok {
		return key, nil
	}
	return nil, fmt.Errorf("%w: no matching key for kid %q", ErrIdentityRejected, kid)
}

func (r *OIDCResolver) cachedKey(kid, algorithm string) (crypto.PublicKey, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.keys == nil || r.clock().UTC().Sub(r.fetchedAt) > r.cacheTTL {
		return nil, false
	}
	return selectKey(r.keys, kid, algorithm)
}

func (r *OIDCResolver) refresh(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.jwksURL, nil)
	if err != nil {
		return fmt.Errorf("%w: build JWKS request: %v", ErrIdentityRejected, err)
	}
	response, err := r.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: fetch JWKS: %v", ErrIdentityRejected, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: JWKS endpoint returned HTTP %d", ErrIdentityRejected, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%w: read JWKS: %v", ErrIdentityRejected, err)
	}
	var document JWKS
	if err := json.Unmarshal(body, &document); err != nil {
		return fmt.Errorf("%w: decode JWKS: %v", ErrIdentityRejected, err)
	}
	keys, err := parseJWKS(document)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.keys = keys
	r.fetchedAt = r.clock().UTC()
	r.mu.Unlock()
	return nil
}

func (r *OIDCResolver) validateClaims(claims map[string]interface{}) error {
	if issuer := claimString(claims, "iss"); issuer != r.issuer {
		return fmt.Errorf("%w: unexpected issuer", ErrIdentityRejected)
	}
	if r.audience != "" && !claimContains(claims, "aud", r.audience) {
		return fmt.Errorf("%w: unexpected audience", ErrIdentityRejected)
	}
	now := r.clock().UTC()
	if expiry, ok := claimNumber(claims, "exp"); ok {
		if now.After(time.Unix(int64(expiry), 0).UTC().Add(r.leeway)) {
			return fmt.Errorf("%w: token is expired", ErrIdentityRejected)
		}
	}
	if notBefore, ok := claimNumber(claims, "nbf"); ok {
		if now.Before(time.Unix(int64(notBefore), 0).UTC().Add(-r.leeway)) {
			return fmt.Errorf("%w: token is not yet valid", ErrIdentityRejected)
		}
	}
	return nil
}

type tokenHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

func splitToken(token string) (tokenHeader, map[string]interface{}, []byte, []byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return tokenHeader{}, nil, nil, nil, fmt.Errorf("%w: token is not a compact JWT", ErrIdentityRejected)
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return tokenHeader{}, nil, nil, nil, fmt.Errorf("%w: decode token header", ErrIdentityRejected)
	}
	var header tokenHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return tokenHeader{}, nil, nil, nil, fmt.Errorf("%w: parse token header", ErrIdentityRejected)
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return tokenHeader{}, nil, nil, nil, fmt.Errorf("%w: decode token claims", ErrIdentityRejected)
	}
	var claims map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return tokenHeader{}, nil, nil, nil, fmt.Errorf("%w: parse token claims", ErrIdentityRejected)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return tokenHeader{}, nil, nil, nil, fmt.Errorf("%w: decode token signature", ErrIdentityRejected)
	}
	return header, claims, []byte(parts[0] + "." + parts[1]), signature, nil
}

func headerAlgorithm(header tokenHeader) (string, error) {
	algorithm := strings.ToUpper(strings.TrimSpace(header.Alg))
	switch algorithm {
	case "RS256", "RS384", "RS512", "ES256", "ES384", "ES512":
		return algorithm, nil
	default:
		return "", fmt.Errorf("%w: unsupported JWT algorithm %q", ErrIdentityRejected, header.Alg)
	}
}

func verifySignature(algorithm string, key crypto.PublicKey, signingInput, signature []byte) error {
	switch algorithm {
	case "RS256", "RS384", "RS512":
		public, ok := key.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("%w: token algorithm does not match key type", ErrIdentityRejected)
		}
		hash, digest := hashFor(algorithm, signingInput)
		if err := rsa.VerifyPKCS1v15(public, hash, digest, signature); err != nil {
			return fmt.Errorf("%w: signature verification failed", ErrIdentityRejected)
		}
		return nil
	case "ES256", "ES384", "ES512":
		public, ok := key.(*ecdsa.PublicKey)
		if !ok {
			return fmt.Errorf("%w: token algorithm does not match key type", ErrIdentityRejected)
		}
		_, digest := hashFor(algorithm, signingInput)
		size := (public.Curve.Params().BitSize + 7) / 8
		if len(signature) != 2*size {
			return fmt.Errorf("%w: malformed ECDSA signature", ErrIdentityRejected)
		}
		r := new(big.Int).SetBytes(signature[:size])
		s := new(big.Int).SetBytes(signature[size:])
		if !ecdsa.Verify(public, digest, r, s) {
			return fmt.Errorf("%w: signature verification failed", ErrIdentityRejected)
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported JWT algorithm %q", ErrIdentityRejected, algorithm)
	}
}

func hashFor(algorithm string, signingInput []byte) (crypto.Hash, []byte) {
	switch algorithm {
	case "RS256", "ES256":
		sum := sha256.Sum256(signingInput)
		return crypto.SHA256, sum[:]
	case "RS384", "ES384":
		sum := sha512.Sum384(signingInput)
		return crypto.SHA384, sum[:]
	default:
		sum := sha512.Sum512(signingInput)
		return crypto.SHA512, sum[:]
	}
}

func parseJWKS(document JWKS) (map[string]crypto.PublicKey, error) {
	keys := make(map[string]crypto.PublicKey, len(document.Keys))
	for index, key := range document.Keys {
		public, err := publicKeyFromJWK(key)
		if err != nil {
			return nil, err
		}
		id := key.Kid
		if id == "" {
			id = fmt.Sprintf("#%d", index)
		}
		keys[id] = public
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: JWKS contains no usable keys", ErrIdentityRejected)
	}
	return keys, nil
}

func publicKeyFromJWK(key JWK) (crypto.PublicKey, error) {
	switch strings.ToUpper(strings.TrimSpace(key.Kty)) {
	case "RSA":
		modulus, err := decodeBigInt(key.N)
		if err != nil {
			return nil, err
		}
		exponent, err := decodeBigInt(key.E)
		if err != nil {
			return nil, err
		}
		if !exponent.IsInt64() || exponent.Int64() <= 0 || exponent.Int64() > 1<<31 {
			return nil, fmt.Errorf("%w: invalid RSA exponent", ErrIdentityRejected)
		}
		return &rsa.PublicKey{N: modulus, E: int(exponent.Int64())}, nil
	case "EC":
		curve, err := curveFor(key.Crv)
		if err != nil {
			return nil, err
		}
		x, err := decodeBigInt(key.X)
		if err != nil {
			return nil, err
		}
		y, err := decodeBigInt(key.Y)
		if err != nil {
			return nil, err
		}
		if !curve.IsOnCurve(x, y) {
			return nil, fmt.Errorf("%w: EC key is not on curve", ErrIdentityRejected)
		}
		return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported JWK key type %q", ErrIdentityRejected, key.Kty)
	}
}

func curveFor(name string) (elliptic.Curve, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("%w: unsupported EC curve %q", ErrIdentityRejected, name)
	}
}

func decodeBigInt(value string) (*big.Int, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("%w: decode JWK integer", ErrIdentityRejected)
	}
	return new(big.Int).SetBytes(decoded), nil
}

func selectKey(keys map[string]crypto.PublicKey, kid, algorithm string) (crypto.PublicKey, bool) {
	if kid != "" {
		key, ok := keys[kid]
		return key, ok
	}
	if len(keys) == 1 {
		for _, key := range keys {
			return key, true
		}
	}
	return nil, false
}

func claimString(claims map[string]interface{}, name string) string {
	value, ok := claims[name]
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func claimNumber(claims map[string]interface{}, name string) (float64, bool) {
	value, ok := claims[name]
	if !ok {
		return 0, false
	}
	number, ok := value.(float64)
	return number, ok
}

func claimContains(claims map[string]interface{}, name, expected string) bool {
	value, ok := claims[name]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case string:
		return typed == expected
	case []interface{}:
		for _, entry := range typed {
			if text, ok := entry.(string); ok && text == expected {
				return true
			}
		}
	}
	return false
}
