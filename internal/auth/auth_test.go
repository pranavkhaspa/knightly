package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

func testConfig() Config {
	return Config{
		Token:      "token-for-tests",
		Scope:      identity.Scope{TenantID: "tenant-a", UserID: "user-a"},
		ApproverID: "approver-a",
	}
}

func TestWrapAuthenticatesBearerAndInstallsTrustedIdentity(t *testing.T) {
	middleware, err := New(testConfig())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	var got Identity
	handler := middleware.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var err error
		got, err = IdentityFromRequest(request)
		if err != nil {
			t.Fatalf("IdentityFromRequest() error = %v", err)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	request.Header.Set("Authorization", "Bearer token-for-tests")
	request.Header.Set("X-Tenant-ID", "attacker-tenant")
	request.Header.Set("X-User-ID", "attacker-user")
	request.Header.Set("X-Approver-ID", "attacker-approver")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
	if got.Scope != (identity.Scope{TenantID: "tenant-a", UserID: "user-a"}) || got.ApproverID != "approver-a" {
		t.Fatalf("trusted identity = %#v", got)
	}
	for header, want := range map[string]string{
		"Cache-Control":          "no-store",
		"Permissions-Policy":     "camera=(), geolocation=(), microphone=()",
		"Referrer-Policy":        "no-referrer",
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
	} {
		if response.Header().Get(header) != want {
			t.Fatalf("%s = %q, want %q", header, response.Header().Get(header), want)
		}
	}
}

func TestWrapRejectsInvalidBearer(t *testing.T) {
	middleware, err := New(testConfig())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	handler := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("protected handler ran with an invalid bearer token")
	}))

	request := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	request.Header.Set("Authorization", "Bearer wrong-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
	if response.Header().Get("WWW-Authenticate") != `Bearer realm="nightshift"` {
		t.Fatalf("WWW-Authenticate = %q", response.Header().Get("WWW-Authenticate"))
	}
}

func TestTokenPolicyDoesNotAcceptEmptyBasicCredentials(t *testing.T) {
	middleware, err := New(testConfig())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	handler := middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("protected handler ran with empty Basic credentials")
	}))

	request := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	request.SetBasicAuth("", "")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
}

func TestWrapAcceptsBasicCredentials(t *testing.T) {
	config := testConfig()
	config.Token = ""
	config.Username = "operator"
	config.Password = "password-for-tests"
	middleware, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	handler := middleware.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if _, err := ScopeFromRequest(request); err != nil {
			t.Fatalf("ScopeFromRequest() error = %v", err)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	request.SetBasicAuth("operator", "password-for-tests")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
}

func TestWrapCanExposeOnlyExplicitPublicPaths(t *testing.T) {
	config := testConfig()
	config.PublicPaths = []string{"/health/live"}
	middleware, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	handler := middleware.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health/live" {
			if _, err := IdentityFromRequest(request); err == nil {
				t.Fatal("public health request unexpectedly received an authenticated identity")
			}
		}
		writer.WriteHeader(http.StatusNoContent)
	}))

	publicRequest := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	publicResponse := httptest.NewRecorder()
	handler.ServeHTTP(publicResponse, publicRequest)
	if publicResponse.Code != http.StatusNoContent {
		t.Fatalf("public status = %d, want 204", publicResponse.Code)
	}

	protectedRequest := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	protectedResponse := httptest.NewRecorder()
	handler.ServeHTTP(protectedResponse, protectedRequest)
	if protectedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("protected status = %d, want 401", protectedResponse.Code)
	}
}

func TestApproverFromRequestIgnoresClientSuppliedApprovalAuthority(t *testing.T) {
	middleware, err := New(testConfig())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	handler := middleware.Wrap(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		approver, err := ApproverFromRequest(request)
		if err != nil {
			t.Fatalf("ApproverFromRequest() error = %v", err)
		}
		if approver != "approver-a" {
			t.Fatalf("approver = %q, want trusted approver", approver)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodPost, "/api/v1/actions/action-1/approve", strings.NewReader(`{"approved_by":"attacker","note":"approve it"}`))
	request.Header.Set("Authorization", "Bearer token-for-tests")
	request.Header.Set("X-Approver-ID", "attacker")
	request.Header.Set("Content-Type", "application/json")
	request.Body = http.NoBody
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", response.Code)
	}
}

func TestNewRequiresCredentialsWhenRequested(t *testing.T) {
	config := testConfig()
	config.Token = ""
	config.RequireAuthentication = true
	if _, err := New(config); err == nil {
		t.Fatal("New() accepted required authentication without credentials")
	}
}

func TestNewRejectsInvalidTrustedIdentity(t *testing.T) {
	config := testConfig()
	config.Scope.UserID = ""
	if _, err := New(config); err == nil {
		t.Fatal("New() accepted an invalid trusted scope")
	}
}
