package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/auth"
	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/observability"
	"github.com/pranavkhaspa/knightly/internal/profile"
	"github.com/pranavkhaspa/knightly/internal/scheduler"
)

type profileRouteRepository struct {
	saved profile.Profile
}

func (r *profileRouteRepository) Save(_ context.Context, value profile.Profile) error {
	r.saved = value
	return nil
}

func (r *profileRouteRepository) Get(_ context.Context, scope identity.Scope) (profile.Profile, error) {
	if r.saved.UserID == "" {
		return profile.Profile{}, profile.ErrProfileNotFound
	}
	if r.saved.TenantID != scope.TenantID || r.saved.UserID != scope.UserID {
		return profile.Profile{}, profile.ErrProfileNotFound
	}
	return r.saved, nil
}

func TestProfileRouteUsesConfiguredScopeInsteadOfRequestIdentity(t *testing.T) {
	repository := &profileRouteRepository{}
	app := &application{
		scope:          identity.Scope{TenantID: "tenant_configured", UserID: "user_configured"},
		profiles:       profile.NewService(repository),
		initialization: stateReady,
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/profile", strings.NewReader(`{
		"tenant_id":"tenant_attacker","user_id":"user_attacker","display_name":"Configured user"
	}`))
	response := httptest.NewRecorder()
	app.profileSave(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("profile save status = %d, body=%s", response.Code, response.Body.String())
	}
	if repository.saved.TenantID != "tenant_configured" || repository.saved.UserID != "user_configured" {
		t.Fatalf("saved profile scope = %q/%q", repository.saved.TenantID, repository.saved.UserID)
	}
	var returned profile.Profile
	if err := json.Unmarshal(response.Body.Bytes(), &returned); err != nil {
		t.Fatalf("decode profile response: %v", err)
	}
	if returned.TenantID != repository.saved.TenantID || returned.UserID != repository.saved.UserID {
		t.Fatalf("returned profile scope = %q/%q", returned.TenantID, returned.UserID)
	}
}

func TestVerifiedIdentityOverridesInstallationScope(t *testing.T) {
	resolver, err := auth.NewTrustedProxyResolver(auth.TrustedProxyConfig{Secret: []byte("proxy-secret")})
	if err != nil {
		t.Fatalf("NewTrustedProxyResolver() error = %v", err)
	}
	policy, err := auth.New(auth.Config{
		Resolver: resolver, RequireAuthentication: true,
		Scope:      identity.Scope{TenantID: "tenant_install", UserID: "user_install"},
		ApproverID: "user_install",
	})
	if err != nil {
		t.Fatalf("auth.New() error = %v", err)
	}

	repository := &profileRouteRepository{}
	app := &application{
		scope:          identity.Scope{TenantID: "tenant_install", UserID: "user_install"},
		profiles:       profile.NewService(repository),
		initialization: stateReady,
	}
	handler := policy.Wrap(app.routes())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/profile", strings.NewReader(`{
		"tenant_id":"tenant_attacker","user_id":"user_attacker","display_name":"Verified user"
	}`))
	signature, timestamp := resolver.Sign("tenant_verified", "user_verified", "user_verified", time.Now().UTC())
	request.Header.Set("X-Nightshift-Tenant", "tenant_verified")
	request.Header.Set("X-Nightshift-User", "user_verified")
	request.Header.Set("X-Nightshift-Approver", "user_verified")
	request.Header.Set("X-Nightshift-Proxy-Signature", signature)
	request.Header.Set("X-Nightshift-Proxy-Timestamp", timestamp)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("profile save status = %d, body=%s", response.Code, response.Body.String())
	}
	if repository.saved.TenantID != "tenant_verified" || repository.saved.UserID != "user_verified" {
		t.Fatalf("saved profile scope = %q/%q, want verified identity", repository.saved.TenantID, repository.saved.UserID)
	}

	unsigned := httptest.NewRecorder()
	handler.ServeHTTP(unsigned, httptest.NewRequest(http.MethodPost, "/api/v1/profile", strings.NewReader(`{"display_name":"x"}`)))
	if unsigned.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned request status = %d, want 401", unsigned.Code)
	}
}

func TestTwoVerifiedUsersAreScopeIsolated(t *testing.T) {
	resolver, err := auth.NewTrustedProxyResolver(auth.TrustedProxyConfig{Secret: []byte("proxy-secret")})
	if err != nil {
		t.Fatalf("NewTrustedProxyResolver() error = %v", err)
	}
	policy, err := auth.New(auth.Config{Resolver: resolver, RequireAuthentication: true,
		Scope: identity.Scope{TenantID: "tenant_install", UserID: "user_install"}, ApproverID: "user_install"})
	if err != nil {
		t.Fatalf("auth.New() error = %v", err)
	}
	repository := &profileRouteRepository{}
	app := &application{
		scope:          identity.Scope{TenantID: "tenant_install", UserID: "user_install"},
		profiles:       profile.NewService(repository),
		initialization: stateReady,
	}
	handler := policy.Wrap(app.routes())

	save := func(tenant, user, name string) int {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/profile", strings.NewReader(`{"display_name":"`+name+`"}`))
		signature, timestamp := resolver.Sign(tenant, user, user, time.Now().UTC())
		request.Header.Set("X-Nightshift-Tenant", tenant)
		request.Header.Set("X-Nightshift-User", user)
		request.Header.Set("X-Nightshift-Approver", user)
		request.Header.Set("X-Nightshift-Proxy-Signature", signature)
		request.Header.Set("X-Nightshift-Proxy-Timestamp", timestamp)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code
	}
	read := func(tenant, user string) int {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/profile", nil)
		signature, timestamp := resolver.Sign(tenant, user, user, time.Now().UTC())
		request.Header.Set("X-Nightshift-Tenant", tenant)
		request.Header.Set("X-Nightshift-User", user)
		request.Header.Set("X-Nightshift-Approver", user)
		request.Header.Set("X-Nightshift-Proxy-Signature", signature)
		request.Header.Set("X-Nightshift-Proxy-Timestamp", timestamp)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code
	}

	if code := save("tenant-a", "user-a", "Alice"); code != http.StatusOK {
		t.Fatalf("save as user-a status = %d", code)
	}
	if repository.saved.UserID != "user-a" {
		t.Fatalf("saved user = %q", repository.saved.UserID)
	}
	if code := read("tenant-b", "user-b"); code != http.StatusNotFound {
		t.Fatalf("read as user-b status = %d, want 404", code)
	}
}

func TestRunAndSchedulerControlRoutesUseScheduler(t *testing.T) {
	runnerStarted := make(chan struct{}, 1)
	schedule, err := scheduler.New(scheduler.Config{Interval: time.Hour}, scheduler.RunFunc(func(context.Context) error {
		runnerStarted <- struct{}{}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := schedule.Start(); err != nil {
		t.Fatal(err)
	}
	defer schedule.Stop(context.Background())
	app := &application{initialization: stateReady, schedule: schedule}
	handler := app.routes()

	runResponse := httptest.NewRecorder()
	handler.ServeHTTP(runResponse, httptest.NewRequest(http.MethodPost, "/api/v1/runs", nil))
	if runResponse.Code != http.StatusAccepted {
		t.Fatalf("run status = %d, body=%s", runResponse.Code, runResponse.Body.String())
	}
	select {
	case <-runnerStarted:
	case <-time.After(time.Second):
		t.Fatal("manual run was not dispatched to the scheduler")
	}

	pauseResponse := httptest.NewRecorder()
	handler.ServeHTTP(pauseResponse, httptest.NewRequest(http.MethodPost, "/api/v1/scheduler/pause", nil))
	if pauseResponse.Code != http.StatusOK || !schedule.Status().Paused {
		t.Fatalf("pause status = %d scheduler=%+v", pauseResponse.Code, schedule.Status())
	}
	resumeResponse := httptest.NewRecorder()
	handler.ServeHTTP(resumeResponse, httptest.NewRequest(http.MethodPost, "/api/v1/scheduler/resume", nil))
	if resumeResponse.Code != http.StatusOK || schedule.Status().Paused {
		t.Fatalf("resume status = %d scheduler=%+v", resumeResponse.Code, schedule.Status())
	}
}

func TestMetricsEndpointRendersPrometheusText(t *testing.T) {
	metrics := observability.NewMetrics()
	metrics.RecordApproval("approved")
	app := &application{metrics: metrics, initialization: stateReady}
	recorder := httptest.NewRecorder()
	app.routes().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", recorder.Code)
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.Contains(contentType, "text/plain") {
		t.Fatalf("metrics content type = %q", contentType)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "nightshift_approvals_total{result=\"approved\"} 1") {
		t.Fatalf("metrics body missing approvals counter:\n%s", body)
	}
}
