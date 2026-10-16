package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
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
