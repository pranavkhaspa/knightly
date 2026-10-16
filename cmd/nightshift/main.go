package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/pranavkhaspa/knightly/internal/config"
	"github.com/pranavkhaspa/knightly/internal/falkor"
	"github.com/pranavkhaspa/knightly/internal/web"
)

type application struct {
	config    config.Config
	graph     *falkor.Client
	startedAt time.Time
}

type statusResponse struct {
	Service      string        `json:"service"`
	Version      string        `json:"version"`
	Environment  string        `json:"environment"`
	InstanceID   string        `json:"instance_id"`
	Uptime       string        `json:"uptime"`
	GoVersion    string        `json:"go_version"`
	Architecture string        `json:"architecture"`
	Graph        falkor.Status `json:"graph"`
	Actions      bool          `json:"actions_enabled"`
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	graph, err := falkor.New(cfg.FalkorURL, cfg.ControlGraph, cfg.UserGraph, cfg.QueryTimeoutMillis)
	if err != nil {
		log.Fatalf("FalkorDB configuration error: %v", err)
	}
	defer graph.Close()

	app := &application{config: cfg, graph: graph, startedAt: time.Now()}

	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Keep liveness independent from FalkorDB. A remote graph may be slow or
	// temporarily offline, but the daemon should still expose diagnostics.
	go func() {
		if err := graph.Initialize(); err != nil {
			log.Printf("event=graph_initialize_failed error=%q; HTTP health remains available", err)
		} else {
			log.Printf("event=graph_initialized control_graph=%q user_graph=%q", cfg.ControlGraph, cfg.UserGraph)
		}
	}()

	stopContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-stopContext.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			log.Printf("event=shutdown_failed error=%q", err)
		}
	}()

	log.Printf("event=server_started listen=%q instance_id=%q", cfg.Listen, cfg.InstanceID)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func (a *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", a.live)
	mux.HandleFunc("GET /health/ready", a.ready)
	mux.HandleFunc("GET /api/v1/status", a.status)
	mux.Handle("GET /", web.Handler())
	return requestID(mux)
}

func (a *application) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *application) ready(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	status := a.graph.Status(ctx)
	if !status.Available {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"status": "not_ready",
			"graph":  status,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ready",
		"graph":  status,
	})
}

func (a *application) status(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	writeJSON(w, http.StatusOK, statusResponse{
		Service:      "nightshift",
		Version:      "0.1-foundation",
		Environment:  a.config.Environment,
		InstanceID:   a.config.InstanceID,
		Uptime:       time.Since(a.startedAt).Round(time.Second).String(),
		GoVersion:    runtime.Version(),
		Architecture: runtime.GOOS + "/" + runtime.GOARCH,
		Graph:        a.graph.Status(ctx),
		Actions:      a.config.ActionsEnabled,
	})
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requestID := request.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = fmt.Sprintf("req_%d", time.Now().UnixNano())
		}
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, request)
	})
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("event=response_encode_failed error=%q", err)
	}
}
