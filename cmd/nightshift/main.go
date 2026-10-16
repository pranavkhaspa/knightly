package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pranavkhaspa/knightly/internal/actions"
	"github.com/pranavkhaspa/knightly/internal/agents"
	"github.com/pranavkhaspa/knightly/internal/config"
	"github.com/pranavkhaspa/knightly/internal/falkor"
	"github.com/pranavkhaspa/knightly/internal/ingestion"
	"github.com/pranavkhaspa/knightly/internal/mcp"
	"github.com/pranavkhaspa/knightly/internal/memory"
	"github.com/pranavkhaspa/knightly/internal/recommendation"
	"github.com/pranavkhaspa/knightly/internal/web"
)

type application struct {
	config      config.Config
	graph       *falkor.Client
	startedAt   time.Time
	pipeline    *ingestion.Pipeline
	ranking     *recommendation.Service
	memory      *memory.Service
	coordinator *agents.Coordinator
	actions     *actions.Service
	tools       *mcp.Server
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

	memoryService := memory.NewService(falkor.NewMemoryRepository(graph))
	app := &application{
		config:      cfg,
		graph:       graph,
		startedAt:   time.Now(),
		pipeline:    ingestion.NewPipeline(graph),
		ranking:     recommendation.NewService(graph),
		memory:      memoryService,
		coordinator: agents.NewCoordinator(falkor.NewAgentRepository(graph)),
		actions:     actions.NewService(falkor.NewActionRepository(graph), cfg.ActionsEnabled),
	}
	app.tools = mcp.NewServer(mcp.Dependencies{
		Memory:   memoryService,
		Context:  graph,
		Planner:  graph,
		Feedback: graph,
		Calls:    falkor.NewToolCallRepository(graph),
	})

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
	mux.HandleFunc("GET /api/v1/dashboard", a.dashboard)
	mux.HandleFunc("POST /api/v1/runs", a.runFixture)
	mux.HandleFunc("GET /api/v1/recommendations", a.recommendations)
	mux.HandleFunc("GET /api/v1/graph/path/{id}", a.graphPath)
	mux.HandleFunc("POST /api/v1/recommendations/{id}/feedback", a.feedback)
	mux.HandleFunc("POST /api/v1/recommendations/{id}/plan", a.plan)
	mux.HandleFunc("GET /api/v1/memories", a.memories)
	mux.HandleFunc("POST /api/v1/actions/{id}/approve", a.approveAction)
	mux.HandleFunc("POST /api/v1/actions/{id}/execute", a.executeAction)
	mux.HandleFunc("GET /api/v1/mcp/tools", a.mcpTools)
	mux.HandleFunc("POST /api/v1/mcp", a.mcpInvoke)
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

func (a *application) dashboard(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	dashboard, err := a.graph.Dashboard(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":  "dashboard_unavailable",
			"detail": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, dashboard)
}

type runResponse struct {
	Source          ingestion.SourceConfig          `json:"source"`
	RawItems        int                             `json:"raw_items"`
	Normalized      int                             `json:"normalized"`
	Duplicates      int                             `json:"duplicates"`
	Recommendations []recommendation.Recommendation `json:"recommendations"`
	HandoffCreated  bool                            `json:"handoff_created"`
}

// runFixture is the deterministic end-to-end demo trigger. It exercises the
// real graph writer, graph path query, recommendation persistence, and agent
// handoff without depending on live internet sources.
func (a *application) runFixture(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
	defer cancel()
	result, err := a.pipeline.Run(ctx, ingestion.DefaultFixtureSource())
	if err != nil {
		writeError(w, http.StatusBadGateway, "INGESTION_FAILED", err, true)
		return
	}
	candidates := make([]recommendation.Candidate, 0, len(result.Items))
	for _, item := range result.Items {
		score := 0.5
		for _, technology := range item.Technologies {
			if technology == "falkordb" {
				score += 5
			}
		}
		candidates = append(candidates, recommendation.Candidate{
			ItemID: item.ID, Title: item.Title, Summary: item.Summary,
			URL: item.CanonicalURL, SourceID: item.SourceID,
			ContentHash: item.ContentHash, PublishedAt: item.PublishedAt, Score: score,
		})
	}
	recommendations, err := a.ranking.Recommend(ctx, "user_demo", "project_nightshift", candidates)
	if err != nil {
		writeError(w, http.StatusBadGateway, "RECOMMENDATION_FAILED", err, true)
		return
	}
	handoffCreated := false
	if len(recommendations) > 0 {
		now := time.Now().UTC()
		sourceTaskID := "task_relevance_" + result.Source.ID
		_, taskErr := a.coordinator.CreateTask(ctx, agents.Task{
			ID: sourceTaskID, TenantID: "tenant_demo", UserID: "user_demo",
			ProjectID: "project_nightshift", Agent: agents.RelevanceAgent,
			Status: agents.Running, Priority: 1, CorrelationID: requestIdentifier(request),
			InputRefs: []string{recommendations[0].ID}, CreatedAt: now, UpdatedAt: now,
		})
		if taskErr == nil {
			_, handoffErr := a.coordinator.Handoff(ctx, agents.HandoffRequest{
				ID: "handoff_" + recommendations[0].ID, UserID: "user_demo",
				FromTaskID: sourceTaskID, ToTaskID: "task_planner_" + recommendations[0].ID,
				ToAgent: agents.PlannerAgent, CorrelationID: requestIdentifier(request),
				Notes: "Graph-backed recommendation is ready for a bounded plan.", InputRefs: []string{recommendations[0].ID},
			})
			handoffCreated = handoffErr == nil
		}
	}
	writeJSON(w, http.StatusOK, runResponse{
		Source: result.Source, RawItems: result.RawCount, Normalized: result.Normalized,
		Duplicates: result.Duplicates, Recommendations: recommendations, HandoffCreated: handoffCreated,
	})
}

func (a *application) recommendations(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	items, err := a.graph.ListRecommendations(ctx, "user_demo", request.URL.Query().Get("project_id"), parseLimit(request.URL.Query().Get("limit"), 10))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "RECOMMENDATIONS_UNAVAILABLE", err, true)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"recommendations": items})
}

func (a *application) graphPath(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	explanation, err := a.tools.Explain(ctx, mcp.ExplainRecommendationInput{
		Meta:             mcp.RequestMeta{TenantID: "tenant_demo", UserID: "user_demo", RequestID: requestIdentifier(request)},
		RecommendationID: request.PathValue("id"),
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "GRAPH_PATH_NOT_FOUND", err, false)
		return
	}
	writeJSON(w, http.StatusOK, explanation)
}

type feedbackRequest struct {
	Kind mcp.FeedbackKind `json:"kind"`
	Note string           `json:"note,omitempty"`
}

func (a *application) feedback(w http.ResponseWriter, request *http.Request) {
	var body feedbackRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	result, err := a.tools.Record(request.Context(), mcp.FeedbackInput{
		Meta:             mcp.RequestMeta{TenantID: "tenant_demo", UserID: "user_demo", SessionID: request.Header.Get("X-Session-ID"), RequestID: requestIdentifier(request)},
		RecommendationID: request.PathValue("id"), Kind: body.Kind, Note: body.Note,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "FEEDBACK_FAILED", err, false)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type planRequest struct {
	Goal string `json:"goal"`
}

func (a *application) plan(w http.ResponseWriter, request *http.Request) {
	var body planRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	if strings.TrimSpace(body.Goal) == "" {
		body.Goal = "Prepare a bounded implementation plan from the graph evidence."
	}
	result, err := a.tools.Propose(request.Context(), mcp.ProposePlanInput{
		Meta:      mcp.RequestMeta{TenantID: "tenant_demo", UserID: "user_demo", SessionID: request.Header.Get("X-Session-ID"), RequestID: requestIdentifier(request)},
		ProjectID: "project_nightshift", RecommendationID: request.PathValue("id"), Goal: body.Goal,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "PLAN_FAILED", err, false)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *application) memories(w http.ResponseWriter, request *http.Request) {
	ctx, cancel := falkor.ReadinessContext(request.Context())
	defer cancel()
	items, err := a.memory.Retrieve(ctx, memory.RetrievalQuery{
		TenantID: "tenant_demo", UserID: "user_demo", ProjectID: request.URL.Query().Get("project_id"),
		Limit: parseLimit(request.URL.Query().Get("limit"), 20), MaxBytes: 64 * 1024,
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "MEMORY_UNAVAILABLE", err, true)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"memories": items})
}

type approvalRequest struct {
	ApprovedBy string `json:"approved_by"`
	Note       string `json:"note,omitempty"`
}

func (a *application) approveAction(w http.ResponseWriter, request *http.Request) {
	var body approvalRequest
	if err := decodeJSON(w, request, &body); err != nil {
		return
	}
	approved, err := a.actions.Approve(request.Context(), "user_demo", request.PathValue("id"), actions.Approval{
		ID: requestIdentifier(request), ApprovedBy: body.ApprovedBy, Note: body.Note,
	})
	if err != nil {
		writeError(w, http.StatusConflict, "APPROVAL_FAILED", err, false)
		return
	}
	writeJSON(w, http.StatusOK, approved)
}

func (a *application) executeAction(w http.ResponseWriter, request *http.Request) {
	result, err := a.actions.Execute(request.Context(), "user_demo", request.PathValue("id"), localPlanExecutor{dataDir: a.config.DataDir})
	if err != nil {
		writeError(w, http.StatusConflict, "ACTION_FAILED", err, !a.config.ActionsEnabled)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type mcpEnvelope struct {
	Tool  mcp.ToolName    `json:"tool"`
	Input json.RawMessage `json:"input"`
}

func (a *application) mcpTools(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"tools": []map[string]string{
		{"name": string(mcp.SearchRelevantItems), "description": "Search connected project items through a bounded graph query."},
		{"name": string(mcp.ExplainRecommendation), "description": "Return stored graph path and evidence for a recommendation."},
		{"name": string(mcp.RetrieveMemory), "description": "Retrieve bounded typed memories from the user graph."},
		{"name": string(mcp.ProposePlan), "description": "Persist a planner task and approval-gated local plan proposal."},
		{"name": string(mcp.RecordFeedback), "description": "Persist user feedback and its evolving memory."},
	}})
}

func (a *application) mcpInvoke(w http.ResponseWriter, request *http.Request) {
	var envelope mcpEnvelope
	if err := decodeJSON(w, request, &envelope); err != nil {
		return
	}
	if len(envelope.Input) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", errors.New("input is required"), false)
		return
	}
	meta := mcp.RequestMeta{TenantID: "tenant_demo", UserID: "user_demo", SessionID: request.Header.Get("X-Session-ID"), RequestID: requestIdentifier(request)}
	var input interface{}
	switch envelope.Tool {
	case mcp.SearchRelevantItems:
		value := mcp.SearchRelevantItemsInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	case mcp.ExplainRecommendation:
		value := mcp.ExplainRecommendationInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	case mcp.RetrieveMemory:
		value := mcp.MemoryRetrieveInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	case mcp.ProposePlan:
		value := mcp.ProposePlanInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	case mcp.RecordFeedback:
		value := mcp.FeedbackInput{}
		if err := json.Unmarshal(envelope.Input, &value); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_TOOL_INPUT", err, false)
			return
		}
		value.Meta = meta
		input = value
	default:
		writeError(w, http.StatusNotFound, "UNKNOWN_TOOL", fmt.Errorf("unknown tool %q", envelope.Tool), false)
		return
	}
	result, err := a.tools.Invoke(request.Context(), envelope.Tool, input)
	if err != nil {
		writeError(w, http.StatusBadRequest, "TOOL_FAILED", err, false)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"tool": envelope.Tool, "result": result})
}

type localPlanExecutor struct {
	dataDir string
}

func (e localPlanExecutor) Execute(ctx context.Context, action actions.Action) (actions.ExecutionResult, error) {
	if action.Kind != actions.LocalPlan {
		return actions.ExecutionResult{}, fmt.Errorf("unsupported local executor action kind %q", action.Kind)
	}
	if err := ctx.Err(); err != nil {
		return actions.ExecutionResult{}, err
	}
	planDir := filepath.Join(e.dataDir, "plans")
	if err := os.MkdirAll(planDir, 0o700); err != nil {
		return actions.ExecutionResult{}, err
	}
	goal := action.Payload["goal"]
	recommendationID := action.Payload["recommendation_id"]
	content := fmt.Sprintf("# NightShift implementation plan\n\n- Recommendation: `%s`\n- Project: `%s`\n- Goal: %s\n\nThis plan was generated only after explicit user approval.\n", recommendationID, action.Target, goal)
	path := filepath.Join(planDir, action.ID+".md")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return actions.ExecutionResult{}, err
	}
	return actions.ExecutionResult{Reference: path, Output: "local plan written"}, nil
}

func decodeJSON(w http.ResponseWriter, request *http.Request, destination interface{}) error {
	request.Body = http.MaxBytesReader(w, request.Body, 16*1024)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", err, false)
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		trailing := errors.New("request body must contain one JSON value")
		writeError(w, http.StatusBadRequest, "INVALID_JSON", trailing, false)
		return trailing
	}
	return nil
}

func parseLimit(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return fallback
	}
	if parsed > 50 {
		return 50
	}
	return parsed
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requestID := requestIdentifier(request)
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, request)
	})
}

func requestIdentifier(request *http.Request) string {
	if value := request.Header.Get("X-Request-ID"); value != "" {
		return value
	}
	return fmt.Sprintf("req_%d", time.Now().UnixNano())
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("event=response_encode_failed error=%q", err)
	}
}

func writeError(w http.ResponseWriter, status int, code string, err error, retryable bool) {
	writeJSON(w, status, map[string]interface{}{"error": map[string]interface{}{
		"code": code, "message": err.Error(), "request_id": w.Header().Get("X-Request-ID"), "retryable": retryable,
	}})
}
