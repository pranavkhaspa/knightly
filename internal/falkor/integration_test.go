package falkor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/actions"
	"github.com/pranavkhaspa/knightly/internal/coordination"
	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/ingestion"
	"github.com/pranavkhaspa/knightly/internal/mcp"
	"github.com/pranavkhaspa/knightly/internal/recommendation"
)

// These tests intentionally skip in the default suite. Set
// FALKORDB_INTEGRATION=1 to make the test process prove the graph-backed
// behavior against the FalkorDB endpoint in FALKORDB_URL (or localhost:6379).
func integrationClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("FALKORDB_INTEGRATION") != "1" {
		t.Skip("set FALKORDB_INTEGRATION=1 to run against live FalkorDB")
	}

	url := os.Getenv("FALKORDB_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	timeoutMillis := 5000
	if raw := os.Getenv("FALKORDB_QUERY_TIMEOUT_MS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("invalid FALKORDB_QUERY_TIMEOUT_MS %q", raw)
		}
		timeoutMillis = parsed
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	controlGraph := "nightshift_it_control_" + suffix
	userGraph := "nightshift_it_user_" + suffix
	client, err := New(url, controlGraph, userGraph, timeoutMillis)
	if err != nil {
		t.Fatalf("create FalkorDB client: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, graphName := range []string{controlGraph, userGraph} {
			if err := client.db.Conn.Do(ctx, "GRAPH.DELETE", graphName).Err(); err != nil {
				t.Logf("delete integration graph %q: %v", graphName, err)
			}
		}
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("FalkorDB integration endpoint is unavailable: %v", err)
	}
	return client
}

func TestFalkorDBIntegrationPersistsIngestionAndEvidencePath(t *testing.T) {
	client := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Initialize(); err != nil {
		t.Fatalf("initialize graph: %v", err)
	}
	pipeline := ingestion.NewPipeline(client)
	result, err := pipeline.Run(ctx, ingestion.DefaultFixtureSource())
	if err != nil {
		t.Fatalf("ingest fixture: %v", err)
	}
	if result.Normalized != 2 {
		t.Fatalf("normalized items = %d, want 2", result.Normalized)
	}

	item := result.Items[0]
	ranking := recommendation.NewService(client)
	recommendations, err := ranking.Recommend(ctx, "user_demo", "project_nightshift", []recommendation.Candidate{{
		ItemID:      item.ID,
		Title:       item.Title,
		Summary:     item.Summary,
		URL:         item.CanonicalURL,
		SourceID:    item.SourceID,
		ContentHash: item.ContentHash,
		PublishedAt: item.PublishedAt,
		Score:       5,
	}})
	if err != nil {
		t.Fatalf("persist recommendation: %v", err)
	}
	if len(recommendations) != 1 || !recommendations[0].Path.ValidFor("project_nightshift", item.ID) {
		t.Fatalf("recommendations = %#v, want one complete graph-backed path", recommendations)
	}

	path, err := client.FindRecommendationPath(ctx, "project_nightshift", item.ID)
	if err != nil {
		t.Fatalf("read evidence path: %v", err)
	}
	if len(path.Nodes) != 5 || path.SourceID != item.SourceID {
		t.Fatalf("evidence path = %#v, want five-hop project-to-source path", path)
	}
	listed, err := client.ListRecommendations(ctx, "user_demo", "project_nightshift", 10)
	if err != nil {
		t.Fatalf("list persisted recommendation: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != recommendations[0].ID {
		t.Fatalf("listed recommendations = %#v, want persisted recommendation", listed)
	}
}

func TestFalkorDBIntegrationRejectsPlannerOwnershipMismatchBeforeWrites(t *testing.T) {
	client := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.Initialize(); err != nil {
		t.Fatalf("initialize graph: %v", err)
	}
	result, err := ingestion.NewPipeline(client).Run(ctx, ingestion.DefaultFixtureSource())
	if err != nil {
		t.Fatalf("ingest fixture: %v", err)
	}
	item := result.Items[0]
	recommendations, err := recommendation.NewService(client).Recommend(ctx, "user_demo", "project_nightshift", []recommendation.Candidate{{
		ItemID: item.ID, Title: item.Title, Summary: item.Summary, URL: item.CanonicalURL, SourceID: item.SourceID,
		ContentHash: item.ContentHash, Score: 5,
	}})
	if err != nil || len(recommendations) != 1 {
		t.Fatalf("create recommendation: recommendations=%#v err=%v", recommendations, err)
	}
	_, err = client.ProposePlan(ctx, mcp.ProposePlanInput{
		Meta:             mcp.RequestMeta{TenantID: "tenant_demo", UserID: "user_demo"},
		ProjectID:        "project_other",
		RecommendationID: recommendations[0].ID,
		Goal:             "must not be persisted",
	})
	if err == nil {
		t.Fatal("planner ownership mismatch unexpectedly succeeded")
	}
	for _, query := range []string{
		`MATCH (n:Task) WHERE n.tenant_id = $tenant_id AND n.user_id = $user_id RETURN count(n) AS count`,
		`MATCH (n:Action) WHERE n.tenant_id = $tenant_id AND n.user_id = $user_id RETURN count(n) AS count`,
	} {
		countResult, countErr := client.user.ROQuery(query, map[string]interface{}{"tenant_id": "tenant_demo", "user_id": "user_demo"}, client.options())
		if countErr != nil {
			t.Fatalf("count planner writes: %v", countErr)
		}
		if !countResult.Next() || int64Value(recordValue(countResult.Record(), "count")) != 0 {
			t.Fatalf("planner wrote task/action after ownership failure")
		}
	}
}

func TestFalkorDBIntegrationMakesApprovedActionIdempotent(t *testing.T) {
	client := integrationClient(t)
	if err := client.Initialize(); err != nil {
		t.Fatalf("initialize graph: %v", err)
	}

	service := actions.NewService(NewActionRepository(client), true)
	base := actions.Action{
		ID:             "action_it_" + strconv.FormatInt(time.Now().UnixNano(), 10),
		TenantID:       "tenant_demo",
		UserID:         "user_demo",
		ProjectID:      "project_nightshift",
		Kind:           actions.LocalPlan,
		Target:         "project_nightshift",
		Payload:        map[string]string{"goal": "exercise graph-backed recovery"},
		IdempotencyKey: "action-it-" + strconv.FormatInt(time.Now().UnixNano(), 10),
	}
	first, err := service.Propose(context.Background(), base)
	if err != nil {
		t.Fatalf("first action proposal: %v", err)
	}
	retry := base
	retry.ID = base.ID + "_retry"
	second, err := service.Propose(context.Background(), retry)
	if err != nil {
		t.Fatalf("idempotent action proposal: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("idempotent proposal IDs = %q and %q, want one graph action", first.ID, second.ID)
	}
	if _, err := service.Approve(context.Background(), "user_demo", first.ID, actions.Approval{ID: "approval_it", ApprovedBy: "user_demo"}); err != nil {
		t.Fatalf("approve action: %v", err)
	}

	executor := &integrationExecutor{}
	completed, err := service.Execute(context.Background(), "user_demo", first.ID, executor)
	if err != nil {
		t.Fatalf("execute approved action: %v", err)
	}
	repeated, err := service.Execute(context.Background(), "user_demo", first.ID, executor)
	if err != nil {
		t.Fatalf("repeat completed action: %v", err)
	}
	if completed.Status != actions.Completed || repeated.ID != first.ID || executor.calls.Load() != 1 {
		t.Fatalf("completed=%#v repeated=%#v executor_calls=%d; want one external call", completed, repeated, executor.calls.Load())
	}
}

func TestFalkorDBIntegrationCoordinatesConcurrentCompletion(t *testing.T) {
	client := integrationClient(t)
	manager := coordination.NewManager(client)
	const callers = 12
	key := "coordination-it-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	start := make(chan struct{})
	results := make(chan error, callers)
	var executed atomic.Int32
	var wait sync.WaitGroup
	wait.Add(callers)
	for index := 0; index < callers; index++ {
		go func(index int) {
			defer wait.Done()
			<-start
			_, err := manager.Execute(context.Background(), key, fmt.Sprintf("integration-instance-%d", index), time.Minute, func(context.Context) ([]byte, error) {
				executed.Add(1)
				time.Sleep(25 * time.Millisecond)
				return []byte("live-falkordb-result"), nil
			})
			if err != nil && !errors.Is(err, coordination.ErrLeaseUnavailable) {
				results <- err
				return
			}
			results <- nil
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent graph execution error: %v", err)
		}
	}
	if executed.Load() != 1 {
		t.Fatalf("graph-backed work executed %d times, want exactly once", executed.Load())
	}

	record, found, err := client.FindIdempotency(context.Background(), key)
	if err != nil || !found || string(record.Payload) != "live-falkordb-result" {
		t.Fatalf("graph idempotency record = %#v found=%v err=%v", record, found, err)
	}
}

func TestFalkorDBIntegrationRecoversStaleTaskActionAndSourceRun(t *testing.T) {
	client := integrationClient(t)
	if err := client.Initialize(); err != nil {
		t.Fatalf("initialize graph: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	old := time.Now().UTC().Add(-time.Hour).UnixMilli()
	_, err := client.user.Query(`
		MERGE (t:Task {id: "stale_task", tenant_id: "tenant_demo", user_id: "user_demo"})
		SET t.status = "running", t.created_at = $old, t.updated_at = $old
		MERGE (a:Action {id: "stale_action", tenant_id: "tenant_demo", user_id: "user_demo"})
		SET a.status = "executing", a.created_at = $old, a.updated_at = $old
		MERGE (s:Source {id: "stale_source", tenant_id: "tenant_demo", user_id: "user_demo"})
		SET s.name = "stale", s.kind = "fixture", s.url = "https://example.invalid/stale"
		MERGE (run:SourceRun {id: "stale_run", source_id: "stale_source", tenant_id: "tenant_demo", user_id: "user_demo"})
		SET run.status = "running", run.started_at = $old, run.updated_at = $old
	`, map[string]interface{}{"old": old}, client.options())
	if err != nil {
		t.Fatalf("seed stale records: %v", err)
	}
	cutoff := time.Now().UTC().Add(-30 * time.Minute)
	if recovered, err := NewAgentRepository(client).RecoverStaleRunning(ctx, "tenant_demo", "user_demo", cutoff); err != nil || recovered != 1 {
		t.Fatalf("recover stale task = %d, err=%v", recovered, err)
	}
	if recovered, err := NewActionRepository(client).RecoverStaleExecuting(ctx, "tenant_demo", "user_demo", cutoff); err != nil || recovered != 1 {
		t.Fatalf("recover stale action = %d, err=%v", recovered, err)
	}
	if recovered, err := NewSourceRepository(client, identity.Scope{TenantID: "tenant_demo", UserID: "user_demo"}).RecoverStaleRuns(ctx, "tenant_demo", "user_demo", cutoff); err != nil || recovered != 1 {
		t.Fatalf("recover stale source run = %d, err=%v", recovered, err)
	}
}

type integrationExecutor struct {
	calls atomic.Int32
}

func (e *integrationExecutor) Execute(context.Context, actions.Action) (actions.ExecutionResult, error) {
	e.calls.Add(1)
	return actions.ExecutionResult{Reference: "local-plan-integration", Output: "created"}, nil
}
