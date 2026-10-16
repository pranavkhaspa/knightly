package falkor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/actions"
	"github.com/pranavkhaspa/knightly/internal/agents"
	"github.com/pranavkhaspa/knightly/internal/mcp"
	"github.com/pranavkhaspa/knightly/internal/memory"
)

var (
	_ mcp.ContextProvider  = (*Client)(nil)
	_ mcp.Planner          = (*Client)(nil)
	_ mcp.FeedbackRecorder = (*Client)(nil)
)

const plannerValidationQuery = `
MATCH (p:Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
MATCH (p)-[:HAS_REQUIREMENT]->(:Requirement {tenant_id: $tenant_id, user_id: $user_id})
MATCH (r:Recommendation {id: $recommendation_id, tenant_id: $tenant_id, user_id: $user_id, project_id: $project_id})
RETURN p.id AS project_id, r.id AS recommendation_id
LIMIT 1
`

// SearchRelevantItems is a fixed, bounded graph tool. The caller supplies a
// natural-language hint, but it cannot supply Cypher or a graph name.
func (c *Client) SearchRelevantItems(ctx context.Context, input mcp.SearchRelevantItemsInput) (mcp.SearchRelevantItemsOutput, error) {
	if err := c.ready(ctx); err != nil {
		return mcp.SearchRelevantItemsOutput{}, err
	}
	limit := input.Limit
	if limit <= 0 || limit > 20 {
		limit = 10
	}
	query := strings.ToLower(strings.TrimSpace(input.Query))
	result, err := c.user.ROQuery(`
		MATCH (p:Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})-[:HAS_REQUIREMENT]->(r:Requirement)
		      -[:NEEDS]->(tech:Technology)<-[:MENTIONS]-(item:Item)-[:FROM]->(source:Source)
		WHERE r.tenant_id = $tenant_id AND r.user_id = $user_id
		  AND tech.tenant_id = $tenant_id AND tech.user_id = $user_id
		  AND item.tenant_id = $tenant_id AND item.user_id = $user_id
		  AND source.tenant_id = $tenant_id AND source.user_id = $user_id
		  AND ($query = "" OR toLower(item.title) CONTAINS $query OR toLower(item.summary) CONTAINS $query)
		RETURN DISTINCT item.id AS item_id, item.title AS item_title,
		       item.summary AS item_summary, source.id AS source_id,
		       tech.id AS technology_id
		ORDER BY item.published_at DESC, item.id
		LIMIT $limit
	`, map[string]interface{}{
		"project_id": input.ProjectID,
		"tenant_id":  input.Meta.TenantID,
		"user_id":    input.Meta.UserID,
		"query":      query,
		"limit":      limit,
	}, c.options())
	if err != nil {
		return mcp.SearchRelevantItemsOutput{}, fmt.Errorf("search relevant graph items: %w", err)
	}
	items := make([]mcp.RelevantItem, 0, limit)
	seen := make(map[string]struct{}, limit)
	for result.Next() {
		record := result.Record()
		id := stringValue(recordField(record, "item_id"))
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		items = append(items, mcp.RelevantItem{
			ID:            id,
			Title:         stringValue(recordField(record, "item_title")),
			Summary:       stringValue(recordField(record, "item_summary")),
			SourceID:      stringValue(recordField(record, "source_id")),
			TechnologyIDs: []string{stringValue(recordField(record, "technology_id"))},
		})
	}
	return mcp.SearchRelevantItemsOutput{Items: items}, nil
}

func (c *Client) ExplainRecommendation(ctx context.Context, input mcp.ExplainRecommendationInput) (mcp.RecommendationExplanation, error) {
	if err := c.ready(ctx); err != nil {
		return mcp.RecommendationExplanation{}, err
	}
	result, err := c.user.ROQuery(`
		MATCH (r:Recommendation {id: $recommendation_id, tenant_id: $tenant_id, user_id: $user_id})
		OPTIONAL MATCH (r)-[:SUPPORTED_BY]->(e:Evidence {tenant_id: $tenant_id, user_id: $user_id})
		RETURN r.id AS recommendation_id, r.why AS why,
		       r.path_readable AS path_readable, collect(e.id) AS evidence_ids
		LIMIT 1
	`, map[string]interface{}{
		"recommendation_id": input.RecommendationID,
		"tenant_id":         input.Meta.TenantID,
		"user_id":           input.Meta.UserID,
	}, c.options())
	if err != nil {
		return mcp.RecommendationExplanation{}, fmt.Errorf("explain recommendation: %w", err)
	}
	if !result.Next() {
		return mcp.RecommendationExplanation{}, fmt.Errorf("recommendation %q not found", input.RecommendationID)
	}
	record := result.Record()
	pathText := stringValue(recordField(record, "path_readable"))
	return mcp.RecommendationExplanation{
		RecommendationID: stringValue(recordField(record, "recommendation_id")),
		Why:              stringValue(recordField(record, "why")),
		Path:             splitPath(pathText),
		EvidenceIDs:      stringSliceValue(recordField(record, "evidence_ids")),
	}, nil
}

// ProposePlan persists both the Planner task and the proposed local-plan
// action in FalkorDB. No filesystem or external write happens at this stage.
func (c *Client) ProposePlan(ctx context.Context, input mcp.ProposePlanInput) (mcp.PlanProposal, error) {
	if err := c.ready(ctx); err != nil {
		return mcp.PlanProposal{}, err
	}
	if strings.TrimSpace(input.Meta.UserID) == "" || strings.TrimSpace(input.ProjectID) == "" || strings.TrimSpace(input.RecommendationID) == "" {
		return mcp.PlanProposal{}, errors.New("user, project, and recommendation are required")
	}
	if strings.TrimSpace(input.Meta.TenantID) == "" {
		return mcp.PlanProposal{}, errors.New("tenant is required")
	}
	if err := c.validatePlannerContext(ctx, input); err != nil {
		return mcp.PlanProposal{}, err
	}
	taskID := stableID("task_planner", input.Meta.UserID+"\x00"+input.RecommendationID)
	actionID := stableID("action_plan", input.Meta.UserID+"\x00"+input.RecommendationID)
	idempotencyKey := stableID("action_key", input.Meta.UserID+"\x00"+input.ProjectID+"\x00"+input.RecommendationID)
	now := time.Now().UTC()
	coordinator := agents.NewCoordinator(NewAgentRepository(c))
	if _, err := coordinator.CreateTask(ctx, agents.Task{
		ID:            taskID,
		TenantID:      input.Meta.TenantID,
		UserID:        input.Meta.UserID,
		ProjectID:     input.ProjectID,
		SessionID:     input.Meta.SessionID,
		Agent:         agents.PlannerAgent,
		Status:        agents.Pending,
		Priority:      1,
		CorrelationID: input.Meta.RequestID,
		InputRefs:     []string{input.RecommendationID},
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		return mcp.PlanProposal{}, fmt.Errorf("persist planner task: %w", err)
	}
	actionService := actions.NewService(NewActionRepository(c), false)
	action, err := actionService.Propose(ctx, actions.Action{
		ID:             actionID,
		TenantID:       input.Meta.TenantID,
		UserID:         input.Meta.UserID,
		ProjectID:      input.ProjectID,
		TaskID:         taskID,
		Kind:           actions.LocalPlan,
		Target:         input.ProjectID,
		Payload:        map[string]string{"goal": input.Goal, "recommendation_id": input.RecommendationID},
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return mcp.PlanProposal{}, fmt.Errorf("persist proposed action: %w", err)
	}
	if _, err := c.user.Query(`
		MATCH (a:Action {id: $action_id, tenant_id: $tenant_id, user_id: $user_id})
		MATCH (r:Recommendation {id: $recommendation_id, tenant_id: $tenant_id, user_id: $user_id, project_id: $project_id})
		MERGE (a)-[:BASED_ON]->(r)
	`, map[string]interface{}{
		"action_id":         action.ID,
		"recommendation_id": input.RecommendationID,
		"project_id":        input.ProjectID,
		"tenant_id":         input.Meta.TenantID,
		"user_id":           input.Meta.UserID,
	}, c.options()); err != nil {
		return mcp.PlanProposal{}, fmt.Errorf("link plan to recommendation: %w", err)
	}
	return mcp.PlanProposal{
		ID:               action.ID,
		ProjectID:        input.ProjectID,
		RecommendationID: input.RecommendationID,
		Summary:          input.Goal,
		Steps: []mcp.PlanStep{{
			ID:          "step_1",
			Description: "Review the graph evidence and implement the approved bounded change.",
		}},
		TaskID:   taskID,
		ActionID: action.ID,
	}, nil
}

func (c *Client) RecordFeedback(ctx context.Context, input mcp.FeedbackInput) (mcp.FeedbackRecord, error) {
	if err := c.ready(ctx); err != nil {
		return mcp.FeedbackRecord{}, err
	}
	if !input.Kind.Valid() || strings.TrimSpace(input.Meta.UserID) == "" || strings.TrimSpace(input.RecommendationID) == "" {
		return mcp.FeedbackRecord{}, errors.New("user, recommendation, and valid feedback are required")
	}
	now := time.Now().UTC()
	feedbackID := stableID("feedback", input.Meta.UserID+"\x00"+input.RecommendationID+"\x00"+string(input.Kind))
	result, err := c.user.Query(`
		MATCH (r:Recommendation {id: $recommendation_id, tenant_id: $tenant_id, user_id: $user_id})
		MERGE (f:Feedback {id: $id, tenant_id: $tenant_id, user_id: $user_id})
		SET f.recommendation_id = $recommendation_id, f.project_id = r.project_id,
		    f.kind = $kind, f.note = $note, f.recorded_at = $recorded_at
		WITH f, r
		MERGE (f)-[:ABOUT]->(r)
		WITH f, r
		MATCH (u:User {id: $user_id, tenant_id: $tenant_id})
		MERGE (u)-[:GAVE_FEEDBACK]->(f)
		RETURN f.id AS id, r.project_id AS project_id
	`, map[string]interface{}{
		"id":                feedbackID,
		"tenant_id":         input.Meta.TenantID,
		"user_id":           input.Meta.UserID,
		"recommendation_id": input.RecommendationID,
		"kind":              string(input.Kind),
		"note":              input.Note,
		"recorded_at":       now.UnixMilli(),
	}, c.options())
	if err != nil {
		return mcp.FeedbackRecord{}, fmt.Errorf("save feedback: %w", err)
	}
	if err := requireQueryRow(result, "save feedback"); err != nil {
		return mcp.FeedbackRecord{}, err
	}
	projectID := stringValue(recordValue(result.Record(), "project_id"))
	memoryService := memory.NewService(NewMemoryRepository(c))
	_, memoryErr := memoryService.Remember(ctx, memory.Memory{
		ID:        stableID("memory_feedback", feedbackID),
		TenantID:  input.Meta.TenantID,
		UserID:    input.Meta.UserID,
		SessionID: input.Meta.SessionID,
		ProjectID: projectID,
		Type:      memory.Episodic,
		Content:   fmt.Sprintf("User marked recommendation %s as %s.", input.RecommendationID, input.Kind),
		Summary:   input.Note,
		Metadata:  map[string]string{"feedback_id": feedbackID, "kind": string(input.Kind)},
		CreatedAt: now,
		UpdatedAt: now,
	})
	if memoryErr != nil {
		return mcp.FeedbackRecord{}, fmt.Errorf("persist feedback memory: %w", memoryErr)
	}
	return mcp.FeedbackRecord{ID: feedbackID, RecommendationID: input.RecommendationID, Kind: input.Kind, Note: input.Note, RecordedAt: now}, nil
}

func (c *Client) validatePlannerContext(ctx context.Context, input mcp.ProposePlanInput) error {
	result, err := c.user.ROQuery(plannerValidationQuery, map[string]interface{}{
		"project_id":        input.ProjectID,
		"recommendation_id": input.RecommendationID,
		"tenant_id":         input.Meta.TenantID,
		"user_id":           input.Meta.UserID,
	}, c.options())
	if err != nil {
		return fmt.Errorf("validate planner project and recommendation: %w", err)
	}
	if err := requireQueryRow(result, "validate planner context"); err != nil {
		return fmt.Errorf("planner recommendation %q and project %q are not in the same user scope: %w", input.RecommendationID, input.ProjectID, err)
	}
	return nil
}

func (c *Client) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.user == nil {
		return errors.New("FalkorDB user graph is not configured")
	}
	return nil
}

func splitPath(value string) []string {
	parts := strings.Split(value, " → ")
	if len(parts) == 1 && strings.TrimSpace(parts[0]) == "" {
		return nil
	}
	return parts
}

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + "_" + hex.EncodeToString(sum[:])[:24]
}
