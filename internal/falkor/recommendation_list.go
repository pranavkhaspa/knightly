package falkor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pranavkhaspa/knightly/internal/recommendation"
)

func (c *Client) ListRecommendations(ctx context.Context, userID, projectID string, limit int) ([]recommendation.Recommendation, error) {
	if err := c.ready(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(userID) == "" {
		return nil, fmt.Errorf("user ID is required")
	}
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	result, err := c.user.ROQuery(`
		MATCH (u:User {id: $user_id})
		MATCH (r:Recommendation {user_id: $user_id})
		WHERE ($project_id = "" OR r.project_id = $project_id)
		  AND r.tenant_id = u.tenant_id
		RETURN r.id AS id, r.tenant_id AS tenant_id, r.user_id AS user_id, r.project_id AS project_id,
		       r.item_id AS item_id, r.title AS title, r.summary AS summary,
		       r.why AS why, r.score AS score, r.ranking_version AS ranking_version,
		       r.query_name AS query_name, r.path_readable AS path_readable,
		       r.created_at AS created_at
		ORDER BY r.score DESC, r.created_at DESC, r.id
		LIMIT $limit
	`, map[string]interface{}{"user_id": userID, "project_id": projectID, "limit": limit}, c.options())
	if err != nil {
		return nil, fmt.Errorf("list recommendations: %w", err)
	}
	items := make([]recommendation.Recommendation, 0, limit)
	for result.Next() {
		record := result.Record()
		items = append(items, recommendation.Recommendation{
			ID:             stringValue(recordField(record, "id")),
			TenantID:       stringValue(recordField(record, "tenant_id")),
			UserID:         stringValue(recordField(record, "user_id")),
			ProjectID:      stringValue(recordField(record, "project_id")),
			ItemID:         stringValue(recordField(record, "item_id")),
			Title:          stringValue(recordField(record, "title")),
			Summary:        stringValue(recordField(record, "summary")),
			Why:            stringValue(recordField(record, "why")),
			PathReadable:   stringValue(recordField(record, "path_readable")),
			Score:          float64Value(recordField(record, "score")),
			RankingVersion: stringValue(recordField(record, "ranking_version")),
			QueryName:      stringValue(recordField(record, "query_name")),
			Path:           recommendation.EvidencePath{ProjectID: projectID, ItemID: stringValue(recordField(record, "item_id"))},
			CreatedAt:      timeValue(recordField(record, "created_at")),
		})
	}
	return items, nil
}

func float64Value(value interface{}) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case int64:
		return float64(typed)
	case int:
		return float64(typed)
	default:
		return 0
	}
}
