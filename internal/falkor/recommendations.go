package falkor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
	"github.com/pranavkhaspa/knightly/internal/recommendation"
)

const RecommendationPathQuery = `
MATCH path = (project:Project {id: $project_id})
  -[:HAS_REQUIREMENT]->(requirement:Requirement)
  -[:NEEDS]->(technology:Technology)
  <-[:MENTIONS]-(item:Item)
  -[:FROM]->(source:Source)
WHERE item.id = $item_id
  AND requirement.tenant_id = project.tenant_id
  AND requirement.user_id = project.user_id
  AND technology.tenant_id = project.tenant_id
  AND technology.user_id = project.user_id
  AND item.tenant_id = project.tenant_id
  AND item.user_id = project.user_id
  AND source.tenant_id = project.tenant_id
  AND source.user_id = project.user_id
RETURN project.tenant_id AS tenant_id,
       project.id AS project_id,
       project.name AS project_name,
       requirement.id AS requirement_id,
       requirement.title AS requirement_name,
       technology.id AS technology_id,
       technology.name AS technology_name,
       item.id AS item_id,
       item.title AS item_name,
       source.id AS source_id,
       source.name AS source_name,
       source.url AS source_url,
       nodes(path) AS path_nodes
LIMIT 1
`

const RecommendationUpsertQuery = `
MATCH (project:Project {id: $project_id, tenant_id: $tenant_id, user_id: $user_id})
MATCH (project)-[:HAS_REQUIREMENT]->(requirement:Requirement {id: $requirement_id, tenant_id: $tenant_id, user_id: $user_id})
MATCH (item:Item {id: $item_id, tenant_id: $tenant_id, user_id: $user_id})
MATCH (item)-[:FROM]->(source:Source {id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
WITH project, item, requirement
MERGE (recommendation:Recommendation {id: $recommendation_id, tenant_id: $tenant_id, user_id: $user_id})
SET recommendation.tenant_id = $tenant_id,
    recommendation.user_id = $user_id,
    recommendation.project_id = $project_id,
    recommendation.item_id = $item_id,
    recommendation.title = $title,
    recommendation.summary = $summary,
    recommendation.why = $why,
    recommendation.score = $score,
    recommendation.ranking_version = $ranking_version,
    recommendation.query_name = $query_name,
    recommendation.path_readable = $path_readable,
    recommendation.created_at = $created_at,
    recommendation.updated_at = $updated_at
MERGE (user:User {id: $user_id, tenant_id: $tenant_id})
MERGE (recommendation)-[:FOR]->(user)
MERGE (recommendation)-[:ABOUT]->(item)
MERGE (recommendation)-[:MATCHES]->(requirement)
MERGE (evidence:Evidence {id: $evidence_id, tenant_id: $tenant_id, user_id: $user_id})
SET evidence.source_id = $source_id,
	 evidence.tenant_id = $tenant_id,
	 evidence.user_id = $user_id,
	 evidence.item_id = $item_id,
    evidence.url = $evidence_url,
    evidence.excerpt = $evidence_excerpt,
    evidence.evidence_kind = $evidence_kind,
    evidence.content_hash = $evidence_content_hash,
    evidence.observed_at = $evidence_observed_at,
    evidence.updated_at = $updated_at
MERGE (recommendation)-[:SUPPORTED_BY]->(evidence)
MERGE (evidence)-[:SUPPORTS]->(item)
RETURN recommendation.id AS id
`

var _ recommendation.GraphRepository = (*Client)(nil)

func (c *Client) FindRecommendationPath(ctx context.Context, projectID, itemID string) (recommendation.EvidencePath, error) {
	if err := ctx.Err(); err != nil {
		return recommendation.EvidencePath{}, err
	}
	if c == nil || c.user == nil {
		return recommendation.EvidencePath{}, errors.New("FalkorDB user graph is not configured")
	}
	if strings.TrimSpace(projectID) == "" || strings.TrimSpace(itemID) == "" {
		return recommendation.EvidencePath{}, errors.New("project ID and item ID are required")
	}

	result, err := c.user.ROQuery(RecommendationPathQuery, map[string]interface{}{
		"project_id": projectID,
		"item_id":    itemID,
	}, c.options())
	if err != nil {
		return recommendation.EvidencePath{}, fmt.Errorf("query recommendation path: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return recommendation.EvidencePath{}, err
	}
	if !result.Next() {
		return recommendation.EvidencePath{}, nil
	}

	record := result.Record()
	pathNodes, err := parsePathNodes(recordValue(record, "path_nodes"))
	if err != nil {
		return recommendation.EvidencePath{}, fmt.Errorf("parse recommendation path: %w", err)
	}
	path := recommendation.EvidencePath{
		TenantID:      stringValue(recordValue(record, "tenant_id")),
		ProjectID:     stringValue(recordValue(record, "project_id")),
		RequirementID: stringValue(recordValue(record, "requirement_id")),
		TechnologyID:  stringValue(recordValue(record, "technology_id")),
		ItemID:        stringValue(recordValue(record, "item_id")),
		SourceID:      stringValue(recordValue(record, "source_id")),
		Nodes:         pathNodes,
	}
	if !path.ValidFor(projectID, itemID) {
		return recommendation.EvidencePath{}, errors.New("FalkorDB returned an incomplete recommendation path")
	}
	return path, nil
}

func (c *Client) SaveRecommendation(ctx context.Context, value recommendation.Recommendation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.user == nil {
		return errors.New("FalkorDB user graph is not configured")
	}
	if strings.TrimSpace(value.ID) == "" || strings.TrimSpace(value.TenantID) == "" || strings.TrimSpace(value.UserID) == "" || strings.TrimSpace(value.ProjectID) == "" || strings.TrimSpace(value.ItemID) == "" || !value.Path.ValidFor(value.ProjectID, value.ItemID) {
		return errors.New("recommendation requires a complete FalkorDB evidence path")
	}
	if (value.Path.TenantID != "" && value.Path.TenantID != value.TenantID) ||
		(value.Evidence.TenantID != "" && value.Evidence.TenantID != value.TenantID) ||
		(value.Evidence.UserID != "" && value.Evidence.UserID != value.UserID) {
		return errors.New("recommendation evidence is outside the recommendation scope")
	}
	createdAt := value.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	observedAt := value.Evidence.ObservedAt
	if observedAt.IsZero() {
		observedAt = createdAt
	}
	result, err := c.user.Query(RecommendationUpsertQuery, map[string]interface{}{
		"recommendation_id":     value.ID,
		"tenant_id":             value.TenantID,
		"user_id":               value.UserID,
		"project_id":            value.ProjectID,
		"item_id":               value.ItemID,
		"title":                 value.Title,
		"summary":               value.Summary,
		"why":                   value.Why,
		"score":                 value.Score,
		"ranking_version":       value.RankingVersion,
		"query_name":            value.QueryName,
		"path_readable":         value.Path.Readable(),
		"created_at":            createdAt.UnixMilli(),
		"requirement_id":        value.Path.RequirementID,
		"evidence_id":           value.Evidence.ID,
		"source_id":             value.Evidence.SourceID,
		"evidence_url":          value.Evidence.URL,
		"evidence_excerpt":      value.Evidence.Excerpt,
		"evidence_kind":         value.Evidence.Kind,
		"evidence_content_hash": value.Evidence.ContentHash,
		"evidence_observed_at":  optionalMillis(observedAt),
		"updated_at":            time.Now().UTC().UnixMilli(),
	}, c.options())
	if err != nil {
		return fmt.Errorf("save recommendation %q: %w", value.ID, err)
	}
	if err := requireQueryRow(result, "save recommendation"); err != nil {
		return err
	}
	return nil
}

func parsePathNodes(value interface{}) ([]recommendation.PathNode, error) {
	rawNodes, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("path_nodes has type %T, want []interface{}", value)
	}
	nodes := make([]recommendation.PathNode, 0, len(rawNodes))
	for _, rawNode := range rawNodes {
		node, ok := rawNode.(*dbclient.Node)
		if !ok {
			return nil, fmt.Errorf("path node has type %T, want FalkorDB node", rawNode)
		}
		label := ""
		for _, candidate := range node.Labels {
			switch candidate {
			case "Project", "Requirement", "Technology", "Item", "Source":
				label = candidate
			}
			if label != "" {
				break
			}
		}
		if label == "" {
			return nil, errors.New("path node has no supported label")
		}
		name := stringValue(node.Properties["name"])
		if name == "" {
			name = stringValue(node.Properties["title"])
		}
		nodes = append(nodes, recommendation.PathNode{
			Label: label,
			ID:    stringValue(node.Properties["id"]),
			Name:  name,
			URL:   stringValue(node.Properties["url"]),
		})
	}
	return nodes, nil
}
