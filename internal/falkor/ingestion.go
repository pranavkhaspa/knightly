package falkor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pranavkhaspa/knightly/internal/ingestion"
)

const (
	SourceUpsertQuery = `
MERGE (source:Source {id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
SET source.name = $name,
    source.kind = $kind,
    source.url = $url,
    source.enabled = $enabled,
    source.trust_score = $trust_score,
    source.poll_interval_seconds = $poll_interval_seconds,
    source.max_items = $max_items,
    source.technology_hints = $technology_hints,
    source.interest_hints = $interest_hints,
    source.updated_at = $updated_at
`

	ItemUpsertQuery = `
MERGE (item:Item {id: $item_id, tenant_id: $tenant_id, user_id: $user_id})
SET item.source_id = $source_id,
    item.canonical_url = $canonical_url,
    item.title = $title,
    item.summary = $summary,
    item.published_at = $published_at,
    item.content_hash = $content_hash,
    item.fetched_at = $fetched_at,
    item.updated_at = $updated_at
WITH item
MATCH (source:Source {id: $source_id, tenant_id: $tenant_id, user_id: $user_id})
MERGE (item)-[:FROM]->(source)
`

	TechnologyLinkQuery = `
MATCH (item:Item {id: $item_id, tenant_id: $tenant_id, user_id: $user_id})
MERGE (technology:Technology {id: $technology_id, tenant_id: $tenant_id, user_id: $user_id})
SET technology.name = $name,
    technology.normalized_name = $name
MERGE (item)-[:MENTIONS]->(technology)
`

	InterestLinkQuery = `
MATCH (item:Item {id: $item_id, tenant_id: $tenant_id, user_id: $user_id})
MERGE (interest:Interest {id: $interest_id, tenant_id: $tenant_id, user_id: $user_id})
SET interest.name = $name,
    interest.normalized_name = $name
MERGE (item)-[:ABOUT]->(interest)
`
)

// Keep the concrete client behind both vertical-slice interfaces. These
// assertions also make accidental signature drift visible at compile time.
var _ ingestion.GraphWriter = (*Client)(nil)

func (c *Client) UpsertSource(ctx context.Context, source ingestion.SourceConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.user == nil {
		return errors.New("FalkorDB user graph is not configured")
	}
	normalized, err := ingestion.NewNormalizer(ingestion.DefaultLimits()).NormalizeSource(source)
	if err != nil {
		return fmt.Errorf("normalize source %q: %w", source.ID, err)
	}
	source = normalized
	if strings.TrimSpace(source.TenantID) == "" || strings.TrimSpace(source.UserID) == "" {
		return errors.New("source tenant ID and user ID are required")
	}
	pollIntervalSeconds := int64(source.PollInterval / time.Second)
	if pollIntervalSeconds <= 0 {
		pollIntervalSeconds = int64(ingestion.DefaultSourcePollInterval / time.Second)
	}
	maxItems := source.MaxItems
	if maxItems <= 0 {
		maxItems = ingestion.DefaultSourceMaxItems
	}
	if maxItems > 100 {
		maxItems = 100
	}
	_, err = c.user.Query(SourceUpsertQuery, map[string]interface{}{
		"source_id":             source.ID,
		"tenant_id":             source.TenantID,
		"user_id":               source.UserID,
		"name":                  source.Name,
		"kind":                  source.Kind,
		"url":                   source.URL,
		"enabled":               source.Enabled,
		"trust_score":           source.TrustScore,
		"poll_interval_seconds": pollIntervalSeconds,
		"max_items":             maxItems,
		"technology_hints":      source.TechnologyHints,
		"interest_hints":        source.InterestHints,
		"updated_at":            time.Now().UTC().UnixMilli(),
	}, c.options())
	if err != nil {
		return fmt.Errorf("upsert source %q: %w", source.ID, err)
	}
	return nil
}

func (c *Client) UpsertItem(ctx context.Context, item ingestion.Item) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.user == nil {
		return errors.New("FalkorDB user graph is not configured")
	}
	if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.TenantID) == "" || strings.TrimSpace(item.UserID) == "" || strings.TrimSpace(item.SourceID) == "" || strings.TrimSpace(item.CanonicalURL) == "" || strings.TrimSpace(item.Title) == "" {
		return errors.New("item tenant ID, user ID, ID, source ID, canonical URL, and title are required")
	}

	_, err := c.user.Query(ItemUpsertQuery, map[string]interface{}{
		"item_id":       item.ID,
		"tenant_id":     item.TenantID,
		"user_id":       item.UserID,
		"source_id":     item.SourceID,
		"canonical_url": item.CanonicalURL,
		"title":         item.Title,
		"summary":       item.Summary,
		"published_at":  optionalMillis(item.PublishedAt),
		"content_hash":  item.ContentHash,
		"fetched_at":    optionalMillis(item.FetchedAt),
		"updated_at":    time.Now().UTC().UnixMilli(),
	}, c.options())
	if err != nil {
		return fmt.Errorf("upsert item %q: %w", item.ID, err)
	}

	for _, technology := range item.Technologies {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := c.user.Query(TechnologyLinkQuery, map[string]interface{}{
			"item_id":       item.ID,
			"tenant_id":     item.TenantID,
			"user_id":       item.UserID,
			"technology_id": graphTermID("technology", technology),
			"name":          technology,
		}, c.options()); err != nil {
			return fmt.Errorf("link technology %q to item %q: %w", technology, item.ID, err)
		}
	}
	for _, interest := range item.Interests {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := c.user.Query(InterestLinkQuery, map[string]interface{}{
			"item_id":     item.ID,
			"tenant_id":   item.TenantID,
			"user_id":     item.UserID,
			"interest_id": graphTermID("interest", interest),
			"name":        interest,
		}, c.options()); err != nil {
			return fmt.Errorf("link interest %q to item %q: %w", interest, item.ID, err)
		}
	}
	return nil
}

func graphTermID(prefix, value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.Join(strings.Fields(value), "_")
	return prefix + "_" + value
}
