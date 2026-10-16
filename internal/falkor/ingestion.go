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
MERGE (source:Source {id: $source_id})
SET source.name = $name,
    source.kind = $kind,
    source.url = $url,
    source.enabled = $enabled,
    source.trust_score = $trust_score,
    source.updated_at = timestamp()
`

	ItemUpsertQuery = `
MERGE (item:Item {id: $item_id})
SET item.source_id = $source_id,
    item.canonical_url = $canonical_url,
    item.title = $title,
    item.summary = $summary,
    item.published_at = $published_at,
    item.content_hash = $content_hash,
    item.fetched_at = $fetched_at,
    item.updated_at = timestamp()
WITH item
MATCH (source:Source {id: $source_id})
MERGE (item)-[:FROM]->(source)
`

	TechnologyLinkQuery = `
MATCH (item:Item {id: $item_id})
MERGE (technology:Technology {id: $technology_id})
SET technology.name = $name,
    technology.normalized_name = $name
MERGE (item)-[:MENTIONS]->(technology)
`

	InterestLinkQuery = `
MATCH (item:Item {id: $item_id})
MERGE (interest:Interest {id: $interest_id})
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
	if strings.TrimSpace(source.ID) == "" || strings.TrimSpace(source.Name) == "" || strings.TrimSpace(source.URL) == "" {
		return errors.New("source ID, name, and URL are required")
	}
	_, err := c.user.Query(SourceUpsertQuery, map[string]interface{}{
		"source_id":   source.ID,
		"name":        source.Name,
		"kind":        source.Kind,
		"url":         source.URL,
		"enabled":     source.Enabled,
		"trust_score": source.TrustScore,
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
	if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.SourceID) == "" || strings.TrimSpace(item.CanonicalURL) == "" || strings.TrimSpace(item.Title) == "" {
		return errors.New("item ID, source ID, canonical URL, and title are required")
	}

	_, err := c.user.Query(ItemUpsertQuery, map[string]interface{}{
		"item_id":       item.ID,
		"source_id":     item.SourceID,
		"canonical_url": item.CanonicalURL,
		"title":         item.Title,
		"summary":       item.Summary,
		"published_at":  optionalUnix(item.PublishedAt),
		"content_hash":  item.ContentHash,
		"fetched_at":    item.FetchedAt.Unix(),
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
			"interest_id": graphTermID("interest", interest),
			"name":        interest,
		}, c.options()); err != nil {
			return fmt.Errorf("link interest %q to item %q: %w", interest, item.ID, err)
		}
	}
	return nil
}

func optionalUnix(value time.Time) interface{} {
	if value.IsZero() {
		return nil
	}
	return value.Unix()
}

func graphTermID(prefix, value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.Join(strings.Fields(value), "_")
	return prefix + "_" + value
}
