package falkor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
	"github.com/pranavkhaspa/knightly/internal/search"
)

// SearchEvidenceRepository persists bounded web-search citations as Evidence
// nodes in the routed user graph. Only normalized fields are stored; raw page
// bodies never enter FalkorDB.
type SearchEvidenceRepository struct {
	selector UserGraphSelector
	now      func() time.Time
}

var _ search.EvidenceStore = (*SearchEvidenceRepository)(nil)

func NewSearchEvidenceRepository(selector UserGraphSelector) *SearchEvidenceRepository {
	return &SearchEvidenceRepository{selector: selector, now: func() time.Time { return time.Now().UTC() }}
}

func (r *SearchEvidenceRepository) SaveSearchEvidence(ctx context.Context, scope identity.Scope, query string, results []search.SearchResult) ([]search.Evidence, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("search evidence scope: %w", err)
	}
	graph, err := routedGraph(ctx, r.selector, scope, "search evidence save")
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	observedAt := r.now().UTC()
	evidence := make([]search.Evidence, 0, len(results))
	rows := make([]map[string]interface{}, 0, len(results))
	for _, result := range results {
		value := search.Evidence{
			ID:          searchEvidenceID(scope, result.URL),
			TenantID:    scope.TenantID,
			UserID:      scope.UserID,
			Query:       boundRunes(query, search.DefaultMaxQueryLength),
			Title:       boundRunes(result.Title, search.MaxResultTitle),
			URL:         result.URL,
			Excerpt:     boundRunes(result.Snippet, search.MaxEvidenceExcerpt),
			Source:      boundRunes(result.Source, search.MaxResultSource),
			Origin:      search.EvidenceOrigin,
			ContentHash: searchContentHash(result),
			ObservedAt:  observedAt,
		}
		evidence = append(evidence, value)
		rows = append(rows, map[string]interface{}{
			"id": value.ID, "query": value.Query, "title": value.Title, "url": value.URL,
			"excerpt": value.Excerpt, "source": value.Source, "origin": value.Origin,
			"content_hash": value.ContentHash, "observed_at": optionalMillis(value.ObservedAt),
		})
	}
	_, err = graph.Query(`
		UNWIND $rows AS row
		MERGE (e:Evidence {id: row.id, tenant_id: $tenant_id, user_id: $user_id})
		ON CREATE SET e.created_at = $observed_at
		SET e.query = row.query,
		    e.title = row.title,
		    e.url = row.url,
		    e.excerpt = row.excerpt,
		    e.source = row.source,
		    e.evidence_kind = "web_search",
		    e.untrusted_origin = row.origin,
		    e.content_hash = row.content_hash,
		    e.observed_at = row.observed_at,
		    e.updated_at = $observed_at
		RETURN count(e) AS recorded
	`, map[string]interface{}{
		"tenant_id":   scope.TenantID,
		"user_id":     scope.UserID,
		"rows":        rows,
		"observed_at": optionalMillis(observedAt),
	}, queryOptionsFor(r.selector))
	if err != nil {
		return nil, fmt.Errorf("save search evidence in graph: %w", err)
	}
	return evidence, nil
}

func searchEvidenceID(scope identity.Scope, url string) string {
	sum := sha256.Sum256([]byte(scope.TenantID + "\x00" + scope.UserID + "\x00" + url))
	return "evidence_web_" + hex.EncodeToString(sum[:])[:24]
}

func searchContentHash(result search.SearchResult) string {
	sum := sha256.Sum256([]byte(result.Title + "\x00" + result.URL + "\x00" + result.Snippet))
	return hex.EncodeToString(sum[:])
}

func boundRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return string(runes)
}
