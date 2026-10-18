package search

import (
	"context"
	"time"

	"github.com/pranavkhaspa/knightly/internal/identity"
)

// EvidenceOrigin marks persisted search evidence as untrusted external input.
const EvidenceOrigin = "web"

// MaxEvidenceExcerpt bounds the excerpt stored for a search result so an
// unbounded page body is never written to the graph.
const MaxEvidenceExcerpt = 2048

// Evidence is a bounded, durable citation derived from a search result. It
// carries only normalized fields, never a raw provider payload.
type Evidence struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	UserID      string    `json:"user_id"`
	Query       string    `json:"query,omitempty"`
	Title       string    `json:"title,omitempty"`
	URL         string    `json:"url"`
	Excerpt     string    `json:"excerpt,omitempty"`
	Source      string    `json:"source,omitempty"`
	Origin      string    `json:"origin"`
	ContentHash string    `json:"content_hash,omitempty"`
	ObservedAt  time.Time `json:"observed_at"`
}

// EvidenceStore persists bounded search citations for a trusted scope. It is
// the only storage seam the search package needs; the FalkorDB implementation
// lives outside this package.
type EvidenceStore interface {
	SaveSearchEvidence(context.Context, identity.Scope, string, []SearchResult) ([]Evidence, error)
}
