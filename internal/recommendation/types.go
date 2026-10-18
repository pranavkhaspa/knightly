package recommendation

import (
	"fmt"
	"strings"
	"time"
)

const (
	QueryName      = "project_requirement_item_source_path_v1"
	RankingVersion = "deterministic-graph-v1"
)

// PathNode is a node returned by the FalkorDB path query. The readable name is
// display data; ID and Label are the identity used to validate the path.
type PathNode struct {
	Label string `json:"label"`
	ID    string `json:"id"`
	Name  string `json:"name,omitempty"`
	URL   string `json:"url,omitempty"`
}

// EvidencePath is deliberately represented as returned graph data, not as a
// locally inferred explanation. A recommendation is valid only when all five
// nodes in the required traversal are present and ordered correctly.
type EvidencePath struct {
	TenantID      string     `json:"tenant_id,omitempty"`
	ProjectID     string     `json:"project_id"`
	RequirementID string     `json:"requirement_id"`
	TechnologyID  string     `json:"technology_id"`
	ItemID        string     `json:"item_id"`
	SourceID      string     `json:"source_id"`
	Nodes         []PathNode `json:"nodes"`
}

func (p EvidencePath) ValidFor(projectID, itemID string) bool {
	if projectID == "" || itemID == "" || p.ProjectID != projectID || p.ItemID != itemID || p.RequirementID == "" || p.TechnologyID == "" || p.SourceID == "" {
		return false
	}
	if len(p.Nodes) < 5 {
		return false
	}
	expected := []struct {
		label string
		id    string
	}{
		{label: "Project", id: p.ProjectID},
		{label: "Requirement", id: p.RequirementID},
		{label: "Technology", id: p.TechnologyID},
		{label: "Item", id: p.ItemID},
		{label: "Source", id: p.SourceID},
	}
	for index, node := range p.Nodes[:5] {
		if node.Label != expected[index].label || node.ID != expected[index].id {
			return false
		}
	}
	return true
}

func (p EvidencePath) SourceURL() string {
	if len(p.Nodes) < 5 {
		return ""
	}
	return p.Nodes[4].URL
}

func (p EvidencePath) Readable() string {
	if len(p.Nodes) == 0 {
		return ""
	}
	parts := make([]string, 0, len(p.Nodes))
	for _, node := range p.Nodes {
		name := node.Name
		if name == "" {
			name = node.ID
		}
		parts = append(parts, fmt.Sprintf("%s(%s)", node.Label, name))
	}
	return strings.Join(parts, " -> ")
}

type Evidence struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id,omitempty"`
	UserID      string    `json:"user_id,omitempty"`
	SourceID    string    `json:"source_id"`
	ItemID      string    `json:"item_id"`
	URL         string    `json:"url"`
	Excerpt     string    `json:"excerpt,omitempty"`
	Kind        string    `json:"kind"`
	ContentHash string    `json:"content_hash,omitempty"`
	ObservedAt  time.Time `json:"observed_at"`
}

type Candidate struct {
	ItemID      string
	Title       string
	Summary     string
	URL         string
	SourceID    string
	ContentHash string
	PublishedAt time.Time
	Score       float64
}

type Recommendation struct {
	ID             string       `json:"id"`
	TenantID       string       `json:"tenant_id,omitempty"`
	UserID         string       `json:"user_id"`
	ProjectID      string       `json:"project_id"`
	ItemID         string       `json:"item_id"`
	Title          string       `json:"title"`
	Summary        string       `json:"summary"`
	Why            string       `json:"why"`
	PathReadable   string       `json:"path_readable,omitempty"`
	Score          float64      `json:"score"`
	RankingVersion string       `json:"ranking_version"`
	QueryName      string       `json:"query_name"`
	Path           EvidencePath `json:"path"`
	Evidence       Evidence     `json:"evidence"`
	CreatedAt      time.Time    `json:"created_at"`
}
