package falkor

import (
	"strings"
	"testing"

	dbclient "github.com/FalkorDB/falkordb-go/v2"
)

func TestParsePathNodesPreservesGraphOrderAndEvidenceProperties(t *testing.T) {
	raw := []interface{}{
		dbclient.NodeNew([]string{"Project"}, "", map[string]interface{}{"id": "project_nightshift", "name": "NightShift"}),
		dbclient.NodeNew([]string{"Requirement"}, "", map[string]interface{}{"id": "requirement_graph_context", "title": "Explainable recommendations"}),
		dbclient.NodeNew([]string{"Technology"}, "", map[string]interface{}{"id": "technology_falkordb", "name": "FalkorDB"}),
		dbclient.NodeNew([]string{"Item"}, "", map[string]interface{}{"id": "item_fixture", "title": "Graph paths"}),
		dbclient.NodeNew([]string{"Source"}, "", map[string]interface{}{"id": "source_fixture", "name": "Fixture", "url": "https://example.invalid/feed"}),
	}
	nodes, err := parsePathNodes(raw)
	if err != nil {
		t.Fatalf("parsePathNodes() error = %v", err)
	}
	if len(nodes) != 5 || nodes[2].ID != "technology_falkordb" || nodes[4].URL != "https://example.invalid/feed" {
		t.Fatalf("unexpected parsed path nodes: %+v", nodes)
	}
}

func TestRecommendationPathQueryKeepsTheRequiredFiveHopTraversal(t *testing.T) {
	for _, fragment := range []string{
		"HAS_REQUIREMENT",
		"NEEDS",
		"MENTIONS",
		"FROM",
		"nodes(path)",
		"LIMIT 1",
	} {
		if !strings.Contains(RecommendationPathQuery, fragment) {
			t.Errorf("RecommendationPathQuery is missing %q", fragment)
		}
	}
}
