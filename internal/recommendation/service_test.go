package recommendation

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeGraphRepository struct {
	paths map[string]EvidencePath
	saved []Recommendation
	err   error
}

func (f *fakeGraphRepository) FindRecommendationPath(_ context.Context, _, itemID string) (EvidencePath, error) {
	if f.err != nil {
		return EvidencePath{}, f.err
	}
	return f.paths[itemID], nil
}

func (f *fakeGraphRepository) SaveRecommendation(_ context.Context, recommendation Recommendation) error {
	f.saved = append(f.saved, recommendation)
	return nil
}

func validPath(projectID, itemID string) EvidencePath {
	return EvidencePath{
		ProjectID:     projectID,
		RequirementID: "requirement_graph_context",
		TechnologyID:  "technology_falkordb",
		ItemID:        itemID,
		SourceID:      "source_fixture_nightshift",
		Nodes: []PathNode{
			{Label: "Project", ID: projectID, Name: "NightShift"},
			{Label: "Requirement", ID: "requirement_graph_context", Name: "Explainable recommendations"},
			{Label: "Technology", ID: "technology_falkordb", Name: "FalkorDB"},
			{Label: "Item", ID: itemID, Name: "Graph paths"},
			{Label: "Source", ID: "source_fixture_nightshift", Name: "Fixture", URL: "https://example.invalid/feed"},
		},
	}
}

func TestRecommendRequiresReturnedGraphPath(t *testing.T) {
	projectID := "project_nightshift"
	graph := &fakeGraphRepository{paths: map[string]EvidencePath{
		"item_connected":   validPath(projectID, "item_connected"),
		"item_unconnected": {},
	}}
	service := NewService(graph)
	service.Now = func() time.Time { return time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC) }

	got, err := service.Recommend(context.Background(), "user_demo", projectID, []Candidate{
		{ItemID: "item_connected", Title: "Connected", Summary: "Evidence", URL: "https://example.invalid/item"},
		{ItemID: "item_unconnected", Title: "Unconnected", Summary: "No graph path"},
	})
	if err != nil {
		t.Fatalf("Recommend() error = %v", err)
	}
	if len(got) != 1 || len(graph.saved) != 1 {
		t.Fatalf("recommendations = %d, saved = %d; want one each", len(got), len(graph.saved))
	}
	if got[0].ItemID != "item_connected" || !got[0].Path.ValidFor(projectID, "item_connected") {
		t.Fatalf("recommendation did not retain the returned graph path: %+v", got[0])
	}
	if got[0].Evidence.SourceID != "source_fixture_nightshift" {
		t.Fatalf("evidence source = %q, want fixture source", got[0].Evidence.SourceID)
	}
}

func TestRecommendPropagatesGraphFailure(t *testing.T) {
	graph := &fakeGraphRepository{err: errors.New("graph unavailable")}
	service := NewService(graph)
	_, err := service.Recommend(context.Background(), "user_demo", "project_nightshift", []Candidate{{ItemID: "item_1"}})
	if err == nil {
		t.Fatal("Recommend() returned nil error for graph failure")
	}
}

func TestEvidencePathValidationChecksOrderAndIdentity(t *testing.T) {
	path := validPath("project_nightshift", "item_1")
	if !path.ValidFor("project_nightshift", "item_1") {
		t.Fatal("valid path rejected")
	}
	path.Nodes[3].Label = "Source"
	if path.ValidFor("project_nightshift", "item_1") {
		t.Fatal("path with wrong node order accepted")
	}
}
