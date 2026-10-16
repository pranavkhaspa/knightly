package recommendation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// GraphRepository is the only dependency needed to turn candidates into
// recommendations. The path lookup is intentionally mandatory: callers cannot
// substitute local keyword matching for graph evidence.
type GraphRepository interface {
	FindRecommendationPath(ctx context.Context, projectID, itemID string) (EvidencePath, error)
	SaveRecommendation(ctx context.Context, recommendation Recommendation) error
}

type Service struct {
	Graph              GraphRepository
	MaxRecommendations int
	Now                func() time.Time
	RankingVersion     string
}

func NewService(graph GraphRepository) *Service {
	return &Service{
		Graph:              graph,
		MaxRecommendations: 10,
		Now:                func() time.Time { return time.Now().UTC() },
		RankingVersion:     RankingVersion,
	}
}

// Recommend performs a bounded graph path lookup for each candidate and stores
// only candidates whose path was returned and validated. A missing path is an
// expected non-recommendation, not an error.
func (s *Service) Recommend(ctx context.Context, userID, projectID string, candidates []Candidate) ([]Recommendation, error) {
	if s == nil || s.Graph == nil {
		return nil, errors.New("graph repository is required")
	}
	if strings.TrimSpace(userID) == "" || strings.TrimSpace(projectID) == "" {
		return nil, errors.New("user ID and project ID are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	limit := s.MaxRecommendations
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	rankingVersion := s.RankingVersion
	if rankingVersion == "" {
		rankingVersion = RankingVersion
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}

	recommendations := make([]Recommendation, 0, min(len(candidates), limit))
	seenItems := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if len(recommendations) == limit {
			break
		}
		if candidate.ItemID == "" {
			continue
		}
		if _, seen := seenItems[candidate.ItemID]; seen {
			continue
		}
		seenItems[candidate.ItemID] = struct{}{}

		path, err := s.Graph.FindRecommendationPath(ctx, projectID, candidate.ItemID)
		if err != nil {
			return recommendations, fmt.Errorf("find graph path for item %q: %w", candidate.ItemID, err)
		}
		if !path.ValidFor(projectID, candidate.ItemID) {
			continue
		}

		recommendation := Recommendation{
			ID:             stableRecommendationID(projectID, candidate.ItemID),
			UserID:         userID,
			ProjectID:      projectID,
			ItemID:         candidate.ItemID,
			Title:          candidate.Title,
			Summary:        candidate.Summary,
			Why:            "Graph path: " + path.Readable(),
			PathReadable:   path.Readable(),
			Score:          candidate.Score,
			RankingVersion: rankingVersion,
			QueryName:      QueryName,
			Path:           path,
			Evidence: Evidence{
				ID:          stableEvidenceID(candidate.ItemID),
				SourceID:    path.SourceID,
				ItemID:      candidate.ItemID,
				URL:         firstNonEmpty(candidate.URL, path.SourceURL()),
				Excerpt:     boundedExcerpt(candidate.Summary),
				Kind:        "source_item",
				ContentHash: candidate.ContentHash,
				ObservedAt:  now,
			},
			CreatedAt: now,
		}
		if recommendation.Score <= 0 {
			recommendation.Score = 1
		}
		if err := s.Graph.SaveRecommendation(ctx, recommendation); err != nil {
			return recommendations, fmt.Errorf("save recommendation for item %q: %w", candidate.ItemID, err)
		}
		recommendations = append(recommendations, recommendation)
	}
	return recommendations, nil
}

func stableRecommendationID(projectID, itemID string) string {
	sum := sha256.Sum256([]byte(projectID + "\x00" + itemID))
	return "recommendation_" + hex.EncodeToString(sum[:])
}

func stableEvidenceID(itemID string) string {
	sum := sha256.Sum256([]byte("source_item\x00" + itemID))
	return "evidence_" + hex.EncodeToString(sum[:])
}

func boundedExcerpt(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= 1024 {
		return value
	}
	return value[:1024]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
