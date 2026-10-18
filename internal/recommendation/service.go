package recommendation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
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

type RankingSignalsRepository interface {
	RankingSignals(context.Context, string, string, string) (RankingSignals, error)
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
		signals := RankingSignals{}
		if repository, ok := s.Graph.(RankingSignalsRepository); ok {
			// Feedback is useful ranking context, but a historical read failure
			// must not suppress a graph-qualified recommendation.
			signals, _ = repository.RankingSignals(ctx, userID, projectID, candidate.ItemID)
		}
		score := candidate.Score
		if score < 0 {
			score = 0
		}
		// The required graph path is itself the strongest signal. Recency and
		// persisted user feedback then adjust the deterministic score without
		// requiring an LLM or an opaque keyword-only decision.
		score += 11 + freshnessScore(candidate.PublishedAt, now) + signals.FeedbackAdjustment
		if score < 0 {
			score = 0
		}

		recommendation := Recommendation{
			ID:             stableRecommendationID(projectID, candidate.ItemID),
			TenantID:       path.TenantID,
			UserID:         userID,
			ProjectID:      projectID,
			ItemID:         candidate.ItemID,
			Title:          candidate.Title,
			Summary:        candidate.Summary,
			Why:            rankingExplanation(path, score, signals),
			PathReadable:   path.Readable(),
			Score:          score,
			RankingVersion: rankingVersion,
			QueryName:      QueryName,
			Path:           path,
			Evidence: Evidence{
				ID:          stableEvidenceID(candidate.ItemID),
				TenantID:    path.TenantID,
				UserID:      userID,
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
		if err := s.Graph.SaveRecommendation(ctx, recommendation); err != nil {
			return recommendations, fmt.Errorf("save recommendation for item %q: %w", candidate.ItemID, err)
		}
		recommendations = append(recommendations, recommendation)
	}
	sort.SliceStable(recommendations, func(left, right int) bool {
		if recommendations[left].Score == recommendations[right].Score {
			return recommendations[left].ItemID < recommendations[right].ItemID
		}
		return recommendations[left].Score > recommendations[right].Score
	})
	if len(recommendations) > limit {
		recommendations = recommendations[:limit]
	}
	return recommendations, nil
}

func freshnessScore(publishedAt, now time.Time) float64 {
	if publishedAt.IsZero() || now.IsZero() || publishedAt.After(now) {
		return 0
	}
	age := now.Sub(publishedAt)
	switch {
	case age <= 24*time.Hour:
		return 2
	case age <= 7*24*time.Hour:
		return 1
	default:
		return 0
	}
}

func rankingExplanation(path EvidencePath, score float64, signals RankingSignals) string {
	feedback := "no prior feedback adjustment"
	if signals.FeedbackAdjustment > 0 {
		feedback = fmt.Sprintf("positive feedback adjustment %.1f", signals.FeedbackAdjustment)
	} else if signals.FeedbackAdjustment < 0 {
		feedback = fmt.Sprintf("negative feedback adjustment %.1f", signals.FeedbackAdjustment)
	}
	return fmt.Sprintf("Graph relationship path matched project requirements and technology; deterministic score %.1f (%s). Path: %s", score, feedback, path.Readable())
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
