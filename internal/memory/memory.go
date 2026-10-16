package memory

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

// Type identifies the kind of durable context being stored. Keeping the type
// explicit lets retrieval order and downstream tools evolve without treating
// every memory as an opaque transcript.
type Type string

const (
	Episodic    Type = "episodic"
	Semantic    Type = "semantic"
	Procedural  Type = "procedural"
	Operational Type = "operational"
)

func (t Type) Valid() bool {
	switch t {
	case Episodic, Semantic, Procedural, Operational:
		return true
	default:
		return false
	}
}

var (
	ErrInvalidMemory       = errors.New("invalid memory")
	ErrInvalidQuery        = errors.New("invalid memory retrieval query")
	ErrMemoryNotFound      = errors.New("memory not found")
	ErrInvalidSupersession = errors.New("invalid memory supersession")
)

// Memory is an append-oriented durable fact. A replacement keeps the prior
// record and points to it through SupersedesID; repositories persist that as a
// SUPERSEDES relationship rather than overwriting history.
type Memory struct {
	ID           string            `json:"id"`
	TenantID     string            `json:"tenant_id"`
	UserID       string            `json:"user_id"`
	SessionID    string            `json:"session_id,omitempty"`
	TaskID       string            `json:"task_id,omitempty"`
	ProjectID    string            `json:"project_id,omitempty"`
	Type         Type              `json:"type"`
	Content      string            `json:"content"`
	Summary      string            `json:"summary,omitempty"`
	EvidenceIDs  []string          `json:"evidence_ids,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	SupersedesID string            `json:"supersedes_id,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// RetrievalQuery deliberately exposes only bounded, typed filters. It is not
// a Cypher fragment and cannot be used to select labels or clauses.
type RetrievalQuery struct {
	TenantID          string
	UserID            string
	SessionID         string
	TaskID            string
	ProjectID         string
	Types             []Type
	Limit             int
	MaxBytes          int
	IncludeSuperseded bool
}

// Repository is the persistence boundary for memory. The Supersede method is
// intentionally separate from Save so a graph implementation can persist the
// replacement and SUPERSEDES edge as one bounded operation.
type Repository interface {
	Save(context.Context, Memory) error
	Supersede(context.Context, string, Memory) error
	Get(context.Context, string, string) (Memory, error)
	Retrieve(context.Context, RetrievalQuery) ([]Memory, error)
}

// MemoryRepository is an explicit alias useful to callers that prefer the
// domain name over the generic repository name.
type MemoryRepository = Repository

type Service struct {
	repository Repository
	now        func() time.Time
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository, now: func() time.Time { return time.Now().UTC() }}
}

// Remember validates and appends a memory. A caller-provided ID is preferred;
// otherwise a stable content ID makes replaying the same request harmless for
// repositories that use MERGE.
func (s *Service) Remember(ctx context.Context, item Memory) (Memory, error) {
	if s == nil || s.repository == nil {
		return Memory{}, fmt.Errorf("%w: repository is required", ErrInvalidMemory)
	}
	item = cloneMemory(item)
	if item.ID == "" {
		item.ID = stableID(item)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = s.now().UTC()
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = item.CreatedAt
	}
	if err := validateMemory(item); err != nil {
		return Memory{}, err
	}
	if err := s.repository.Save(ctx, item); err != nil {
		return Memory{}, fmt.Errorf("save memory %q: %w", item.ID, err)
	}
	return item, nil
}

// Evolve appends a typed replacement while preserving the previous memory.
// The old record must exist in the same user scope and the type cannot change
// silently; a caller that wants a different type should create a new memory.
func (s *Service) Evolve(ctx context.Context, previousID string, replacement Memory) (Memory, error) {
	if s == nil || s.repository == nil {
		return Memory{}, fmt.Errorf("%w: repository is required", ErrInvalidSupersession)
	}
	if strings.TrimSpace(previousID) == "" {
		return Memory{}, fmt.Errorf("%w: previous memory ID is required", ErrInvalidSupersession)
	}
	if strings.TrimSpace(replacement.UserID) == "" {
		return Memory{}, fmt.Errorf("%w: replacement user ID is required", ErrInvalidSupersession)
	}
	previous, err := s.repository.Get(ctx, replacement.UserID, previousID)
	if err != nil {
		return Memory{}, fmt.Errorf("%w: %s: %v", ErrInvalidSupersession, previousID, err)
	}
	if replacement.TenantID == "" {
		replacement.TenantID = previous.TenantID
	}
	if replacement.Type == "" {
		replacement.Type = previous.Type
	}
	if replacement.Type != previous.Type {
		return Memory{}, fmt.Errorf("%w: type %q cannot replace %q", ErrInvalidSupersession, replacement.Type, previous.Type)
	}
	replacement.SupersedesID = previousID
	if replacement.ID == "" {
		replacement.ID = stableID(replacement)
	}
	if replacement.CreatedAt.IsZero() {
		replacement.CreatedAt = s.now().UTC()
	}
	if replacement.UpdatedAt.IsZero() {
		replacement.UpdatedAt = replacement.CreatedAt
	}
	if err := validateMemory(replacement); err != nil {
		return Memory{}, err
	}
	if err := s.repository.Supersede(ctx, previousID, replacement); err != nil {
		return Memory{}, fmt.Errorf("supersede memory %q with %q: %w", previousID, replacement.ID, err)
	}
	return cloneMemory(replacement), nil
}

func (s *Service) Get(ctx context.Context, userID, id string) (Memory, error) {
	if s == nil || s.repository == nil {
		return Memory{}, fmt.Errorf("%w: repository is required", ErrMemoryNotFound)
	}
	if strings.TrimSpace(userID) == "" || strings.TrimSpace(id) == "" {
		return Memory{}, fmt.Errorf("%w: user and memory IDs are required", ErrMemoryNotFound)
	}
	item, err := s.repository.Get(ctx, userID, id)
	if err != nil {
		return Memory{}, fmt.Errorf("get memory %q: %w", id, err)
	}
	return cloneMemory(item), nil
}

func (s *Service) Retrieve(ctx context.Context, query RetrievalQuery) ([]Memory, error) {
	if s == nil || s.repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidQuery)
	}
	query = normalizeQuery(query)
	if err := validateQuery(query); err != nil {
		return nil, err
	}
	items, err := s.repository.Retrieve(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("retrieve memories: %w", err)
	}
	result := make([]Memory, 0, len(items))
	bytes := 0
	for _, item := range items {
		item = cloneMemory(item)
		itemBytes := len(item.Content) + len(item.Summary)
		for _, evidenceID := range item.EvidenceIDs {
			itemBytes += len(evidenceID)
		}
		for key, value := range item.Metadata {
			itemBytes += len(key) + len(value)
		}
		if len(result) >= query.Limit || bytes+itemBytes > query.MaxBytes {
			break
		}
		result = append(result, item)
		bytes += itemBytes
	}
	return result, nil
}

func normalizeQuery(query RetrievalQuery) RetrievalQuery {
	if query.Limit == 0 {
		query.Limit = 20
	}
	if query.MaxBytes == 0 {
		query.MaxBytes = 64 * 1024
	}
	query.Types = append([]Type(nil), query.Types...)
	sort.Slice(query.Types, func(i, j int) bool { return query.Types[i] < query.Types[j] })
	return query
}

func validateQuery(query RetrievalQuery) error {
	if strings.TrimSpace(query.UserID) == "" {
		return fmt.Errorf("%w: user ID is required", ErrInvalidQuery)
	}
	if query.Limit < 1 || query.Limit > 100 {
		return fmt.Errorf("%w: limit must be between 1 and 100", ErrInvalidQuery)
	}
	if query.MaxBytes < 1 || query.MaxBytes > 1024*1024 {
		return fmt.Errorf("%w: max bytes must be between 1 and 1048576", ErrInvalidQuery)
	}
	for _, memoryType := range query.Types {
		if !memoryType.Valid() {
			return fmt.Errorf("%w: unknown memory type %q", ErrInvalidQuery, memoryType)
		}
	}
	return nil
}

func validateMemory(item Memory) error {
	if strings.TrimSpace(item.ID) == "" || strings.TrimSpace(item.UserID) == "" {
		return fmt.Errorf("%w: ID and user ID are required", ErrInvalidMemory)
	}
	if !item.Type.Valid() {
		return fmt.Errorf("%w: unknown memory type %q", ErrInvalidMemory, item.Type)
	}
	if strings.TrimSpace(item.Content) == "" {
		return fmt.Errorf("%w: content is required", ErrInvalidMemory)
	}
	if item.SupersedesID == item.ID {
		return fmt.Errorf("%w: a memory cannot supersede itself", ErrInvalidMemory)
	}
	seenEvidence := make(map[string]struct{}, len(item.EvidenceIDs))
	for _, evidenceID := range item.EvidenceIDs {
		if strings.TrimSpace(evidenceID) == "" {
			return fmt.Errorf("%w: evidence IDs cannot be empty", ErrInvalidMemory)
		}
		if _, ok := seenEvidence[evidenceID]; ok {
			return fmt.Errorf("%w: duplicate evidence ID %q", ErrInvalidMemory, evidenceID)
		}
		seenEvidence[evidenceID] = struct{}{}
	}
	return nil
}

func stableID(item Memory) string {
	hash := sha256.New()
	for _, value := range []string{item.UserID, item.SessionID, item.TaskID, item.ProjectID, string(item.Type), item.Content, item.SupersedesID} {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return "memory_" + hex.EncodeToString(hash.Sum(nil))[:24]
}

func cloneMemory(item Memory) Memory {
	item.EvidenceIDs = append([]string(nil), item.EvidenceIDs...)
	if item.Metadata != nil {
		item.Metadata = make(map[string]string, len(item.Metadata))
		for key, value := range item.Metadata {
			item.Metadata[key] = value
		}
	}
	return item
}
