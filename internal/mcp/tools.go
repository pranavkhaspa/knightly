package mcp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pranavkhaspa/knightly/internal/memory"
)

type ToolName string

const (
	SearchRelevantItems   ToolName = "context.search_relevant_items"
	ExplainRecommendation ToolName = "context.explain_recommendation"
	RetrieveMemory        ToolName = "memory.retrieve"
	ProposePlan           ToolName = "planner.propose_plan"
	RecordFeedback        ToolName = "feedback.record"
)

var (
	ErrInvalidToolInput        = errors.New("invalid tool input")
	ErrUnknownTool             = errors.New("unknown tool")
	ErrToolCallRepository      = errors.New("tool call repository is required")
	ErrToolProviderUnavailable = errors.New("tool provider is unavailable")
)

type RequestMeta struct {
	CallID    string `json:"call_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	TenantID  string `json:"tenant_id"`
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
}

type MemoryRetrieveInput struct {
	Meta      RequestMeta
	SessionID string
	TaskID    string
	ProjectID string
	Types     []memory.Type
	Limit     int
	MaxBytes  int
}

type MemoryRetrieveOutput struct {
	Memories []memory.Memory `json:"memories"`
}

type SearchRelevantItemsInput struct {
	Meta      RequestMeta
	ProjectID string
	Query     string
	Limit     int
}

type RelevantItem struct {
	ID            string   `json:"id"`
	Title         string   `json:"title"`
	Summary       string   `json:"summary,omitempty"`
	SourceID      string   `json:"source_id,omitempty"`
	TechnologyIDs []string `json:"technology_ids,omitempty"`
}

type SearchRelevantItemsOutput struct {
	Items []RelevantItem `json:"items"`
}

type ExplainRecommendationInput struct {
	Meta             RequestMeta
	RecommendationID string
}

type RecommendationExplanation struct {
	RecommendationID string   `json:"recommendation_id"`
	Why              string   `json:"why"`
	Path             []string `json:"path,omitempty"`
	EvidenceIDs      []string `json:"evidence_ids,omitempty"`
}

type ProposePlanInput struct {
	Meta             RequestMeta
	ProjectID        string
	RecommendationID string
	Goal             string
}

type PlanStep struct {
	ID          string `json:"id"`
	Description string `json:"description"`
}

type PlanProposal struct {
	ID               string     `json:"id"`
	ProjectID        string     `json:"project_id"`
	RecommendationID string     `json:"recommendation_id"`
	Summary          string     `json:"summary"`
	Steps            []PlanStep `json:"steps"`
	TaskID           string     `json:"task_id,omitempty"`
	ActionID         string     `json:"action_id,omitempty"`
}

type FeedbackKind string

const (
	Useful       FeedbackKind = "useful"
	NotUseful    FeedbackKind = "not_useful"
	AlreadyKnown FeedbackKind = "already_known"
	DoNotShow    FeedbackKind = "do_not_show_similar"
	SaveForLater FeedbackKind = "save_for_later"
	ActNow       FeedbackKind = "act_now"
)

func (k FeedbackKind) Valid() bool {
	switch k {
	case Useful, NotUseful, AlreadyKnown, DoNotShow, SaveForLater, ActNow:
		return true
	default:
		return false
	}
}

type FeedbackInput struct {
	Meta             RequestMeta
	RecommendationID string
	Kind             FeedbackKind
	Note             string
}

type FeedbackRecord struct {
	ID               string       `json:"id"`
	RecommendationID string       `json:"recommendation_id"`
	Kind             FeedbackKind `json:"kind"`
	Note             string       `json:"note,omitempty"`
	RecordedAt       time.Time    `json:"recorded_at"`
}

type ContextProvider interface {
	SearchRelevantItems(context.Context, SearchRelevantItemsInput) (SearchRelevantItemsOutput, error)
	ExplainRecommendation(context.Context, ExplainRecommendationInput) (RecommendationExplanation, error)
}

type Planner interface {
	ProposePlan(context.Context, ProposePlanInput) (PlanProposal, error)
}

type FeedbackRecorder interface {
	RecordFeedback(context.Context, FeedbackInput) (FeedbackRecord, error)
}

type MemoryReader interface {
	Retrieve(context.Context, memory.RetrievalQuery) ([]memory.Memory, error)
}

type CallStatus string

const (
	CallStarted   CallStatus = "started"
	CallCompleted CallStatus = "completed"
	CallFailed    CallStatus = "failed"
)

type ToolCall struct {
	ID          string     `json:"id"`
	Name        ToolName   `json:"name"`
	RequestID   string     `json:"request_id,omitempty"`
	TenantID    string     `json:"tenant_id"`
	UserID      string     `json:"user_id"`
	SessionID   string     `json:"session_id,omitempty"`
	TaskID      string     `json:"task_id,omitempty"`
	InputType   string     `json:"input_type"`
	Status      CallStatus `json:"status"`
	Error       string     `json:"error,omitempty"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt time.Time  `json:"completed_at,omitempty"`
}

type ToolCallRepository interface {
	Record(context.Context, ToolCall) error
	Complete(context.Context, ToolCall) error
}

type Dependencies struct {
	Memory   MemoryReader
	Context  ContextProvider
	Planner  Planner
	Feedback FeedbackRecorder
	Calls    ToolCallRepository
	Now      func() time.Time
}

type Server struct {
	memory   MemoryReader
	context  ContextProvider
	planner  Planner
	feedback FeedbackRecorder
	calls    ToolCallRepository
	now      func() time.Time
}

var callSequence uint64

func NewServer(dependencies Dependencies) *Server {
	now := dependencies.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Server{
		memory:   dependencies.Memory,
		context:  dependencies.Context,
		planner:  dependencies.Planner,
		feedback: dependencies.Feedback,
		calls:    dependencies.Calls,
		now:      now,
	}
}

func (s *Server) RetrieveMemory(ctx context.Context, input MemoryRetrieveInput) (MemoryRetrieveOutput, error) {
	if err := validateMeta(input.Meta); err != nil {
		return MemoryRetrieveOutput{}, err
	}
	if s == nil {
		return MemoryRetrieveOutput{}, ErrToolProviderUnavailable
	}
	query := memory.RetrievalQuery{
		TenantID:  input.Meta.TenantID,
		UserID:    input.Meta.UserID,
		SessionID: input.SessionID,
		TaskID:    input.TaskID,
		ProjectID: input.ProjectID,
		Types:     append([]memory.Type(nil), input.Types...),
		Limit:     input.Limit,
		MaxBytes:  input.MaxBytes,
	}
	callMeta := input.Meta
	if callMeta.SessionID == "" {
		callMeta.SessionID = input.SessionID
	}
	if callMeta.TaskID == "" {
		callMeta.TaskID = input.TaskID
	}
	var output MemoryRetrieveOutput
	result, err := s.run(ctx, RetrieveMemory, callMeta, "MemoryRetrieveInput", func() (interface{}, error) {
		if s.memory == nil {
			return nil, ErrToolProviderUnavailable
		}
		items, retrieveErr := s.memory.Retrieve(ctx, query)
		if retrieveErr != nil {
			return nil, retrieveErr
		}
		output = MemoryRetrieveOutput{Memories: items}
		return output, nil
	})
	if err != nil {
		return MemoryRetrieveOutput{}, err
	}
	return result.(MemoryRetrieveOutput), nil
}

func (s *Server) SearchItems(ctx context.Context, input SearchRelevantItemsInput) (SearchRelevantItemsOutput, error) {
	if err := validateMeta(input.Meta); err != nil {
		return SearchRelevantItemsOutput{}, err
	}
	if strings.TrimSpace(input.ProjectID) == "" || strings.TrimSpace(input.Query) == "" {
		return SearchRelevantItemsOutput{}, fmt.Errorf("%w: project ID and query are required", ErrInvalidToolInput)
	}
	if s == nil {
		return SearchRelevantItemsOutput{}, ErrToolProviderUnavailable
	}
	result, err := s.run(ctx, SearchRelevantItems, input.Meta, "SearchRelevantItemsInput", func() (interface{}, error) {
		if s.context == nil {
			return nil, ErrToolProviderUnavailable
		}
		return s.context.SearchRelevantItems(ctx, input)
	})
	if err != nil {
		return SearchRelevantItemsOutput{}, err
	}
	return result.(SearchRelevantItemsOutput), nil
}

func (s *Server) Explain(ctx context.Context, input ExplainRecommendationInput) (RecommendationExplanation, error) {
	if err := validateMeta(input.Meta); err != nil {
		return RecommendationExplanation{}, err
	}
	if strings.TrimSpace(input.RecommendationID) == "" {
		return RecommendationExplanation{}, fmt.Errorf("%w: recommendation ID is required", ErrInvalidToolInput)
	}
	if s == nil {
		return RecommendationExplanation{}, ErrToolProviderUnavailable
	}
	result, err := s.run(ctx, ExplainRecommendation, input.Meta, "ExplainRecommendationInput", func() (interface{}, error) {
		if s.context == nil {
			return nil, ErrToolProviderUnavailable
		}
		return s.context.ExplainRecommendation(ctx, input)
	})
	if err != nil {
		return RecommendationExplanation{}, err
	}
	return result.(RecommendationExplanation), nil
}

func (s *Server) Propose(ctx context.Context, input ProposePlanInput) (PlanProposal, error) {
	if err := validateMeta(input.Meta); err != nil {
		return PlanProposal{}, err
	}
	if strings.TrimSpace(input.ProjectID) == "" || strings.TrimSpace(input.RecommendationID) == "" || strings.TrimSpace(input.Goal) == "" {
		return PlanProposal{}, fmt.Errorf("%w: project, recommendation, and goal are required", ErrInvalidToolInput)
	}
	if s == nil {
		return PlanProposal{}, ErrToolProviderUnavailable
	}
	result, err := s.run(ctx, ProposePlan, input.Meta, "ProposePlanInput", func() (interface{}, error) {
		if s.planner == nil {
			return nil, ErrToolProviderUnavailable
		}
		return s.planner.ProposePlan(ctx, input)
	})
	if err != nil {
		return PlanProposal{}, err
	}
	return result.(PlanProposal), nil
}

func (s *Server) Record(ctx context.Context, input FeedbackInput) (FeedbackRecord, error) {
	if err := validateMeta(input.Meta); err != nil {
		return FeedbackRecord{}, err
	}
	if strings.TrimSpace(input.RecommendationID) == "" || !input.Kind.Valid() {
		return FeedbackRecord{}, fmt.Errorf("%w: recommendation ID and valid feedback kind are required", ErrInvalidToolInput)
	}
	if s == nil {
		return FeedbackRecord{}, ErrToolProviderUnavailable
	}
	result, err := s.run(ctx, RecordFeedback, input.Meta, "FeedbackInput", func() (interface{}, error) {
		if s.feedback == nil {
			return nil, ErrToolProviderUnavailable
		}
		return s.feedback.RecordFeedback(ctx, input)
	})
	if err != nil {
		return FeedbackRecord{}, err
	}
	return result.(FeedbackRecord), nil
}

// Invoke is the only dynamic-looking entry point. The switch accepts exactly
// the five declared typed input structs; there is no map-to-Cypher or shell
// escape hatch.
func (s *Server) Invoke(ctx context.Context, name ToolName, input interface{}) (interface{}, error) {
	switch name {
	case SearchRelevantItems:
		request, ok := input.(SearchRelevantItemsInput)
		if !ok {
			return nil, fmt.Errorf("%w: %s expects SearchRelevantItemsInput", ErrInvalidToolInput, name)
		}
		return s.SearchItems(ctx, request)
	case ExplainRecommendation:
		request, ok := input.(ExplainRecommendationInput)
		if !ok {
			return nil, fmt.Errorf("%w: %s expects ExplainRecommendationInput", ErrInvalidToolInput, name)
		}
		return s.Explain(ctx, request)
	case RetrieveMemory:
		request, ok := input.(MemoryRetrieveInput)
		if !ok {
			return nil, fmt.Errorf("%w: %s expects MemoryRetrieveInput", ErrInvalidToolInput, name)
		}
		return s.RetrieveMemory(ctx, request)
	case ProposePlan:
		request, ok := input.(ProposePlanInput)
		if !ok {
			return nil, fmt.Errorf("%w: %s expects ProposePlanInput", ErrInvalidToolInput, name)
		}
		return s.Propose(ctx, request)
	case RecordFeedback:
		request, ok := input.(FeedbackInput)
		if !ok {
			return nil, fmt.Errorf("%w: %s expects FeedbackInput", ErrInvalidToolInput, name)
		}
		return s.Record(ctx, request)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownTool, name)
	}
}

func (s *Server) run(ctx context.Context, name ToolName, meta RequestMeta, inputType string, operation func() (interface{}, error)) (interface{}, error) {
	if s == nil || s.calls == nil {
		return nil, ErrToolCallRepository
	}
	started := s.now().UTC()
	callID := meta.CallID
	if callID == "" {
		callID = fmt.Sprintf("toolcall_%d", atomic.AddUint64(&callSequence, 1))
	}
	call := ToolCall{ID: callID, Name: name, RequestID: meta.RequestID, TenantID: meta.TenantID, UserID: meta.UserID, SessionID: meta.SessionID, TaskID: meta.TaskID, InputType: inputType, Status: CallStarted, StartedAt: started}
	if err := s.calls.Record(ctx, call); err != nil {
		return nil, fmt.Errorf("record tool call %q: %w", callID, err)
	}
	result, operationErr := operation()
	call.CompletedAt = s.now().UTC()
	call.Status = CallCompleted
	if operationErr != nil {
		call.Status = CallFailed
		call.Error = operationErr.Error()
	}
	if err := s.calls.Complete(ctx, call); err != nil {
		return nil, fmt.Errorf("complete tool call %q: %w", callID, err)
	}
	if operationErr != nil {
		return nil, operationErr
	}
	return result, nil
}

func validateMeta(meta RequestMeta) error {
	if strings.TrimSpace(meta.UserID) == "" {
		return fmt.Errorf("%w: user ID is required", ErrInvalidToolInput)
	}
	return nil
}
