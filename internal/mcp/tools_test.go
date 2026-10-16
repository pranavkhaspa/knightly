package mcp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pranavkhaspa/knightly/internal/memory"
)

type fakeMemoryReader struct {
	items []memory.Memory
	query memory.RetrievalQuery
}

func (f *fakeMemoryReader) Retrieve(_ context.Context, query memory.RetrievalQuery) ([]memory.Memory, error) {
	f.query = query
	return append([]memory.Memory(nil), f.items...), nil
}

type fakeCallRepository struct {
	started   []ToolCall
	completed []ToolCall
}

func (f *fakeCallRepository) Record(_ context.Context, call ToolCall) error {
	f.started = append(f.started, call)
	return nil
}

func (f *fakeCallRepository) Complete(_ context.Context, call ToolCall) error {
	f.completed = append(f.completed, call)
	return nil
}

func TestRetrieveMemoryIsTypedAndRecordsToolCall(t *testing.T) {
	reader := &fakeMemoryReader{items: []memory.Memory{{ID: "m1", UserID: "u1", Type: memory.Semantic, Content: "graph context"}}}
	calls := &fakeCallRepository{}
	server := NewServer(Dependencies{
		Memory: reader,
		Calls:  calls,
		Now: func() time.Time {
			return time.Date(2026, 10, 16, 13, 0, 0, 0, time.UTC)
		},
	})

	output, err := server.Invoke(context.Background(), RetrieveMemory, MemoryRetrieveInput{
		Meta:      RequestMeta{CallID: "call_1", RequestID: "request_1", UserID: "u1", SessionID: "session_1"},
		ProjectID: "project_1",
		Types:     []memory.Type{memory.Semantic},
		Limit:     5,
		MaxBytes:  2048,
	})
	if err != nil {
		t.Fatalf("invoke memory.retrieve: %v", err)
	}
	result, ok := output.(MemoryRetrieveOutput)
	if !ok || len(result.Memories) != 1 || result.Memories[0].ID != "m1" {
		t.Fatalf("typed output = %#v", output)
	}
	if reader.query.UserID != "u1" || reader.query.ProjectID != "project_1" || reader.query.Limit != 5 {
		t.Fatalf("typed query = %#v", reader.query)
	}
	if len(calls.started) != 1 || calls.started[0].Name != RetrieveMemory || calls.started[0].InputType != "MemoryRetrieveInput" {
		t.Fatalf("started calls = %#v", calls.started)
	}
	if len(calls.completed) != 1 || calls.completed[0].Status != CallCompleted {
		t.Fatalf("completed calls = %#v", calls.completed)
	}
}

func TestInvokeRejectsUnknownToolsAndWrongInputTypes(t *testing.T) {
	server := NewServer(Dependencies{Calls: &fakeCallRepository{}})
	_, err := server.Invoke(context.Background(), ToolName("context.arbitrary_cypher"), SearchRelevantItemsInput{})
	if !errors.Is(err, ErrUnknownTool) {
		t.Fatalf("unknown tool error = %v, want ErrUnknownTool", err)
	}
	_, err = server.Invoke(context.Background(), RetrieveMemory, SearchRelevantItemsInput{})
	if !errors.Is(err, ErrInvalidToolInput) {
		t.Fatalf("wrong input error = %v, want ErrInvalidToolInput", err)
	}
}
