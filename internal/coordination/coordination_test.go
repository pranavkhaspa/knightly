package coordination

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaseHarnessExecutesConcurrentKeyOnce(t *testing.T) {
	repository := newHarnessRepository()
	manager := NewManager(repository)
	const callers = 32
	start := make(chan struct{})
	var executed atomic.Int32
	results := make(chan error, callers)

	for index := 0; index < callers; index++ {
		go func(index int) {
			<-start
			_, err := manager.Execute(context.Background(), "action:demo", fmt.Sprintf("instance-%d", index), time.Minute, func(context.Context) ([]byte, error) {
				executed.Add(1)
				time.Sleep(20 * time.Millisecond)
				return []byte("falkordb-result"), nil
			})
			if err != nil && !errors.Is(err, ErrLeaseUnavailable) {
				results <- err
				return
			}
			results <- nil
		}(index)
	}
	close(start)
	for index := 0; index < callers; index++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent execution returned unexpected error: %v", err)
		}
	}

	if got := executed.Load(); got != 1 {
		t.Fatalf("work executed %d times, want exactly once", got)
	}

	var retryCalls atomic.Int32
	execution, err := manager.Execute(context.Background(), "action:demo", "retrying-instance", time.Minute, func(context.Context) ([]byte, error) {
		retryCalls.Add(1)
		return nil, errors.New("retry must use the stored result")
	})
	if err != nil {
		t.Fatalf("idempotent retry returned error: %v", err)
	}
	if string(execution.Payload) != "falkordb-result" || !execution.Completed || execution.Executed {
		t.Fatalf("unexpected retry result: %+v", execution)
	}
	if retryCalls.Load() != 0 {
		t.Fatal("idempotent retry invoked work")
	}
}

func TestLeaseHarnessReleasesFailedWorkForRetry(t *testing.T) {
	repository := newHarnessRepository()
	manager := NewManager(repository)
	firstErr := errors.New("temporary provider failure")

	if _, err := manager.Execute(context.Background(), "fetch:demo", "instance-a", time.Minute, func(context.Context) ([]byte, error) {
		return nil, firstErr
	}); !errors.Is(err, firstErr) {
		t.Fatalf("first execution error = %v, want %v", err, firstErr)
	}

	var calls atomic.Int32
	execution, err := manager.Execute(context.Background(), "fetch:demo", "instance-b", time.Minute, func(context.Context) ([]byte, error) {
		calls.Add(1)
		return []byte("recovered"), nil
	})
	if err != nil {
		t.Fatalf("retry returned error: %v", err)
	}
	if !execution.Executed || string(execution.Payload) != "recovered" || calls.Load() != 1 {
		t.Fatalf("unexpected recovered execution: %+v calls=%d", execution, calls.Load())
	}
}

func TestLeaseRepositoryRejectsStaleOwnerAndPreservesResult(t *testing.T) {
	repository := newHarnessRepository()
	now := time.Unix(100, 0)
	first := LeaseRequest{Key: "task:1", Owner: "one", Token: "token-one", Now: now, Lifetime: time.Second}
	second := LeaseRequest{Key: "task:1", Owner: "two", Token: "token-two", Now: now, Lifetime: time.Second}

	claimed, err := repository.AcquireLease(context.Background(), first)
	if err != nil || !claimed {
		t.Fatalf("first lease claimed=%v err=%v", claimed, err)
	}
	claimed, err = repository.AcquireLease(context.Background(), second)
	if err != nil || claimed {
		t.Fatalf("second lease claimed=%v err=%v, want rejection", claimed, err)
	}
	stored, err := repository.StoreIdempotency(context.Background(), IdempotencyWrite{Key: "task:1", Token: "token-two", Payload: []byte("stale"), StoredAt: now})
	if err != nil || stored {
		t.Fatalf("stale owner stored=%v err=%v", stored, err)
	}
	stored, err = repository.StoreIdempotency(context.Background(), IdempotencyWrite{Key: "task:1", Token: "token-one", Payload: []byte("done"), StoredAt: now})
	if err != nil || !stored {
		t.Fatalf("lease owner stored=%v err=%v", stored, err)
	}
	record, found, err := repository.FindIdempotency(context.Background(), "task:1")
	if err != nil || !found || string(record.Payload) != "done" {
		t.Fatalf("stored record=%+v found=%v err=%v", record, found, err)
	}
}

type harnessLease struct {
	owner     string
	token     string
	expiresAt time.Time
}

type harnessRepository struct {
	mu            sync.Mutex
	leases        map[string]harnessLease
	idempotencies map[string]IdempotencyRecord
}

func newHarnessRepository() *harnessRepository {
	return &harnessRepository{
		leases:        make(map[string]harnessLease),
		idempotencies: make(map[string]IdempotencyRecord),
	}
}

func (r *harnessRepository) FindIdempotency(_ context.Context, key string) (IdempotencyRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, found := r.idempotencies[key]
	if !found {
		return IdempotencyRecord{}, false, nil
	}
	record.Payload = cloneBytes(record.Payload)
	return record, true, nil
}

func (r *harnessRepository) AcquireLease(_ context.Context, request LeaseRequest) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lease, found := r.leases[request.Key]; found && lease.expiresAt.After(request.Now) {
		return false, nil
	}
	r.leases[request.Key] = harnessLease{owner: request.Owner, token: request.Token, expiresAt: request.Now.Add(request.Lifetime)}
	return true, nil
}

func (r *harnessRepository) StoreIdempotency(_ context.Context, write IdempotencyWrite) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, found := r.leases[write.Key]
	if !found || lease.token != write.Token {
		return false, nil
	}
	if _, exists := r.idempotencies[write.Key]; exists {
		return false, nil
	}
	r.idempotencies[write.Key] = IdempotencyRecord{Key: write.Key, Token: write.Token, Payload: cloneBytes(write.Payload), StoredAt: write.StoredAt}
	return true, nil
}

func (r *harnessRepository) ReleaseLease(_ context.Context, key, token string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lease, found := r.leases[key]; found && lease.token == token {
		delete(r.leases, key)
	}
	return nil
}
