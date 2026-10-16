// Package coordination contains the small contract used by graph-backed
// workers to claim work and make retries safe. Implementations belong at the
// storage boundary; FalkorDB is the production implementation.
package coordination

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

var (
	ErrLeaseUnavailable = errors.New("coordination lease unavailable")
	ErrLeaseLost        = errors.New("coordination lease lost")
	ErrInvalidRequest   = errors.New("invalid coordination request")
)

// LeaseRequest describes an atomic claim. The repository must treat Key as a
// unique graph coordination key and may replace only an expired lease.
type LeaseRequest struct {
	Key      string
	Owner    string
	Token    string
	Now      time.Time
	Lifetime time.Duration
}

// IdempotencyRecord is the durable result of a completed operation.
type IdempotencyRecord struct {
	Key      string
	Token    string
	Payload  []byte
	StoredAt time.Time
}

// IdempotencyWrite is accepted only while the caller still owns the lease.
// This prevents a stale worker from publishing a result after lease recovery.
type IdempotencyWrite struct {
	Key      string
	Token    string
	Payload  []byte
	StoredAt time.Time
}

// Repository is the storage seam for coordination. A FalkorDB implementation
// should make AcquireLease and StoreIdempotency single graph transactions (or
// equivalent atomic Cypher operations). Tests can use an in-memory fake
// without changing the production authority boundary.
type Repository interface {
	FindIdempotency(context.Context, string) (IdempotencyRecord, bool, error)
	AcquireLease(context.Context, LeaseRequest) (bool, error)
	StoreIdempotency(context.Context, IdempotencyWrite) (bool, error)
	ReleaseLease(context.Context, string, string) error
}

type Execution struct {
	Key       string
	Payload   []byte
	Completed bool
	Executed  bool
}

type Manager struct {
	repository Repository
	now        func() time.Time
	token      func() (string, error)
}

func NewManager(repository Repository) *Manager {
	return &Manager{
		repository: repository,
		now:        time.Now,
		token:      newToken,
	}
}

// Execute runs work once for an idempotency key when a lease can be claimed.
// A later retry receives the original payload without invoking work again.
// Concurrent callers that arrive before the winner stores its result receive
// ErrLeaseUnavailable and can retry with their normal bounded backoff.
func (m *Manager) Execute(ctx context.Context, key, owner string, lifetime time.Duration, work func(context.Context) ([]byte, error)) (Execution, error) {
	if m == nil || m.repository == nil || key == "" || owner == "" || lifetime <= 0 || work == nil {
		return Execution{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return Execution{}, err
	}

	if record, found, err := m.repository.FindIdempotency(ctx, key); err != nil {
		return Execution{}, fmt.Errorf("find idempotency record: %w", err)
	} else if found {
		return Execution{Key: key, Payload: cloneBytes(record.Payload), Completed: true}, nil
	}

	token, err := m.token()
	if err != nil {
		return Execution{}, fmt.Errorf("create lease token: %w", err)
	}
	acquired, err := m.repository.AcquireLease(ctx, LeaseRequest{
		Key:      key,
		Owner:    owner,
		Token:    token,
		Now:      m.now(),
		Lifetime: lifetime,
	})
	if err != nil {
		return Execution{}, fmt.Errorf("acquire lease: %w", err)
	}
	if !acquired {
		return Execution{}, ErrLeaseUnavailable
	}

	// A lease can be reclaimed between the first read and the claim. Recheck
	// after claiming so a recovered worker returns the completed result instead
	// of performing duplicate work.
	if record, found, err := m.repository.FindIdempotency(ctx, key); err != nil {
		_ = m.repository.ReleaseLease(ctx, key, token)
		return Execution{}, fmt.Errorf("recheck idempotency record: %w", err)
	} else if found {
		if err := m.repository.ReleaseLease(ctx, key, token); err != nil {
			return Execution{}, fmt.Errorf("release completed lease: %w", err)
		}
		return Execution{Key: key, Payload: cloneBytes(record.Payload), Completed: true}, nil
	}

	payload, workErr := work(ctx)
	if workErr != nil {
		if releaseErr := m.repository.ReleaseLease(ctx, key, token); releaseErr != nil {
			return Execution{}, fmt.Errorf("work failed: %v; release lease: %w", workErr, releaseErr)
		}
		return Execution{}, workErr
	}

	stored, err := m.repository.StoreIdempotency(ctx, IdempotencyWrite{
		Key:      key,
		Token:    token,
		Payload:  cloneBytes(payload),
		StoredAt: m.now(),
	})
	if err != nil {
		_ = m.repository.ReleaseLease(ctx, key, token)
		return Execution{}, fmt.Errorf("store idempotency record: %w", err)
	}
	if !stored {
		_ = m.repository.ReleaseLease(ctx, key, token)
		return Execution{}, ErrLeaseLost
	}
	if err := m.repository.ReleaseLease(ctx, key, token); err != nil {
		return Execution{}, fmt.Errorf("release lease after completion: %w", err)
	}
	return Execution{Key: key, Payload: cloneBytes(payload), Completed: true, Executed: true}, nil
}

func newToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
