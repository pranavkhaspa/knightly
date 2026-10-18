package search

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type stubProvider struct {
	mu      sync.Mutex
	calls   int
	results []struct {
		response SearchResponse
		err      error
	}
}

func (s *stubProvider) Search(context.Context, SearchRequest) (SearchResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := s.calls
	s.calls++
	if index >= len(s.results) {
		index = len(s.results) - 1
	}
	if index < 0 {
		return SearchResponse{}, nil
	}
	return s.results[index].response, s.results[index].err
}

func (s *stubProvider) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	slept   time.Duration
	sleeps  int
	blocked bool
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0).UTC()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blocked {
		return context.Canceled
	}
	c.now = c.now.Add(delay)
	c.slept += delay
	c.sleeps++
	return nil
}

func (c *fakeClock) advance(delay time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delay)
}

func (c *fakeClock) totalSlept() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.slept
}

func newTestProvider(t *testing.T, inner SearchProvider, config ResilienceConfig) *ResilientProvider {
	t.Helper()
	provider, err := NewResilientProvider(inner, config)
	if err != nil {
		t.Fatalf("NewResilientProvider() error = %v", err)
	}
	return provider
}

func disableLimits(config ResilienceConfig) ResilienceConfig {
	config.RateLimit = -1
	config.BreakerThreshold = -1
	return config
}

func TestResilientProviderRetriesRetryableStatusThenSucceeds(t *testing.T) {
	clock := newFakeClock()
	inner := &stubProvider{results: []struct {
		response SearchResponse
		err      error
	}{
		{err: &HTTPStatusError{Code: 503}},
		{err: &HTTPStatusError{Code: 429, RetryAfter: 3 * time.Second}},
		{response: SearchResponse{Query: "ok", Total: 1}},
	}}
	provider := newTestProvider(t, inner, disableLimits(ResilienceConfig{
		MaxAttempts: 3, BaseBackoff: time.Second, MaxBackoff: 5 * time.Second,
		Now: clock.Now, Sleep: clock.Sleep,
	}))
	response, err := provider.Search(context.Background(), SearchRequest{Query: "ok"})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if response.Total != 1 || inner.callCount() != 3 {
		t.Fatalf("response = %+v, calls = %d", response, inner.callCount())
	}
	// First backoff is the base, the second honors Retry-After (capped at max).
	if clock.totalSlept() != 1*time.Second+3*time.Second {
		t.Fatalf("total slept = %v", clock.totalSlept())
	}
}

func TestResilientProviderDoesNotRetryCallerErrors(t *testing.T) {
	clock := newFakeClock()
	inner := &stubProvider{results: []struct {
		response SearchResponse
		err      error
	}{{err: ErrInvalidResponse}}}
	provider := newTestProvider(t, inner, disableLimits(ResilienceConfig{
		MaxAttempts: 4, Now: clock.Now, Sleep: clock.Sleep,
	}))
	_, err := provider.Search(context.Background(), SearchRequest{Query: "ok"})
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("Search() error = %v", err)
	}
	if inner.callCount() != 1 {
		t.Fatalf("caller error was retried %d times", inner.callCount())
	}
}

func TestResilientProviderExhaustsAttemptsAndReturnsLastError(t *testing.T) {
	clock := newFakeClock()
	inner := &stubProvider{results: []struct {
		response SearchResponse
		err      error
	}{{err: &HTTPStatusError{Code: 502}}}}
	provider := newTestProvider(t, inner, disableLimits(ResilienceConfig{
		MaxAttempts: 2, BaseBackoff: 10 * time.Millisecond, Now: clock.Now, Sleep: clock.Sleep,
	}))
	_, err := provider.Search(context.Background(), SearchRequest{Query: "ok"})
	if !errors.Is(err, ErrHTTPStatus) {
		t.Fatalf("Search() error = %v", err)
	}
	if inner.callCount() != 2 {
		t.Fatalf("attempts = %d", inner.callCount())
	}
}

func TestResilientProviderCircuitBreakerOpensAndShortCircuits(t *testing.T) {
	clock := newFakeClock()
	inner := &stubProvider{results: []struct {
		response SearchResponse
		err      error
	}{{err: &HTTPStatusError{Code: 500}}}}
	provider := newTestProvider(t, inner, ResilienceConfig{
		MaxAttempts: 1, BreakerThreshold: 2, BreakerCooldown: time.Minute,
		RateLimit: -1, Now: clock.Now, Sleep: clock.Sleep,
	})
	for i := 0; i < 2; i++ {
		if _, err := provider.Search(context.Background(), SearchRequest{Query: "ok"}); err == nil {
			t.Fatal("expected provider failure")
		}
	}
	callsBefore := inner.callCount()
	_, err := provider.Search(context.Background(), SearchRequest{Query: "ok"})
	if !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("open breaker error = %v", err)
	}
	if inner.callCount() != callsBefore {
		t.Fatalf("open breaker called the provider: %d -> %d", callsBefore, inner.callCount())
	}
}

func TestResilientProviderCircuitBreakerHalfOpenRecovers(t *testing.T) {
	clock := newFakeClock()
	inner := &stubProvider{results: []struct {
		response SearchResponse
		err      error
	}{{err: &HTTPStatusError{Code: 500}}}}
	provider := newTestProvider(t, inner, ResilienceConfig{
		MaxAttempts: 1, BreakerThreshold: 1, BreakerCooldown: 10 * time.Second,
		RateLimit: -1, Now: clock.Now, Sleep: clock.Sleep,
	})
	if _, err := provider.Search(context.Background(), SearchRequest{Query: "ok"}); err == nil {
		t.Fatal("expected provider failure")
	}
	if _, err := provider.Search(context.Background(), SearchRequest{Query: "ok"}); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("open breaker error = %v", err)
	}
	// Replace the provider result with a success and advance past the cooldown.
	inner.mu.Lock()
	inner.results = []struct {
		response SearchResponse
		err      error
	}{{response: SearchResponse{Query: "ok", Total: 1}}}
	inner.calls = 0
	inner.mu.Unlock()
	clock.advance(11 * time.Second)
	if _, err := provider.Search(context.Background(), SearchRequest{Query: "ok"}); err != nil {
		t.Fatalf("half-open recovery error = %v", err)
	}
	if _, err := provider.Search(context.Background(), SearchRequest{Query: "ok"}); err != nil {
		t.Fatalf("closed breaker error = %v", err)
	}
}

func TestResilientProviderRateLimitsBurst(t *testing.T) {
	clock := newFakeClock()
	inner := &stubProvider{results: []struct {
		response SearchResponse
		err      error
	}{{response: SearchResponse{Query: "ok", Total: 1}}}}
	provider := newTestProvider(t, inner, ResilienceConfig{
		MaxAttempts: 1, RateLimit: 1, RateBurst: 1, BreakerThreshold: -1,
		Now: clock.Now, Sleep: clock.Sleep,
	})
	if _, err := provider.Search(context.Background(), SearchRequest{Query: "ok"}); err != nil {
		t.Fatalf("first call error = %v", err)
	}
	if _, err := provider.Search(context.Background(), SearchRequest{Query: "ok"}); err != nil {
		t.Fatalf("second call error = %v", err)
	}
	if clock.totalSlept() < 900*time.Millisecond {
		t.Fatalf("rate limiter did not delay: slept %v", clock.totalSlept())
	}
}

func TestRetryableClassification(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{ErrInvalidRequest, false},
		{ErrInvalidResponse, false},
		{ErrInvalidConfig, false},
		{ErrResponseTooLarge, false},
		{&HTTPStatusError{Code: 400}, false},
		{&HTTPStatusError{Code: 429}, true},
		{&HTTPStatusError{Code: 503}, true},
		{context.DeadlineExceeded, true},
	}
	for _, test := range cases {
		if got := retryable(test.err); got != test.want {
			t.Fatalf("retryable(%v) = %v, want %v", test.err, got, test.want)
		}
	}
}

func TestResilientProviderRejectsMissingInner(t *testing.T) {
	if _, err := NewResilientProvider(nil, ResilienceConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v", err)
	}
}
