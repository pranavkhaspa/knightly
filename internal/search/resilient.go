package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"
)

const (
	defaultMaxAttempts      = 3
	defaultBaseBackoff      = 200 * time.Millisecond
	defaultMaxBackoff       = 2 * time.Second
	defaultRateLimit        = 5
	defaultRateBurst        = 5
	defaultBreakerThreshold = 5
	defaultBreakerCooldown  = 30 * time.Second
)

// ResilienceConfig bounds retries, client-side rate limiting, and circuit
// breaking for a search provider. Zero values select safe defaults; a negative
// MaxAttempts, rate limit, or breaker threshold disables that mechanism.
type ResilienceConfig struct {
	MaxAttempts      int
	BaseBackoff      time.Duration
	MaxBackoff       time.Duration
	RateLimit        float64
	RateBurst        int
	BreakerThreshold int
	BreakerCooldown  time.Duration

	// Now and Sleep are injectable so tests can assert timing behavior
	// deterministically without real waiting.
	Now   func() time.Time
	Sleep func(context.Context, time.Duration) error
}

// ResilientProvider decorates a SearchProvider with retry classification,
// a token-bucket rate limiter, and a consecutive-failure circuit breaker. It
// never retries a caller error (invalid request or malformed response).
type ResilientProvider struct {
	inner    SearchProvider
	maxTries int
	base     time.Duration
	max      time.Duration
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
	limiter  *rateLimiter
	breaker  *breaker
}

var _ SearchProvider = (*ResilientProvider)(nil)

func NewResilientProvider(inner SearchProvider, config ResilienceConfig) (*ResilientProvider, error) {
	if inner == nil {
		return nil, fmt.Errorf("%w: inner provider is required", ErrInvalidConfig)
	}
	provider := &ResilientProvider{
		inner:    inner,
		maxTries: config.MaxAttempts,
		base:     config.BaseBackoff,
		max:      config.MaxBackoff,
		now:      config.Now,
		sleep:    config.Sleep,
	}
	if provider.maxTries == 0 {
		provider.maxTries = defaultMaxAttempts
	}
	if provider.base <= 0 {
		provider.base = defaultBaseBackoff
	}
	if provider.max <= 0 {
		provider.max = defaultMaxBackoff
	}
	if provider.max < provider.base {
		provider.max = provider.base
	}
	if provider.now == nil {
		provider.now = func() time.Time { return time.Now() }
	}
	if provider.sleep == nil {
		provider.sleep = sleepWithContext
	}
	rateLimit := config.RateLimit
	if rateLimit == 0 {
		rateLimit = defaultRateLimit
	}
	burst := config.RateBurst
	if burst == 0 {
		burst = defaultRateBurst
	}
	if rateLimit > 0 && burst > 0 {
		provider.limiter = newRateLimiter(rateLimit, float64(burst), provider.now)
	}
	threshold := config.BreakerThreshold
	if threshold == 0 {
		threshold = defaultBreakerThreshold
	}
	cooldown := config.BreakerCooldown
	if cooldown <= 0 {
		cooldown = defaultBreakerCooldown
	}
	if threshold > 0 {
		provider.breaker = newBreaker(threshold, cooldown)
	}
	return provider, nil
}

func (p *ResilientProvider) Search(ctx context.Context, request SearchRequest) (SearchResponse, error) {
	if p == nil || p.inner == nil {
		return SearchResponse{}, ErrProviderUnavailable
	}
	if ctx == nil {
		return SearchResponse{}, fmt.Errorf("%w: context is required", ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return SearchResponse{}, err
	}
	var lastErr error
	attempts := p.maxTries
	if attempts < 1 {
		attempts = 1
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		now := p.now()
		if p.breaker != nil && !p.breaker.allow(now) {
			return SearchResponse{}, fmt.Errorf("%w: circuit breaker is open", ErrProviderUnavailable)
		}
		if p.limiter != nil {
			if err := p.limiter.wait(ctx, p.sleep); err != nil {
				return SearchResponse{}, err
			}
		}
		response, err := p.inner.Search(ctx, request)
		if err == nil {
			if p.breaker != nil {
				p.breaker.success()
			}
			return response, nil
		}
		if p.breaker != nil {
			p.breaker.failure(p.now())
		}
		lastErr = err
		if ctx.Err() != nil || !retryable(err) || attempt == attempts {
			return SearchResponse{}, err
		}
		if err := p.sleep(ctx, p.backoff(attempt, err)); err != nil {
			return SearchResponse{}, err
		}
	}
	return SearchResponse{}, lastErr
}

func (p *ResilientProvider) backoff(attempt int, err error) time.Duration {
	delay := time.Duration(float64(p.base) * math.Pow(2, float64(attempt-1)))
	if delay > p.max || delay <= 0 {
		delay = p.max
	}
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) && statusErr.RetryAfter > delay {
		delay = statusErr.RetryAfter
	}
	if delay > p.max {
		delay = p.max
	}
	return delay
}

// retryable classifies transient provider failures. Caller errors such as an
// invalid request or malformed response are never retried.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrInvalidResponse) ||
		errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrInvalidURL) ||
		errors.Is(err, ErrResponseTooLarge) {
		return false
	}
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return statusErr.Retryable()
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type rateLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func newRateLimiter(rate, burst float64, now func() time.Time) *rateLimiter {
	return &rateLimiter{rate: rate, burst: burst, tokens: burst, last: now(), now: now}
}

// wait blocks until a token is available, sleeping in bounded increments and
// respecting context cancellation. The injected sleep advances the injected
// clock in tests, so the limiter never busy-loops there.
func (l *rateLimiter) wait(ctx context.Context, sleep func(context.Context, time.Duration) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		delay, ok := l.reserve()
		if ok {
			return nil
		}
		if err := sleep(ctx, delay); err != nil {
			return err
		}
	}
}

// reserve consumes one token if available; otherwise it reports how long to
// wait before a token will exist.
func (l *rateLimiter) reserve() (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens = math.Min(l.burst, l.tokens+elapsed*l.rate)
		l.last = now
	}
	if l.tokens >= 1 {
		l.tokens--
		return 0, true
	}
	wait := time.Duration(((1 - l.tokens) / l.rate) * float64(time.Second))
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return wait, false
}

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	failures  int
	state     breakerState
	openUntil time.Time
}

func newBreaker(threshold int, cooldown time.Duration) *breaker {
	return &breaker{threshold: threshold, cooldown: cooldown, state: breakerClosed}
}

func (b *breaker) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		return true
	case breakerOpen:
		if !now.Before(b.openUntil) {
			b.state = breakerHalfOpen
			return true
		}
		return false
	default: // half-open: exactly one trial is already in flight
		return false
	}
}

func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = breakerClosed
}

func (b *breaker) failure(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == breakerHalfOpen {
		b.state = breakerOpen
		b.openUntil = now.Add(b.cooldown)
		b.failures = 0
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.state = breakerOpen
		b.openUntil = now.Add(b.cooldown)
		b.failures = 0
	}
}
