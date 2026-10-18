package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
	timers  []*fakeTimer
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTicker(_ time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	ticker := &fakeTicker{ch: make(chan time.Time, 4)}
	c.tickers = append(c.tickers, ticker)
	return ticker
}

func (c *fakeClock) NewTimer(duration time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeTimer{ch: make(chan time.Time, 1), at: c.now.Add(duration)}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	now := c.now
	for _, ticker := range c.tickers {
		if !ticker.stopped {
			ticker.ch <- now
		}
	}
	for _, timer := range c.timers {
		if !timer.stopped && !timer.fired && !timer.at.After(now) {
			timer.fired = true
			timer.ch <- now
		}
	}
	c.mu.Unlock()
}

type fakeTicker struct {
	ch      chan time.Time
	stopped bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.ch }
func (t *fakeTicker) Stop()               { t.stopped = true }

type fakeTimer struct {
	ch      chan time.Time
	at      time.Time
	stopped bool
	fired   bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool {
	wasActive := !t.stopped && !t.fired
	t.stopped = true
	return wasActive
}

func TestSchedulerCoalescesManualTriggers(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC))
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	count := 0
	var mu sync.Mutex
	runner := RunFunc(func(ctx context.Context) error {
		mu.Lock()
		count++
		run := count
		mu.Unlock()
		started <- struct{}{}
		if run == 1 {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	s, err := New(Config{Interval: time.Hour, MaxRunDuration: time.Minute}, runner, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())

	result, err := s.Trigger()
	if err != nil || result != TriggerQueued {
		t.Fatalf("first Trigger() = %q, %v; want queued", result, err)
	}
	waitSignal(t, started)
	for range 2 {
		result, err = s.Trigger()
		if err != nil || result != TriggerCoalesced {
			t.Fatalf("coalesced Trigger() = %q, %v; want coalesced", result, err)
		}
	}
	close(release)
	waitSignal(t, started)
	waitFor(t, func() bool { return s.Status().Running == false })
	if got := s.Status().RunCount; got != 2 {
		t.Fatalf("RunCount = %d, want one initial and one coalesced follow-up", got)
	}
	if got := s.Status().CoalescedCount; got != 2 {
		t.Fatalf("CoalescedCount = %d, want 2", got)
	}
}

func TestSchedulerSkipsQuietHoursAndResumesTicker(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 10, 16, 23, 0, 0, 0, time.UTC))
	started := make(chan struct{}, 2)
	s, err := New(Config{
		Interval:       time.Hour,
		MaxRunDuration: time.Minute,
		QuietHours:     QuietHours{Start: 22 * time.Hour, End: 7 * time.Hour, Location: time.UTC},
	}, RunFunc(func(context.Context) error { started <- struct{}{}; return nil }), clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	clock.Advance(time.Hour)
	assertNoSignal(t, started)

	clock.Advance(8 * time.Hour)
	waitSignal(t, started)
	if got := s.Status().RunCount; got != 1 {
		t.Fatalf("RunCount = %d, want one run after quiet hours", got)
	}
}

func TestSchedulerCancelsRunAtMaximumDuration(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC))
	started := make(chan struct{}, 1)
	s, err := New(Config{Interval: time.Hour, MaxRunDuration: 10 * time.Minute}, RunFunc(func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}), clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if _, err := s.Trigger(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started)
	clock.Advance(10 * time.Minute)
	waitFor(t, func() bool { return s.Status().TimedOutCount == 1 })
	status := s.Status()
	if status.TimedOutCount != 1 || !strings.Contains(status.LastRunError, ErrRunTimedOut.Error()) {
		t.Fatalf("timeout status = %+v", status)
	}
}

func TestSchedulerPauseAndResumeControlsManualAndScheduledRuns(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC))
	started := make(chan struct{}, 1)
	s, err := New(Config{Interval: time.Hour, MaxRunDuration: time.Minute}, RunFunc(func(context.Context) error {
		started <- struct{}{}
		return nil
	}), clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())

	s.Pause()
	if _, err := s.Trigger(); !errors.Is(err, ErrPaused) {
		t.Fatalf("Trigger() while paused = %v, want ErrPaused", err)
	}
	clock.Advance(time.Hour)
	assertNoSignal(t, started)

	s.Resume()
	if _, err := s.Trigger(); err != nil {
		t.Fatalf("Trigger() after Resume() error = %v", err)
	}
	waitSignal(t, started)
}

type manualTestRunner struct {
	manual   chan struct{}
	schedule chan struct{}
}

func (r manualTestRunner) Run(context.Context) error {
	r.schedule <- struct{}{}
	return nil
}

func (r manualTestRunner) RunManual(context.Context) error {
	r.manual <- struct{}{}
	return nil
}

func TestSchedulerUsesManualRunnerForExplicitTrigger(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 10, 16, 8, 0, 0, 0, time.UTC))
	runner := manualTestRunner{manual: make(chan struct{}, 1), schedule: make(chan struct{}, 1)}
	s, err := New(Config{Interval: time.Hour, MaxRunDuration: time.Minute}, runner, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop(context.Background())
	if _, err := s.Trigger(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, runner.manual)
	assertNoSignal(t, runner.schedule)
}

func waitSignal[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduler signal")
	}
}

func assertNoSignal[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case value := <-ch:
		t.Fatalf("unexpected scheduler signal: %v", value)
	case <-time.After(20 * time.Millisecond):
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for scheduler state")
}
