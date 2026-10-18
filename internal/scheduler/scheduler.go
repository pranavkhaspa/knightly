// Package scheduler provides a bounded, process-local trigger loop. Durable
// source configuration and collection state belongs to the graph repository;
// this package only coordinates one in-flight collection run at a time.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrRunnerRequired = errors.New("scheduler runner is required")
	ErrNotStarted     = errors.New("scheduler is not started")
	ErrAlreadyStarted = errors.New("scheduler is already started")
	ErrStopped        = errors.New("scheduler is stopped")
	ErrPaused         = errors.New("scheduler is paused")
	ErrRunTimedOut    = errors.New("scheduler run exceeded maximum duration")
)

const (
	DefaultInterval       = 15 * time.Minute
	DefaultMaxRunDuration = 5 * time.Minute
	day                   = 24 * time.Hour
)

// Runner is the bounded unit of work invoked by the scheduler. Implementations
// must honor cancellation so a timed-out run can release its worker promptly.
type Runner interface {
	Run(context.Context) error
}

// ManualRunner is optional. When implemented, Scheduler uses RunManual for an
// explicit Trigger so the runner can bypass normal due-time checks while
// scheduled ticks continue through Run.
type ManualRunner interface {
	RunManual(context.Context) error
}

// RunFunc adapts a function to Runner.
type RunFunc func(context.Context) error

func (f RunFunc) Run(ctx context.Context) error { return f(ctx) }

// Ticker and Timer are deliberately small clock seams. They make scheduler
// behavior deterministic in unit tests without putting time.Sleep in tests.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Clock is the only source of time used by Scheduler. A nil clock uses the
// system clock.
type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
	NewTimer(time.Duration) Timer
}

// QuietHours describes a daily interval in the clock's local day. An interval
// whose start is after its end crosses midnight. Start == End disables quiet
// hours, which makes the zero value useful for normal deployments.
type QuietHours struct {
	Start    time.Duration
	End      time.Duration
	Location *time.Location
}

func (q QuietHours) validate() error {
	if q.Start < 0 || q.Start >= day || q.End < 0 || q.End >= day {
		return fmt.Errorf("quiet-hour bounds must be within one day")
	}
	return nil
}

func (q QuietHours) contains(now time.Time) bool {
	if q.Start == q.End {
		return false
	}
	if q.Location != nil {
		now = now.In(q.Location)
	}
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	position := now.Sub(midnight)
	if q.Start < q.End {
		return position >= q.Start && position < q.End
	}
	return position >= q.Start || position < q.End
}

type Config struct {
	Interval       time.Duration
	MaxRunDuration time.Duration
	QuietHours     QuietHours
	RunOnStart     bool
}

func (c Config) bounded() (Config, error) {
	if c.Interval <= 0 {
		c.Interval = DefaultInterval
	}
	if c.MaxRunDuration <= 0 {
		c.MaxRunDuration = DefaultMaxRunDuration
	}
	if err := c.QuietHours.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

type TriggerResult string

const (
	TriggerQueued    TriggerResult = "queued"
	TriggerCoalesced TriggerResult = "coalesced"
)

type Status struct {
	Started           bool
	Paused            bool
	Running           bool
	Pending           bool
	StartedAt         time.Time
	LastRunStartedAt  time.Time
	LastRunFinishedAt time.Time
	LastRunDuration   time.Duration
	LastRunError      string
	RunCount          uint64
	CoalescedCount    uint64
	TimedOutCount     uint64
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

func (realClock) NewTicker(interval time.Duration) Ticker {
	return realTicker{ticker: time.NewTicker(interval)}
}

func (realClock) NewTimer(duration time.Duration) Timer {
	return realTimer{timer: time.NewTimer(duration)}
}

type realTicker struct{ ticker *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.ticker.C }
func (t realTicker) Stop()               { t.ticker.Stop() }

type realTimer struct{ timer *time.Timer }

func (t realTimer) C() <-chan time.Time { return t.timer.C }
func (t realTimer) Stop() bool          { return t.timer.Stop() }

type activeRun struct {
	startedAt time.Time
	cancel    context.CancelFunc
	done      chan error
	timer     Timer
	manual    bool
	timedOut  bool
}

// Scheduler coalesces all triggers while a run is active into at most one
// follow-up run. Scheduled ticks during quiet hours are discarded; manual
// triggers are explicit and bypass quiet hours.
type Scheduler struct {
	config Config
	runner Runner
	clock  Clock

	mu            sync.RWMutex
	started       bool
	stopped       bool
	paused        bool
	running       bool
	pending       bool
	pendingManual bool
	status        Status

	triggerCh chan struct{}
	stopCh    chan struct{}
	loopDone  chan struct{}
}

func New(config Config, runner Runner, clocks ...Clock) (*Scheduler, error) {
	if runner == nil {
		return nil, ErrRunnerRequired
	}
	bounded, err := config.bounded()
	if err != nil {
		return nil, err
	}
	clock := Clock(realClock{})
	if len(clocks) > 0 && clocks[0] != nil {
		clock = clocks[0]
	}
	return &Scheduler{
		config:    bounded,
		runner:    runner,
		clock:     clock,
		triggerCh: make(chan struct{}, 1),
	}, nil
}

func (s *Scheduler) Start() error {
	if s == nil {
		return ErrRunnerRequired
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ErrStopped
	}
	if s.started {
		s.mu.Unlock()
		return ErrAlreadyStarted
	}
	s.started = true
	s.status.Started = true
	s.status.Paused = s.paused
	s.status.StartedAt = s.clock.Now()
	s.stopCh = make(chan struct{})
	s.loopDone = make(chan struct{})
	ticker := s.clock.NewTicker(s.config.Interval)
	if s.config.RunOnStart {
		s.pending = true
		s.pendingManual = true
		s.status.Pending = true
		s.signalLocked()
	}
	s.mu.Unlock()
	go s.loop(ticker)
	return nil
}

func (s *Scheduler) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.started {
		if s.stopped {
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()
		return ErrNotStarted
	}
	if !s.stopped {
		s.stopped = true
		close(s.stopCh)
	}
	done := s.loopDone
	s.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Trigger requests one manual run. If work is already pending or active, the
// request is folded into the one permitted follow-up run.
func (s *Scheduler) Trigger() (TriggerResult, error) {
	if s == nil {
		return "", ErrNotStarted
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return "", ErrNotStarted
	}
	if s.stopped {
		return "", ErrStopped
	}
	if s.paused {
		return "", ErrPaused
	}
	coalesced := s.running || s.pending
	s.pending = true
	s.pendingManual = true
	s.status.Pending = true
	if coalesced {
		s.status.CoalescedCount++
	} else {
		s.signalLocked()
	}
	if coalesced {
		return TriggerCoalesced, nil
	}
	return TriggerQueued, nil
}

func (s *Scheduler) Pause() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.paused = true
	s.status.Paused = true
	s.mu.Unlock()
}

func (s *Scheduler) Resume() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.paused = false
	s.status.Paused = false
	shouldSignal := s.started && !s.stopped && s.pending && s.running == false
	if shouldSignal {
		s.signalLocked()
	}
	s.mu.Unlock()
}

func (s *Scheduler) Status() Status {
	if s == nil {
		return Status{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *Scheduler) loop(ticker Ticker) {
	defer close(s.loopDone)
	defer ticker.Stop()
	var active *activeRun

	for {
		var done <-chan error
		var timeout <-chan time.Time
		if active != nil {
			done = active.done
			timeout = active.timer.C()
		}

		select {
		case <-ticker.C():
			s.handleScheduled(&active)
		case <-s.triggerCh:
			s.startPending(&active)
		case err := <-done:
			s.finishRun(&active, err)
		case <-timeout:
			if active != nil && !active.timedOut {
				active.timedOut = true
				active.cancel()
			}
		case <-s.stopCh:
			if active != nil {
				active.cancel()
			}
			return
		}
	}
}

func (s *Scheduler) handleScheduled(active **activeRun) {
	s.mu.Lock()
	if s.paused || s.stopped || s.config.QuietHours.contains(s.clock.Now()) {
		s.mu.Unlock()
		return
	}
	if *active != nil || s.pending {
		s.pending = true
		s.status.Pending = true
		if *active != nil {
			s.status.CoalescedCount++
		}
		s.mu.Unlock()
		if *active == nil {
			s.startPending(active)
		}
		return
	}
	s.pending = true
	s.status.Pending = true
	s.mu.Unlock()
	s.startPending(active)
}

func (s *Scheduler) startPending(active **activeRun) {
	s.mu.Lock()
	if *active != nil || s.paused || s.stopped || !s.pending {
		s.mu.Unlock()
		return
	}
	manual := s.pendingManual
	if !manual && s.config.QuietHours.contains(s.clock.Now()) {
		s.mu.Unlock()
		return
	}
	s.pending = false
	s.pendingManual = false
	startedAt := s.clock.Now()
	ctx, cancel := context.WithCancel(context.Background())
	run := &activeRun{
		startedAt: startedAt,
		cancel:    cancel,
		done:      make(chan error, 1),
		timer:     s.clock.NewTimer(s.config.MaxRunDuration),
		manual:    manual,
	}
	*active = run
	s.running = true
	s.status.Running = true
	s.status.Pending = false
	s.status.LastRunStartedAt = startedAt
	s.status.RunCount++
	s.mu.Unlock()

	go func() {
		if run.manual {
			if manualRunner, ok := s.runner.(ManualRunner); ok {
				run.done <- manualRunner.RunManual(ctx)
				return
			}
		}
		run.done <- s.runner.Run(ctx)
	}()
}

func (s *Scheduler) finishRun(active **activeRun, err error) {
	run := *active
	if run == nil {
		return
	}
	run.timer.Stop()
	finishedAt := s.clock.Now()
	if run.timedOut {
		err = fmt.Errorf("%w: limit %s", ErrRunTimedOut, s.config.MaxRunDuration)
	}
	s.mu.Lock()
	s.running = false
	s.status.Running = false
	s.status.LastRunFinishedAt = finishedAt
	s.status.LastRunDuration = finishedAt.Sub(run.startedAt)
	s.status.LastRunError = ""
	if err != nil {
		s.status.LastRunError = err.Error()
	}
	if run.timedOut {
		s.status.TimedOutCount++
	}
	s.status.Pending = s.pending
	s.mu.Unlock()
	*active = nil
	s.startPending(active)
}

func (s *Scheduler) signalLocked() {
	select {
	case s.triggerCh <- struct{}{}:
	default:
	}
}
