// rungroup manages the lifecycle of concurrent background services.
//
// Services are supervised independently: panics are recovered, errors are
// aggregated, and each service carries its own restart policy and backoff.
//
// A [*Group] satisfies [Service], so rungroups can be nested. [ErrShutdownAll]
// propagates to the parent by default; use [WithShutdownIsolation] to absorb it.
package rungroup

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrDoNotRestart is a sentinel error. If a Service's Run method returns this
	// error, or an error that wraps it, the rungroup will not restart it,
	// regardless of the configured restart policy.
	ErrDoNotRestart = errors.New("do not restart")

	// ErrAlreadyRunning is returned if Run is called while the rungroup is
	// already active.
	ErrAlreadyRunning = errors.New("already running")

	// ErrRestartLimitExceeded is wrapped when a service is permanently abandoned
	// because its backoff function returned a negative duration.
	ErrRestartLimitExceeded = errors.New("restart limit exceeded")

	// ErrIntentionalHalt is wrapped when a service explicitly returns
	// ErrDoNotRestart, causing the rungroup to permanently abandon it.
	ErrIntentionalHalt = errors.New("service halted intentionally")

	// ErrPolicyHalt is wrapped when a service exits with an error and is not
	// restarted because its configured RestartPolicy prevents it.
	ErrPolicyHalt = errors.New("halted by restart policy")

	// ErrShutdownTimeout is wrapped when a service did not exit within its
	// configured shutdown timeout after the rungroup context was cancelled.
	ErrShutdownTimeout = errors.New("shutdown timeout exceeded")

	// ErrServicePanic is wrapped in the error returned when a managed service
	// panics during execution. The underlying panic value and stack trace are
	// included in the formatted error string.
	ErrServicePanic = errors.New("service panicked")

	// ErrShutdownAll is a sentinel error. If a Service's Run method returns this
	// error, or an error that wraps it, the rungroup cancels its internal
	// context and initiates a graceful shutdown of all other running services.
	// The error is recorded and returned by Run. Use this when a service detects
	// an application-wide failure state that makes continued operation impossible.
	//
	// When used in a nested rungroup, the shutdown signal propagates to the
	// parent by default. Use [WithShutdownIsolation] on the child rungroup's
	// service entry to absorb the signal at that boundary.
	ErrShutdownAll = errors.New("shutdown all services")
)

// EventType identifies the kind of event emitted by the rungroup.
type EventType int

const (
	// EventServiceRestarting is emitted when a service has failed and will be
	// restarted after a backoff delay. Err, Attempt, and Delay are populated.
	EventServiceRestarting EventType = iota

	// EventServiceHalted is emitted when a service has permanently stopped for
	// any reason (policy, sentinel error, backoff limit, or clean exit).
	// Err is nil for a clean exit.
	EventServiceHalted

	// EventShutdownTimeout is emitted when a service did not exit within its
	// configured shutdown timeout. ServiceName identifies the hung service.
	EventShutdownTimeout
)

// Event is the value passed to event handlers registered via [WithEventHandler]
// or [WithServiceEventHandler].
type Event struct {
	Type        EventType
	ServiceName string
	Attempt     int           // populated for EventServiceRestarting
	Delay       time.Duration // populated for EventServiceRestarting
	Err         error         // the triggering error; nil for a clean halt
}

// Service represents a managed background operation.
type Service interface {
	// Run executes the service. It must respect ctx cancellation for clean
	// shutdowns. If it panics, the rungroup catches it, converts the panic to
	// an error, and evaluates it against the restart policy.
	Run(ctx context.Context) error
}

// ServiceFunc is an adapter to allow the use of ordinary functions as
// rungroup Services. If f is a function with the appropriate signature,
// ServiceFunc(f) is a Service that calls f.
type ServiceFunc func(ctx context.Context) error

// Run calls f(ctx).
func (f ServiceFunc) Run(ctx context.Context) error {
	return f(ctx)
}

// RestartPolicy defines when a service should be restarted after exiting.
type RestartPolicy int

const (
	// RestartAlways restarts the service unconditionally, even if it returns nil.
	RestartAlways RestartPolicy = iota
	// RestartOnFailure restarts the service only if it returns a non-nil error.
	RestartOnFailure
	// RestartNever means the service is never restarted, regardless of its
	// return value.
	RestartNever
)

// managedService holds a service and all of its operational parameters.
type managedService struct {
	svc             Service
	name            string
	policy          RestartPolicy
	backoff         func(attempt int) time.Duration
	stabilityWindow time.Duration
	shutdownTimeout time.Duration
	isolateShutdown bool
	onEvent         func(Event) // service-level handler
}

// wantsRestart reports whether the service should be restarted given its exit
// error.
func (m managedService) wantsRestart(err error) bool {
	switch m.policy {
	case RestartAlways:
		return true
	case RestartOnFailure:
		return err != nil
	default: // RestartNever
		return false
	}
}

// Option configures how a Service is supervised.
type Option func(*managedService)

// WithName assigns an identity to the service. Highly recommended for
// observability.
func WithName(name string) Option {
	return func(m *managedService) {
		m.name = name
	}
}

// WithRestartPolicy sets the restart behaviour. Defaults to RestartAlways.
func WithRestartPolicy(p RestartPolicy) Option {
	return func(m *managedService) {
		m.policy = p
	}
}

// WithBackoff provides a function to calculate the delay before a restart,
// based on the number of consecutive restarts.
//
// If the function returns a negative duration, the rungroup treats it as a
// hard limit and permanently stops the service.
func WithBackoff(fn func(attempt int) time.Duration) Option {
	return func(m *managedService) {
		m.backoff = fn
	}
}

// WithStabilityWindow sets the minimum duration a service must run
// continuously before its restart attempt counter is reset to zero. This
// prevents a service that runs stably for a long time from being penalised
// with a large backoff delay after an eventual crash.
func WithStabilityWindow(d time.Duration) Option {
	return func(m *managedService) {
		m.stabilityWindow = d
	}
}

// WithServiceShutdownTimeout sets the maximum duration the rungroup will
// wait for this specific service to exit after the rungroup context is
// cancelled. If exceeded, an [EventShutdownTimeout] event is emitted and the
// goroutine is abandoned.
//
// This takes precedence over the rungroup-level [WithShutdownTimeout] for
// this service. If both are set, whichever expires first wins.
func WithServiceShutdownTimeout(d time.Duration) Option {
	return func(m *managedService) {
		m.shutdownTimeout = d
	}
}

// WithShutdownIsolation prevents [ErrShutdownAll] from propagating to the parent
// rungroup when this service (typically a nested [*Group]) triggers one.
// The shutdown is absorbed at this service boundary and treated as a normal
// policy halt.
func WithShutdownIsolation() Option {
	return func(m *managedService) {
		m.isolateShutdown = true
	}
}

// WithServiceEventHandler registers an event handler that fires only for
// events originating from this service. It runs before the rungroup-level
// handler registered via [WithEventHandler].
func WithServiceEventHandler(fn func(Event)) Option {
	return func(m *managedService) {
		m.onEvent = fn
	}
}

// GroupOption configures the rungroup instance itself.
type GroupOption func(*Group)

// WithShutdownTimeout sets the maximum duration the rungroup will wait for
// all services to exit after the Run context is cancelled. Acts as a global
// ceiling; per-service timeouts set via [WithServiceShutdownTimeout] cannot
// exceed this value.
func WithShutdownTimeout(d time.Duration) GroupOption {
	return func(s *Group) {
		s.shutdownTimeout = d
	}
}

// WithEventHandler registers a rungroup-level event handler that fires for
// all events from all services. It runs after any service-level handler
// registered via [WithServiceEventHandler].
func WithEventHandler(fn func(Event)) GroupOption {
	return func(s *Group) {
		s.onEvent = fn
	}
}

// WithShutdownBoundary prevents [ErrShutdownAll] from propagating to a parent
// rungroup when a service inside this group triggers one. The shutdown is
// absorbed at this group's boundary: the group shuts itself down normally but
// returns an error wrapping [ErrPolicyHalt] rather than [ErrShutdownAll].
//
// This is the group-level counterpart to [WithShutdownIsolation]. Prefer this
// when you own the group definition; use [WithShutdownIsolation] when you are
// registering a third-party or pre-built group into a parent.
func WithShutdownBoundary() GroupOption {
	return func(s *Group) {
		s.isShutdownBoundary = true
	}
}

// Group manages a group of concurrent services.
type Group struct {
	// TODO(introspection): Add a Status() method to expose active service names,
	// restart counts, and current states (running, backing off, halted).
	services           []managedService
	running            atomic.Bool
	shutdownTimeout    time.Duration
	isShutdownBoundary bool
	serviceNameCounter atomic.Int64
	onEvent            func(Event)

	mu       sync.Mutex
	termErrs []error
}

// New creates a new Group.
func New(opts ...GroupOption) *Group {
	s := &Group{}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Add registers a service to be managed. All services must be added before
// calling Run. Calling Add after Run has started returns [ErrAlreadyRunning].
func (s *Group) Add(svc Service, opts ...Option) error {
	if s.running.Load() {
		return ErrAlreadyRunning
	}

	ms := managedService{
		svc:    svc,
		policy: RestartAlways,
	}
	for _, opt := range opts {
		opt(&ms)
	}

	if ms.name == "" {
		ms.name = fmt.Sprintf("service-%d", s.serviceNameCounter.Add(1))
	}

	if ms.backoff == nil {
		ms.backoff = func(attempt int) time.Duration { return 50 * time.Millisecond }
	}

	s.services = append(s.services, ms)
	return nil
}

// Run starts all registered services concurrently. It blocks until the
// provided context is cancelled AND all services have exited (or their
// individual shutdown timeouts are exceeded).
//
// If zero services are registered, Run returns nil immediately.
// It returns an aggregation of any terminal errors produced by the services.
func (s *Group) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer s.running.Store(false)

	if len(s.services) == 0 {
		return nil
	}

	ctx, shutdownAll := context.WithCancel(ctx)
	defer shutdownAll()

	// doneChs holds one channel per service, closed when the service goroutine
	// exits. Used for per-service shutdown timeout tracking.
	doneChs := make([]chan struct{}, len(s.services))
	for i := range doneChs {
		doneChs[i] = make(chan struct{})
	}

	for i, ms := range s.services {
		done := doneChs[i]
		go func(ms managedService) {
			defer close(done)
			if err := s.runServiceLoop(ctx, shutdownAll, ms); err != nil {
				s.appendTerminalErr(err)
			}
		}(ms)
	}

	// Wait for all services, honouring per-service and global timeouts.
	<-ctx.Done()
	s.waitForServices(doneChs)

	s.mu.Lock()
	errs := s.termErrs
	s.mu.Unlock()

	if s.isShutdownBoundary {
		for i, e := range errs {
			if errors.Is(e, ErrShutdownAll) {
				errs[i] = fmt.Errorf("%w: %s", ErrPolicyHalt, e.Error())
			}
		}
	}

	return errors.Join(errs...)
}

// waitForServices waits for each service goroutine to exit after the
// rungroup context has been cancelled. Per-service shutdown timeouts are
// checked individually; the global shutdown timeout is the outer deadline.
func (s *Group) waitForServices(doneChs []chan struct{}) {
	var globalDeadline <-chan time.Time
	if s.shutdownTimeout > 0 {
		t := time.NewTimer(s.shutdownTimeout)
		defer t.Stop()
		globalDeadline = t.C
	}

	for i, done := range doneChs {
		ms := s.services[i]

		// Determine the effective per-service timeout.
		svcTimeout := ms.shutdownTimeout

		if svcTimeout <= 0 {
			// No per-service timeout: wait until done or global deadline.
			select {
			case <-done:
			case <-globalDeadline:
				s.emitEvent(ms, Event{
					Type:        EventShutdownTimeout,
					ServiceName: ms.name,
				})
				s.appendTerminalErr(fmt.Errorf("%w: service %q", ErrShutdownTimeout, ms.name))
			}
			continue
		}

		// Per-service timeout: race it against the global deadline.
		svcTimer := time.NewTimer(svcTimeout)
		defer svcTimer.Stop()

		select {
		case <-done:
		case <-svcTimer.C:
			s.emitEvent(ms, Event{
				Type:        EventShutdownTimeout,
				ServiceName: ms.name,
			})
			s.appendTerminalErr(fmt.Errorf("%w: service %q", ErrShutdownTimeout, ms.name))
		case <-globalDeadline:
			s.emitEvent(ms, Event{
				Type:        EventShutdownTimeout,
				ServiceName: ms.name,
			})
			s.appendTerminalErr(fmt.Errorf("%w: service %q", ErrShutdownTimeout, ms.name))
		}
	}
}

// appendTerminalErr records a terminal error from a service goroutine.
func (s *Group) appendTerminalErr(err error) {
	s.mu.Lock()
	s.termErrs = append(s.termErrs, err)
	s.mu.Unlock()
}

// emitEvent dispatches an event to the service-level handler (if any) and
// then the rungroup-level handler (if any).
func (s *Group) emitEvent(ms managedService, e Event) {
	if ms.onEvent != nil {
		ms.onEvent(e)
	}
	if s.onEvent != nil {
		s.onEvent(e)
	}
}

// runServiceLoop handles the execution, panic recovery, and restart logic for
// a single service.
func (s *Group) runServiceLoop(ctx context.Context, shutdownAll context.CancelFunc, m managedService) error {
	attempt := 0

	for {
		if ctx.Err() != nil {
			return nil
		}

		start := time.Now()
		err := s.executeWithRecovery(ctx, m.svc)

		if ctx.Err() != nil {
			return nil
		}

		if m.stabilityWindow > 0 && time.Since(start) >= m.stabilityWindow {
			attempt = 0
		}

		if errors.Is(err, ErrShutdownAll) {
			if m.isolateShutdown {
				termErr := fmt.Errorf("%w: service %q exited: %s", ErrPolicyHalt, m.name, err.Error())
				s.emitEvent(m, Event{Type: EventServiceHalted, ServiceName: m.name, Err: termErr})
				return termErr
			}

			shutdownAll()
			termErr := fmt.Errorf("service %q triggered rungroup shutdown: %w", m.name, err)
			s.emitEvent(m, Event{Type: EventServiceHalted, ServiceName: m.name, Err: termErr})
			return termErr
		}

		if errors.Is(err, ErrDoNotRestart) {
			termErr := fmt.Errorf("%w: service %q: %w", ErrIntentionalHalt, m.name, err)
			s.emitEvent(m, Event{Type: EventServiceHalted, ServiceName: m.name, Err: termErr})
			return termErr
		}

		if !m.wantsRestart(err) {
			if err != nil {
				termErr := fmt.Errorf("%w: service %q exited: %w", ErrPolicyHalt, m.name, err)
				s.emitEvent(m, Event{Type: EventServiceHalted, ServiceName: m.name, Err: termErr})
				return termErr
			}

			s.emitEvent(m, Event{Type: EventServiceHalted, ServiceName: m.name})
			return nil
		}

		attempt++
		delay := m.backoff(attempt)

		if delay < 0 {
			var termErr error
			if err != nil {
				termErr = fmt.Errorf("%w: service %q (attempt %d): %w", ErrRestartLimitExceeded, m.name, attempt, err)
			} else {
				termErr = fmt.Errorf("%w: service %q (attempt %d)", ErrRestartLimitExceeded, m.name, attempt)
			}
			s.emitEvent(m, Event{Type: EventServiceHalted, ServiceName: m.name, Err: termErr})
			return termErr
		}

		s.emitEvent(m, Event{
			Type:        EventServiceRestarting,
			ServiceName: m.name,
			Attempt:     attempt,
			Delay:       delay,
			Err:         err,
		})

		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil // Group shutting down during backoff.
			case <-timer.C:
				// Delay complete, loop around and restart.
			}
		}
	}
}

// executeWithRecovery wraps the service execution in a deferred recovery block
// to ensure panics are translated into standard errors.
func (s *Group) executeWithRecovery(ctx context.Context, svc Service) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrServicePanic, r, debug.Stack())
		}
	}()

	return svc.Run(ctx)
}

// #####################################################################
// IntervalService
// #####################################################################

// IntervalService is a Service that calls a handler on a fixed interval.
// The handler is called immediately on the first tick and then repeatedly
// every interval until the context is cancelled. If the handler returns a
// non-nil error, Run returns that error immediately.
type IntervalService struct {
	interval time.Duration
	handler  func(ctx context.Context) error
}

// NewIntervalService creates an IntervalService that calls handler every
// interval. The handler signature matches ServiceFunc.
func NewIntervalService(interval time.Duration, handler func(ctx context.Context) error) *IntervalService {
	return &IntervalService{interval: interval, handler: handler}
}

// Run calls the handler immediately, then on every interval tick, until ctx
// is cancelled or the handler returns a non-nil error.
func (s *IntervalService) Run(ctx context.Context) error {
	if err := s.handler(ctx); err != nil {
		return err
	}
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.handler(ctx); err != nil {
				return err
			}
		}
	}
}
