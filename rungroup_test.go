package rungroup_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"lowbit.dev/rungroup"
)

// noDelay is a backoff function that removes restart delay, keeping tests fast.
func noDelay(_ int) time.Duration { return 0 }

// --- Basic lifecycle ---

func TestRun_ZeroServicesReturnsNil(t *testing.T) {
	s := rungroup.New()
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestRun_ErrAlreadyRunning(t *testing.T) {
	s := rungroup.New()
	s.Add(rungroup.ServiceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	go func() {
		close(ready)
		s.Run(ctx)
	}()
	<-ready
	time.Sleep(10 * time.Millisecond)

	if err := s.Run(ctx); !errors.Is(err, rungroup.ErrAlreadyRunning) {
		t.Fatalf("expected ErrAlreadyRunning, got %v", err)
	}
}

func TestAdd_AfterRunStarted_ReturnsErrAlreadyRunning(t *testing.T) {
	s := rungroup.New()
	running := make(chan struct{})
	s.Add(rungroup.ServiceFunc(func(ctx context.Context) error {
		close(running)
		<-ctx.Done()
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)
	<-running

	err := s.Add(rungroup.ServiceFunc(func(_ context.Context) error { return nil }))
	if !errors.Is(err, rungroup.ErrAlreadyRunning) {
		t.Fatalf("expected ErrAlreadyRunning from Add, got %v", err)
	}
}

func TestRun_ContextCancellationStopsAllServices(t *testing.T) {
	s := rungroup.New()
	s.Add(rungroup.ServiceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil after clean shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// --- Restart policies ---

func TestRun_RestartAlways_RestartsAfterNilReturn(t *testing.T) {
	var calls atomic.Int64
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls.Add(1)
			return nil
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		rungroup.WithBackoff(noDelay),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)

	deadline := time.After(2 * time.Second)
	for calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("service was not restarted: call count = %d", calls.Load())
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
}

func TestRun_RestartOnFailure_RestartsOnError(t *testing.T) {
	var calls atomic.Int64
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls.Add(1)
			return errors.New("transient failure")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartOnFailure),
		rungroup.WithBackoff(noDelay),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)

	deadline := time.After(2 * time.Second)
	for calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("service was not restarted on failure: call count = %d", calls.Load())
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
}

func TestRun_RestartOnFailure_DoesNotRestartOnNilReturn(t *testing.T) {
	var calls atomic.Int64
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls.Add(1)
			return nil
		}),
		rungroup.WithRestartPolicy(rungroup.RestartOnFailure),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.Run(ctx); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected exactly 1 call, got %d", calls.Load())
	}
}

func TestRun_RestartNever_PropagatesError(t *testing.T) {
	boom := errors.New("boom")
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error { return boom }),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if !errors.Is(err, rungroup.ErrPolicyHalt) {
		t.Fatalf("expected ErrPolicyHalt, got %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped original error in chain, got %v", err)
	}
}

// --- Sentinel errors ---

func TestRun_ErrDoNotRestart_HaltsServiceRegardlessOfPolicy(t *testing.T) {
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrDoNotRestart
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if !errors.Is(err, rungroup.ErrIntentionalHalt) {
		t.Fatalf("expected ErrIntentionalHalt, got %v", err)
	}
}

func TestRun_ErrShutdownAll_StopsAllServicesAndSurfacesError(t *testing.T) {
	secondStopped := make(chan struct{})

	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrShutdownAll
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithName("trigger"),
	)
	s.Add(
		rungroup.ServiceFunc(func(ctx context.Context) error {
			<-ctx.Done()
			close(secondStopped)
			return nil
		}),
		rungroup.WithName("bystander"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if !errors.Is(err, rungroup.ErrShutdownAll) {
		t.Fatalf("expected ErrShutdownAll in error chain, got %v", err)
	}

	select {
	case <-secondStopped:
	default:
		t.Fatal("bystander service was not stopped after ErrShutdownAll")
	}
}

func TestRun_ErrShutdownAll_MessageIsNotRepetitive(t *testing.T) {
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrShutdownAll
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithName("trigger"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.Run(ctx)
	msg := err.Error()
	first := strings.Index(msg, "shutdown all services")
	last := strings.LastIndex(msg, "shutdown all services")
	if first != last {
		t.Fatalf("sentinel text appears more than once: %q", msg)
	}
}

// --- Panic recovery ---

func TestRun_PanicIsRecoveredAndWrappedAsError(t *testing.T) {
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			panic("something exploded")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if !errors.Is(err, rungroup.ErrServicePanic) {
		t.Fatalf("expected ErrServicePanic in error chain, got %v", err)
	}
}

// --- Backoff ---

func TestRun_BackoffNegativeDurationStopsRestarts(t *testing.T) {
	var calls atomic.Int64
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls.Add(1)
			return errors.New("failure")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		rungroup.WithBackoff(func(attempt int) time.Duration {
			if attempt >= 3 {
				return -1
			}
			return 0
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if !errors.Is(err, rungroup.ErrRestartLimitExceeded) {
		t.Fatalf("expected ErrRestartLimitExceeded, got %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected exactly 3 calls before limit, got %d", calls.Load())
	}
}

func TestAdd_DefaultBackoffThrottlesRestarts(t *testing.T) {
	var calls atomic.Int64
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls.Add(1)
			return errors.New("failure")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	s.Run(ctx)

	if calls.Load() > 5 {
		t.Fatalf("default backoff not applied: got %d calls in 200ms (expected ≤ 5)", calls.Load())
	}
}

// --- Shutdown timeouts ---

func TestRun_GlobalShutdownTimeoutReturnsError(t *testing.T) {
	s := rungroup.New(rungroup.WithShutdownTimeout(50 * time.Millisecond))
	serviceStarted := make(chan struct{})
	s.Add(rungroup.ServiceFunc(func(_ context.Context) error {
		close(serviceStarted)
		time.Sleep(10 * time.Second)
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	<-serviceStarted
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, rungroup.ErrShutdownTimeout) {
			t.Fatalf("expected ErrShutdownTimeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after shutdown timeout")
	}
}

func TestRun_PerServiceShutdownTimeout_OnlyAffectsHungService(t *testing.T) {
	s := rungroup.New()
	hungStarted := make(chan struct{})
	cleanStopped := make(chan struct{})

	s.Add(rungroup.ServiceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		close(cleanStopped)
		return nil
	}), rungroup.WithName("clean"))

	s.Add(rungroup.ServiceFunc(func(_ context.Context) error {
		close(hungStarted)
		time.Sleep(10 * time.Second)
		return nil
	}),
		rungroup.WithName("hung"),
		rungroup.WithServiceShutdownTimeout(50*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	<-hungStarted
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, rungroup.ErrShutdownTimeout) {
			t.Fatalf("expected ErrShutdownTimeout, got %v", err)
		}
		select {
		case <-cleanStopped:
		default:
			t.Fatal("clean service did not stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after per-service shutdown timeout")
	}
}

// --- Stability window ---

func TestWithStabilityWindow_ResetsAttemptCounterOnLongRuns(t *testing.T) {
	var calls atomic.Int64
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls.Add(1)
			time.Sleep(60 * time.Millisecond)
			return errors.New("transient")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		rungroup.WithStabilityWindow(40*time.Millisecond),
		rungroup.WithBackoff(func(attempt int) time.Duration {
			if attempt >= 2 {
				return -1
			}
			return 0
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	cancel()

	if calls.Load() < 3 {
		t.Fatalf("expected at least 3 calls with stability window resetting counter, got %d", calls.Load())
	}
}

// --- Event bus ---

func TestEventBus_ServiceEventHandler_FiresOnRestartAndHalt(t *testing.T) {
	var events []rungroup.Event
	var mu sync.Mutex

	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return errors.New("boom")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		rungroup.WithBackoff(func(attempt int) time.Duration {
			if attempt >= 2 {
				return -1
			}
			return 0
		}),
		rungroup.WithServiceEventHandler(func(e rungroup.Event) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	var restarting, halted int
	for _, e := range events {
		switch e.Type {
		case rungroup.EventServiceRestarting:
			restarting++
		case rungroup.EventServiceHalted:
			halted++
		}
	}
	if restarting < 1 {
		t.Fatalf("expected at least one EventServiceRestarting, got %d", restarting)
	}
	if halted != 1 {
		t.Fatalf("expected exactly one EventServiceHalted, got %d", halted)
	}
}

func TestEventBus_SupervisorEventHandler_FiresForAllServices(t *testing.T) {
	var names []string
	var mu sync.Mutex

	s := rungroup.New(
		rungroup.WithEventHandler(func(e rungroup.Event) {
			if e.Type == rungroup.EventServiceHalted {
				mu.Lock()
				names = append(names, e.ServiceName)
				mu.Unlock()
			}
		}),
	)

	s.Add(rungroup.ServiceFunc(func(_ context.Context) error { return nil }),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithName("alpha"),
	)
	s.Add(rungroup.ServiceFunc(func(_ context.Context) error { return nil }),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithName("beta"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if len(names) != 2 {
		t.Fatalf("expected 2 halted events, got %d: %v", len(names), names)
	}
}

func TestEventBus_ServiceHandlerFiresBeforeSupervisorHandler(t *testing.T) {
	var order []string
	var mu sync.Mutex

	record := func(label string) func(rungroup.Event) {
		return func(e rungroup.Event) {
			if e.Type == rungroup.EventServiceHalted {
				mu.Lock()
				order = append(order, label)
				mu.Unlock()
			}
		}
	}

	s := rungroup.New(rungroup.WithEventHandler(record("supervisor")))
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error { return nil }),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithServiceEventHandler(record("service")),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 2 || order[0] != "service" || order[1] != "supervisor" {
		t.Fatalf("expected [service supervisor], got %v", order)
	}
}

func TestEventBus_IntentionalHalt_DoesNotFireRestartingEvent(t *testing.T) {
	var restartingFired atomic.Bool

	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrDoNotRestart
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		rungroup.WithServiceEventHandler(func(e rungroup.Event) {
			if e.Type == rungroup.EventServiceRestarting {
				restartingFired.Store(true)
			}
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.Run(ctx)

	if restartingFired.Load() {
		t.Fatal("EventServiceRestarting must not fire for intentional halts")
	}
}

func TestEventBus_EventServiceRestarting_HasCorrectFields(t *testing.T) {
	boom := errors.New("boom")
	var captured rungroup.Event
	var once sync.Once

	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error { return boom }),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		rungroup.WithBackoff(func(attempt int) time.Duration {
			if attempt >= 2 {
				return -1
			}
			return 5 * time.Millisecond
		}),
		rungroup.WithName("flaky"),
		rungroup.WithServiceEventHandler(func(e rungroup.Event) {
			if e.Type == rungroup.EventServiceRestarting {
				once.Do(func() { captured = e })
			}
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.Run(ctx)

	if captured.ServiceName != "flaky" {
		t.Fatalf("expected ServiceName=flaky, got %q", captured.ServiceName)
	}
	if captured.Attempt != 1 {
		t.Fatalf("expected Attempt=1, got %d", captured.Attempt)
	}
	if captured.Delay != 5*time.Millisecond {
		t.Fatalf("expected Delay=5ms, got %v", captured.Delay)
	}
	if !errors.Is(captured.Err, boom) {
		t.Fatalf("expected Err to wrap boom, got %v", captured.Err)
	}
}

// --- Naming ---

func TestWithName_AppearsInErrorMessages(t *testing.T) {
	s := rungroup.New()
	s.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return errors.New("boom")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithName("my-named-service"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "my-named-service") {
		t.Fatalf("expected service name in error message, got %v", err)
	}
}

// --- Nested supervisors ---

func TestNestedSupervisor_RunsChildServices(t *testing.T) {
	childRan := make(chan struct{})

	child := rungroup.New()
	child.Add(rungroup.ServiceFunc(func(ctx context.Context) error {
		close(childRan)
		<-ctx.Done()
		return nil
	}))

	parent := rungroup.New()
	parent.Add(child, rungroup.WithName("child-supervisor"))

	ctx, cancel := context.WithCancel(context.Background())
	go parent.Run(ctx)

	select {
	case <-childRan:
	case <-time.After(2 * time.Second):
		t.Fatal("child service never ran")
	}
	cancel()
}

func TestNestedSupervisor_ErrShutdownAll_PropagatesToParentByDefault(t *testing.T) {
	child := rungroup.New()
	child.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrShutdownAll
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
	)

	parent := rungroup.New()
	parent.Add(child,
		rungroup.WithName("child-supervisor"),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := parent.Run(ctx)
	if !errors.Is(err, rungroup.ErrShutdownAll) {
		t.Fatalf("expected ErrShutdownAll to propagate to parent, got %v", err)
	}
}

func TestNestedSupervisor_WithIsolateShutdown_AbsorbsErrShutdownAll(t *testing.T) {
	child := rungroup.New()
	child.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrShutdownAll
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
	)

	parentStopped := make(chan error, 1)
	parent := rungroup.New()
	parent.Add(child,
		rungroup.WithName("child-supervisor"),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithIsolateShutdown(),
	)
	parent.Add(rungroup.ServiceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}), rungroup.WithName("sibling"))

	ctx, cancel := context.WithCancel(context.Background())
	go func() { parentStopped <- parent.Run(ctx) }()

	// Give the child time to crash and be isolated.
	time.Sleep(100 * time.Millisecond)

	select {
	case err := <-parentStopped:
		t.Fatalf("parent stopped unexpectedly: %v", err)
	default:
	}

	cancel()

	select {
	case err := <-parentStopped:
		if errors.Is(err, rungroup.ErrShutdownAll) {
			t.Fatalf("ErrShutdownAll leaked into parent despite WithIsolateShutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parent did not stop after context cancellation")
	}
}

// --- IntervalService ---

func TestIntervalService_HandlerCalledImmediately(t *testing.T) {
	called := make(chan struct{}, 1)
	svc := rungroup.NewIntervalService(10*time.Second, func(_ context.Context) error {
		select {
		case called <- struct{}{}:
		default:
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go svc.Run(ctx)

	select {
	case <-called:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler was not called immediately")
	}
}

func TestIntervalService_HandlerCalledRepeatedly(t *testing.T) {
	var calls atomic.Int64
	svc := rungroup.NewIntervalService(20*time.Millisecond, func(_ context.Context) error {
		calls.Add(1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	svc.Run(ctx)

	if calls.Load() < 3 {
		t.Fatalf("expected at least 3 calls, got %d", calls.Load())
	}
}

func TestIntervalService_ErrorOnFirstCallReturnedImmediately(t *testing.T) {
	boom := errors.New("first-call-failure")
	svc := rungroup.NewIntervalService(10*time.Second, func(_ context.Context) error {
		return boom
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := svc.Run(ctx)
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func TestIntervalService_ErrorOnTickReturnedImmediately(t *testing.T) {
	boom := errors.New("tick-failure")
	var calls atomic.Int64
	svc := rungroup.NewIntervalService(20*time.Millisecond, func(_ context.Context) error {
		if calls.Add(1) >= 2 {
			return boom
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := svc.Run(ctx)
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func TestIntervalService_ContextCancellationReturnsNil(t *testing.T) {
	svc := rungroup.NewIntervalService(10*time.Second, func(_ context.Context) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- svc.Run(ctx) }()

	// Let the first immediate call complete, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil on clean shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
