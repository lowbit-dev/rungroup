package rungroup_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"lowbit.dev/rungroup"
)

// Example demonstrates the minimal setup: register a service and run until the
// parent context is cancelled.
func Example() {
	g := rungroup.New()
	g.Add(rungroup.ServiceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}), rungroup.WithName("worker"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // stop immediately

	if err := g.Run(ctx); err != nil {
		fmt.Println("error:", err)
	} else {
		fmt.Println("ok")
	}
	// Output:
	// ok
}

// ExampleNew shows creating a Group with a global shutdown timeout option.
func ExampleNew() {
	// Zero services: Run returns nil without blocking.
	g := rungroup.New(rungroup.WithShutdownTimeout(5 * time.Second))
	fmt.Println(g.Run(context.Background()))
	// Output:
	// <nil>
}

// ExampleGroup_Add shows that Add returns nil for a valid service registration.
func ExampleGroup_Add() {
	g := rungroup.New()
	err := g.Add(
		rungroup.ServiceFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}),
		rungroup.WithName("worker"),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
	)
	fmt.Println(err)
	// Output:
	// <nil>
}

// ExampleGroup_Run_zeroServices shows that Run returns nil immediately when no
// services have been registered.
func ExampleGroup_Run_zeroServices() {
	g := rungroup.New()
	fmt.Println(g.Run(context.Background()))
	// Output:
	// <nil>
}

// ExampleServiceFunc shows the function adapter, which lets an ordinary
// function satisfy the Service interface.
func ExampleServiceFunc() {
	svc := rungroup.ServiceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fmt.Println(svc.Run(ctx))
	// Output:
	// <nil>
}

// ExampleNewIntervalService shows an IntervalService that stops itself by
// returning ErrDoNotRestart from the handler.
func ExampleNewIntervalService() {
	var calls int
	svc := rungroup.NewIntervalService(50*time.Millisecond, func(_ context.Context) error {
		calls++
		if calls >= 3 {
			return rungroup.ErrDoNotRestart
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc.Run(ctx) //nolint:errcheck
	fmt.Println(calls)
	// Output:
	// 3
}

// ExampleIntervalService_Run shows that an already-cancelled context causes
// the handler to be invoked exactly once before Run returns.
func ExampleIntervalService_Run() {
	var ticks int
	svc := rungroup.NewIntervalService(time.Second, func(_ context.Context) error {
		ticks++
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	svc.Run(ctx) //nolint:errcheck
	fmt.Println(ticks)
	// Output:
	// 1
}

// ExampleErrAlreadyRunning shows that calling Run on an already-active Group
// returns ErrAlreadyRunning.
func ExampleErrAlreadyRunning() {
	g := rungroup.New()
	ready := make(chan struct{})
	g.Add(rungroup.ServiceFunc(func(ctx context.Context) error {
		close(ready)
		<-ctx.Done()
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go g.Run(ctx) //nolint:errcheck
	<-ready       // wait until the first Run is active

	fmt.Println(errors.Is(g.Run(ctx), rungroup.ErrAlreadyRunning))
	// Output:
	// true
}

// ExampleErrDoNotRestart shows that a service returning ErrDoNotRestart is
// permanently stopped; Run wraps ErrIntentionalHalt in the returned error.
func ExampleErrDoNotRestart() {
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrDoNotRestart
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	fmt.Println(errors.Is(g.Run(ctx), rungroup.ErrIntentionalHalt))
	// Output:
	// true
}

// ExampleErrShutdownAll shows that a service can trigger a group-wide
// shutdown; the error is surfaced to the Run caller.
func ExampleErrShutdownAll() {
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrShutdownAll
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithName("trigger"),
	)
	g.Add(
		rungroup.ServiceFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}),
		rungroup.WithName("bystander"),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println(errors.Is(g.Run(ctx), rungroup.ErrShutdownAll))
	// Output:
	// true
}

// ExampleErrServicePanic shows that a panicking service is recovered and Run
// returns a wrapped ErrServicePanic.
func ExampleErrServicePanic() {
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			panic("unexpected state")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	fmt.Println(errors.Is(g.Run(ctx), rungroup.ErrServicePanic))
	// Output:
	// true
}

// ExampleWithRestartPolicy_restartNever shows that a service is not restarted
// after an error; Run wraps ErrPolicyHalt in the returned error.
func ExampleWithRestartPolicy_restartNever() {
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return errors.New("fatal")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
		rungroup.WithName("worker"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	fmt.Println(errors.Is(g.Run(ctx), rungroup.ErrPolicyHalt))
	// Output:
	// true
}

// ExampleWithRestartPolicy_restartOnFailure shows that a service is restarted
// only after a non-nil return; a clean nil exit stops the loop.
func ExampleWithRestartPolicy_restartOnFailure() {
	var calls int
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return rungroup.ErrDoNotRestart
		}),
		rungroup.WithRestartPolicy(rungroup.RestartOnFailure),
		rungroup.WithBackoff(func(_ int) time.Duration { return 0 }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	g.Run(ctx) //nolint:errcheck
	fmt.Println(calls)
	// Output:
	// 3
}

// ExampleWithRestartPolicy_restartAlways shows that RestartAlways restarts the
// service even after a clean (nil) return.
func ExampleWithRestartPolicy_restartAlways() {
	var calls int
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls++
			if calls >= 3 {
				return rungroup.ErrDoNotRestart
			}
			return nil // clean exit: RestartAlways restarts anyway
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		rungroup.WithBackoff(func(_ int) time.Duration { return 0 }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	g.Run(ctx) //nolint:errcheck
	fmt.Println(calls)
	// Output:
	// 3
}

// ExampleWithBackoff shows a backoff function that enforces a maximum retry
// count; Run wraps ErrRestartLimitExceeded once the limit is reached.
func ExampleWithBackoff() {
	var calls int
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			calls++
			return errors.New("failing")
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		rungroup.WithBackoff(func(attempt int) time.Duration {
			if attempt >= 3 {
				return -1 // negative → give up
			}
			return 0
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := g.Run(ctx)
	fmt.Println(errors.Is(err, rungroup.ErrRestartLimitExceeded))
	fmt.Println(calls)
	// Output:
	// true
	// 3
}

// ExampleWithName shows that WithName labels events so restarts and halts are
// identifiable in the event stream.
func ExampleWithName() {
	var halted string
	g := rungroup.New(rungroup.WithEventHandler(func(e rungroup.Event) {
		if e.Type == rungroup.EventServiceHalted {
			halted = e.ServiceName
		}
	}))
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrDoNotRestart
		}),
		rungroup.WithName("database"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	g.Run(ctx) //nolint:errcheck
	fmt.Println(halted)
	// Output:
	// database
}

// ExampleWithEventHandler shows receiving lifecycle events for every service
// in the group.
func ExampleWithEventHandler() {
	g := rungroup.New(
		rungroup.WithEventHandler(func(e rungroup.Event) {
			switch e.Type {
			case rungroup.EventServiceRestarting:
				fmt.Printf("%s: restarting (attempt %d)\n", e.ServiceName, e.Attempt)
			case rungroup.EventServiceHalted:
				fmt.Printf("%s: halted\n", e.ServiceName)
			}
		}),
	)
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrDoNotRestart
		}),
		rungroup.WithName("worker"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	g.Run(ctx) //nolint:errcheck
	// Output:
	// worker: halted
}

// ExampleWithServiceEventHandler shows a per-service event handler that fires
// independently of any group-level handler.
func ExampleWithServiceEventHandler() {
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrDoNotRestart
		}),
		rungroup.WithName("cache"),
		rungroup.WithServiceEventHandler(func(e rungroup.Event) {
			if e.Type == rungroup.EventServiceHalted {
				fmt.Printf("%s halted, has err: %v\n", e.ServiceName, e.Err != nil)
			}
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	g.Run(ctx) //nolint:errcheck
	// Output:
	// cache halted, has err: true
}

// ExampleWithShutdownTimeout shows that a service which does not exit within
// the global deadline is abandoned; Run wraps ErrShutdownTimeout.
func ExampleWithShutdownTimeout() {
	g := rungroup.New(rungroup.WithShutdownTimeout(50 * time.Millisecond))
	started := make(chan struct{})
	g.Add(rungroup.ServiceFunc(func(_ context.Context) error {
		close(started)
		time.Sleep(10 * time.Second) // simulate a stuck service
		return nil
	}), rungroup.WithName("stuck"))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	fmt.Println(errors.Is(g.Run(ctx), rungroup.ErrShutdownTimeout))
	// Output:
	// true
}

// ExampleWithServiceShutdownTimeout shows a per-service deadline that only
// affects the service it is attached to.
func ExampleWithServiceShutdownTimeout() {
	g := rungroup.New()
	started := make(chan struct{})
	g.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			close(started)
			time.Sleep(10 * time.Second)
			return nil
		}),
		rungroup.WithName("stuck"),
		rungroup.WithServiceShutdownTimeout(50*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	fmt.Println(errors.Is(g.Run(ctx), rungroup.ErrShutdownTimeout))
	// Output:
	// true
}

// ExampleWithIsolateShutdown shows that ErrShutdownAll from a nested Group is
// absorbed at the service boundary; the parent only sees ErrPolicyHalt.
func ExampleWithIsolateShutdown() {
	inner := rungroup.New()
	inner.Add(
		rungroup.ServiceFunc(func(_ context.Context) error {
			return rungroup.ErrShutdownAll
		}),
		rungroup.WithRestartPolicy(rungroup.RestartNever),
	)

	outer := rungroup.New()
	outer.Add(inner,
		rungroup.WithName("inner"),
		rungroup.WithIsolateShutdown(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := outer.Run(ctx)
	fmt.Println(errors.Is(err, rungroup.ErrShutdownAll)) // absorbed at boundary
	fmt.Println(errors.Is(err, rungroup.ErrPolicyHalt))  // surfaced instead
	// Output:
	// false
	// true
}

// ExampleWithStabilityWindow shows configuring a window that resets the
// restart attempt counter for services that run stably for a long period.
// This example is compiled but not executed (no Output comment) because the
// behaviour is timing-dependent.
func ExampleWithStabilityWindow() {
	g := rungroup.New()
	g.Add(
		rungroup.ServiceFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}),
		rungroup.WithRestartPolicy(rungroup.RestartAlways),
		// Reset restart counter after the service has run for 30 s without crashing.
		rungroup.WithStabilityWindow(30*time.Second),
		// Exponential backoff; give up after 5 consecutive crashes.
		rungroup.WithBackoff(func(attempt int) time.Duration {
			if attempt > 5 {
				return -1
			}
			return time.Duration(attempt) * 500 * time.Millisecond
		}),
	)
}
