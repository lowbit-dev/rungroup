# RunGroup

[![Go Report Card](https://goreportcard.com/badge/lowbit.dev/rungroup)](https://goreportcard.com/report/lowbit.dev/rungroup) [![Go Reference](https://pkg.go.dev/badge/lowbit.dev/rungroup.svg)](https://pkg.go.dev/lowbit.dev/rungroup) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE) 

RunGroup is a concurrency primitive for managing long-lived background services in Go. It supervises goroutines independently, handling restarts, panic recovery, backoff, and clean shutdown. It blocks until all services have exited before returning a joined aggregate of any errors.

- **Restart policies** — always, on failure, or never; configured per service
- **Backoff** — custom strategy with optional hard restart limits
- **Panic recovery** — panics are caught and converted to errors, stack trace included
- **Sentinel errors** — signal intentional halts or trigger group-wide shutdown
- **Shutdown timeouts** — per-service and global, with graceful goroutine abandonment
- **Events** — observe restarts, halts, and timeouts via callbacks
- **Composable** — `*Group` implements `Service`, enabling supervision trees
- **Zero dependencies**

## Install

```sh
go get lowbit.dev/rungroup
```

## Usage

```go
g := rungroup.New()

g.Add(
    rungroup.ServiceFunc(func(ctx context.Context) error {
        return server.Serve(ctx)
    }),
    rungroup.WithName("http-server"),
    rungroup.WithRestartPolicy(rungroup.RestartOnFailure),
    rungroup.WithBackoff(func(attempt int) time.Duration {
        return time.Duration(attempt) * 500 * time.Millisecond
    }),
)

if err := g.Run(ctx); err != nil {
    log.Fatal(err)
}
```

## Restart Policies

| Policy                    | On nil  | On error |
| ------------------------- | ------- | -------- |
| `RestartAlways` (default) | restart | restart  |
| `RestartOnFailure`        | halt    | restart  |
| `RestartNever`            | halt    | halt     |

`WithBackoff` accepts `func(attempt int) time.Duration`. Return a negative duration to permanently stop the service (`ErrRestartLimitExceeded`). Use `WithStabilityWindow` to reset the attempt counter after a service has run continuously for a given duration.

## Sentinel Errors

| Error             | Effect                                                 |
| ----------------- | ------------------------------------------------------ |
| `ErrDoNotRestart` | Permanently halts the service regardless of policy.    |
| `ErrShutdownAll`  | Cancels the group context and shuts down all services. |

Both are matched via `errors.Is` traversal.

## Shutdown Timeouts

```go
g := rungroup.New(rungroup.WithShutdownTimeout(10 * time.Second))

g.Add(svc, rungroup.WithServiceShutdownTimeout(2 * time.Second))
```

Per-service and global timeouts race; whichever fires first wins. Exceeded services emit `EventShutdownTimeout` and are abandoned.

## Nested Groups

`*Group` implements `Service`. Add a group to another to build a supervision tree. By default `ErrShutdownAll` propagates upward; there are two ways to absorb it at a boundary:

- **`WithShutdownBoundary()`** — set on the group itself at construction time. Prefer this when you own the group definition.
- **`WithShutdownIsolation()`** — set on the service entry when registering the group into a parent. Use this when you do not own the group.

```go
// Option A: boundary declared on the group
inner := rungroup.New(rungroup.WithShutdownBoundary())

// Option B: isolation declared at the registration site
outer.Add(inner,
    rungroup.WithName("inner-group"),
    rungroup.WithShutdownIsolation(),
)
```

## Events

```go
rungroup.New(rungroup.WithEventHandler(func(e rungroup.Event) {
    // e.Type: EventServiceRestarting | EventServiceHalted | EventShutdownTimeout
    // e.ServiceName, e.Attempt, e.Delay, e.Err
}))
```

Per-service handlers can be registered with `WithServiceEventHandler` and fire before the group-level handler.

## Interval Service

`IntervalService` runs a handler on a fixed schedule. The handler is called immediately on start, then on every tick. A non-nil return exits `Run` and hands control back to the group's restart policy.

```go
g.Add(
    rungroup.NewIntervalService(30*time.Second, func(ctx context.Context) error {
        return cache.Refresh(ctx)
    }),
    rungroup.WithName("cache-refresh"),
    rungroup.WithRestartPolicy(rungroup.RestartOnFailure),
)
```

## Options

**Service** — `WithName`, `WithRestartPolicy`, `WithBackoff`, `WithStabilityWindow`, `WithServiceShutdownTimeout`, `WithShutdownIsolation`, `WithServiceEventHandler`

**Group** — `WithShutdownTimeout`, `WithEventHandler`, `WithShutdownBoundary`

## License

MIT
