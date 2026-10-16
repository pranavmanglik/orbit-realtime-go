# Orbit Realtime Go

`orbit-realtime-go` is the Go implementation of Orbit's Server-Sent Events capability. It provides bounded SSE framing and a standard-library `net/http` handler; it adds no web framework, server, queue, or provider SDK. Its `Event.WithEvent("")` and `Event.WithID("")` methods preserve explicit empty fields, matching the Python and Rust implementations: an empty event type selects the default event, while an empty ID resets the browser's reconnect ID.

```sh
go get github.com/orbit-projects/orbit-realtime-go
```

```go
package main

import (
    "context"
    "log"
    "net/http"

    realtime "github.com/orbit-projects/orbit-realtime-go"
)

func main() {
    updates := make(chan realtime.Event, 32) // Feed from the application's bounded event source.
    handler, err := realtime.NewHandler(func(ctx context.Context, emit realtime.Emit) error {
        for {
            select {
            case <-ctx.Done():
                return ctx.Err()
            case event, ok := <-updates:
                if !ok { return nil }
                if err := emit(event); err != nil { return err }
            }
        }
    })
    if err != nil { log.Fatal("invalid SSE handler configuration") }
    http.Handle("/events", handler)
    log.Fatal(http.ListenAndServe("127.0.0.1:8080", nil))
}
```

The handler emits each bounded event and flushes it before that `emit` call returns, so writes apply direct backpressure without an internal queue. Concurrent `emit` calls are serialized. The producer must honor request cancellation and join any emitter goroutines before returning; the handler cancels the emitter at producer completion to prevent late writes. SSE headers are committed with the first event, so a producer failure before streaming returns a generic plain-text HTTP 500. The handler disables common proxy buffering with `X-Accel-Buffering: no`; configure and verify the actual ingress separately. Event payloads are excluded from string representations and errors do not include payload data.

The package does not provide authentication, authorization, event retention, replay, shared fanout, heartbeat scheduling, or reconnect policy. Secure the route in the application and bound upstream subscriptions.

See [architecture](docs/architecture/overview.md), [operations](docs/operations/README.md), [security](docs/security/overview.md), and [development](docs/development/README.md).

```sh
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

alpha. Hosted CI, proxy disconnect/flush interoperability, multi-instance load and soak tests, and release provenance remain open release gates. Licensed under Apache-2.0.
