# ADR 0001: Use native net/http streaming for Go SSE

- Status: accepted
- Date: 2026-10-06

## Context

Orbit Realtime has a Python implementation integrated with Orbit Core's ASGI response. The Go option needs to produce SSE in Go without embedding Python or introducing another framework. SSE is an HTTP response stream, and Go's standard library already exposes request cancellation, response writes, and flushing.

## Decision

Expose bounded SSE framing plus a net/http handler that calls a synchronous producer callback. Each emitted frame is written and flushed before its emit call returns, with no internal queue. Emitter calls are serialized, and the emitter is canceled when the producer returns. The producer owns upstream resource cleanup and joins any goroutines that emit. SSE headers are committed on the first frame; pre-stream failures return a sanitized plain-text response.

## Consequences

The API composes with Go HTTP routers and servers without a runtime dependency. Slow clients apply write backpressure to the producer. Applications must honor request cancellation and implement their own authorization, event source, heartbeat, replay, and fanout policies. Proxy flush/disconnect behavior requires deployment-specific validation.
