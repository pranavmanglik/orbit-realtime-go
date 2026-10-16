// Copyright 2026-present Orbit Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package realtime provides bounded Server-Sent Events framing and net/http streaming.
//
// Producers run synchronously with the HTTP request. Emitter calls are serialized, and each event
// is written and flushed before its call returns. The package has no queue, replay, heartbeat,
// authentication, or fan-out behavior; the host application owns those policies and the producer's
// upstream resource lifecycle.
package realtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// MaxEventBytes bounds the complete encoded event frame, including field names and separators.
	MaxEventBytes = 65_536
	// MaxEventNameBytes bounds the UTF-8 event type before framing.
	MaxEventNameBytes = 256
	// MaxEventIDBytes bounds the UTF-8 event ID before framing.
	MaxEventIDBytes = 1_024
	// MaxCommentBytes bounds the complete encoded comment frame.
	MaxCommentBytes = 4_096
	// MaxRetryMS is the largest retry delay accepted in an event.
	MaxRetryMS = 2_147_483_647
)

var (
	// ErrInvalidEvent identifies invalid or over-limit event and comment fields.
	ErrInvalidEvent = errors.New("invalid SSE event")
)

// Event is one bounded SSE event. Data and ID are excluded from String output.
type Event struct {
	// Data is UTF-8 event content; CR/LF line endings are normalized when framed.
	Data string
	// Event is an optional single-line event type.
	Event string
	// EventSet preserves an explicitly empty event type (which selects the default event type).
	// Use WithEvent to set this flag without constructing the struct fields separately.
	EventSet bool
	// ID is an optional single-line reconnect identifier and cannot contain NUL.
	ID string
	// IDSet preserves an explicitly empty ID, which resets the browser's last-event ID.
	// Use WithID to set this flag without constructing the struct fields separately.
	IDSet bool
	// Retry is an optional millisecond delay in the range from zero through MaxRetryMS.
	Retry *uint32
}

// WithEvent returns a copy with an explicitly set event type. An empty value selects the default
// event type and is encoded as an empty `event:` field.
func (e Event) WithEvent(event string) Event {
	e.Event = event
	e.EventSet = true
	return e
}

// WithID returns a copy with an explicitly set reconnect ID. An empty value resets the client's
// last-event ID and is encoded as an empty `id:` field.
func (e Event) WithID(id string) Event {
	e.ID = id
	e.IDSet = true
	return e
}

// String deliberately omits user-controlled event data and IDs.
func (e Event) String() string {
	return fmt.Sprintf("Event{event_set:%t data_bytes:%d id_set:%t}", e.EventSet || e.Event != "", len(e.Data), e.IDSet || e.ID != "")
}

// Encode serializes one event according to the HTML event-stream format.
//
// The returned bytes include the terminating blank line and never exceed MaxEventBytes. Invalid
// UTF-8, line injection in event names/IDs, NUL in IDs, and oversized frames return ErrInvalidEvent.
func (e Event) Encode() ([]byte, error) {
	if err := validateText(e.Data, MaxEventBytes, false, false); err != nil {
		return nil, err
	}
	if e.EventSet || e.Event != "" {
		if err := validateText(e.Event, MaxEventNameBytes, true, false); err != nil {
			return nil, err
		}
	}
	if e.IDSet || e.ID != "" {
		if err := validateText(e.ID, MaxEventIDBytes, true, true); err != nil {
			return nil, err
		}
	}
	if e.Retry != nil && *e.Retry > MaxRetryMS {
		return nil, fmt.Errorf("%w: retry is outside supported range", ErrInvalidEvent)
	}
	var b strings.Builder
	if e.EventSet || e.Event != "" {
		b.WriteString("event: ")
		b.WriteString(e.Event)
		b.WriteByte('\n')
	}
	if e.IDSet || e.ID != "" {
		b.WriteString("id: ")
		b.WriteString(e.ID)
		b.WriteByte('\n')
	}
	if e.Retry != nil {
		fmt.Fprintf(&b, "retry: %d\n", *e.Retry)
	}
	data := strings.ReplaceAll(strings.ReplaceAll(e.Data, "\r\n", "\n"), "\r", "\n")
	for _, line := range strings.Split(data, "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	if b.Len() > MaxEventBytes {
		return nil, fmt.Errorf("%w: encoded event exceeds %d-byte limit", ErrInvalidEvent, MaxEventBytes)
	}
	return []byte(b.String()), nil
}

// EncodeComment returns a bounded SSE comment frame suitable for explicit keepalives.
//
// Keepalive scheduling remains the caller's responsibility.
func EncodeComment(comment string) ([]byte, error) {
	if err := validateText(comment, MaxCommentBytes, false, false); err != nil {
		return nil, err
	}
	normalized := strings.ReplaceAll(strings.ReplaceAll(comment, "\r\n", "\n"), "\r", "\n")
	var b strings.Builder
	for _, line := range strings.Split(normalized, "\n") {
		b.WriteString(": ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	if b.Len() > MaxCommentBytes {
		return nil, fmt.Errorf("%w: encoded comment exceeds %d-byte limit", ErrInvalidEvent, MaxCommentBytes)
	}
	return []byte(b.String()), nil
}

func validateText(value string, max int, lineSafe, rejectNull bool) error {
	if !utf8.ValidString(value) || len(value) > max || (lineSafe && strings.ContainsAny(value, "\r\n")) || (rejectNull && strings.ContainsRune(value, '\x00')) {
		return fmt.Errorf("%w: text is invalid or exceeds its bound", ErrInvalidEvent)
	}
	return nil
}

// Emit writes one event and flushes it before returning, applying downstream backpressure directly.
// Concurrent calls are serialized. The callback is canceled when the producer returns, so the
// producer must join any goroutines that call emit before returning.
type Emit func(Event) error

// Stream produces events synchronously. It must return when ctx is canceled; emit fails after
// disconnect and after the producer returns. Producers must join emitter goroutines before return.
type Stream func(ctx context.Context, emit Emit) error

// NewHandler adapts a producer to net/http without an unbounded intermediate queue.
//
// Before the first frame, a producer failure returns a generic HTTP 500. After a response has
// started, failures simply terminate the stream because an HTTP status can no longer be changed.
// The producer must stop and release upstream resources when the request context is canceled. The
// handler cancels the emitter when the producer returns to prevent writes after the HTTP lifetime.
func NewHandler(stream Stream) (http.Handler, error) {
	if stream == nil {
		return nil, fmt.Errorf("%w: stream is required", ErrInvalidEvent)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streamContext, cancel := context.WithCancel(r.Context())
		defer cancel()
		controller := http.NewResponseController(w)
		var emitMu sync.Mutex
		committed := false
		emit := func(event Event) error {
			if err := streamContext.Err(); err != nil {
				return err
			}
			frame, err := event.Encode()
			if err != nil {
				return err
			}
			emitMu.Lock()
			defer emitMu.Unlock()
			if err := streamContext.Err(); err != nil {
				return err
			}
			if !committed {
				w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
				w.Header().Set("Cache-Control", "no-cache, no-transform")
				w.Header().Set("X-Accel-Buffering", "no")
				w.Header().Set("X-Content-Type-Options", "nosniff")
			}
			committed = true
			if _, err := w.Write(frame); err != nil {
				return err
			}
			return controller.Flush()
		}
		err := stream(streamContext, emit)
		cancel()
		if err != nil && r.Context().Err() == nil {
			emitMu.Lock()
			streamStarted := committed
			emitMu.Unlock()
			if streamStarted {
				return
			}
			// Before the first frame, return a generic error without exposing producer details.
			http.Error(w, "event stream unavailable", http.StatusInternalServerError)
		}
	}), nil
}
