package effect

import (
	"context"
	"encoding/json"
)

// ProgressKind names one kind of progress frame (RUN-EXE-12).
type ProgressKind string

const (
	// ProgressTextDelta and ProgressReasoningDelta carry a model's streamed
	// text; Payload is the delta as a JSON string.
	ProgressTextDelta      ProgressKind = "model_text_delta"
	ProgressReasoningDelta ProgressKind = "model_reasoning_delta"
	// ProgressToolProgress carries a tool's own progress payload.
	ProgressToolProgress ProgressKind = "tool_progress"
	// ProgressReset opens a new generation of the same effect: the Worker
	// re-dispatched it (RUN-EXE-9/11), so every frame of the earlier
	// generation is void and the receiver starts over.
	ProgressReset ProgressKind = "reset"
	// ProgressEnd closes the stream: the execution reached a terminal state
	// and its Outcome is, or is about to be, readable through GetOutcome.
	ProgressEnd ProgressKind = "end"
)

// ProgressFrame is one provisional observation of an execution in flight
// (RUN-EXE-12). Frames are not facts: they may be lost, and a Reset voids
// the ones before it. Generation counts the effect's attempts under the
// Worker (a Reset raises it), Sequence orders frames within the key.
type ProgressFrame struct {
	Key        AssignmentKey   `json:"key"`
	Generation int             `json:"generation"`
	Sequence   uint64          `json:"sequence"`
	Kind       ProgressKind    `json:"kind"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// ProgressSink is where a backend emits progress: the Worker's hub. The
// backend fills Key, Kind and Payload; the sink stamps Generation and
// Sequence.
type ProgressSink interface {
	Publish(context.Context, ProgressFrame)
}

// ProgressPort is the progress side of an ExecutionPort (RUN-EXE-12), an
// optional capability. Progress delivers the frames of key with Sequence
// greater than after to fn, in order, until fn returns false, ctx ends, or
// an End frame closes the stream; a key the port never saw is
// ErrExecutionNotFound. The stream is lossy by contract: a port keeps a
// bounded window per key, frames evicted before a subscriber read them are
// gone, and a gap in Sequence tells the subscriber so.
type ProgressPort interface {
	Progress(ctx context.Context, key AssignmentKey, after uint64, fn func(ProgressFrame) bool) error
}
