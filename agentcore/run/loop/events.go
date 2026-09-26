package loop

import (
	"context"
	"encoding/json"
	"sync"

	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

type serializedEventSink struct {
	sink EventSink
	mu   *sync.Mutex
}

func (s *serializedEventSink) Emit(ctx context.Context, event Event) error { //nolint:gocritic // hugeParam: EventSink contract takes the Event by value
	if s == nil || s.sink == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.Emit(ctx, event)
}

func (l *Loop) emitCommitted(ctx context.Context, events EventSink, scope run.Scope, runID run.RunID, committed []run.Fact) {
	if events == nil || len(committed) == 0 {
		return
	}
	_ = events.Emit(ctx, Event{
		Session:    scope,
		RunID:      runID,
		Kind:       EventAgentCommitted,
		Durability: EventCommitted,
		Committed:  append([]run.Fact(nil), committed...),
	})
}

// progressSink is the ToolProgressSink of one tool execution: each payload
// becomes a tool_progress frame of the call's key (RUN-EXE-12).
type progressSink struct {
	sink effect.ProgressSink
	key  AssignmentKey
}

func (p *progressSink) Publish(ctx context.Context, progress ToolProgress) {
	if p.sink == nil {
		return
	}
	p.sink.Publish(ctx, effect.ProgressFrame{Key: p.key, Kind: effect.ProgressToolProgress, Payload: progress.Payload})
}

// forwardProgress subscribes to key's frames on a ProgressPort and emits
// each as a provisional Event on the drive's sink (RUN-LOP-6): this is how
// deltas produced wherever the effect runs reach the host's observation
// stream. It ends with the stream, the context, or a port that has no
// progress for the key.
func (l *Loop) forwardProgress(ctx context.Context, port effect.ProgressPort, key AssignmentKey, events EventSink) {
	_ = port.Progress(ctx, key, 0, func(f effect.ProgressFrame) bool {
		kind, ok := progressEventKind(f.Kind)
		if !ok {
			return true
		}
		_ = events.Emit(ctx, Event{Session: key.Session, RunID: key.RunID, Effect: key.Effect,
			Generation: f.Generation, Sequence: f.Sequence, Kind: kind, Durability: EventProvisional, Payload: f.Payload})
		return true
	})
}

func progressEventKind(k effect.ProgressKind) (EventKind, bool) {
	switch k {
	case effect.ProgressTextDelta:
		return EventModelTextDelta, true
	case effect.ProgressReasoningDelta:
		return EventModelReasoningDelta, true
	case effect.ProgressToolProgress:
		return EventToolProgress, true
	case effect.ProgressReset:
		return EventProgressReset, true
	default:
		return "", false
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}
