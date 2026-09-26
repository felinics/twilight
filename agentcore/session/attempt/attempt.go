// Package attempt is the first-party module that binds a Turn to the Runs
// that execute it (docs/design/agent-turn.md, ATT-1..3). One fact,
// twilight/attempt/started{turnId, runId, attempt}, records that the Nth
// attempt of a Turn is a given Run; the Turn module consumes it to route a
// Run's settlement to its Turn, and the Run module never learns of it. The
// conversation unit (turn) and the execution scheduling (attempt) are
// therefore separate modules: the Turn stream carries the conversation
// lifecycle only.
package attempt

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/writer"
)

const (
	ModuleID extension.ModuleID = "attempt"
	// StreamDomain is the stream domain of attempt facts: one keyed stream
	// per Turn, bound by the payload's turnId, continued by a fork.
	StreamDomain = "attempt"
	// Version is the payload version the module writes (EXT-REG-2).
	Version extension.PayloadVersion = 1
	// TypeStarted binds one attempt of a Turn to the Run that executes it
	// (ATT-1).
	TypeStarted session.EventType = "twilight/attempt/started"
	// IndexProjectionID is the module's projection (ATT-3).
	IndexProjectionID extension.ProjectionID = "twilight/attempt/index"
)

// TurnID is the Turn an attempt belongs to. The module names Turns by
// identity only; turn.TurnID converts to and from it.
type TurnID string

var streamDefinition = extension.StreamDefinition{Domain: StreamDomain, Key: streamKey, Lineage: session.LineageSession}

// streamKey binds the started fact to its Turn's stream (EXT-STR-1).
func streamKey(value any) (string, error) {
	p, ok := value.(StartedPayload)
	if !ok {
		return "", fmt.Errorf("value is %T, want attempt.StartedPayload", value)
	}
	return string(p.TurnID), nil
}

// Stream is the logical stream of one Turn's attempts.
func Stream(turnID TurnID) session.StreamRef { return streamDefinition.Ref(string(turnID)) }

// StartedPayload is twilight/attempt/started.
type StartedPayload struct {
	TurnID  TurnID    `json:"turnId"`
	RunID   run.RunID `json:"runId"`
	Attempt uint32    `json:"attempt"`
}

// Started is the batch the Turn coordinator adds to the unit that creates a
// Run for a Turn (ATT-2): the binding lands in the same commit as the Run's
// creation and the Turn's own facts.
func Started(turnID TurnID, runID run.RunID, attempt uint32, now int64) writer.TypedBatch {
	return writer.TypedBatch{Stream: Stream(turnID), Events: []writer.TypedEvent{{
		Type: TypeStarted, RecordedAtUnixMilli: now, Value: StartedPayload{TurnID: turnID, RunID: runID, Attempt: attempt}}}}
}

// Record is one attempt: the Run that executes the Turn's Nth try.
type Record struct {
	TurnID  TurnID    `json:"turnId"`
	RunID   run.RunID `json:"runId"`
	Attempt uint32    `json:"attempt"`
}

// Index is the module's projection state (ATT-3): every attempt of the
// Session by Run and by Turn. Whether an attempt ended, and how, is the
// Run's own fact (twilight/run/run_ended); the Turn surface joins the two.
type Index struct {
	ByRun  map[run.RunID]Record `json:"byRun"`
	ByTurn map[TurnID][]Record  `json:"byTurn"`
}

// Of returns the attempt a Run executes.
func (x *Index) Of(runID run.RunID) (Record, bool) {
	r, ok := x.ByRun[runID]
	return r, ok
}

// Attempts lists a Turn's attempts in order of their ordinal.
func (x *Index) Attempts(turnID TurnID) []Record {
	return append([]Record(nil), x.ByTurn[turnID]...)
}

func (x Index) clone() Index {
	out := Index{ByRun: make(map[run.RunID]Record, len(x.ByRun)), ByTurn: make(map[TurnID][]Record, len(x.ByTurn))}
	for k, v := range x.ByRun {
		out.ByRun[k] = v
	}
	for k, v := range x.ByTurn {
		out.ByTurn[k] = append([]Record(nil), v...)
	}
	return out
}

// IndexProjection folds twilight/attempt/started. It refuses a Run bound
// twice and an ordinal that is not the Turn's next (ATT-1): the binding is
// the routing key of every settlement, so a malformed one must not land.
var IndexProjection = extension.ProjectionDefinition{
	ID: IndexProjectionID, Version: 1,
	Consumes:      []session.EventType{TypeStarted},
	Authoritative: true,
	Initial: func() (any, error) {
		return Index{ByRun: map[run.RunID]Record{}, ByTurn: map[TurnID][]Record{}}, nil
	},
	Apply:      applyIndex,
	StateCodec: extension.JSONStateCodec[Index]{},
}

//nolint:gocritic // hugeParam: DecodedEvent is the extension Apply shape
func applyIndex(state any, e extension.DecodedEvent) (any, error) {
	prev, ok := state.(Index)
	if !ok {
		return nil, fmt.Errorf("attempt index: state is %T", state)
	}
	p, ok := e.Value.(StartedPayload)
	if !ok {
		return nil, fmt.Errorf("attempt index: unexpected %T", e.Value)
	}
	x := prev.clone()
	if have, dup := x.ByRun[p.RunID]; dup {
		return nil, fmt.Errorf("attempt index: run %s is already attempt %d of turn %s", p.RunID, have.Attempt, have.TurnID)
	}
	if next := len(x.ByTurn[p.TurnID]) + 1; int64(p.Attempt) != int64(next) {
		return nil, fmt.Errorf("attempt index: turn %s attempt %d out of order, want %d", p.TurnID, p.Attempt, next)
	}
	rec := Record(p)
	x.ByRun[p.RunID] = rec
	x.ByTurn[p.TurnID] = append(x.ByTurn[p.TurnID], rec)
	return x, nil
}

// Module is the attempt ModuleDescriptor: one stream domain, one fact, one
// projection, no requirements.
var Module = extension.ModuleDescriptor{
	Source:  extension.SourceTwilight,
	ID:      ModuleID,
	Streams: []extension.StreamDefinition{streamDefinition},
	Events: []extension.EventDefinition{{
		Type: TypeStarted, Stream: StreamDomain,
		Codecs: map[extension.PayloadVersion]extension.PayloadCodec{Version: extension.JSONCodec[StartedPayload]{Check: func(p *StartedPayload) error {
			if p.TurnID == "" || p.RunID == "" || p.Attempt == 0 {
				return errors.New("attempt started requires turnId, runId and attempt")
			}
			return nil
		}}},
	}},
	Projections: []extension.ProjectionDefinition{IndexProjection},
}

// Read folds the index of a Session through a lease-free reader.
func Read(ctx context.Context, reader extension.ProjectionReader, sid session.SessionID) (Index, error) {
	state, _, err := reader.Load(ctx, sid, IndexProjectionID, IndexProjection.Version)
	if err != nil {
		return Index{}, err
	}
	x, ok := state.(Index)
	if !ok {
		return Index{}, fmt.Errorf("attempt index: state is %T", state)
	}
	return x, nil
}
