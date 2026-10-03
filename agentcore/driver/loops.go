// Package driver advances durable Turns. Loops composes and caches the Loop
// of each AgentPreset over the shared Executor; Driver drives an active
// Turn to its next quiescent point; Recovery runs the takeover disposition
// when a Session is opened and settles Outcomes that survived a previous
// owner; Responders answer the waits a tool leaves for the owner process.
// None of them decides where inputs go or what a reply is; those are the
// caller's.
package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Presets resolves a PresetRef to its immutable AgentPreset.
type Presets interface {
	Resolve(preset.PresetRef) (preset.AgentPreset, error)
}

// Planner is the application's between-steps hook: it runs while a Run is
// Open and about to plan a model request, with the Writer of the Session
// being driven, so what it commits (an in-turn checkpoint) is what the
// PromptBuilder reads next. Errors stop the drive.
type Planner interface {
	BeforePrepare(ctx context.Context, w writer.Writer, input decision.Input) error
}

// Loops builds the Loop of each AgentPreset once and hands the same Loop to
// every drive, redispatch and reattachment of a Run under that preset, so
// all of them meet the same already-driving guard. The Loop settings are
// fixed when the first Loop is built; a field changed later reaches only
// presets not built yet.
type Loops struct {
	Executor  effect.ExecutionPort
	Presets   Presets
	Decisions *decision.Catalog
	Sources   decision.Sources
	// Targets resolves the opaque target of each effect; every Loop shares
	// it.
	Targets loop.TargetResolver
	// Dispatch is the re-offer policy of every Loop for a retryable dispatch
	// refusal; the zero value selects loop's defaults.
	Dispatch loop.DispatchPolicy
	// Watcher is where every Loop waits for Outcomes: one settlement
	// subscription to the Executor shared with the Recovery; required.
	Watcher *effect.Watcher
	// Planner, when set, is consulted through every Loop's BeforePrepare:
	// the application's between-steps context policy, given the Writer the
	// drive commits through.
	Planner Planner

	mu    sync.Mutex
	loops map[preset.PresetRef]*loop.Loop
}

// For returns the Loop of the preset, building it on first use.
func (l *Loops) For(ref preset.PresetRef) (*loop.Loop, error) {
	if l.Watcher == nil {
		return nil, errors.New("driver: loops require a watcher")
	}
	ap, err := l.Presets.Resolve(ref)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if lp, ok := l.loops[ref]; ok {
		return lp, nil
	}
	builder, err := l.Decisions.Resolve(ap, l.Sources)
	if err != nil {
		return nil, err
	}
	settings := loop.Settings{
		Scheduling:       ap.Scheduling,
		MalformedRetries: ap.MalformedRetries,
		TargetResolver:   l.Targets,
		Dispatch:         l.Dispatch,
		Watcher:          l.Watcher,
	}
	if l.Planner != nil {
		settings.BeforePrepare = l.beforePrepare
	}
	lp, err := loop.New(l.Executor, builder, settings)
	if err != nil {
		return nil, err
	}
	if l.loops == nil {
		l.loops = make(map[preset.PresetRef]*loop.Loop)
	}
	l.loops[ref] = lp
	return lp, nil
}

// ForRun returns the Loop of the Turn that owns the Run, read from the
// Session's turn surface through w, and that Turn's ID.
func (l *Loops) ForRun(ctx context.Context, w writer.Writer, runID run.RunID) (*loop.Loop, turn.TurnID, error) {
	turnID, err := ownerOf(ctx, w, runID)
	if err != nil {
		return nil, "", err
	}
	surface, err := turn.ReadSurface(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return nil, "", err
	}
	lp, err := l.For(surface.Turns[turnID].Preset)
	if err != nil {
		return nil, "", err
	}
	return lp, turnID, nil
}

// ownerOf is the Turn that owns the Run, read from the Session's turn
// surface through w.
func ownerOf(ctx context.Context, w writer.Writer, runID run.RunID) (turn.TurnID, error) {
	surface, err := turn.ReadSurface(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return "", err
	}
	turnID, ok := surface.OwnerOf(runID)
	if !ok {
		return "", fmt.Errorf("driver: run %s: no owning turn", runID)
	}
	return turnID, nil
}

// beforePrepare hands the Loop's hook to the Planner with the Writer the
// bound store commits through.
func (l *Loops) beforePrepare(ctx context.Context, st store.RunStore, input decision.Input) error {
	owned, ok := st.(interface{ Writer() writer.Writer })
	if !ok {
		return fmt.Errorf("driver: run store %T exposes no writer for the planner", st)
	}
	return l.Planner.BeforePrepare(ctx, owned.Writer(), input)
}
