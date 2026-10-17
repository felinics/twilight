// Package driver performs finite advancement of durable Turns. Loops caches
// preset interpreters, Driver reconciles and advances without waiting, and
// Compactions coordinates durable context maintenance. Ownership, activation,
// response scheduling and future work discovery belong to the host.
package driver

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"sync"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/loop"
	"github.com/felinics/twilight/agentcore/turn/store"
)

// Presets resolves a PresetRef to its immutable AgentPreset.
type Presets interface {
	Resolve(preset.PresetRef) (preset.AgentPreset, error)
}

// Planner is the application's between-steps hook: it runs while a Turn is
// Open and about to plan a model request, with the Writer of the Session
// being driven, so what it commits (an in-turn checkpoint) is what the
// PromptBuilder reads next. Errors stop the drive.
type Planner interface {
	BeforePrepare(ctx context.Context, w writer.Writer, input prompt.Input) error
}

// CandidateChecker checks a complete candidate before it becomes a Turn fact.
// Maintenance returns control to the Session driver; it is not a Turn step.
type CandidateChecker interface {
	CheckCandidate(context.Context, writer.Writer, prompt.Input, *prompt.Prompt) (loop.CandidateDecision, error)
}

// Loops builds the Loop of each AgentPreset once and hands the same Loop to
// every drive, redispatch and reattachment of a Turn under that preset, so
// all of them meet the same already-driving guard. The Loop settings are
// fixed when the first Loop is built; a field changed later reaches only
// presets not built yet.
type Loops struct {
	Executor       effect.ExecutionPort
	Presets        Presets
	PromptBuilders *prompt.Catalog
	Sources        prompt.Sources
	// Targets resolves the opaque target of each effect; every Loop shares
	// it.
	Targets loop.TargetResolver
	// Dispatch is the re-offer policy of every Loop for a retryable dispatch
	// refusal; the zero value selects loop's defaults.
	Dispatch loop.DispatchPolicy
	// Planner, when set, is consulted through every Loop's BeforePrepare:
	// the application's between-steps context policy, given the Writer the
	// drive commits through.
	Planner Planner
	// Candidates evaluates requests after Build, before PrepareModelRequest.
	Candidates CandidateChecker

	mu    sync.Mutex
	loops map[preset.PresetRef]*loop.Loop
}

// For returns the Loop of the preset, building it on first use.
func (l *Loops) For(ref preset.PresetRef) (*loop.Loop, error) {
	ap, err := l.Presets.Resolve(ref)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if lp, ok := l.loops[ref]; ok {
		return lp, nil
	}
	builder, err := l.PromptBuilders.Resolve(ap, l.Sources)
	if err != nil {
		return nil, err
	}
	settings := loop.Settings{
		Scheduling:       ap.Scheduling,
		MalformedRetries: ap.MalformedRetries,
		TargetResolver:   l.Targets,
		Dispatch:         l.Dispatch,
	}
	if l.Planner != nil {
		settings.BeforePrepare = l.beforePrepare
	}
	if l.Candidates != nil {
		settings.CheckCandidate = l.checkCandidate
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

// ForTurn returns the Loop selected by the Turn's immutable preset.
func (l *Loops) ForTurn(ctx context.Context, w writer.Writer, turnID turn.TurnID) (*loop.Loop, error) {
	surface, err := sessionstore.ReadSurface(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return nil, err
	}
	view, ok := surface.Turns[turnID]
	if !ok {
		return nil, fmt.Errorf("driver: unknown turn %s", turnID)
	}
	lp, err := l.For(view.Preset)
	if err != nil {
		return nil, err
	}
	return lp, nil
}

func (l *Loops) checkCandidate(ctx context.Context, st store.TurnStore, input prompt.Input, candidate *prompt.Prompt) (loop.CandidateDecision, error) {
	owned, ok := st.(interface{ Writer() writer.Writer })
	if !ok {
		return loop.CandidateReady, fmt.Errorf("driver: turn store %T exposes no writer for candidate checks", st)
	}
	return l.Candidates.CheckCandidate(ctx, owned.Writer(), input, candidate)
}

// beforePrepare hands the Loop's hook to the Planner with the Writer the
// bound store commits through.
func (l *Loops) beforePrepare(ctx context.Context, st store.TurnStore, input prompt.Input) error {
	owned, ok := st.(interface{ Writer() writer.Writer })
	if !ok {
		return fmt.Errorf("driver: turn store %T exposes no writer for the planner", st)
	}
	return l.Planner.BeforePrepare(ctx, owned.Writer(), input)
}
