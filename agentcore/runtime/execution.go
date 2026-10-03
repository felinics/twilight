package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// ExecutionSources are the Session-side services the drive chain reads;
// a composition root assembles them over the same Kernel the Sessions
// live in.
type ExecutionSources struct {
	// Runs is the Run module's Session adapter.
	Runs *sessionstore.SessionRunStore
	// Projections reads the Sessions' folded state.
	Projections session.ProjectionReader
	// Content materializes the frozen bodies projections name.
	Content chatlog.ContentResolver
}

// ExecutionConfig composes one Execution: the effect port it drives, the
// decision identities it resolves and the policies of the drive chain.
type ExecutionConfig struct {
	// Executor is the effect layer port (RUN-EXE-3): required.
	Executor effect.ExecutionPort
	// Presets is the registry of decision identities; nil selects an
	// in-memory registry.
	Presets preset.Registry
	// Decisions resolve each preset's PromptBuilderRef (DEC-CAT): required.
	// The runtime ships no builder; the agent built on it supplies its
	// catalog (agent/prompt.DefaultPromptBuilders for the reference agent).
	Decisions *decision.Catalog
	// MissingEffects is the takeover policy for an Executing effect the
	// Executor holds nothing for (RUN-CMT-7): the zero value disposes it,
	// reconcile.RedispatchMissing hands it to the Executor again within a
	// budget (RUN-EXE-15) and requires Redispatches.
	MissingEffects reconcile.MissingPolicy
	// Redispatches is the dispatch ledger RedispatchMissing writes; durable
	// like every store (OWN-PRT-3). Unused under DisposeMissing.
	Redispatches redispatch.Store
	// Planner, when set, is consulted between the steps of every Run with
	// the Writer of the Session being driven: the application's in-turn
	// context policy.
	Planner driver.Planner
	// Responders answer ExternalResponse waits by the ToolRef whose calls
	// they answer; nil answers none.
	Responders map[run.ToolRef]driver.Responder
	// TargetResolver supplies the opaque resource target of each effect
	// (RUN-LOP-9). It belongs to the application's resource layer; nil
	// gives every effect no target (APP-TGT-1).
	TargetResolver loop.TargetResolver
	// Fail receives failures of work the Execution's components do outside
	// any caller's call, such as settling a reattached Outcome; nil
	// discards them.
	Fail func(session.SessionID, error)
	// OrphanProbe is how often an effect still waiting is attached and, when
	// orphaned, handed to RecoverExecution: by the Watcher of a live drive
	// and by the Reconciler of a takeover. Zero selects the defaults.
	OrphanProbe time.Duration
}

// Execution is the execution side assembled over one Session kernel: the
// settlement subscription, the drive chain (Loops, Driver, Recovery,
// Responders), the decision identities and the effect port. It is what
// advances an agent from Session durable state; which process hosts it
// next to which kernel is a deployment choice.
type Execution struct {
	// Executor is the effect port the drive chain dispatches through.
	Executor effect.ExecutionPort
	// Presets is the registry of decision identities the Loops resolve and
	// the Turn protocol starts Turns under.
	Presets preset.Registry
	// Watcher is the settlement subscription every Loop and Reconciler of
	// this Execution waits on; a host component that waits for an effect of
	// its own (compaction's summary) shares it instead of subscribing again.
	Watcher *effect.Watcher
	// Driver drives an active Turn; Recovery runs the takeover disposition
	// when a Session opens and ends its listeners when it closes. Both sit
	// on the Loops and the Watcher NewExecution built.
	Driver   *driver.Driver
	Recovery *driver.Recovery
	// Progress is the transient stream of the running effects: the drive
	// chain's provisional observations and the failures this side reports.
	Progress *observe.Progresses
}

// NewExecution assembles the execution side over the Session-side sources
// of the same process. The kernel and the execution side assemble
// independently (sessionkernel.New and NewExecution); pairing them is the
// composition root's act.
func NewExecution(cfg ExecutionConfig, src ExecutionSources) (*Execution, error) { //nolint:gocritic // hugeParam: ExecutionConfig is a by-value options struct read once
	if cfg.Executor == nil {
		return nil, errors.New("runtime: an Executor port is required")
	}
	if cfg.Decisions == nil {
		return nil, errors.New("runtime: a prompt builder catalog is required (ExecutionConfig.Decisions); the runtime ships no default")
	}
	if cfg.MissingEffects == reconcile.RedispatchMissing && cfg.Redispatches == nil {
		return nil, errors.New("runtime: MissingEffects=redispatch requires a dispatch ledger (ExecutionConfig.Redispatches, RUN-EXE-15)")
	}
	presets := cfg.Presets
	if presets == nil {
		presets = preset.NewMemory()
	}
	x := &Execution{Executor: cfg.Executor, Presets: presets, Progress: observe.NewProgresses()}
	x.Watcher = &effect.Watcher{Port: cfg.Executor, Probe: cfg.OrphanProbe}
	// Failures the components report outside any caller's call reach the
	// caller's callback and the Session's transient stream (OBS-1).
	fail := cfg.Fail
	if fail == nil {
		fail = func(session.SessionID, error) {}
	}
	report := func(sid session.SessionID, err error) {
		fail(sid, err)
		x.Progress.Failed(sid, err)
	}
	// A nil resolver gives every effect no target (APP-TGT-1).
	loops := &driver.Loops{Executor: cfg.Executor, Presets: presets, Decisions: cfg.Decisions, Targets: cfg.TargetResolver,
		Sources: decision.Sources{Projections: src.Projections, Content: src.Content}, Watcher: x.Watcher, Planner: cfg.Planner}
	x.Recovery = &driver.Recovery{Runs: src.Runs, Executor: cfg.Executor, Loops: loops, Watcher: x.Watcher, Fail: report,
		MissingEffects: cfg.MissingEffects, Redispatches: cfg.Redispatches, OrphanProbe: cfg.OrphanProbe, Sink: progressSink{x.Progress}}
	var responders *driver.Responders
	if len(cfg.Responders) > 0 {
		responders = &driver.Responders{Runs: src.Runs, Tools: cfg.Responders, Fail: report}
	}
	x.Driver = &driver.Driver{Runs: src.Runs, Loops: loops, Recovery: x.Recovery, Responders: responders, Sink: progressSink{x.Progress}}
	return x, nil
}

// Open runs the takeover disposition for a Session whose Writer a host just
// acquired (RUN-EXE-8 order): the returned count is the recovery commands
// the disposition issued.
func (x *Execution) Open(ctx context.Context, w writer.Writer) (int, error) {
	return x.Recovery.Open(ctx, w)
}

// ResumeWaiting answers, with this Execution's Responders, the waits a
// previous owner left open: the Responder continues from its durable state.
func (x *Execution) ResumeWaiting(ctx context.Context, w writer.Writer) {
	x.Driver.ResumeWaiting(ctx, w)
}

// Stop ends the recovery listeners of one Session: its Writer's release (or
// takeover by another process) follows.
func (x *Execution) Stop(sid session.SessionID) {
	x.Recovery.Stop(sid)
}

// Close ends the execution side in the order its drives require: the
// recovery listeners, then the settlement subscription. Every Session it
// drove was released (Owner.Close) before this.
func (x *Execution) Close() {
	x.Recovery.Close()
	x.Watcher.Close()
}

// progressSink is the drive's loop.EventSink: provisional observations
// become transient Progress events; committed observations are already on
// the committed stream from the Writer, so they are dropped here.
type progressSink struct{ progress *observe.Progresses }

func (s progressSink) Emit(_ context.Context, e loop.Event) error { //nolint:gocritic // hugeParam: EventSink contract takes the Event by value
	if e.Durability != loop.EventProvisional {
		return nil
	}
	s.progress.Publish(session.SessionID(e.Session), observe.Progress{RunID: e.RunID, Effect: e.Effect, Generation: e.Generation,
		Sequence: e.Sequence, Kind: string(e.Kind), Payload: e.Payload})
	return nil
}
