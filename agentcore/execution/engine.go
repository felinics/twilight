// Package execution advances Runs from a Session's durable state: the drive
// chain over one effect layer, exposed to hosts as the Engine.
package execution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/effect/watch"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Engine is what a host advances Runs with. Every method returns once the
// step it was asked for is committed; nothing in it waits on an effect. The
// Outcomes of the effects a step dispatched, the Outcomes of attempts a
// takeover kept and the answers a Responder gives are settled by the Engine
// on its own and reported through Config.Notify, and the host advances the
// Session again from there. Which Run a host drives, and under which preset,
// is the host's reading of its own facts; the Engine knows Runs.
type Engine interface {
	// Drive advances the Run by one step through the caller's Writer, under
	// the Builder of the preset ref names: the Run's next transitions are
	// committed and the effects they request are dispatched and awaited. The
	// result says what the step left behind.
	Drive(ctx context.Context, w writer.Writer, runID run.RunID, ref preset.PresetRef) (DriveResult, error)
	// Takeover runs the takeover disposition for a Session whose Writer was
	// just acquired and returns the recovery commands it issued.
	Takeover(ctx context.Context, w writer.Writer) (recovered int, err error)
	// ResumeWaiting answers the waits a previous owner left with a
	// Responder. Each answer is committed and reported through Notify;
	// nothing is driven here.
	ResumeWaiting(ctx context.Context, w writer.Writer)
	// Detach ends the Session's listeners; the Writer's release follows.
	Detach(sid session.SessionID)
	// Once performs one effect outside any Run: it dispatches a, waits for
	// its Outcome and acknowledges it once read. The key is a's as given, so
	// a retry under the same key replays. A cancelled ctx cancels the effect.
	Once(ctx context.Context, a effect.Assignment) (effect.Outcome, error)
	// Close ends the listeners of every Session and the settlement
	// subscription; every Session was detached before this.
	Close()
}

// DriveResult is what one Drive reports. Exactly one of Dispatched > 0,
// Waiting and Finished describes the step; InFlight says whether this
// process carries the Run on from here.
type DriveResult struct {
	// Dispatched is the number of effects the step handed to the executor.
	Dispatched int
	// InFlight is the number of effects of the Run whose Outcomes this
	// process awaits after the step, the dispatched ones included, and the
	// waits a Responder is answering; each settles in the background and
	// reaches the host as Notify. A Run still active with none in flight
	// waits on something this process does not carry: a response, or an
	// execution left to the control plane.
	InFlight int
	// Waiting reports a Run with no executable action.
	Waiting bool
	// Finished reports a Run that is terminal.
	Finished bool
}

// Sources are the Session-side services the Engine reads; a
// composition root assembles them over the same stores the Sessions live
// in.
type Sources struct {
	// Runs is the Run module's Session adapter.
	Runs *sessionstore.SessionRunStore
	// Projections reads the Sessions' folded state.
	Projections session.ProjectionReader
	// Content materializes the frozen bodies projections name.
	Content chatlog.ContentResolver
}

// Config composes one Engine: the effect layer it drives, the presets it
// resolves and the policies it drives under.
type Config struct {
	// Executor is the effect layer (RUN-EXE-3): Execution is required, the
	// optional capabilities are used when set.
	Executor effect.Ports
	// Presets is the registry of the presets Runs are driven under:
	// required, and the host's to register into.
	Presets preset.Registry
	// Progress is the transient stream the Engine publishes the frames of
	// running effects and, without Fail, its background failures to; nil
	// publishes none.
	Progress *observe.Progresses
	// PromptBuilders resolves each preset's PromptBuilderRef: required.
	PromptBuilders *prompt.Catalog
	// Planner, when set, is consulted between the steps of every Run with
	// the Writer of the Session being driven.
	Planner Planner
	// Responders answer ExternalResponse waits by the ToolRef whose calls
	// they answer; nil answers none.
	Responders map[run.ToolRef]Responder
	// TargetResolver supplies the opaque resource target of each effect
	// (RUN-LOP-9); nil gives every effect no target.
	TargetResolver loop.TargetResolver
	// Dispatch bounds the re-offers of an Assignment the executor refused as
	// retryable; the zero value selects loop's defaults.
	Dispatch loop.DispatchPolicy
	// Notify is called after the Engine settles an Outcome or commits an
	// answer outside a caller's Drive: the host advances the Session from
	// there. nil discards.
	Notify func(session.SessionID)
	// Fail receives failures of work the Engine's components do outside any
	// caller's call. The callback decides whether a failure reaches the
	// Progress stream; when nil, the Engine publishes it to Progress itself.
	Fail func(session.SessionID, error)
	// MissingEffects is the takeover policy for an Executing effect the
	// Executor holds nothing for (RUN-CMT-7): the zero value disposes it,
	// reconcile.RedispatchMissing hands it to the Executor again within a
	// budget (RUN-EXE-15) and requires Redispatches.
	MissingEffects reconcile.MissingPolicy
	// Redispatches is the dispatch ledger RedispatchMissing writes; durable
	// like every store. Unused under DisposeMissing.
	Redispatches redispatch.Store
	// MaxRedispatches bounds the redispatches of one effect under
	// RedispatchMissing; zero selects reconcile.DefaultMaxRedispatches.
	MaxRedispatches int
	// OrphanProbe is how often the Watcher attaches a key still waiting and
	// hands an orphaned one to RecoverExecution; zero selects
	// watch.DefaultProbe.
	OrphanProbe time.Duration
}

// engine is the assembly behind Engine.
type engine struct {
	ports    effect.Ports
	watcher  *watch.Watcher
	driver   *driver
	recovery *recovery
}

// New assembles an Engine over the Session-side sources of the same
// process. The Session stores and the Engine assemble independently;
// pairing them is the composition root's act.
func New(cfg Config, src Sources) (Engine, error) { //nolint:gocritic // hugeParam: Config is a by-value options struct read once
	if cfg.Executor.Execution == nil {
		return nil, errors.New("execution: an Executor port is required")
	}
	if cfg.PromptBuilders == nil {
		return nil, errors.New("execution: a prompt builder catalog is required (Config.PromptBuilders)")
	}
	if cfg.MissingEffects == reconcile.RedispatchMissing && cfg.Redispatches == nil {
		return nil, errors.New("execution: MissingEffects=redispatch requires a dispatch ledger (Config.Redispatches, RUN-EXE-15)")
	}
	if cfg.Presets == nil {
		return nil, errors.New("execution: a preset registry is required (Config.Presets)")
	}
	presets := cfg.Presets
	x := &engine{ports: cfg.Executor}
	x.watcher = &watch.Watcher{Port: cfg.Executor.Execution, Settlements: cfg.Executor.Settlements, Recover: cfg.Executor.Recover, Probe: cfg.OrphanProbe}
	// Failures the components report outside any caller's call reach the
	// caller's callback, which owns their delivery to the transient stream;
	// without one they reach the stream directly (OBS-1).
	report := cfg.Fail
	if report == nil && cfg.Progress != nil {
		report = cfg.Progress.Failed
	}
	notify := func(lt *lifetime) {
		if cfg.Notify != nil {
			cfg.Notify(lt.w.SessionID())
		}
	}
	// One Loop steps every Run: what differs per Run is its preset's Builder,
	// resolved per drive. The Planner runs with the Writer of the Session
	// being driven, found among the lifetimes this process owns.
	settings := loop.Settings{TargetResolver: cfg.TargetResolver, Dispatch: cfg.Dispatch}
	if planner := cfg.Planner; planner != nil {
		settings.BeforePrepare = func(ctx context.Context, scope run.Scope, input run.PromptInput) error {
			w, ok := x.recovery.writerOf(session.SessionID(scope))
			if !ok {
				return fmt.Errorf("execution: planner: session %s is not open in this process", scope)
			}
			return planner.BeforePrepare(ctx, w, input)
		}
	}
	lp, err := loop.New(cfg.Executor, settings)
	if err != nil {
		return nil, err
	}
	bs := &builders{presets: presets, catalog: cfg.PromptBuilders, sources: prompt.Sources{Projections: src.Projections, Content: src.Content}}
	x.recovery = &recovery{runs: src.Runs, ports: cfg.Executor, loop: lp, watcher: x.watcher, fail: report, notify: notify,
		missingEffects: cfg.MissingEffects, redispatches: cfg.Redispatches, maxRedispatches: cfg.MaxRedispatches, progress: cfg.Progress}
	var rs *responders
	if len(cfg.Responders) > 0 {
		rs = &responders{runs: src.Runs, tools: cfg.Responders, fail: report}
	}
	x.driver = &driver{runs: src.Runs, loop: lp, builders: bs, recovery: x.recovery, responders: rs}
	return x, nil
}

func (x *engine) Drive(ctx context.Context, w writer.Writer, runID run.RunID, ref preset.PresetRef) (DriveResult, error) {
	return x.driver.Drive(ctx, w, runID, ref)
}

func (x *engine) Takeover(ctx context.Context, w writer.Writer) (int, error) {
	return x.recovery.Open(ctx, w)
}

func (x *engine) ResumeWaiting(ctx context.Context, w writer.Writer) {
	x.driver.ResumeWaiting(ctx, w, x.recovery.settled)
}

func (x *engine) Detach(sid session.SessionID) { x.recovery.Stop(sid) }

// Once dispatches a, waits on the shared settlement subscription for its
// Outcome and acknowledges the key once the Outcome is read, so the
// executor may collect the record. A ctx that ends first cancels the effect.
func (x *engine) Once(ctx context.Context, a effect.Assignment) (effect.Outcome, error) {
	if err := x.ports.Execution.Dispatch(ctx, a); err != nil {
		return effect.Outcome{}, err
	}
	out, err := x.watcher.Await(ctx, a.Key())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			_ = x.ports.Execution.Cancel(context.WithoutCancel(ctx), a.Key())
		}
		return effect.Outcome{}, err
	}
	if x.ports.Ack != nil {
		_ = x.ports.Ack.Acknowledge(ctx, a.Key())
	}
	return out, nil
}

// Close ends the recovery listeners, then the settlement subscription.
func (x *engine) Close() {
	x.recovery.Close()
	x.watcher.Close()
}
