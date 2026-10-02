package loop

import (
	"context"
	"errors"
	"time"

	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// EffectContext identifies the effect a target is resolved for: the Run's
// Scope and identity, the Step, the call of a tool effect, the EffectID the
// effect is about to be started under, the kind of the effect and, for a tool
// effect, the tool's ref.
type EffectContext struct {
	Session run.Scope
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	Effect  run.EffectID
	Kind    effect.AssignmentKind
	Tool    run.ToolRef
	// Placement is the tool's declared placement for a tool effect; a
	// resolver supplies a workspace target only for PlacementWorkspace.
	Placement run.ToolPlacement
}

// TargetResolver supplies an opaque target for one effect (RUN-LOP-9). It
// belongs to the application/resource layer: the Loop asks it once for every
// effect it is about to start, before the start barrier, and only copies the
// returned reference into that effect's Assignment. A nil target means the
// effect has no resource target. The mapping must be durable when a Run can
// outlive the process that started it.
type TargetResolver interface {
	ResolveTarget(context.Context, EffectContext) (*run.TargetRef, error)
}

// Settings are the deployment's parameters of a Loop, the same for every
// Run it steps. What a preset decides for a step travels in the Prompt the
// Builder returns and is frozen onto the step.
type Settings struct {
	TargetResolver TargetResolver
	// BeforePrepare runs each time the Run is Open and about to plan a model
	// request, before the PromptBuilder reads the context (RUN-LOP-10). It
	// is the application's seam for reshaping that context between steps,
	// such as an in-turn checkpoint (APP-CKP-1); what it commits goes
	// through the host's own capability over the Scope. An error stops the
	// drive with no fact written; nil is no hook.
	BeforePrepare PrepareHook
	// Dispatch bounds the re-offers of an Assignment the Executor refused
	// as retryable (RUN-EXE-3); the zero value selects the defaults.
	Dispatch DispatchPolicy
}

// PrepareHook is Settings.BeforePrepare: scope is the Run's Scope and input
// the PromptInput the plan is about to hand the PromptBuilder.
type PrepareHook func(ctx context.Context, scope run.Scope, input run.PromptInput) error

type LoopDisposition uint8

const (
	// LoopWaiting: no executable action; the Run waits for a response or for
	// the Outcome of an effect in flight (Executing lists them).
	LoopWaiting LoopDisposition = iota
	// LoopFinished: the Run is terminal; Result is set.
	LoopFinished
	// LoopDispatched: Advance handed at least one Assignment to the Executor
	// and returned; Dispatched lists them. The Run moves again when their
	// Outcomes are read by key and then settled.
	LoopDispatched
	// LoopDelivered: Deliver settled an Outcome and the Run is not terminal;
	// the host advances it next.
	LoopDelivered
	// LoopDropped: Deliver found no Executing target under the Outcome's
	// key -- a late Outcome of a settled or disposed attempt -- and wrote
	// nothing.
	LoopDropped
)

type LoopResult struct {
	Disposition LoopDisposition
	// Executing are the keys of the effects a waiting Run has in flight, one
	// per Executing target. Whether this process awaits their Outcomes or
	// has to reconcile them with the executor is the host's knowledge, not
	// the Run's.
	Executing []effect.AssignmentKey
	Result    *run.RunResult
	// Dispatched lists the assignments an Advance handed to the Executor.
	Dispatched []effect.AssignmentKey
}

// DispatchPolicy is how a Loop repeats a Dispatch the Executor refused with
// effect.ErrDispatchRetryable (RUN-EXE-3): at most Retries offers inside one
// Advance, Backoff multiplied by the attempts so far between them. It is a
// deployment's setting, not part of the AgentPreset.
type DispatchPolicy struct {
	Retries int
	Backoff time.Duration
}

// The defaults a zero DispatchPolicy selects.
const (
	DefaultDispatchRetries = 3
	DefaultDispatchBackoff = 50 * time.Millisecond
)

func (p DispatchPolicy) retries() int {
	if p.Retries <= 0 {
		return DefaultDispatchRetries
	}
	return p.Retries
}

func (p DispatchPolicy) backoff() time.Duration {
	if p.Backoff <= 0 {
		return DefaultDispatchBackoff
	}
	return p.Backoff
}

// ErrModelUnavailable reports a model assignment the executor cannot serve:
// the pre-start check fails with the step still Prepared and no start or
// recovery fact.
var ErrModelUnavailable = errors.New("agent: loop: executor cannot serve the model")
