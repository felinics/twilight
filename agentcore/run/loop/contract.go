package loop

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	run "github.com/felinics/twilight/agentcore/run"
	effect "github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/runtime"

	"github.com/felinics/twilight/sdk"
)

// ErrRunAlreadyRunning identifies a second local driver for the same Run.
// A Loop permits concurrent execution of different Runs and serializes each
// Run locally so every Executing target has one in-process owner (RUN-CMT-6).
var ErrRunAlreadyRunning = errors.New("agent: loop: run already running")

// PromptBuilder is the decision-layer port the host resolves from the
// AgentPreset (DEC-PMT): it builds the next prompt from the surrounding
// conversation, which it reads by the PromptInput's Scope (RUN-LOP-2). Loop
// freezes the prompt into an agent-owned ModelRequest before crossing the
// RunStore boundary.
type PromptBuilder interface {
	Build(context.Context, plan.PromptInput) (Prompt, error)
}

// Prompt is one built model input: the model to call, the provider request
// (messages and tool definitions), the inputs it consumed, the freshness
// token of the context it was built from, and the frozen tool specs.
type Prompt struct {
	Model    run.ModelRef
	Request  sdk.Request
	InputIDs []run.InputID
	Token    run.PromptToken
	Tools    []run.ToolSpec
}

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
	Kind    AssignmentKind
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

// Settings are the execution parameters the Loop takes from the AgentPreset
// (RUN-LOP-1). Scheduling is frozen onto each ToolStep; MalformedRetries
// bounds the retries of one model step after malformed results.
type Settings struct {
	Scheduling       run.ToolScheduling
	MalformedRetries uint8
	TargetResolver   TargetResolver
	// BeforePrepare runs each time the Run is Open and about to plan a model
	// request, before the PromptBuilder reads the context (RUN-LOP-10). It
	// is the application's seam for reshaping that context between steps,
	// such as an in-turn checkpoint (APP-CKP-1); it commits through the
	// Writer the store is bound to. An error stops the drive with no fact
	// written; nil is no hook.
	BeforePrepare PrepareHook
	// Dispatch bounds the re-offers of an Assignment the Executor refused
	// as retryable (RUN-EXE-3); the zero value selects the defaults.
	Dispatch DispatchPolicy
	// Watcher is where a blocking Run waits for the Outcomes it dispatched:
	// one per (owner, executor), shared with the Reconciler, so waiting on
	// N effects costs one settlement subscription. Its Port must be the
	// Loop's Executor. Nil builds a private one over the Executor, which
	// serves a single-process host; a host with a Reconciler shares its.
	Watcher *effect.Watcher
}

// PrepareHook is Settings.BeforePrepare: the store is the Loop's own bound
// RunStore and input the PromptInput the plan is about to hand the
// PromptBuilder.
type PrepareHook func(ctx context.Context, store runtime.RunStore, input plan.PromptInput) error

// ModelCatalog resolves a frozen run.ModelRef into an invoker at execution time;
// provider binding never enters the frozen request. The same ModelRef must
// resolve to equivalent execution semantics for the life of a Run (RUN-LOP-7).
type ModelCatalog interface {
	ResolveModel(run.ModelRef) (ModelInvoker, error)
}

type ModelInvoker interface {
	Generate(context.Context, sdk.Request) (sdk.ModelResult, error)
}

// StreamingModelInvoker is an optional optimization; it must produce the same
// final ModelResult as Generate.
type StreamingModelInvoker interface {
	Stream(context.Context, sdk.Request) (sdk.ModelStream, error)
}

type ToolCatalog interface {
	ResolveTool(run.ToolRef) (ExecutableTool, error)
}

type ToolExecutionRequest struct {
	RunID  run.RunID
	StepID run.StepID
	CallID run.CallID
	// Effect is the tool effect the call was started under; it identifies the
	// Outcome the Executor returns through its message-shaped port (RUN-EXE-2).
	Effect           run.EffectID
	ToolRef          run.ToolRef
	DefinitionDigest run.Digest
	Arguments        run.CanonicalJSON
	// Target is an opaque resource reference supplied by the application; the
	// Loop does not interpret it.
	Target   *run.TargetRef
	Progress ToolProgressSink
}

// ExecutableTool is the application-side execution contract (RUN-LOP-1).
type ExecutableTool interface {
	Ref() run.ToolRef
	Definition() sdk.ToolDefinition
	ResponsePolicy() run.ResponsePolicy
	// ValidateArguments runs before the start barrier and must not produce
	// external effects.
	ValidateArguments(run.CanonicalJSON) error
	Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
	// Replay declares whether Execute may run again for the same call after
	// an earlier execution was lost (RUN-EXE-9, TRN-DUR-4). Every tool
	// answers; the zero value ReplayUnknown is the answer of a tool whose
	// author has not judged it. The declaration is frozen into the ToolSpec
	// and carried on every call and Assignment.
	Replay() run.ReplayPolicy
	// Placement declares where the tool runs (RUN-LOP-9): in the executor
	// process, or inside the Session's workspace, in which case its calls
	// need a target and are routed to the workspace backend. Every tool
	// answers; the declaration is frozen into the ToolSpec.
	Placement() run.ToolPlacement
}

// Tool outcomes belong to the process-independent effect protocol. Aliases
// keep the local tool implementation source-compatible.
type ToolExecutionOutcome = effect.ToolExecutionOutcome
type ToolExecutionSucceeded = effect.ToolExecutionSucceeded
type ToolExecutionFailed = effect.ToolExecutionFailed
type ToolExecutionUnknown = effect.ToolExecutionUnknown

type ToolProgressSink interface {
	Publish(context.Context, ToolProgress)
}

type ToolProgress struct {
	Payload json.RawMessage
}

// --- EventSink: realtime observation, never authority (RUN-LOP-6) ---

type EventSink interface {
	Emit(context.Context, Event) error
}

type EventDurability uint8

const (
	EventProvisional EventDurability = iota
	EventCommitted
)

type EventKind string

const (
	EventAgentCommitted      EventKind = "agent_committed"
	EventModelTextDelta      EventKind = "model_text_delta"
	EventModelReasoningDelta EventKind = "model_reasoning_delta"
	EventToolProgress        EventKind = "tool_progress"
	EventToolStarted         EventKind = "tool_started"
	EventToolCompleted       EventKind = "tool_completed"
	EventRunFinished         EventKind = "run_finished"
	// EventProgressReset voids the provisional observations of an effect
	// received so far: the Worker re-dispatched it and a new Generation of
	// deltas begins (RUN-EXE-12).
	EventProgressReset EventKind = "progress_reset"
)

type Event struct {
	// Session is the Run's Scope.
	Session run.Scope
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	// Effect names the effect a provisional observation belongs to and
	// Generation its attempt under the Worker; both are zero for committed
	// observations.
	Effect     run.EffectID
	Generation int
	// Sequence orders provisional observations within one stream. Committed
	// observations never set it: the commit order is the authority.
	Sequence   uint64
	Kind       EventKind
	Durability EventDurability
	Payload    json.RawMessage
	// Committed is set for an EventAgentCommitted observation: the Run facts
	// the accepted command produced, in stream order; nil for provisional.
	Committed []run.Fact
}

type LoopDisposition uint8

const (
	// LoopWaiting: no executable action; the Run waits for a response, a
	// recovery, or an Outcome of an effect this Loop did not dispatch.
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
	// Reason is execution_recovery when ExecutionRecovery is true; otherwise empty.
	Reason WaitReason
	// ExecutionRecovery is true when NeedsRecovery(state) is true after this
	// Loop has no further executable action: a ModelStep is Executing, or a
	// ToolStep has Executing calls and no Pending calls, and none of them was
	// dispatched by this Loop. Under Session-level ownership this only happens
	// before the owner's takeover disposition.
	ExecutionRecovery bool
	Result            *run.RunResult
	// Dispatched lists the assignments an Advance handed to the Executor.
	Dispatched []AssignmentKey
}

type WaitReason string

const (
	ExecutionRecovery WaitReason = "execution_recovery"
)

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
