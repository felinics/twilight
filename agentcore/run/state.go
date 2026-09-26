package run

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run/model"
)

type RunStatus uint8

const (
	RunActive RunStatus = iota
	RunCompleted
	RunStopped
	RunFailed
)

func (s RunStatus) Terminal() bool { return s != RunActive }

type RunReason string

const (
	ReasonCancelled       RunReason = "cancelled"
	ReasonProviderFailure RunReason = "provider_failure"
	ReasonMalformedModel  RunReason = "malformed_model_result"
	// Unknown model outcomes end the Run with this reason; unknown tool
	// outcomes use FailureEffectUnknown on ToolCallFailed and leave the Run
	// active.
	ReasonEffectUnknown RunReason = "effect_unknown"
)

type RunFailure struct {
	Class   string `json:"class"`
	Message string `json:"message,omitempty"`
	CallID  CallID `json:"callId,omitempty"`
}

// RunResult is the read-side restatement of how a Run ended: the RunEnded
// fact's End as status, reason and failure, with the Usage accumulated up to
// it. Evolve folds it for readers (the Loop's LoopResult, the host); Decide
// never reads it. The authority for the end is the RunEnded fact.
type RunResult struct {
	Status  RunStatus   `json:"status"`
	Reason  RunReason   `json:"reason,omitempty"`
	Failure *RunFailure `json:"failure,omitempty"`
	// UncertainCalls are tool calls settled as Unknown when the Run stopped.
	UncertainCalls []CallID `json:"uncertainCalls,omitempty"`
	// UncertainModel is the ModelStep left Executing when the Run stopped.
	UncertainModel StepID      `json:"uncertainModel,omitempty"`
	Usage          model.Usage `json:"usage"`
}

type StepFailure struct {
	Class   string `json:"class"`
	Message string `json:"message,omitempty"`
}

const (
	FailurePermissionDenied   = "permission_denied"
	FailureResponseRejected   = "response_rejected"
	FailureToolLookup         = "tool_lookup_failed"
	FailureInvalidArguments   = "invalid_arguments"
	FailureMalformedModel     = "malformed_model_result"
	FailureDefinitionMismatch = "tool_definition_mismatch"
	FailureExecution          = "execution_failed"
	FailureEffectUnknown      = "effect_unknown"
	FailureCancelled          = "cancelled"
	FailureProvider           = "provider_failure"
)

// Tool execution failure classes (RUN-EXE-11): what went wrong inside a
// tool's Execute, as the tool reports it in ToolFailure.Class. The class
// describes the error; whether it is worth retrying is the failure's own
// RetryDisposition (effect.ToolExecutionFailed.Retry), the two are not
// derived from each other. FailureExecution remains the unclassified class.
const (
	FailureNotFound     = "not_found"     // the named resource does not exist
	FailureInvalidInput = "invalid_input" // the arguments were understood and refused
	FailureTimeout      = "timeout"       // the tool's own deadline elapsed
	FailureUnavailable  = "unavailable"   // a dependency of the tool was unreachable or temporarily failing
	FailureRateLimited  = "rate_limited"  // a dependency refused for rate or quota reasons
	FailureConflict     = "conflict"      // the world changed under the tool (version, lock, uniqueness)
	FailureInternal     = "internal"      // the tool itself broke
)

type ResponsePolicy uint8

const (
	DirectExecution ResponsePolicy = iota
	ApprovalRequired
	ExternalResponse
)

// ReplayPolicy is a tool's judgment of its own side effects: whether the
// Worker that adopts a lost execution of the tool may run it again for the
// same call (RUN-EXE-9, TRN-DUR-4). It is declared by the tool
// implementation, frozen into the ToolSpec and copied onto each call and its
// Assignment, so every Worker that reads the execution record decides alike.
// Only ReplayAllowed is re-dispatched; ReplayForbidden and the zero value
// ReplayUnknown are settled Unknown, told apart in the settlement's message.
type ReplayPolicy uint8

const (
	// ReplayUnknown: the tool has not been judged. Zero value, so a missing
	// declaration never replays; omitted on the wire.
	ReplayUnknown ReplayPolicy = iota
	// ReplayAllowed: read-only or idempotent by CallID; a second run has no
	// second effect on the world.
	ReplayAllowed
	// ReplayForbidden: side effects a second run would repeat.
	ReplayForbidden
)

func (p ReplayPolicy) String() string {
	switch p {
	case ReplayAllowed:
		return "allowed"
	case ReplayForbidden:
		return "forbidden"
	default:
		return "unknown"
	}
}

// ToolPlacement is where a tool call must run (RUN-LOP-9): in the process
// that executes it, or inside the workspace the Session is bound to. It is
// declared by the tool implementation, frozen into the ToolSpec and copied
// onto each call and its Assignment, so the target resolver asks for a
// workspace only for calls that need one and the Worker routes by the
// declaration, never by whether a target happens to be present. Every tool
// declares it; PlacementProcess is the zero value and omitted on the wire.
type ToolPlacement uint8

const (
	// PlacementProcess: the tool runs in the executor process that holds
	// its implementation (a pure computation, spawn_agent).
	PlacementProcess ToolPlacement = iota
	// PlacementWorkspace: the tool runs inside the workspace bound to the
	// Session (a shell, a file tool); its Assignment must carry a target.
	PlacementWorkspace
)

func (p ToolPlacement) String() string {
	if p == PlacementWorkspace {
		return "workspace"
	}
	return "process"
}

// RetryDisposition is a Known failure's own answer to whether the next
// attempt of the same Assignment may run (RUN-EXE-11). RetryAllowed means
// this failure suffices to confirm the attempt produced no external effect
// that cannot safely be repeated; RetryNever means the failure is definite
// or a second attempt is not safe to assert. It is a property of the
// failure, never of the tool: a read-only tool's "file not found" is
// RetryNever while its "storage unavailable" is RetryAllowed. The effect
// layer derives it for model failures from their FailureCode and stores
// nothing; a tool declares it on each ToolExecutionFailed. The zero value
// RetryUnknown is never retried.
type RetryDisposition uint8

const (
	RetryUnknown RetryDisposition = iota
	RetryNever
	RetryAllowed
)

func (d RetryDisposition) String() string {
	switch d {
	case RetryAllowed:
		return "allowed"
	case RetryNever:
		return "never"
	default:
		return "unknown"
	}
}

type ResponseKind string

const (
	ResponseApproval ResponseKind = "approval"
	ResponseExternal ResponseKind = "external_response"
)

type ResponseDecision string

const (
	ResponseDecisionApproved ResponseDecision = "approved"
	ResponseDecisionRejected ResponseDecision = "rejected"
)

// ResponseRequest is the Wait of one tool call: the external input the call
// lacks, named so the application can route it. Kind says which input --
// the approval a call needs before its tool effect may be requested, or the
// external response that settles the call in place of a tool effect. ID is
// the derived ResponseID the resolving command must name, Payload the
// arguments shown to whoever answers. A Wait is not an effect: nothing is
// dispatched for a Waiting call and no attempt exists for it.
type ResponseRequest struct {
	RunID   RunID         `json:"runId"`
	StepID  StepID        `json:"stepId"`
	CallID  CallID        `json:"callId"`
	ID      ResponseID    `json:"id"`
	Kind    ResponseKind  `json:"kind"`
	Payload CanonicalJSON `json:"payload,omitzero"`
}

// ToolSpec is the agent-side sidecar for a provider-neutral ToolDefinition.
// The definition body lives inside the frozen request (RUN-WIR-4); the spec
// keeps its model-facing Name for binding tool calls, the digest for
// execution-time verification, and the ResponsePolicy, which is intentionally
// kept out of sdk to preserve package layering.
type ToolSpec struct {
	Ref              ToolRef        `json:"ref"`
	Name             string         `json:"name"`
	DefinitionDigest Digest         `json:"definitionDigest"`
	Policy           ResponsePolicy `json:"policy"`
	Replay           ReplayPolicy   `json:"replay,omitempty"`
	Placement        ToolPlacement  `json:"placement,omitempty"`
}

// ToolCallBinding is one frozen call inside ToolStepOpened.
type ToolCallBinding struct {
	CallID CallID `json:"callId"`
	// ProviderCallID is the tool_call_id the model emitted. PromptBuilders echo it
	// back when they replay the call and its result; the Run never keys on it.
	ProviderCallID   string         `json:"providerCallId,omitempty"`
	ToolRef          ToolRef        `json:"toolRef"`
	DefinitionDigest Digest         `json:"definitionDigest"`
	Arguments        CanonicalJSON  `json:"arguments"`
	Policy           ResponsePolicy `json:"policy"` // unresolved ToolRef uses DirectExecution
	Replay           ReplayPolicy   `json:"replay,omitempty"`
	Placement        ToolPlacement  `json:"placement,omitempty"`
	// Response is derived and filled by Decide inside ToolStepOpened; callers
	// leave it empty when submitting.
	Response *ResponseRequest `json:"response,omitempty"`
}

type StepRef struct {
	RunID RunID  `json:"runId"`
	ID    StepID `json:"id"`
}

// Step is sealed by the agent package: only ModelStep and ToolStep exist.
type Step interface {
	step()
	Ref() StepRef
}

// Current is the contents of an Active run. Open is the prompt building interval:
// Prepare is legal and Next returns NeedModelRequest. AcceptInput is legal in
// every non-terminal state; PendingInputs is the durable queue it feeds.
type Current interface{ current() }

// Open is Active with no ModelStep or ToolStep.
type Open struct{}

func (Open) current() {}

func atOpen(c Current) bool {
	_, ok := c.(Open)
	return ok
}

type ModelStepStatus uint8

const (
	ModelPrepared ModelStepStatus = iota
	ModelExecuting
)

func (s ModelStepStatus) String() string {
	switch s {
	case ModelPrepared:
		return "Prepared"
	case ModelExecuting:
		return "Executing"
	default:
		return fmt.Sprintf("ModelStepStatus(%d)", uint8(s))
	}
}

type ModelStep struct {
	RefValue StepRef `json:"ref"`
	// RequestDigest identifies the frozen request; its body is kept in the
	// frozen.Store for the life of the step (RUN-WIR-4).
	RequestDigest Digest          `json:"requestDigest"`
	Model         ModelRef        `json:"model"`
	Tools         []ToolSpec      `json:"tools,omitempty"`
	Status        ModelStepStatus `json:"status"`
	// Effect is the model effect the step most recently requested (from
	// ModelStepStarted); empty before the first start. The step fixes the
	// effect's kind (a model call) and binding (RequestDigest), so the Run
	// keeps no effect record beside the step. A rejected result returns the
	// step to Prepared without clearing it, and the next start requests a new
	// effect. A new owner recovers an Executing step under a CommandID derived
	// from it (RUN-CMT-7).
	Effect EffectID `json:"effect,omitempty"`
	// Rejects counts accepted ModelStepRejected facts.
	Rejects int `json:"rejects,omitempty"`
}

func (ModelStep) step()    {}
func (ModelStep) current() {}

//nolint:gocritic // hugeParam: value receiver keeps ModelStep satisfying sealed Step as a value.
func (s ModelStep) Ref() StepRef { return s.RefValue }

type ToolCallStatus uint8

const (
	ToolPending ToolCallStatus = iota
	ToolExecuting
	ToolWaiting
	ToolCompleted
	ToolFailed
)

func (s ToolCallStatus) String() string {
	switch s {
	case ToolPending:
		return "Pending"
	case ToolExecuting:
		return "Executing"
	case ToolWaiting:
		return "Waiting"
	case ToolCompleted:
		return "Completed"
	case ToolFailed:
		return "Failed"
	default:
		return fmt.Sprintf("ToolCallStatus(%d)", uint8(s))
	}
}

// Terminal reports Completed or Failed.
func (s ToolCallStatus) Terminal() bool { return s == ToolCompleted || s == ToolFailed }

// ToolExecutionResult is the transient output a tool worker submits. The
// state and the fact keep only its digest; the body is frozen under that
// digest before the fact is committed (RUN-WIR-4).
type ToolExecutionResult struct {
	Output CanonicalJSON `json:"output"`
}

// ToolCallResult is the persisted record of a completed call.
type ToolCallResult struct {
	OutputDigest Digest `json:"outputDigest"`
}

type ToolFailure struct {
	Class   string `json:"class"`
	Message string `json:"message,omitempty"`
}

type ToolFailureOutcome uint8

const (
	ToolOutcomeKnown ToolFailureOutcome = iota
	ToolOutcomeUnknown
)

type ToolCallFailure struct {
	Failure ToolFailure        `json:"failure"`
	Outcome ToolFailureOutcome `json:"outcome"`
}

type ToolCallState struct {
	CallID           CallID         `json:"callId"`
	ProviderCallID   string         `json:"providerCallId,omitempty"`
	ToolRef          ToolRef        `json:"toolRef"`
	DefinitionDigest Digest         `json:"definitionDigest"`
	Arguments        CanonicalJSON  `json:"arguments"`
	Policy           ResponsePolicy `json:"policy"`
	Replay           ReplayPolicy   `json:"replay,omitempty"`
	Placement        ToolPlacement  `json:"placement,omitempty"`
	Status           ToolCallStatus `json:"status"`
	// Effect is the tool effect the call requested (from ToolCallStarted);
	// empty before the start and for a call an external response settles
	// without one. The call fixes the effect's kind (a tool call) and binding
	// (definition, policy and arguments). A call starts at most once.
	Effect  EffectID         `json:"effect,omitempty"`
	Result  *ToolCallResult  `json:"result,omitempty"`
	Failure *ToolCallFailure `json:"failure,omitempty"`
	// Waiting is the call's Wait while Status is ToolWaiting: the approval or
	// external response it lacks (RUN-MCH-2). Nil in every other status.
	Waiting *ResponseRequest `json:"waiting,omitempty"`
}

// ValidateToolCallState rejects illegal field combinations (RUN-MCH-2).
//
//nolint:gocritic // hugeParam: public validator accepts the value stored in facts/state without mutating it.
func ValidateToolCallState(c ToolCallState) error {
	switch c.Status {
	case ToolPending, ToolExecuting:
		if c.Result != nil || c.Failure != nil || c.Waiting != nil {
			return fmt.Errorf("agent: call %s: pending/executing must have no result/failure/waiting", c.CallID)
		}
	case ToolWaiting:
		if c.Waiting == nil {
			return fmt.Errorf("agent: call %s: waiting requires a ResponseRequest", c.CallID)
		}
		if c.Policy != ApprovalRequired && c.Policy != ExternalResponse {
			return fmt.Errorf("agent: call %s: waiting requires approval or external-response policy", c.CallID)
		}
		if c.Result != nil || c.Failure != nil {
			return fmt.Errorf("agent: call %s: waiting must have no result/failure", c.CallID)
		}
	case ToolCompleted:
		if c.Result == nil || c.Result.OutputDigest == "" {
			return fmt.Errorf("agent: call %s: completed requires a result digest", c.CallID)
		}
		if c.Failure != nil || c.Waiting != nil {
			return fmt.Errorf("agent: call %s: completed must have no failure/waiting", c.CallID)
		}
	case ToolFailed:
		if c.Failure == nil {
			return fmt.Errorf("agent: call %s: failed requires a failure", c.CallID)
		}
		if c.Result != nil || c.Waiting != nil {
			return fmt.Errorf("agent: call %s: failed must have no result/waiting", c.CallID)
		}
		if c.Failure.Failure.Class == "" {
			return fmt.Errorf("agent: call %s: failed requires a failure class", c.CallID)
		}
		switch c.Failure.Outcome {
		case ToolOutcomeKnown:
			if c.Failure.Failure.Class == FailureEffectUnknown {
				return fmt.Errorf("agent: call %s: known outcome cannot use %s", c.CallID, FailureEffectUnknown)
			}
		case ToolOutcomeUnknown:
			if c.Failure.Failure.Class != FailureEffectUnknown {
				return fmt.Errorf("agent: call %s: unknown outcome must use %s", c.CallID, FailureEffectUnknown)
			}
		default:
			return fmt.Errorf("agent: call %s: unknown failure outcome %d", c.CallID, c.Failure.Outcome)
		}
	default:
		return fmt.Errorf("agent: call %s: unknown status %d", c.CallID, c.Status)
	}
	return nil
}

// ToolScheduleMode is frozen onto a ToolStep at open. Resume must honor it.
type ToolScheduleMode string

const (
	ToolScheduleParallel   ToolScheduleMode = "parallel"
	ToolScheduleSequential ToolScheduleMode = "sequential"
)

// ToolScheduling is the durable dispatch constraint for one ToolStep.
// Empty Mode means parallel. MaxParallel 0 means every Pending call in the
// current Start batch may run; a positive value caps that batch.
type ToolScheduling struct {
	Mode        ToolScheduleMode `json:"mode,omitempty"`
	MaxParallel int              `json:"maxParallel,omitempty"`
}

func normalizeToolScheduling(s ToolScheduling) (ToolScheduling, error) {
	if s.Mode != "" && s.Mode != ToolScheduleParallel && s.Mode != ToolScheduleSequential {
		return ToolScheduling{}, fmt.Errorf("unknown mode %q", s.Mode)
	}
	if s.MaxParallel < 0 {
		return ToolScheduling{}, errors.New("negative MaxParallel")
	}
	return s, nil
}

type ToolStep struct {
	RefValue   StepRef         `json:"ref"`
	Source     StepID          `json:"source"`
	Calls      []ToolCallState `json:"calls"`
	Scheduling ToolScheduling  `json:"scheduling,omitzero"`
}

func (ToolStep) step()    {}
func (ToolStep) current() {}

//nolint:gocritic // hugeParam: value receiver keeps ToolStep satisfying sealed Step as a value.
func (s ToolStep) Ref() StepRef { return s.RefValue }

func (s *ToolStep) callIndex(id CallID) int {
	for i := range s.Calls {
		if s.Calls[i].CallID == id {
			return i
		}
	}
	return -1
}

// MachineState is the complete semantic state of one Run (RUN-MCH-1): the
// fold of its facts along four dimensions. Progress is which Step the Run
// is in and what it has frozen (Current, ModelSteps, LastToolStep). Inbox is
// the input accepted and not yet consumed by a prepare (PendingInputs).
// Effects are the EffectID an Executing ModelStep or tool call requested and
// the Wait a ToolWaiting call holds (inside Current). End is Status and
// Result. Control metadata never appears here: which worker executes an
// effect, under what lease, epoch or backend handle, is the execution
// plane's attempt record; the Session's owner fence and queue claims are
// the host's. Content bodies (model output, tool output) never appear
// either: facts record digests and the frozen.Store holds the bodies.
type MachineState struct {
	RunID         RunID        `json:"runId"`
	Status        RunStatus    `json:"status"`
	Current       Current      `json:"-"`
	PendingInputs []AgentInput `json:"pendingInputs,omitempty"`
	ModelSteps    int          `json:"modelSteps"`
	// LastToolStep retains the most recently closed ToolStep so the prompt builder can
	// locate the step boundary it continues from. Its RefValue.ID is the
	// SourceStep of the next PromptInput.
	LastToolStep *ToolStep `json:"lastToolStep,omitempty"`
	// Usage and Result are projections folded from facts for readers, not
	// inputs to Decide: Usage accumulates the usage every model fact
	// reports, Result restates RunEnded with that Usage.
	Usage  model.Usage `json:"usage"`
	Result *RunResult  `json:"result,omitempty"`
}

// ValidateMachineState checks the structural invariants required by Runtime
// snapshots. It does not inspect transition history; Record and Rebuild use
// FoldRun for that stronger verification.
func ValidateMachineState(s *MachineState) error {
	if s == nil {
		return errors.New("agent: state: nil state")
	}
	if s.RunID == "" {
		return errors.New("agent: state: empty RunID")
	}
	switch s.Status {
	case RunActive, RunCompleted, RunStopped, RunFailed:
	default:
		return fmt.Errorf("agent: state: unknown RunStatus %d", s.Status)
	}
	if s.ModelSteps < 0 {
		return errors.New("agent: state: negative model step count")
	}
	if err := validatePendingInputs(s.PendingInputs); err != nil {
		return err
	}
	if err := validateLastToolStep(s); err != nil {
		return err
	}
	if s.Status.Terminal() {
		if s.Current != nil {
			return errors.New("agent: state: terminal state has a current step")
		}
		if s.Result == nil || s.Result.Status != s.Status {
			return errors.New("agent: state: terminal state has no matching result")
		}
		return nil
	}
	if s.Result != nil {
		return errors.New("agent: state: active state has a result")
	}
	return validateCurrent(s)
}

func validatePendingInputs(inputs []AgentInput) error {
	seen := make(map[InputID]struct{}, len(inputs))
	for _, input := range inputs {
		if input.ID == "" {
			return errors.New("agent: state: pending input has empty InputID")
		}
		if _, dup := seen[input.ID]; dup {
			return fmt.Errorf("agent: state: duplicate pending InputID %q", input.ID)
		}
		seen[input.ID] = struct{}{}
	}
	return nil
}

func validateLastToolStep(s *MachineState) error {
	last := s.LastToolStep
	if last == nil {
		return nil
	}
	if last.RefValue.RunID != s.RunID || last.RefValue.ID == "" || last.Source == "" {
		return errors.New("agent: state: invalid LastToolStep projection")
	}
	if len(last.Calls) == 0 {
		return errors.New("agent: state: LastToolStep has no calls")
	}
	for i := range last.Calls {
		if err := ValidateToolCallState(last.Calls[i]); err != nil {
			return err
		}
		if !last.Calls[i].Status.Terminal() {
			return errors.New("agent: state: LastToolStep contains a live call")
		}
	}
	return nil
}

func validateCurrent(s *MachineState) error {
	switch current := s.Current.(type) {
	case Open:
		return nil
	case nil:
		return errors.New("agent: state: active state has no current")
	case ModelStep:
		if current.RefValue.RunID != s.RunID || current.RefValue.ID == "" || current.Model == "" {
			return errors.New("agent: state: invalid current ModelStep identity")
		}
		if current.Status != ModelPrepared && current.Status != ModelExecuting {
			return fmt.Errorf("agent: state: unknown ModelStep status %d", current.Status)
		}
		return nil
	case ToolStep:
		return validateCurrentToolStep(s.RunID, &current)
	default:
		return fmt.Errorf("agent: state: unknown current step %T", s.Current)
	}
}

func validateCurrentToolStep(runID RunID, ts *ToolStep) error {
	if ts.RefValue.RunID != runID || ts.RefValue.ID == "" || ts.Source == "" || len(ts.Calls) == 0 {
		return errors.New("agent: state: invalid current ToolStep identity")
	}
	seen := make(map[CallID]struct{}, len(ts.Calls))
	live := false
	for i := range ts.Calls {
		call := &ts.Calls[i]
		if call.CallID == "" {
			return errors.New("agent: state: current ToolStep has empty CallID")
		}
		if _, dup := seen[call.CallID]; dup {
			return fmt.Errorf("agent: state: duplicate CallID %q", call.CallID)
		}
		seen[call.CallID] = struct{}{}
		if err := ValidateToolCallState(*call); err != nil {
			return err
		}
		if !call.Status.Terminal() {
			live = true
		}
	}
	if !live {
		return errors.New("agent: state: current ToolStep has no live calls")
	}
	return nil
}

// InitializeRun builds the minimal initial MachineState (Revision 0) for a
// new Run. It does not encode fixed-model policy, limits, or seed input; those
// belong to host policy and accepted transitions.
func InitializeRun(run RunID) (MachineState, error) {
	if run == "" {
		return MachineState{}, errors.New("agent: initialize: empty RunID")
	}
	return MachineState{RunID: run, Status: RunActive, Current: Open{}}, nil
}
