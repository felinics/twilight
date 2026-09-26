package run

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run/model"
)

// Fact is one committed outcome produced by Machine.Decide. Facts are wrapped
// as AgentEvents; Machine.Evolve folds them mechanically (RUN-MCH-3). The
// interface is sealed: only the variants below exist. Facts carry execution
// state and content digests only: a digest is the canonical identity of an
// immutable body in the frozen.Store, and the fact that names it is the
// body's retention root (RUN-WIR-4). Conversation and Turn state are
// projections of these facts; no second copy of a body is written.
type Fact interface{ fact() }

// RunCreated is the first fact of a Run (RUN-NEW-1). Folding it onto the zero
// MachineState yields the initial state; a second RunCreated is an error.
type RunCreated struct {
	RunID       RunID          `json:"runId"`
	CausationID es.CausationID `json:"causationId,omitempty"`
}

func (RunCreated) fact() {}

// ModelStepPrepared establishes the frozen ModelStep and consumes the listed
// pending inputs. The request body is not in the fact: RequestDigest names it
// in the frozen.Store. StepID derives from the Run and the preparing
// command (RUN-WIR-3); the fact carries the step's content beside it.
type ModelStepPrepared struct {
	StepID        StepID     `json:"stepId"`
	Model         ModelRef   `json:"model"`
	RequestDigest Digest     `json:"requestDigest"`
	InputIDs      []InputID  `json:"inputIds,omitempty"`
	Tools         []ToolSpec `json:"tools,omitempty"`
}

func (ModelStepPrepared) fact() {}

// ModelStepWithdrawn: Prepared -> Open. The frozen request was never sent;
// inputs arrived while it was Prepared and the next Prepare must include them.
type ModelStepWithdrawn struct {
	StepID StepID `json:"stepId"`
}

func (ModelStepWithdrawn) fact() {}

// ModelStepStarted: Prepared -> Executing. Effect identifies the model effect
// the step requested: a late Outcome is accepted only under it, and a new
// owner recovers the step under a CommandID derived from it (RUN-WIR-1,
// RUN-CMT-7).
type ModelStepStarted struct {
	StepID StepID   `json:"stepId"`
	Effect EffectID `json:"effect"`
}

func (ModelStepStarted) fact() {}

// ModelStepRecovered: Executing -> Open. The model effect the step requested
// is lost and its result cannot be reached (RUN-CMT-7), so the step is withdrawn
// like a Prepared step whose request went stale: the frozen request is not
// resent. Recovery is a decision point -- the next Prepare plans again from
// the state at recovery time, including inputs delivered meanwhile -- and the
// record shows it as one (TRN-DUR-1). ModelSteps is not counted for it.
type ModelStepRecovered struct {
	StepID StepID `json:"stepId"`
	// Effect is the model effect the recovery closes (RUN-CMT-7).
	Effect EffectID `json:"effect,omitempty"`
}

func (ModelStepRecovered) fact() {}

// ModelStepRejected records one structurally malformed result: usage is
// accumulated, Rejects is incremented, the step returns to Prepared. It
// settles the model effect the step was executing.
type ModelStepRejected struct {
	StepID StepID `json:"stepId"`
	// Effect is the model effect this rejection settles (RUN-WIR-1).
	Effect  EffectID    `json:"effect,omitempty"`
	Usage   model.Usage `json:"usage"`
	Failure StepFailure `json:"failure"`
}

func (ModelStepRejected) fact() {}

// ModelStepFailed: Executing -> Open. It settles the model effect the step
// was executing with its final failure; the Run does not continue from it.
// SubmitModelFailure writes it before RunEnded(failed) in the same group;
// CancelRun writes it with class effect_unknown for a model call still
// executing, before RunEnded(stopped). RunEnded itself settles no effect:
// every effect a Run requested is closed by a fact that names it
// (RUN-WIR-1), so a Run never ends with a step or call still Executing.
type ModelStepFailed struct {
	StepID StepID `json:"stepId"`
	// Effect is the model effect this failure settles (RUN-WIR-1).
	Effect  EffectID    `json:"effect"`
	Failure StepFailure `json:"failure"`
}

func (ModelStepFailed) fact() {}

// ModelStepCompleted accepts one model result: usage is accumulated and
// Current becomes Open. The result body is not in the fact; ResultDigest is
// its canonical identity in the frozen.Store. The same transition may then
// open a ToolStep or end the Run.
type ModelStepCompleted struct {
	StepID StepID `json:"stepId"`
	// Effect is the model effect this settlement closes: the fact names the
	// execution it answers, as its start fact did (RUN-WIR-1).
	Effect       EffectID           `json:"effect,omitempty"`
	Usage        model.Usage        `json:"usage"`
	FinishReason model.FinishReason `json:"finishReason"`
	ResultDigest Digest             `json:"resultDigest"`
}

func (ModelStepCompleted) fact() {}

// ToolStepOpened establishes the ToolStep with its full frozen call set.
// A ModelStep completes once, so the ToolStep it opens derives its identity
// from the source alone: DeriveToolStepID(Source) == StepID always holds.
type ToolStepOpened struct {
	StepID     StepID            `json:"stepId"` // the new ToolStep
	Source     StepID            `json:"source"` // the completed ModelStep
	Calls      []ToolCallBinding `json:"calls"`
	Scheduling ToolScheduling    `json:"scheduling,omitzero"`
}

func (ToolStepOpened) fact() {}

// ToolCallStarted: Pending -> Executing. Effect identifies the tool effect
// the call requested (see ModelStepStarted).
type ToolCallStarted struct {
	StepID StepID   `json:"stepId"`
	CallID CallID   `json:"callId"`
	Effect EffectID `json:"effect"`
}

func (ToolCallStarted) fact() {}

// ToolCallApproved: Waiting(Approval) -> Pending.
type ToolCallApproved struct {
	StepID         StepID     `json:"stepId"`
	CallID         CallID     `json:"callId"`
	ResponseID     ResponseID `json:"responseId"`
	ResponseDigest Digest     `json:"responseDigest"`
}

func (ToolCallApproved) fact() {}

// ToolCallCompleted: Executing -> Completed. OutputDigest names the frozen
// tool output.
type ToolCallCompleted struct {
	StepID StepID `json:"stepId"`
	CallID CallID `json:"callId"`
	// Effect is the tool effect this settlement closes (RUN-WIR-1).
	Effect       EffectID `json:"effect,omitempty"`
	OutputDigest Digest   `json:"outputDigest"`
}

func (ToolCallCompleted) fact() {}

// ToolCallAnswered: Waiting(ExternalResponse) -> Completed. ResponseDigest
// names the frozen external answer payload.
type ToolCallAnswered struct {
	StepID         StepID     `json:"stepId"`
	CallID         CallID     `json:"callId"`
	ResponseID     ResponseID `json:"responseId"`
	ResponseDigest Digest     `json:"responseDigest"`
}

func (ToolCallAnswered) fact() {}

// ToolCallFailed: Pending/Executing/Waiting -> Failed(Known/Unknown).
// Effect is the tool effect the failure settles; empty for a call that
// never started (a decline, a rejected response, a cancelled Pending call).
type ToolCallFailed struct {
	StepID  StepID             `json:"stepId"`
	CallID  CallID             `json:"callId"`
	Effect  EffectID           `json:"effect,omitempty"`
	Failure ToolFailure        `json:"failure"`
	Outcome ToolFailureOutcome `json:"outcome"`
}

func (ToolCallFailed) fact() {}

// InputAccepted appends one input to PendingInputs. Legal in every
// non-terminal state; PendingInputs is the durable mid-run input queue.
type InputAccepted struct {
	Input AgentInput `json:"input"`
}

func (InputAccepted) fact() {}

// RunEnd is the closed set of terminal outcomes.
type RunEnd interface{ runEnd() }

type RunCompletedEnd struct{}
type RunStoppedEnd struct {
	Reason         RunReason
	UncertainCalls []CallID `json:"uncertainCalls,omitempty"`
	UncertainModel StepID   `json:"uncertainModel,omitempty"`
}
type RunFailedEnd struct {
	Reason  RunReason
	Failure RunFailure
}

func (RunCompletedEnd) runEnd() {}
func (RunStoppedEnd) runEnd()   {}
func (RunFailedEnd) runEnd()    {}

// RunEnded is the terminal fact. Always the last fact of its transition.
type RunEnded struct {
	End RunEnd `json:"-"`
}

func (RunEnded) fact() {}

func validateRunEnd(end RunEnd) error {
	switch e := end.(type) {
	case RunCompletedEnd:
		return nil
	case RunStoppedEnd:
		if e.Reason == "" {
			return errors.New("agent: run ended: stopped outcome requires a reason")
		}
		return nil
	case RunFailedEnd:
		if e.Reason == "" {
			return errors.New("agent: run ended: failed outcome requires a reason")
		}
		if e.Failure.Class == "" {
			return errors.New("agent: run ended: failed outcome requires a failure class")
		}
		return nil
	default:
		return fmt.Errorf("agent: run ended: unknown end variant %T", end)
	}
}

func endProjection(end RunEnd) (RunStatus, RunReason, *RunFailure) {
	switch e := end.(type) {
	case RunCompletedEnd:
		return RunCompleted, "", nil
	case RunStoppedEnd:
		return RunStopped, e.Reason, nil
	case RunFailedEnd:
		failure := e.Failure
		return RunFailed, e.Reason, &failure
	default:
		return RunActive, "", nil
	}
}

// runEndWire is the tagged-union wire of RunEnded: exactly one variant key is
// present. It mirrors the Go union so the wire cannot express an outcome the
// type system rejects.
type runEndWire struct {
	Completed *struct{}          `json:"completed,omitempty"`
	Stopped   *runStoppedEndWire `json:"stopped,omitempty"`
	Failed    *runFailedEndWire  `json:"failed,omitempty"`
}

type runStoppedEndWire struct {
	Reason         RunReason `json:"reason"`
	UncertainCalls []CallID  `json:"uncertainCalls,omitempty"`
	UncertainModel StepID    `json:"uncertainModel,omitempty"`
}

type runFailedEndWire struct {
	Reason  RunReason  `json:"reason"`
	Failure RunFailure `json:"failure"`
}

func (r RunEnded) MarshalJSON() ([]byte, error) {
	if err := validateRunEnd(r.End); err != nil {
		return nil, err
	}
	var w runEndWire
	switch e := r.End.(type) {
	case RunCompletedEnd:
		w.Completed = &struct{}{}
	case RunStoppedEnd:
		w.Stopped = &runStoppedEndWire{Reason: e.Reason, UncertainCalls: e.UncertainCalls, UncertainModel: e.UncertainModel}
	case RunFailedEnd:
		w.Failed = &runFailedEndWire{Reason: e.Reason, Failure: e.Failure}
	}
	return json.Marshal(w)
}

func (r *RunEnded) UnmarshalJSON(raw []byte) error {
	var w runEndWire
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("agent: run ended: trailing JSON")
		}
		return err
	}
	variants := 0
	var end RunEnd
	if w.Completed != nil {
		variants++
		end = RunCompletedEnd{}
	}
	if w.Stopped != nil {
		variants++
		end = RunStoppedEnd{Reason: w.Stopped.Reason, UncertainCalls: w.Stopped.UncertainCalls, UncertainModel: w.Stopped.UncertainModel}
	}
	if w.Failed != nil {
		variants++
		end = RunFailedEnd{Reason: w.Failed.Reason, Failure: w.Failed.Failure}
	}
	if variants != 1 {
		return fmt.Errorf("agent: run ended: exactly one outcome required, got %d", variants)
	}
	if err := validateRunEnd(end); err != nil {
		return err
	}
	*r = RunEnded{End: end}
	return nil
}
