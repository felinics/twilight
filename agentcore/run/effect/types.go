// Package effect defines the process-independent protocol between the Run
// Loop and the component that performs model/tool effects. It contains no
// transport binding and no persistence implementation.
package effect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/sdk"
)

// AssignmentKind names the effect requested by an Assignment.
type AssignmentKind string

const (
	AssignmentModel AssignmentKind = "model"
	AssignmentTool  AssignmentKind = "tool"
)

// AssignmentKey identifies one effect as the execution plane addresses it.
// Session is the Run's Scope (its Session, in Twilight) and part of the
// identity, so a shared executor cannot collide two stores that happen to use
// the same Run and effect identifiers. Effect is the Run's own name for the
// effect (RUN-WIR-1) and the only handle the two planes share; which step or
// call requested it travels in the Assignment for the backend's use.
type AssignmentKey struct {
	Session run.Scope
	RunID   run.RunID
	Effect  run.EffectID
}

// AssignmentBody is the sealed effect an Assignment asks for: a model call
// or a tool call. Kind is derived from the variant, never stored beside it,
// so an Assignment cannot claim one kind and carry another.
type AssignmentBody interface {
	Kind() AssignmentKind
	assignmentBody()
}

type ModelAssignment struct {
	Model         run.ModelRef        `json:"model"`
	Request       *model.ModelRequest `json:"request,omitempty"`
	RequestDigest run.Digest          `json:"requestDigest"`
}

func (ModelAssignment) Kind() AssignmentKind { return AssignmentModel }
func (ModelAssignment) assignmentBody()      {}

type ToolAssignment struct {
	ToolRef          run.ToolRef
	DefinitionDigest run.Digest
	Arguments        run.CanonicalJSON
	Policy           run.ResponsePolicy
	// Replay is the tool's declared replay policy, copied from the frozen
	// call: the Worker that adopts a lost execution decides from it alone
	// (RUN-EXE-9). Omitted on the wire when unknown.
	Replay run.ReplayPolicy `json:",omitempty"`
	// Placement is the tool's declared placement, copied from the frozen
	// call: the Worker's routes select the backend by it (RUN-LOP-9).
	Placement run.ToolPlacement `json:",omitempty"`
}

func (ToolAssignment) Kind() AssignmentKind { return AssignmentTool }
func (ToolAssignment) assignmentBody()      {}

// Assignment is the complete immutable description of one external effect.
type Assignment struct {
	Session run.Scope
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	Effect  run.EffectID
	Target  *run.TargetRef
	// Body is the effect: exactly one of ModelAssignment or ToolAssignment.
	Body AssignmentBody
}

func (a Assignment) Key() AssignmentKey {
	return AssignmentKey{Session: a.Session, RunID: a.RunID, Effect: a.Effect}
}

// Kind is the body's kind; empty for an Assignment without a body.
func (a Assignment) Kind() AssignmentKind {
	if a.Body == nil {
		return ""
	}
	return a.Body.Kind()
}

// Model returns the model body, if the Assignment is a model call.
func (a Assignment) Model() (ModelAssignment, bool) {
	m, ok := a.Body.(ModelAssignment)
	return m, ok
}

// Tool returns the tool body, if the Assignment is a tool call.
func (a Assignment) Tool() (ToolAssignment, bool) {
	t, ok := a.Body.(ToolAssignment)
	return t, ok
}

func (a Assignment) Digest() (run.Digest, error) {
	return es.DigestCanonical(a)
}

// assignmentWire is the JSON shape: the kind discriminator with one body
// object. It is what the execution store persists and the HTTP protocol
// carries; decoding refuses a shape that names one kind and carries another.
type assignmentWire struct {
	Session run.Scope
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	Effect  run.EffectID
	Target  *run.TargetRef
	Kind    AssignmentKind
	Model   *ModelAssignment
	Tool    *ToolAssignment
}

func (a Assignment) MarshalJSON() ([]byte, error) {
	w := assignmentWire{Session: a.Session, RunID: a.RunID, StepID: a.StepID, CallID: a.CallID, Effect: a.Effect, Target: a.Target, Kind: a.Kind()}
	switch b := a.Body.(type) {
	case ModelAssignment:
		w.Model = &b
	case ToolAssignment:
		w.Tool = &b
	case nil:
	default:
		return nil, fmt.Errorf("agent: effect: unknown assignment body %T", a.Body)
	}
	return json.Marshal(w)
}

func (a *Assignment) UnmarshalJSON(raw []byte) error {
	var w assignmentWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return err
	}
	out := Assignment{Session: w.Session, RunID: w.RunID, StepID: w.StepID, CallID: w.CallID, Effect: w.Effect, Target: w.Target}
	switch {
	case w.Kind == AssignmentModel && w.Model != nil && w.Tool == nil:
		out.Body = *w.Model
	case w.Kind == AssignmentTool && w.Tool != nil && w.Model == nil:
		out.Body = *w.Tool
	case w.Kind == "" && w.Model == nil && w.Tool == nil:
	default:
		return fmt.Errorf("agent: effect: assignment kind %q does not match its body", w.Kind)
	}
	*a = out
	return nil
}

// OutcomeResult is the sealed result of an accepted Assignment: exactly one
// of the variants below. Illegal combinations (a result and an error, a
// cancellation that is also unknown) cannot be expressed.
type OutcomeResult interface{ outcomeResult() }

// FailureCode classifies a ModelFailed; it is wire-stable (protocol).
type FailureCode string

const (
	// FailureExecutor: the provider or executor failed with no more specific
	// classification.
	FailureExecutor FailureCode = "executor_error"
	// FailureFrozenValueMissing: the executor could not read the frozen
	// body the Assignment named (frozen.ErrMissing).
	FailureFrozenValueMissing FailureCode = "frozen_value_missing"
	// FailureMalformedRequest: the frozen request decoded but could not be
	// materialized into a provider request.
	FailureMalformedRequest FailureCode = "malformed_frozen_request"
	// FailureMalformedResult: the provider answered, but the result cannot
	// be frozen (invalid UTF-8 in text or tool input, an unrepresentable
	// part), so it cannot cross the record or the wire unchanged and is not
	// delivered as a success. The Loop rejects it like a malformed request.
	FailureMalformedResult FailureCode = "malformed_result"
	// FailureDeadline: the effect's own deadline elapsed.
	FailureDeadline FailureCode = "deadline_exceeded"
	// FailureRateLimited: the provider refused the call for rate or quota
	// reasons that pass (HTTP 429). Transient.
	FailureRateLimited FailureCode = "rate_limited"
	// FailureProviderUnavailable: the provider answered with a server-side
	// failure (HTTP 5xx). Transient.
	FailureProviderUnavailable FailureCode = "provider_unavailable"
	// FailureConnection: the request or its response did not complete at
	// the transport (connection refused or reset, unexpected EOF, a stream
	// cut short). Transient.
	FailureConnection FailureCode = "connection_failed"
	// FailureAuthentication: the provider refused the credentials (HTTP
	// 401, 403). Not transient.
	FailureAuthentication FailureCode = "authentication_failed"
	// FailureBilling: the provider refused for payment or quota exhaustion
	// (HTTP 402). Not transient.
	FailureBilling FailureCode = "billing"
	// FailureBadRequest: the provider rejected the request itself (HTTP
	// 400, 404, 413, 422). Not transient.
	FailureBadRequest FailureCode = "bad_request"
)

// Retry is the disposition the effect layer derives for a model failure of
// this code (RUN-EXE-11). A model call has no effect on the world, so a
// Known failure never leaves anything that a second call could repeat; the
// only question is whether the failure may pass, and a rate limit, a
// provider outage or a lost connection may, every other code is a definite
// answer. The model call is the template every effect follows: classify the
// failure first, then read the disposition off the class; the disposition
// is never stored beside the code.
func (c FailureCode) Retry() run.RetryDisposition {
	switch c {
	case FailureRateLimited, FailureProviderUnavailable, FailureConnection:
		return run.RetryAllowed
	default:
		return run.RetryNever
	}
}

// ModelSucceeded carries the provider's complete result.
type ModelSucceeded struct{ Result sdk.ModelResult }

// ModelFailed is a provider or executor failure with a wire-stable code.
// Its retry disposition is not stored: it is derived from the code
// (Code.Retry()) wherever it is needed, so the wire cannot carry a
// disposition that disagrees with the code.
type ModelFailed struct {
	Code    FailureCode
	Message string
}

// Retry is the disposition of this failure, derived from its code.
func (f ModelFailed) Retry() run.RetryDisposition { return f.Code.Retry() }

// ToolExecutionOutcome is the sealed result a tool implementation returns:
// succeeded, failed-known, or unknown. Each is also an OutcomeResult.
type ToolExecutionOutcome interface {
	OutcomeResult
	toolExecutionOutcome()
}

type ToolExecutionSucceeded struct{ Result run.ToolExecutionResult }

// ToolExecutionFailed is a Known failure the tool reports: the effect did
// not complete. Failure.Class says what went wrong; Retry is the tool's
// answer for this failure alone (RUN-EXE-11). RetryAllowed asserts more
// than "this looks transient": that this Known failure suffices to confirm
// the attempt produced no external effect that cannot safely be repeated,
// so the next attempt of the same Assignment may run. A failure that cannot
// confirm that (a payment call that timed out) is not a ToolExecutionFailed
// at all but a ToolExecutionUnknown, which enters replay and reconciliation
// (TRN-DUR-4). A tool that does not judge leaves RetryUnknown, never retried.
type ToolExecutionFailed struct {
	Failure run.ToolFailure
	Retry   run.RetryDisposition
}

type ToolExecutionUnknown struct{ Failure run.ToolFailure }

// Cancelled: the executor stopped the effect as requested; Message says why.
type Cancelled struct{ Message string }

// Unknown: the assignment crossed the effect boundary but the executor
// closed its recovery without a terminal provider outcome. It must not be
// read as a dispatch rejection or an ordinary provider failure.
type Unknown struct{ Message string }

func (ModelSucceeded) outcomeResult()         {}
func (ModelFailed) outcomeResult()            {}
func (ToolExecutionSucceeded) outcomeResult() {}
func (ToolExecutionFailed) outcomeResult()    {}
func (ToolExecutionUnknown) outcomeResult()   {}
func (Cancelled) outcomeResult()              {}
func (Unknown) outcomeResult()                {}

func (ToolExecutionSucceeded) toolExecutionOutcome() {}
func (ToolExecutionFailed) toolExecutionOutcome()    {}
func (ToolExecutionUnknown) toolExecutionOutcome()   {}

// Outcome is the process-independent result after an Assignment has been
// accepted. The wire protocol (agent/executor/protocol) encodes Result as a tagged
// envelope; there is no Go error in it.
type Outcome struct {
	Key    AssignmentKey
	Result OutcomeResult
}

// ModelResult returns the provider result of a ModelSucceeded outcome.
func (o Outcome) ModelResult() (sdk.ModelResult, bool) {
	r, ok := o.Result.(ModelSucceeded)
	return r.Result, ok
}

// Status is the ExecutionStatus a terminal Outcome corresponds to.
func (o Outcome) Status() ExecutionStatus {
	switch o.Result.(type) {
	case ModelSucceeded, ToolExecutionSucceeded:
		return ExecutionCompleted
	case ModelFailed, ToolExecutionFailed:
		return ExecutionFailed
	case Cancelled:
		return ExecutionCancelled
	case Unknown, ToolExecutionUnknown:
		return ExecutionUnknown
	default:
		return ExecutionUnknown
	}
}

// ExecutionStatus is the lifecycle state of an accepted effect, independent
// of the Run state machine.
type ExecutionStatus string

const (
	ExecutionNotFound        ExecutionStatus = "not_found"
	ExecutionAccepted        ExecutionStatus = "accepted"
	ExecutionDispatching     ExecutionStatus = "dispatching"
	ExecutionRunning         ExecutionStatus = "running"
	ExecutionCancelRequested ExecutionStatus = "cancel_requested"
	ExecutionCompleted       ExecutionStatus = "completed"
	ExecutionFailed          ExecutionStatus = "failed"
	ExecutionCancelled       ExecutionStatus = "cancelled"
	ExecutionUnknown         ExecutionStatus = "unknown"
	// ExecutionAborted: the key was closed before anything was accepted for
	// it (Abort); no Assignment will ever be accepted under it.
	ExecutionAborted ExecutionStatus = "aborted"
)

// Terminal reports whether the provider execution has a final outcome, or
// never will have one (aborted).
func (s ExecutionStatus) Terminal() bool {
	switch s {
	case ExecutionCompleted, ExecutionFailed, ExecutionCancelled, ExecutionUnknown, ExecutionAborted:
		return true
	default:
		return false
	}
}

var (
	ErrExecutionNotFound = errors.New("agent: effect: execution not found")
	ErrOutcomeNotReady   = errors.New("agent: effect: outcome not ready")
	// ErrOutcomeUnavailable means the executor holds a record for the key but
	// will never produce a readable Outcome for it (a provider this process
	// has no backend for, a record it cannot decode): a definitive answer, as
	// opposed to a transport failure that a later read may not see.
	ErrOutcomeUnavailable = errors.New("agent: effect: outcome unavailable")
	// ErrOutcomeCollected means the executor still holds the key as an
	// accepted, settled execution but no longer serves its Outcome: the
	// Owner acknowledged the settlement (RUN-EXE-13). Definitive, like
	// ErrOutcomeUnavailable, which it wraps.
	ErrOutcomeCollected = fmt.Errorf("agent: effect: outcome collected after acknowledgement: %w", ErrOutcomeUnavailable)
	// ErrExecutionAborted means the key was closed by Abort before any
	// Assignment was accepted for it (RUN-EXE-16): a Dispatch under it is
	// a definite rejection and nothing will ever be read for it, so it is
	// an ErrOutcomeUnavailable as well.
	ErrExecutionAborted = fmt.Errorf("agent: effect: execution aborted before acceptance: %w", ErrOutcomeUnavailable)
	// ErrDispatchUnknown means the dispatch response was lost after the
	// request may have crossed the effect boundary. It must not trigger a
	// compensating re-dispatch or a RecoverModelExecution automatically.
	ErrDispatchUnknown = errors.New("agent: effect: dispatch outcome unknown")
	// ErrDispatchRetryable means the executor refused the Assignment before
	// anything started, for a reason of its own that may pass (its record
	// store was unavailable). Nothing crossed the effect boundary, so the
	// same Assignment may be dispatched again; it is neither a definite
	// rejection of the Assignment nor an unknown outcome (RUN-EXE-3).
	ErrDispatchRetryable = errors.New("agent: effect: dispatch refused before the effect started; retry later")
)

// AttachmentState describes what an executor found for an AssignmentKey.
type AttachmentState string

const (
	AttachmentMissing  AttachmentState = "missing"
	AttachmentActive   AttachmentState = "active"
	AttachmentOrphaned AttachmentState = "orphaned"
	AttachmentTerminal AttachmentState = "terminal"
	// AttachmentAborted: the key holds a tombstone (Abort) and no execution
	// was or will be accepted for it.
	AttachmentAborted AttachmentState = "aborted"
)

// Valid reports whether the executor returned a defined attachment state.
func (s AttachmentState) Valid() bool {
	switch s {
	case AttachmentMissing, AttachmentActive, AttachmentOrphaned, AttachmentTerminal, AttachmentAborted:
		return true
	default:
		return false
	}
}

// Terminal reports whether the executor has a durable terminal observation.
// This describes the attachment observation, not the provider lifecycle; use
// ExecutionStatus.Terminal for the latter.
func (s AttachmentState) Terminal() bool { return s == AttachmentTerminal }

// Attachment is the result of an attach/inspection request. Missing is a
// proof that no execution exists for the key now; it does not by itself
// stop one from being accepted later, so a controller that acts on it
// first closes the key with Abort and disposes on aborted (RUN-EXE-16).
// Orphaned is the absence of
// that proof: a record exists without a live owner, or the executor cannot
// tell whether an execution survived (a record store that may have lost the
// record, a backend that cannot confirm). The control plane must decide
// whether to reconcile, take over, or dispose it; it must not be treated as
// proof of no effect, because the effect may still happen or have happened.
type Attachment struct {
	State               AttachmentState `json:"state"`
	Execution           ExecutionStatus `json:"execution"`
	Owner               string          `json:"owner,omitempty"`
	FencingEpoch        uint64          `json:"fencingEpoch,omitempty"`
	LeaseUntilUnixMilli int64           `json:"leaseUntilUnixMilli,omitempty"`
	BackendAttached     bool            `json:"backendAttached,omitempty"`
}

// Acknowledger is the optional retention hint of an ExecutionPort
// (RUN-EXE-13): the Owner reports that the Outcome of key is settled as a
// Session fact and will not be read again. The executor may then collect
// the record's payload and Outcome, but it keeps the key as accepted: a
// late Dispatch of the same key starts nothing. A port without it relies on
// the executor's time-based collection.
type Acknowledger interface {
	Acknowledge(context.Context, AssignmentKey) error
}

// Recoverer is the optional recovery capability of an execution port
// (RUN-EXE-6). RecoverExecution asks the executor to take the orphaned record
// of key back under a live lease and continue it from its persisted payload
// and ExecutionRef: attach the previous physical execution, restart it once
// the backend proves it missing, or settle it as Unknown when the tool's
// replay declaration forbids a restart. How to recover is the executor's;
// the caller has observed the record as orphaned and decides when to ask.
// Giving up is a separate, explicit act (the executor's Dispose). A record
// under a live lease is left alone. A port without this capability leaves
// orphaned records to an external controller.
type Recoverer interface {
	RecoverExecution(context.Context, AssignmentKey) error
}

// ExecutionPort is the Agent Core effect port: the process-independent
// contract through which the Loop hands effects to whatever executes them.
// It is intentionally message-shaped: none of its methods accepts a
// process-local callback.
type ExecutionPort interface {
	Validate(context.Context, Assignment) (*run.ToolFailure, error)
	Dispatch(context.Context, Assignment) error
	Attach(context.Context, AssignmentKey) (Attachment, error)
	// Abort closes key when nothing has been accepted for it, so that no
	// Dispatch of the key, however late, is accepted afterwards
	// (RUN-EXE-16). Abort and the acceptance a Dispatch writes contend for
	// the first commit of the key's ledger, and exactly one of them wins.
	// The result is the key's attachment after the attempt: aborted when
	// the tombstone stands (written now or before), otherwise the live
	// state an acceptance holds, which the caller must not dispose.
	Abort(context.Context, AssignmentKey) (Attachment, error)
	GetStatus(context.Context, AssignmentKey) (ExecutionStatus, error)
	// GetOutcome is a read, not a wait: it answers at once. An unsettled
	// execution is ErrOutcomeNotReady; a settled one is its Outcome; a key
	// the port never accepted is ErrExecutionNotFound; a definitive refusal
	// to ever answer is ErrOutcomeUnavailable (RUN-EXE-13). Any other error
	// describes the read operation and says nothing about the execution,
	// which remains unsettled until a read returns its explicit Outcome.
	// Learning when to read is SettlementPort's job; a caller without that
	// capability polls.
	GetOutcome(context.Context, AssignmentKey) (Outcome, error)
	Cancel(context.Context, AssignmentKey) error
}
