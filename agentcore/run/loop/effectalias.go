package loop

import (
	"errors"

	effect "github.com/felinics/twilight/agentcore/run/effect"
)

// Aliases keep the protocol types the Loop speaks source-compatible with
// run/effect; the definitions live there.
type AssignmentKind = effect.AssignmentKind

const (
	AssignmentModel = effect.AssignmentModel
	AssignmentTool  = effect.AssignmentTool
)

// Tool outcomes belong to the process-independent effect protocol.
type ToolExecutionOutcome = effect.ToolExecutionOutcome
type ToolExecutionSucceeded = effect.ToolExecutionSucceeded
type ToolExecutionFailed = effect.ToolExecutionFailed
type ToolExecutionUnknown = effect.ToolExecutionUnknown

// ErrModelUnavailable reports a model assignment the executor cannot serve:
// the pre-start check fails with the step still Prepared and no start or
// recovery fact.
var ErrModelUnavailable = errors.New("agent: loop: executor cannot serve the model")

type AssignmentKey = effect.AssignmentKey

type ModelAssignment = effect.ModelAssignment

type ToolAssignment = effect.ToolAssignment

type Assignment = effect.Assignment

type Outcome = effect.Outcome

type OutcomeResult = effect.OutcomeResult

type ModelSucceeded = effect.ModelSucceeded

type ModelFailed = effect.ModelFailed

type Cancelled = effect.Cancelled

type Unknown = effect.Unknown

type FailureCode = effect.FailureCode

type ExecutionStatus = effect.ExecutionStatus

type AttachmentState = effect.AttachmentState

type Attachment = effect.Attachment

type Executor = effect.ExecutionPort

type Acknowledger = effect.Acknowledger

const (
	ExecutionNotFound        = effect.ExecutionNotFound
	ExecutionAccepted        = effect.ExecutionAccepted
	ExecutionRunning         = effect.ExecutionRunning
	ExecutionCancelRequested = effect.ExecutionCancelRequested
	ExecutionCompleted       = effect.ExecutionCompleted
	ExecutionFailed          = effect.ExecutionFailed
	ExecutionCancelled       = effect.ExecutionCancelled
	ExecutionUnknown         = effect.ExecutionUnknown
	ExecutionAborted         = effect.ExecutionAborted

	AttachmentMissing  = effect.AttachmentMissing
	AttachmentActive   = effect.AttachmentActive
	AttachmentOrphaned = effect.AttachmentOrphaned
	AttachmentTerminal = effect.AttachmentTerminal
	AttachmentAborted  = effect.AttachmentAborted

	FailureExecutor           = effect.FailureExecutor
	FailureFrozenValueMissing = effect.FailureFrozenValueMissing
	FailureMalformedRequest   = effect.FailureMalformedRequest
	FailureDeadline           = effect.FailureDeadline
)

var (
	ErrExecutionNotFound = effect.ErrExecutionNotFound
	ErrOutcomeNotReady   = effect.ErrOutcomeNotReady
	ErrDispatchUnknown   = effect.ErrDispatchUnknown
)
