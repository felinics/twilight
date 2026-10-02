package turn

import (
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
)

// StartRequest opens a new Turn under Preset with Inputs delivered into its
// Run.
type StartRequest struct {
	Ref    TurnRef
	Inputs []run.AgentInput
	Preset preset.PresetRef
}

// DeliverRequest carries Inputs into an active Turn's Run.
type DeliverRequest struct {
	Ref    TurnRef
	Inputs []run.AgentInput
}

// StopRequest settles the active Turn as stopped with Reason.
type StopRequest struct {
	Ref    TurnRef
	Reason string
}

// ResumeDisposition is where a still-active Turn stands when a caller looks
// at it again. It is not a Turn's own fact: the Coordinator computes it from
// the Turn's status and its Run's state. It says what the Run waits for;
// whether the process reading it is the one carrying that wait is the
// reader's own knowledge.
type ResumeDisposition string

const (
	// ResumeWaitingForResponse: a tool call waits for a response.
	ResumeWaitingForResponse ResumeDisposition = "waiting_for_response"
	// ResumeExecuting: a model step or tool call is Executing; the Run moves
	// when its Outcome is settled.
	ResumeExecuting ResumeDisposition = "executing"
	ResumeFinished  ResumeDisposition = "finished"
)

// TurnResult is the Turn protocol's answer: the Turn's status, and where it
// stands when it is still active.
type TurnResult struct {
	Ref         TurnRef
	RunID       run.RunID
	Status      TurnStatus
	Disposition ResumeDisposition
	End         run.RunEnd
	Waiting     []run.ResponseRequest
}
