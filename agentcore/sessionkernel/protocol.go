package sessionkernel

import (
	"context"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// StartRequest opens a new Turn under immutable Preset and accepts Inputs.
type StartRequest struct {
	Ref    sessionstore.TurnRef
	Inputs []turn.AgentInput
	Preset preset.PresetRef
}

// DeliverRequest accepts Inputs into an active Turn.
type DeliverRequest struct {
	Ref    sessionstore.TurnRef
	Inputs []turn.AgentInput
}

// StopRequest settles the active Turn as stopped with Reason.
type StopRequest struct {
	Ref    sessionstore.TurnRef
	Reason string
}

// ResumeDisposition is where a still-active Turn stands when a caller looks
// at it again. It is not a Turn's own fact: the Coordinator computes it from
// the Turn's current state.
type ResumeDisposition string

const (
	ResumeWaitingForResponse ResumeDisposition = "waiting_for_response"
	ResumeWaitingForRecovery ResumeDisposition = "waiting_for_recovery"
	ResumeFinished           ResumeDisposition = "finished"
)

// TurnResult is the Turn protocol's answer: the Turn's status, and where it
// stands when it is still active.
type TurnResult struct {
	Ref         sessionstore.TurnRef
	Status      turn.TurnStatus
	Disposition ResumeDisposition
	End         turn.TurnEnd
	Waiting     []turn.ResponseRequest
}

// Commands are the Turn protocol commits. Each takes the Writer of the
// Session it commits to: the caller's ownership capability, so every command
// lands on the same Writer, epoch and projection view as the other domains'
// commands, and a stale owner is fenced by the Writer itself. Driving a Turn
// is not among them: every method returns as soon as its commit landed.
type Commands interface {
	Start(context.Context, writer.Writer, StartRequest) (TurnResult, error)
	Deliver(context.Context, writer.Writer, DeliverRequest) (TurnResult, error)
	Stop(context.Context, writer.Writer, StopRequest) (TurnResult, error)
}

// Reader is the Turn status read; it needs no ownership.
type Reader interface {
	Status(context.Context, sessionstore.TurnRef) (TurnResult, error)
}
