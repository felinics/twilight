// Package protocol defines the wire/application protocol for the process-
// independent effect port in agent/run/effect. It owns message encoding, but
// never mutates a Session or Run; settlement remains the Owner's Loop concern.
package protocol

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/sdk"
)

const ProtocolVersion uint16 = 1

// AssignmentEnvelope is the serializable dispatch message.
type AssignmentEnvelope struct {
	ProtocolVersion uint16            `json:"protocolVersion"`
	Session         run.Scope         `json:"session"`
	Assignment      effect.Assignment `json:"assignment"`
}

// ToolOutcomeEnvelope is the wire representation of loop's sealed outcome.
type ToolOutcomeEnvelope struct {
	Kind    string                   `json:"kind"`
	Result  *run.ToolExecutionResult `json:"result,omitempty"`
	Failure *run.ToolFailure         `json:"failure,omitempty"`
	Retry   run.RetryDisposition     `json:"retry,omitempty"`
}

type WireError struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// OutcomeEnvelope is stable and JSON-safe. In particular it does not contain
// a Go error or a sealed interface.
type OutcomeEnvelope struct {
	ProtocolVersion uint16               `json:"protocolVersion"`
	Key             effect.AssignmentKey `json:"key"`
	Model           *sdk.ModelResult     `json:"model,omitempty"`
	Tool            *ToolOutcomeEnvelope `json:"tool,omitempty"`
	Error           *WireError           `json:"error,omitempty"`
	Cancelled       bool                 `json:"cancelled,omitempty"`
	Unknown         bool                 `json:"unknown,omitempty"`
}

func (o *OutcomeEnvelope) Digest() (run.Digest, error) {
	return es.DigestCanonical(o)
}

// EncodeOutcome renders a sealed Outcome as the wire envelope: a model
// success in Model, a tool result in Tool, a failure in Error with its code,
// a cancellation or an unknown end as the flags.
//
// A model result is encoded only if it freezes (sdkconv.FreezeModelResult):
// JSON would silently rewrite invalid UTF-8 in it, so the record and the
// wire would carry a result the provider never produced. Such a result is
// delivered as a FailureMalformedResult failure instead (RUN-EXE-2).
func EncodeOutcome(out effect.Outcome) OutcomeEnvelope {
	w := OutcomeEnvelope{ProtocolVersion: ProtocolVersion, Key: out.Key}
	switch r := out.Result.(type) {
	case effect.ModelSucceeded:
		if _, err := sdkconv.FreezeModelResult(r.Result); err != nil {
			w.Error = &WireError{Code: string(effect.FailureMalformedResult), Message: "model result cannot be frozen: " + err.Error()}
			break
		}
		res := r.Result
		w.Model = &res
	case effect.ModelFailed:
		code := string(r.Code)
		if code == "" {
			code = string(effect.FailureExecutor)
		}
		w.Error = &WireError{Code: code, Message: r.Message}
	case effect.ToolExecutionSucceeded:
		res := r.Result
		w.Tool = &ToolOutcomeEnvelope{Kind: "succeeded", Result: &res}
	case effect.ToolExecutionFailed:
		f := r.Failure
		w.Tool = &ToolOutcomeEnvelope{Kind: "failed", Failure: &f, Retry: r.Retry}
	case effect.ToolExecutionUnknown:
		f := r.Failure
		w.Tool = &ToolOutcomeEnvelope{Kind: "unknown", Failure: &f}
	case effect.Cancelled:
		w.Cancelled = true
		if r.Message != "" {
			w.Error = &WireError{Code: "cancelled", Message: r.Message}
		}
	case effect.Unknown:
		w.Unknown = true
		if r.Message != "" {
			w.Error = &WireError{Code: string(effect.FailureExecutor), Message: r.Message}
		}
	}
	return w
}

// DecodeOutcome restores the sealed Outcome. The flags win over a body:
// an envelope marked unknown or cancelled is that, whatever else it carries;
// an envelope with neither a body nor a flag is Unknown.
func DecodeOutcome(w *OutcomeEnvelope) effect.Outcome {
	out := effect.Outcome{Key: w.Key}
	message := ""
	if w.Error != nil {
		message = w.Error.Message
	}
	switch {
	case w.Unknown:
		out.Result = effect.Unknown{Message: message}
	case w.Cancelled:
		out.Result = effect.Cancelled{Message: message}
	case w.Tool != nil:
		switch w.Tool.Kind {
		case "succeeded":
			var r run.ToolExecutionResult
			if w.Tool.Result != nil {
				r = *w.Tool.Result
			}
			out.Result = effect.ToolExecutionSucceeded{Result: r}
		case "failed":
			var f run.ToolFailure
			if w.Tool.Failure != nil {
				f = *w.Tool.Failure
			}
			out.Result = effect.ToolExecutionFailed{Failure: f, Retry: w.Tool.Retry}
		case "unknown":
			var f run.ToolFailure
			if w.Tool.Failure != nil {
				f = *w.Tool.Failure
			}
			out.Result = effect.ToolExecutionUnknown{Failure: f}
		default:
			out.Result = effect.Unknown{Message: fmt.Sprintf("executor/protocol: unknown tool outcome %q", w.Tool.Kind)}
		}
	case w.Error != nil:
		out.Result = effect.ModelFailed{Code: effect.FailureCode(w.Error.Code), Message: w.Error.Message}
	case w.Model != nil:
		out.Result = effect.ModelSucceeded{Result: *w.Model}
	default:
		out.Result = effect.Unknown{Message: "executor/protocol: outcome envelope carries no result"}
	}
	return out
}

func StatusTerminal(s effect.ExecutionStatus) bool {
	switch s {
	case effect.ExecutionCompleted, effect.ExecutionFailed, effect.ExecutionCancelled, effect.ExecutionUnknown, effect.ExecutionAborted:
		return true
	default:
		return false
	}
}

// StatusForOutcome is the ExecutionStatus a terminal Outcome corresponds to.
func StatusForOutcome(out effect.Outcome) effect.ExecutionStatus { return out.Status() }
