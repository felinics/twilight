package sdk

// Modality is a kind of content a model accepts or produces.
type Modality string

const (
	ModalityText  Modality = "text"
	ModalityImage Modality = "image"
	ModalityAudio Modality = "audio"
	ModalityVideo Modality = "video"
	ModalityPDF   Modality = "pdf"
)

// Modalities lists what a model accepts and produces.
type Modalities struct {
	Input  []Modality
	Output []Modality
}

func containsModality(modalities []Modality, want Modality) bool {
	for _, modality := range modalities {
		if modality == want {
			return true
		}
	}
	return false
}

// Reasoning effort levels that turn reasoning on, weakest to strongest, for
// Request.ReasoningEffort and ModelReasoning.Efforts. Turning reasoning off is
// described by ModelReasoning.Off and the model's own default by
// ModelReasoning.Default, so neither appears among them.
const (
	ReasoningEffortMinimal = "minimal"
	ReasoningEffortLow     = "low"
	ReasoningEffortMedium  = "medium"
	ReasoningEffortHigh    = "high"
	ReasoningEffortXHigh   = "xhigh"
	ReasoningEffortMax     = "max"
)

// ModelReasoning describes the reasoning controls a host exposes for a model
// and how a request uses them. It separates what the model can do (Toggle,
// Efforts, Budget, Off) from the request shape (Dialect), what it does when
// asked for nothing (Default), and how a Chat Completions host reads
// reasoning back (Replay).
type ModelReasoning struct {
	// Toggle reports a request field that turns reasoning on and off,
	// separate from any effort level.
	Toggle bool
	// Efforts lists the effort levels the host accepts, from the
	// ReasoningEffort constants. Nil means no effort control.
	Efforts []string
	// Budget is the reasoning token budget range; nil means no budget control.
	Budget *ReasoningBudget
	// Off is how the model answers a request to stop reasoning.
	Off ReasoningOffSupport
	// Dialect is the request shape that carries the controls.
	Dialect ReasoningDialect
	// Default is what the model does when a request sets no reasoning
	// control; nil means not stated.
	Default *ReasoningDefault
	// Replay is how a Chat Completions host reads reasoning back on replayed
	// messages; nil means the host returns none on a side channel.
	Replay *ReasoningReplay
}

// Controllable reports whether a request can influence reasoning at all. A
// model that reasons without any control returns false.
func (r *ModelReasoning) Controllable() bool {
	return r.Toggle || len(r.Efforts) > 0 || r.Budget != nil
}

// CanTurnOff reports whether a request can stop the model reasoning. A stated
// Off decides; when it is not stated, a toggle is taken to switch it off.
func (r *ModelReasoning) CanTurnOff() bool {
	switch r.Off {
	case ReasoningOffAccepted, ReasoningOffLowEffortOnly:
		return true
	case ReasoningOffRejected:
		return false
	default:
		return r.Toggle
	}
}

// ReasoningBudget bounds a reasoning token budget. A nil bound is not stated.
type ReasoningBudget struct {
	Min *int
	Max *int
}

// ReasoningOffSupport is how a model answers an explicit request to stop
// reasoning.
type ReasoningOffSupport string

const (
	// ReasoningOffUnknown means not stated.
	ReasoningOffUnknown ReasoningOffSupport = ""
	// ReasoningOffAccepted means reasoning can be turned off at any effort.
	ReasoningOffAccepted ReasoningOffSupport = "accepted"
	// ReasoningOffLowEffortOnly means reasoning can be turned off only at
	// effort high or below; with xhigh or max the request is rejected
	// (Claude Opus 5).
	ReasoningOffLowEffortOnly ReasoningOffSupport = "low_effort_only"
	// ReasoningOffRejected means the model always reasons and rejects a
	// request to stop (Claude Opus 5.5, Fable 5).
	ReasoningOffRejected ReasoningOffSupport = "rejected"
)

// ReasoningDialect is the request shape that carries reasoning controls. The
// same controls travel differently by API and model generation, and the shapes
// are often mutually exclusive on one request.
type ReasoningDialect string

const (
	// ReasoningDialectUnknown means not stated.
	ReasoningDialectUnknown ReasoningDialect = ""
	// ReasoningDialectTier sends a named effort level: OpenAI
	// reasoning_effort and reasoning.effort, Gemini 3 thinkingLevel, DeepSeek
	// reasoning_effort.
	ReasoningDialectTier ReasoningDialect = "tier"
	// ReasoningDialectBudget sends a token budget: Anthropic
	// thinking.budget_tokens on Claude 4.5 and earlier, Gemini 2.5
	// thinkingBudget.
	ReasoningDialectBudget ReasoningDialect = "budget"
	// ReasoningDialectToggle sends only an on/off switch with no level or
	// budget, such as thinking.type on MiniMax.
	ReasoningDialectToggle ReasoningDialect = "toggle"
	// ReasoningDialectAdaptive turns on adaptive thinking and steers depth
	// with an effort level: Anthropic thinking.type "adaptive" with
	// output_config.effort on Claude 4.6 and later.
	ReasoningDialectAdaptive ReasoningDialect = "adaptive"
)

// ReasoningDefault is what a model does when a request sets no reasoning
// control. A nil field is not stated.
type ReasoningDefault struct {
	// Enabled reports whether the model reasons by default. Claude 4.x does
	// not and Claude 5 does, which decides whether "off" has to be sent
	// explicitly.
	Enabled *bool
	// Effort is the level the model uses by default, from the
	// ReasoningEffort constants.
	Effort string
	// Budget is the reasoning token budget the model uses by default.
	Budget *int
}

// ReasoningReplayField is the assistant message field that carries a step's
// reasoning back to a Chat Completions host.
type ReasoningReplayField string

const (
	// ReasoningReplayContent sends the reasoning text as reasoning_content
	// (DeepSeek, Kimi, GLM, Qwen).
	ReasoningReplayContent ReasoningReplayField = "reasoning_content"
	// ReasoningReplayDetails sends the reasoning_details array the host
	// returned, unchanged (OpenRouter, MiniMax). It carries signatures and
	// encrypted reasoning.
	ReasoningReplayDetails ReasoningReplayField = "reasoning_details"
	// ReasoningReplayReasoning sends the reasoning text as reasoning.
	ReasoningReplayReasoning ReasoningReplayField = "reasoning"
)

// ReasoningReplay is how a Chat Completions host reads a step's reasoning back
// on replayed messages. Chat Completions has no standard reasoning field;
// native APIs carry reasoning in their own shapes (Anthropic thinking blocks,
// Responses reasoning items, Gemini thought signatures) and need none.
type ReasoningReplay struct {
	// Field is the message field; empty means ReasoningReplayContent.
	Field ReasoningReplayField
	// Required makes every replayed assistant message with tool calls carry
	// Field, as an empty string when the step produced no reasoning. DeepSeek
	// and Kimi reject a thinking-mode tool call replayed without it. It
	// applies to the text fields only: an empty reasoning_details array
	// carries nothing.
	Required bool
}
