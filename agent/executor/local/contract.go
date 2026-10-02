// Package local is the colocated executor: model and tool effects run in
// goroutines of this process against a static Catalog. It is the
// implementation side of the effect layer; nothing in it enters an
// AgentPreset.
package local

import (
	"context"
	"encoding/json"

	"github.com/felinics/twilight/agentcore/jsonstable"
	run "github.com/felinics/twilight/agentcore/run"
	effect "github.com/felinics/twilight/agentcore/run/effect"

	"github.com/felinics/twilight/sdk"
)

// ModelCatalog resolves a frozen run.ModelRef into an invoker at execution
// time; provider binding never enters the frozen request. The same ModelRef
// must resolve to equivalent execution semantics for the life of a Run.
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
	// Outcome the Executor returns through its message-shaped port.
	Effect           run.EffectID
	ToolRef          run.ToolRef
	DefinitionDigest run.Digest
	Arguments        jsonstable.Value
	// Target is an opaque resource reference supplied by the application; the
	// executor does not interpret it.
	Target   *run.TargetRef
	Progress ToolProgressSink
}

// ExecutableTool is the tool-side execution contract of the colocated
// executor.
type ExecutableTool interface {
	Ref() run.ToolRef
	Definition() sdk.ToolDefinition
	ResponsePolicy() run.ResponsePolicy
	// ValidateArguments runs before the start barrier and must not produce
	// external effects.
	ValidateArguments(jsonstable.Value) error
	Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
	// Replay declares whether Execute may run again for the same call after
	// an earlier execution was lost. Every tool answers; the zero value
	// ReplayUnknown is the answer of a tool whose author has not judged it.
	// The declaration is frozen into the ToolSpec and carried on every call
	// and Assignment.
	Replay() run.ReplayPolicy
	// Placement declares where the tool runs: in the executor process, or
	// inside the Session's workspace, in which case its calls need a target
	// and are routed to the workspace backend. Every tool answers; the
	// declaration is frozen into the ToolSpec.
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
