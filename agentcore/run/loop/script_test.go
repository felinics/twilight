package loop_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/sdk"
)

// Text is a model result that completes without tool calls.
func Text(text string) sdk.ModelResult {
	return sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
}

// Call is one scripted tool call with the default arguments.
func Call(name, id string) sdk.ToolCall {
	return sdk.ToolCall{ToolCallID: id, ToolName: name, Input: sdk.ParseToolArguments(`{"x":1}`)}
}

// Calls is a model result that opens the given tool calls.
func Calls(calls ...sdk.ToolCall) sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2}, ToolCalls: calls}
}

// ToolCalls is a model result that opens the named tool for each call ID.
func ToolCalls(name string, ids ...string) sdk.ModelResult {
	calls := make([]sdk.ToolCall, len(ids))
	for i, id := range ids {
		calls[i] = Call(name, id)
	}
	return Calls(calls...)
}

type scriptInvoker struct {
	results []sdk.ModelResult
	calls   atomic.Int32
	mu      sync.Mutex
	last    *sdk.ModelResult // most recent result handed to the Loop
}

func (s *scriptInvoker) Generate(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ModelResult{}, err
	}
	n := int(s.calls.Add(1)) - 1
	if n >= len(s.results) {
		return sdk.ModelResult{}, errors.New("feature: no scripted model result")
	}
	res := s.results[n]
	s.mu.Lock()
	s.last = &res
	s.mu.Unlock()
	return res, nil
}

func (s *scriptInvoker) lastResult() (sdk.ModelResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		return sdk.ModelResult{}, false
	}
	return *s.last, true
}

type scriptCatalog struct {
	invoker loop.ModelInvoker
	err     error
}

func (c scriptCatalog) ResolveModel(run.ModelRef) (loop.ModelInvoker, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.invoker, nil
}

type scriptTool struct {
	ref     run.ToolRef
	def     sdk.ToolDefinition
	policy  run.ResponsePolicy
	unknown bool
	fail    string
	ran     atomic.Int32
}

func (s *scriptTool) Ref() run.ToolRef                   { return s.ref }
func (s *scriptTool) Definition() sdk.ToolDefinition     { return s.def }
func (s *scriptTool) ResponsePolicy() run.ResponsePolicy { return s.policy }
func (s *scriptTool) Replay() run.ReplayPolicy           { return run.ReplayUnknown }
func (s *scriptTool) Placement() run.ToolPlacement       { return run.PlacementProcess }
func (s *scriptTool) ValidateArguments(run.CanonicalJSON) error {
	return nil
}

func (s *scriptTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	s.ran.Add(1)
	if s.unknown {
		return loop.ToolExecutionUnknown{Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "lost"}}
	}
	if s.fail != "" {
		return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: s.fail, Message: "boom"}}
	}
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}

type scriptToolCatalog struct {
	tools map[run.ToolRef]loop.ExecutableTool
}

func (c scriptToolCatalog) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	tool, ok := c.tools[ref]
	if !ok {
		return nil, fmt.Errorf("unknown tool %q", ref)
	}
	return tool, nil
}

type scriptBuilder struct {
	model    run.ModelRef
	specs    []run.ToolSpec
	defs     map[run.ToolRef]sdk.ToolDefinition
	lastHint plan.PromptInput
}

func (p *scriptBuilder) Build(_ context.Context, hint plan.PromptInput) (loop.Prompt, error) {
	p.lastHint = hint
	model := p.model
	if model == "" {
		model = defaultModel
	}
	req := sdk.Request{Model: string(model), Messages: []sdk.Message{sdk.UserMessage("go")}}
	for _, spec := range p.specs {
		req.Tools = append(req.Tools, p.defs[spec.Ref])
	}
	ids := make([]run.InputID, len(hint.Inputs))
	for i, in := range hint.Inputs {
		ids[i] = in.ID
	}
	return loop.Prompt{Model: model, Request: req, InputIDs: ids, Tools: p.specs}, nil
}
