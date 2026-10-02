package loop

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/sdkconv"
	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

func TestRegressionToolPanicBecomesUnknown(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(context.Context, local.ToolExecutionRequest) effect.ToolExecutionOutcome {
			panic("nil map write")
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("done")}}
	rt, w := loopRuntime(t)
	interpreter, _ := newLoop(t, fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]local.ExecutableTool{"echo": echo}}, Settings{}, false)

	res, err := settle(context.Background(), interpreter, awaiting(t, interpreter), rt.Bind(w), staticBuilder{specs: []ToolSpec{spec}}, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Status != RunCompleted {
		t.Fatalf("res = %+v", res.Result)
	}
	found := false
	for _, e := range recordFacts(t, rt, "run-1") {
		failed, ok := e.(ToolCallFailed)
		if !ok || failed.Outcome != ToolOutcomeUnknown {
			continue
		}
		if strings.Contains(failed.Failure.Message, "panic") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("missing ToolCallFailed effect.Unknown with panic")
	}
}

func TestRegressionAliasedToolRefExecutes(t *testing.T) {
	def := sdk.ToolDefinition{Name: "read", Parameters: &jsonschema.Schema{Type: "object"}}
	frozenDef, err := sdkconv.FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := schema.Canonical().DigestToolDefinition(frozenDef)
	if err != nil {
		t.Fatal(err)
	}
	spec := ToolSpec{Ref: "fs.read", Name: "read", DefinitionDigest: d, Policy: DirectExecution}
	executed := atomic.Bool{}
	tool := &fakeTool{ref: "fs.read", def: def, policy: DirectExecution,
		execute: func(context.Context, local.ToolExecutionRequest) effect.ToolExecutionOutcome {
			executed.Store(true)
			return effect.ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"ok"`)}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{
		func() sdk.ModelResult {
			r := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls}
			r.ToolCalls = []sdk.ToolCall{{ToolCallID: "c1", ToolName: "read", Input: sdk.ParseToolArguments(`{}`)}}
			return r
		}(),
		textResult("done"),
	}}
	rt, w := loopRuntime(t)
	interpreter, _ := newLoop(t, fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]local.ExecutableTool{"fs.read": tool}}, Settings{}, false)

	res, err := settle(context.Background(), interpreter, awaiting(t, interpreter), rt.Bind(w), staticBuilder{specs: []ToolSpec{spec}}, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !executed.Load() {
		t.Fatal("aliased tool never executed")
	}
	if res.Result.Status != RunCompleted {
		t.Fatalf("res = %+v", res.Result)
	}
}

func TestRegressionStreamNilResult(t *testing.T) {
	rt, w := loopRuntime(t)
	interpreter, _ := newLoop(t, fakeCatalog{nilResultStreamer{}}, fakeToolCatalog{}, Settings{}, true)
	res, err := settle(context.Background(), interpreter, awaiting(t, interpreter), rt.Bind(w), staticBuilder{}, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Status != RunFailed {
		t.Fatalf("res = %+v", res.Result)
	}
}

type nilResultStreamer struct{}

func (nilResultStreamer) Generate(context.Context, sdk.Request) (sdk.ModelResult, error) {
	return sdk.ModelResult{}, errors.New("generate should not be called when streaming")
}

func (nilResultStreamer) Stream(context.Context, sdk.Request) (sdk.ModelStream, error) {
	parts := make(chan sdk.StreamPart)
	close(parts)
	return sdk.ModelStream{Parts: parts, Result: func() (*sdk.ModelResult, error) { return nil, nil }}, nil
}
