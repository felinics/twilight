package loop

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

func TestRegressionToolPanicBecomesUnknown(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			panic("nil map write")
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("done")}}
	rt, w := loopRuntime(t)
	interpreter, _ := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticBuilder{specs: []ToolSpec{spec}}, Settings{}, false)

	res, err := interpreter.Run(context.Background(), rt.Bind(w), "run-1", nil)
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
		t.Fatal("missing ToolCallFailed Unknown with panic")
	}
}

func TestRegressionRunFinishedEmitted(t *testing.T) {
	rt, w := loopRuntime(t)
	var kinds []EventKind
	sink := sinkFunc(func(_ context.Context, e Event) error {
		kinds = append(kinds, e.Kind)
		return nil
	})
	interpreter, _ := newLoop(t, nil, fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{textResult("done")}}},
		fakeToolCatalog{}, staticBuilder{}, Settings{}, false)
	if _, err := interpreter.Run(context.Background(), rt.Bind(w), "run-1", sink); err != nil {
		t.Fatal(err)
	}
	for _, k := range kinds {
		if k == EventRunFinished {
			return
		}
	}
	t.Fatalf("EventRunFinished never emitted; kinds = %v", kinds)
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
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			executed.Store(true)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"ok"`)}}
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
	interpreter, _ := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"fs.read": tool}},
		staticBuilder{specs: []ToolSpec{spec}}, Settings{}, false)

	res, err := interpreter.Run(context.Background(), rt.Bind(w), "run-1", nil)
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
	interpreter, _ := newLoop(t, nil, fakeCatalog{nilResultStreamer{}}, fakeToolCatalog{}, staticBuilder{}, Settings{}, true)
	res, err := interpreter.Run(context.Background(), rt.Bind(w), "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Status != RunFailed {
		t.Fatalf("res = %+v", res.Result)
	}
}

type sinkFunc func(context.Context, Event) error

func (f sinkFunc) Emit(ctx context.Context, e Event) error { return f(ctx, e) }

type nilResultStreamer struct{}

func (nilResultStreamer) Generate(context.Context, sdk.Request) (sdk.ModelResult, error) {
	return sdk.ModelResult{}, errors.New("generate should not be called when streaming")
}

func (nilResultStreamer) Stream(context.Context, sdk.Request) (sdk.ModelStream, error) {
	parts := make(chan sdk.StreamPart)
	close(parts)
	return sdk.ModelStream{Parts: parts, Result: func() (*sdk.ModelResult, error) { return nil, nil }}, nil
}
