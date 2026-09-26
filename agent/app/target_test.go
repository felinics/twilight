package app_test

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

var (
	ws1 = run.TargetRef{Kind: "workspace", ID: "ws-1"}
	ws2 = run.TargetRef{Kind: "workspace", ID: "ws-2"}
)

// targetTool reports the target each of its tool effects executes against.
type targetTool struct {
	seen chan *run.TargetRef
}

func (t *targetTool) Ref() run.ToolRef { return "lookup" }
func (t *targetTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: &jsonschema.Schema{Type: "object"}}
}
func (t *targetTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *targetTool) Replay() run.ReplayPolicy                  { return run.ReplayUnknown }
func (t *targetTool) Placement() run.ToolPlacement              { return run.PlacementWorkspace }
func (t *targetTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *targetTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	t.seen <- req.Target
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}

// appResolver stands for the application's resource layer: it owns the
// Session → workspace binding and answers the core's TargetResolver seam
// (APP-TGT-1). Workspace-placed tool effects get the Session's binding;
// model effects and process-placed tools get none (RUN-LOP-9).
type appResolver struct {
	mu       sync.Mutex
	bindings map[session.SessionID]run.TargetRef
	seen     []loop.EffectContext
}

func (r *appResolver) bind(sid session.SessionID, ref run.TargetRef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bindings == nil {
		r.bindings = make(map[session.SessionID]run.TargetRef)
	}
	r.bindings[sid] = ref
}

func (r *appResolver) ResolveTarget(_ context.Context, ec loop.EffectContext) (*run.TargetRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, ec)
	if ec.Kind != loop.AssignmentTool || ec.Placement != run.PlacementWorkspace {
		return nil, nil
	}
	ref, ok := r.bindings[session.SessionID(ec.Session)]
	if !ok {
		return nil, nil
	}
	return &ref, nil
}

func (r *appResolver) contexts() []loop.EffectContext {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]loop.EffectContext(nil), r.seen...)
}

// The core has no target fact of its own: with no resolver every effect has
// no target; with the application's resolver each tool effect is resolved
// once, by its own coordinates, and the answer reaches the tool (APP-TGT-1,
// RUN-LOP-9).
func TestTargetResolverSeam(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "s-1"
	cases := []struct {
		name     string
		resolver *appResolver
		want     *run.TargetRef
	}{
		{name: "no resolver", want: nil},
		{name: "application resolver", resolver: &appResolver{}, want: &ws1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tool := &targetTool{seen: make(chan *run.TargetRef, 1)}
			model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
			cfg := app.Config{}
			if tc.resolver != nil {
				tc.resolver.bind(sid, ws1)
				cfg.TargetResolver = tc.resolver
			}
			h := newHost(t, cfg, map[run.ModelRef]loop.ModelInvoker{"m-1": model}, tool)
			preset, err := h.RegisterPreset("b1", mustPreset("m-1", []loop.ExecutableTool{tool}))
			if err != nil {
				t.Fatal(err)
			}
			s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset})
			if err != nil {
				t.Fatal(err)
			}
			results, err := s.Send(ctx, "what is the weather?")
			if err != nil || len(results) != 1 || results[0].Status != turn.TurnCompleted {
				t.Fatalf("send = %+v %v", results, err)
			}
			seen := <-tool.seen
			if (seen == nil) != (tc.want == nil) || (seen != nil && *seen != *tc.want) {
				t.Fatalf("tool effect target = %v, want %v", seen, tc.want)
			}
			if err := s.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if tc.resolver == nil {
				return
			}
			var tools, models int
			for _, ec := range tc.resolver.contexts() {
				if session.SessionID(ec.Session) != sid || ec.Effect == "" {
					t.Fatalf("effect context %+v lacks its coordinates", ec)
				}
				switch ec.Kind {
				case loop.AssignmentTool:
					tools++
					if ec.Tool != "lookup" || ec.CallID == "" || ec.Placement != run.PlacementWorkspace {
						t.Fatalf("tool effect context %+v, want the frozen workspace placement", ec)
					}
				case loop.AssignmentModel:
					models++
					if ec.Placement != run.PlacementProcess {
						t.Fatalf("model effect context %+v carries a placement", ec)
					}
				}
			}
			if tools != 1 || models != 2 {
				t.Fatalf("resolved %d tool and %d model effects, want 1 and 2", tools, models)
			}
		})
	}
}

// A conversation fork carries no resource binding: the child's target is
// whatever the application's fork policy binds for it, never the parent's
// binding by lineage (APP-TGT-1, TRN-DUR-3).
func TestForkChildTargetIsApplicationPolicy(t *testing.T) {
	ctx := context.Background()
	resolver := &appResolver{}
	tool := &targetTool{seen: make(chan *run.TargetRef, 1)}
	done := sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer(), done, toolCallAnswer(), done, toolCallAnswer(), done}}
	h := newHost(t, app.Config{Store: filestoretest.Store(t), TargetResolver: resolver},
		map[run.ModelRef]loop.ModelInvoker{"m-1": model}, tool)
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", []loop.ExecutableTool{tool}))
	if err != nil {
		t.Fatal(err)
	}
	open := func(sid session.SessionID, prefix string) *app.Session {
		n := 0
		s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset,
			NewTurnID: func() turn.TurnID { n++; return turn.TurnID(prefix + string(rune('0'+n))) }})
		if err != nil {
			t.Fatalf("open %s: %v", sid, err)
		}
		return s
	}
	drive := func(name string, run func() error) *run.TargetRef {
		if err := run(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return <-tool.seen
	}
	resolver.bind("parent", ws1)
	parent := open("parent", "p")
	if got := drive("parent send", func() error { _, err := parent.Send(ctx, "hello"); return err }); got == nil || *got != ws1 {
		t.Fatalf("parent target = %v, want %v", got, ws1)
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Regenerate: fork before the parent's Turn and drain its input again.
	if _, err := h.ForkBeforeTurn(ctx, "parent", "p1", "child"); err != nil {
		t.Fatal(err)
	}
	child := open("child", "c")
	// Until the application binds a workspace for the child, its tool
	// effects have no target: nothing is inherited from the parent.
	if got := drive("child drain", func() error { _, _, err := child.Drain(ctx); return err }); got != nil {
		t.Fatalf("unbound child target = %v, want none", got)
	}
	// The application's fork policy allocates a fresh workspace for the child.
	resolver.bind("child", ws2)
	if got := drive("child send", func() error { _, err := child.Send(ctx, "again"); return err }); got == nil || *got != ws2 {
		t.Fatalf("child target = %v, want %v", got, ws2)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
