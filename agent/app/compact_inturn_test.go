package app_test

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// stepModel answers compactor requests with a fixed summary and every other
// request from a script, recording all of them.
type stepModel struct {
	mu      sync.Mutex
	seen    []sdk.Request
	answers []sdk.ModelResult
}

func (m *stepModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(req.Messages) > 0 && req.Messages[0].Role == sdk.MessageRoleSystem && messageText(req.Messages[0]) == compaction.CompactorSystemPrompt {
		return sdk.ModelResult{Text: "summary-so-far", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	m.seen = append(m.seen, req)
	if len(m.answers) == 0 {
		return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	next := m.answers[0]
	m.answers = m.answers[1:]
	return next, nil
}

func (m *stepModel) requests() []sdk.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sdk.Request(nil), m.seen...)
}

// echoTool answers at once with its arguments.
type echoTool struct{}

func (echoTool) Ref() run.ToolRef { return "lookup" }
func (echoTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: &jsonschema.Schema{Type: "object"}}
}
func (echoTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (echoTool) Replay() run.ReplayPolicy                  { return run.ReplayAllowed }
func (echoTool) Placement() run.ToolPlacement              { return run.PlacementProcess }
func (echoTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (echoTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}

// APP-CKP-1: with an automatic policy, a Turn of several tool steps is
// compacted between its steps: each model request after the threshold
// starts from a compaction's summary, the tool pairs stay closed, and the
// Turn settles normally.
func TestCompactionRunsBetweenStepsOfATurn(t *testing.T) {
	ctx := context.Background()
	model := &stepModel{answers: []sdk.ModelResult{toolCallAnswer(), toolCallAnswer()}}
	h := newHost(t, app.Config{Store: filestoretest.Store(t), Content: durableContent(t), Ownership: session.OpenOptions{Takeover: true}},
		map[run.ModelRef]loop.ModelInvoker{"m-1": model}, echoTool{})
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", []loop.ExecutableTool{echoTool{}}))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, "s-steps", app.SessionOptions{Preset: preset, CompactAfterEntries: 2, CompactRetainEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(ctx) }()
	results, err := s.Send(ctx, "hi")
	if err != nil || len(results) == 0 || results[0].Reply != "done" {
		t.Fatalf("send = %+v %v", results, err)
	}
	reqs := model.requests()
	if len(reqs) != 3 {
		t.Fatalf("model requests = %d, want tool call, tool call, text", len(reqs))
	}
	// Request 1 read [in]. Request 2 read [summary, a1, r1] after the first
	// between-steps compaction; request 3 read [summary, a2, r2] after the
	// second. Each keeps the pending tool pair closed.
	for i, req := range reqs[1:] {
		texts := messageTexts(req)
		if len(texts) != 3 || texts[0] != "assistant: summary-so-far" || texts[1][:10] != "assistant:" || texts[2][:5] != "tool:" {
			t.Fatalf("request %d messages = %v, want summary then a closed tool pair", i+2, texts)
		}
	}
	surface, err := chatlog.ReadSurface(ctx, h.Owner.Projections, "s-steps")
	if err != nil {
		t.Fatal(err)
	}
	// Two compactions between steps and one after the settlement drained.
	if n := surface.Compactions.Len(); n != 3 {
		t.Fatalf("compactions = %d, want 3 (before each later step, then after settlement)", n)
	}
}
