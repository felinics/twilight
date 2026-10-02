package app_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/component/localagent"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// Example_recoverableTurn drives one Turn through a process crash on a
// single Session.
//
// Process 1 owns the Session (Epoch 1), submits the user input and starts the
// Turn. The model asks for a tool; the tool never returns and the process dies
// while the call is Executing. Nothing is written on the way down.
//
// Process 2 reopens the same Session store with Takeover — the crashed owner
// never closed — and takes the Session over (Epoch 2). Its Executor is fresh,
// so no attempt reattaches: the takeover disposition settles the abandoned
// call as Unknown in the same group as its chatlog tool_result, the Run stays
// Active, and Drive runs the Loop: the prompt builder reads the conversation back
// from the chatlog projection and the Turn completes. The dead process's
// worker finally returns and its settlement is fenced by the kernel: nothing
// of Epoch 1 reaches the ledger after the takeover.
func Example_recoverableTurn() {
	ctx := context.Background()
	const sid session.SessionID = "session-1"
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}

	// Shared durable state under one root: the Session ledger and the content
	// store of frozen request bodies; each process opens its own instances.
	root, err := os.MkdirTemp("", "twilight-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)
	tool := &lookupTool{block: make(chan struct{})}
	preset := mustPreset("m-1", []local.ExecutableTool{tool})

	// ---- process 1 ----------------------------------------------------------
	cfg1 := exampleStores(root, "process-1")
	cfg1.Sessions.Clock = clock.Now
	p1 := buildHost(cfg1, map[run.ModelRef]local.ModelInvoker{"m-1": &scriptedModel{}}, tool)
	if err := p1.CreateSession(ctx, sid); err != nil {
		panic(err)
	}
	profile1, err := p1.RegisterPreset("weather-agent", preset)
	if err != nil {
		panic(err)
	}
	s1, err := p1.OpenSession(ctx, sid, app.SessionOptions{Preset: profile1, NewTurnID: func() turn.TurnID { return "turn-1" }})
	if err != nil {
		panic(err)
	}
	ref1, err := s1.SubmitInput(ctx, "in-1", "what is the weather?")
	if err != nil {
		panic(err)
	}
	startDone := make(chan error, 1)
	go func() {
		// SubmitInput committed the route; Settle drives the Turn to its end.
		_, err := s1.Settle(ctx, ref1.TurnID)
		startDone <- err
	}()
	runID := waitForExecutingCall(ctx, p1, sid, ref1.TurnID)
	fmt.Println("process 1: tool call is Executing; process crashes")

	// ---- process 2 ----------------------------------------------------------
	cfg2 := exampleStores(root, "process-2")
	cfg2.Sessions.Ownership, cfg2.Sessions.Clock = session.OpenOptions{Takeover: true}, clock.Now
	p2 := buildHost(cfg2, map[run.ModelRef]local.ModelInvoker{"m-1": &scriptedModel{}}, tool)
	// The preset is re-registered from the same public configuration, so the
	// ref the Session recorded still resolves.
	if _, err := p2.RegisterPreset("weather-agent", preset); err != nil {
		panic(err)
	}
	owned, err := p2.Acquire(ctx, sid)
	if err != nil {
		panic(err)
	}
	chat, err := p2.ChatlogSurface(ctx, sid)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: took over; %d executing target disposed; chatlog has %d tool_result(s) with status %s\n", owned.Recovered, chat.ToolResults.Len(), toolResultStatus(&chat))

	if _, err := owned.Drive(ctx, ref1.TurnID); err != nil {
		panic(err)
	}
	resp, err := p2.TurnStatus(ctx, ref1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: turn %s, disposition %s\n", resp.Status, resp.Disposition)

	record, err := p2.RunRecord(ctx, sid, runID)
	if err != nil {
		panic(err)
	}
	chat, _ = p2.ChatlogSurface(ctx, sid)
	fmt.Printf("record: %d run facts fold to the projection; chatlog entries: %d\n", len(record.Facts), len(chat.EntryOrder))

	// Let the abandoned worker exit; its settlement is fenced because process
	// 1's Epoch was superseded.
	close(tool.block)
	err = <-startDone
	fmt.Printf("process 1: %v\n", errorsIsOwnershipLost(err))
	after, _ := p2.RunRecord(ctx, sid, runID)
	fmt.Printf("stream unchanged by the fenced worker: %v\n", len(after.Facts) == len(record.Facts))

	// Output:
	// process 1: tool call is Executing; process crashes
	// process 2: took over; 1 executing target disposed; chatlog has 1 tool_result(s) with status unknown
	// process 2: turn completed, disposition finished
	// record: 12 run facts fold to the projection; chatlog entries: 4
	// process 1: ownership lost
	// stream unchanged by the fenced worker: true
}

func toolResultStatus(s *chatlog.Surface) string {
	status := "none"
	s.ToolResults.Range(func(_ chatlog.ToolResultID, r chatlog.ToolResult) bool {
		status = string(r.Status)
		return false
	})
	return status
}

func waitForExecutingCall(ctx context.Context, h *localagent.Agent, sid session.SessionID, turnID turn.TurnID) run.RunID {
	deadline := time.Now().Add(10 * time.Second)
	for {
		surface, err := h.TurnSurface(ctx, sid)
		if err == nil {
			if v, ok := surface.Turns[turnID]; ok && v.RunID != "" {
				snap, err := runState(h, sid, v.RunID)
				if err == nil && len(run.ExecutingCalls(snap.State)) == 1 {
					return v.RunID
				}
			}
		}
		if time.Now().After(deadline) {
			panic("tool call never started")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// scriptedModel asks for the tool until a tool result is in the conversation,
// then answers.
type scriptedModel struct{}

func (scriptedModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	if n := len(req.Messages); n > 0 && req.Messages[n-1].Role == sdk.MessageRoleTool {
		return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	return sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 1},
		ToolCalls:    []sdk.ToolCall{{ToolCallID: "c1", ToolName: "lookup", Input: sdk.ParseToolArguments(`{"q":"weather"}`)}},
	}, nil
}

// lookupTool blocks on its first execution until block is closed.
type lookupTool struct {
	block chan struct{}
	ran   atomic.Bool
}

func (t *lookupTool) Ref() run.ToolRef { return "lookup" }
func (t *lookupTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{"q": {Type: "string"}}}}
}
func (t *lookupTool) ResponsePolicy() run.ResponsePolicy       { return run.DirectExecution }
func (t *lookupTool) Replay() run.ReplayPolicy                 { return run.ReplayUnknown }
func (t *lookupTool) Placement() run.ToolPlacement             { return run.PlacementProcess }
func (t *lookupTool) ValidateArguments(jsonstable.Value) error { return nil }
func (t *lookupTool) Execute(_ context.Context, req local.ToolExecutionRequest) effect.ToolExecutionOutcome {
	if t.ran.CompareAndSwap(false, true) {
		<-t.block
		return effect.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
	}
	return effect.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}
