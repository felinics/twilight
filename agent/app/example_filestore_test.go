package app_test

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
	"os"
	"strings"
	"sync"
	"time"
)

// Example_jsonlPrototype is the full prototype on the JSONL file store: one
// Session directory on disk carries the whole agent.
//
// Turn 1 shows steer and queue: while its tool call executes, a second
// Submit goes to Deliver (the input joins the running Turn) and a third
// input is only submitted (it queues). When the Turn settles, the Session's
// background advance starts Turn 2 from the queued input at once.
//
// Turn 2 shows resume: the process "crashes" while its tool call executes.
// A second Store instance over the same directory — a new process — opens
// with Takeover, disposes the abandoned call and Drive completes the Turn.
// The dead process's late settlement is fenced by owner.json. The log stays
// one JSONL file, readable with standard tools.
func Example_jsonlPrototype() {
	ctx := context.Background()
	const sid session.SessionID = "session-jsonl"
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	root, err := os.MkdirTemp("", "twilight-jsonl-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)

	tool := &stagedTool{}
	preset := mustPreset("m-1", []local.ExecutableTool{tool})

	// ---- process 1 ----------------------------------------------------------
	model1 := &scriptedRequests{answers: []sdk.ModelResult{protoToolCall("call-1"), protoText("done"), protoToolCall("call-2")}}
	cfg1 := exampleStores(root, "process-1")
	cfg1.Sessions.Clock = clock.Now
	p1 := buildHost(cfg1, map[run.ModelRef]local.ModelInvoker{"m-1": model1}, tool)
	profile1, err := p1.RegisterPreset("jsonl-agent", preset)
	if err != nil {
		panic(err)
	}
	turnSeq := 0
	s1, err := p1.OpenSession(ctx, sid, app.SessionOptions{Preset: profile1,
		NewTurnID: func() turn.TurnID { turnSeq++; return turn.TurnID(fmt.Sprintf("turn-%d", turnSeq)) }})
	if err != nil {
		panic(err)
	}

	// Turn 1: Submit starts the Turn and advances it in the background; the
	// model asks for the tool, which blocks.
	stage1 := tool.stage()
	if _, err := s1.SubmitInput(ctx, "in-1", "what is the weather?"); err != nil {
		panic(err)
	}
	<-stage1.started

	// Steer: a second Submit while turn-1 runs goes to Deliver (APP-RTE-1):
	// the input joins the running Turn, whose driver carries it on.
	if _, err := s1.SubmitInput(ctx, "in-2", "and tomorrow?"); err != nil {
		panic(err)
	}
	waitUntil(func() bool {
		surface, err := p1.TurnSurface(ctx, sid)
		return err == nil && len(surface.Turns["turn-1"].InputIDs) == 2
	})
	chat, err := p1.ChatlogSurface(ctx, sid)
	if err != nil {
		panic(err)
	}
	steered, _ := chat.Inputs.Get("in-2")
	fmt.Printf("steer: in-2 %s to turn-1 while its tool call executes\n", steered.Status)

	// Queue: in-3 is only submitted; nothing delivers it into the running Turn.
	if _, err := s1.Queue(ctx, "in-3", "book a table"); err != nil {
		panic(err)
	}
	chat, _ = p1.ChatlogSurface(ctx, sid)
	fmt.Printf("queue: %d input pending while turn-1 runs\n", len(chat.SubmittedInputs()))

	// Stage turn-2's tool before the release: when turn-1 settles, the
	// background advance starts turn-2 from the queued input at once and its
	// tool call blocks here. The process dies while the call is Executing.
	stage2 := tool.stage()
	close(stage1.release)
	waitUntil(func() bool {
		surface, err := p1.TurnSurface(ctx, sid)
		if err != nil {
			return false
		}
		t1, ok1 := surface.Turns["turn-1"]
		t2, ok2 := surface.Turns["turn-2"]
		return ok1 && t1.Status == turn.TurnCompleted && ok2 && t2.Status == turn.TurnActive
	})
	<-stage2.started
	fmt.Println("turn-1: completed")
	fmt.Println("turn-2: started from the queued input; tool call is Executing; process 1 crashes")
	subs := s1.Events(ctx)

	// ---- process 2: new store instances over the same directory --------------
	cfg2 := exampleStores(root, "process-2")
	cfg2.Sessions.Ownership, cfg2.Sessions.Clock = session.OpenOptions{Takeover: true}, clock.Now
	store2, ok := cfg2.Sessions.Store.(*filestore.Store)
	if !ok {
		panic("example stores are file-backed")
	}
	p2 := buildHost(cfg2, map[run.ModelRef]local.ModelInvoker{"m-1": &scriptedRequests{}}, tool)
	if _, err := p2.RegisterPreset("jsonl-agent", preset); err != nil {
		panic(err)
	}
	owned, err := p2.Acquire(ctx, sid)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: took over; %d executing target disposed\n", owned.Recovered)

	if _, err := owned.Drive(ctx, "turn-2"); err != nil {
		panic(err)
	}
	resp2, err := p2.TurnStatus(ctx, turn.TurnRef{SessionID: sid, TurnID: "turn-2"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("turn-2: %s, disposition %s\n", resp2.Status, resp2.Disposition)

	// The dead process's worker returns; owner.json fences its settlement.
	// The fenced drive is the Session's background advance; its error
	// reaches the subscribers as a failure Event.
	close(stage2.release)
	var fenced error
	for ev := range subs {
		if ev.Err != nil {
			fenced = ev.Err
			break
		}
	}
	fmt.Printf("process 1: %v\n", errorsIsOwnershipLost(fenced))

	// The whole Session is one JSONL file: one commit per line, digest-chained.
	page, err := store2.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil {
		panic(err)
	}
	var events []ledger.Event
	for _, c := range page.Commits {
		for _, b := range c.Batches {
			events = append(events, b.Events...)
		}
	}
	raw, err := os.ReadFile(store2.LogPath(sid))
	if err != nil {
		panic(err)
	}
	fmt.Printf("log.jsonl: %d lines, chain verified over %d events\n", strings.Count(string(raw), "\n"), len(events))
	fmt.Printf("first event: %s; last event: %s\n", events[0].Type, events[len(events)-1].Type)

	// Output:
	// steer: in-2 delivered to turn-1 while its tool call executes
	// queue: 1 input pending while turn-1 runs
	// turn-1: completed
	// turn-2: started from the queued input; tool call is Executing; process 1 crashes
	// process 2: took over; 1 executing target disposed
	// turn-2: completed, disposition finished
	// process 1: ownership lost
	// log.jsonl: 22 lines, chain verified over 33 events
	// first event: twilight/chatlog/input_submitted; last event: twilight/run/run_ended
}

func protoToolCall(id string) sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: id, ToolName: "lookup", Input: sdk.ParseToolArguments(`{"q":"weather"}`)}}}
}

func protoText(text string) sdk.ModelResult {
	return sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
}

// stagedTool blocks each staged execution until its stage is released;
// executions beyond the staged ones run straight through.
type stagedTool struct {
	mu     sync.Mutex
	stages []*toolStage
}

type toolStage struct {
	started chan struct{}
	release chan struct{}
}

func (t *stagedTool) stage() *toolStage {
	st := &toolStage{started: make(chan struct{}), release: make(chan struct{})}
	t.mu.Lock()
	t.stages = append(t.stages, st)
	t.mu.Unlock()
	return st
}

func (t *stagedTool) Ref() run.ToolRef { return "lookup" }
func (t *stagedTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{"q": {Type: "string"}}}}
}
func (t *stagedTool) ResponsePolicy() run.ResponsePolicy       { return run.DirectExecution }
func (t *stagedTool) Replay() run.ReplayPolicy                 { return run.ReplayUnknown }
func (t *stagedTool) Placement() run.ToolPlacement             { return run.PlacementProcess }
func (t *stagedTool) ValidateArguments(jsonstable.Value) error { return nil }
func (t *stagedTool) Execute(_ context.Context, req local.ToolExecutionRequest) effect.ToolExecutionOutcome {
	t.mu.Lock()
	var st *toolStage
	if len(t.stages) > 0 {
		st = t.stages[0]
		t.stages = t.stages[1:]
	}
	t.mu.Unlock()
	if st != nil {
		close(st.started)
		<-st.release
	}
	return effect.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}
