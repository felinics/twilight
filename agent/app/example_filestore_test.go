package app_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/app"
	agentinput "github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// Example_jsonlPrototype is the full prototype on the JSONL file store: one
// Session directory on disk carries the whole agent.
//
// Turn 1 shows steer and queue: while its tool call executes, a second Route
// goes to Deliver (the input joins the running Turn) and a third input is
// only submitted (it queues). After the Turn settles, Drain starts Turn 2
// from the queued input.
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
	preset := mustPreset("m-1", []loop.ExecutableTool{tool})

	// ---- process 1 ----------------------------------------------------------
	model1 := &scriptedRequests{answers: []sdk.ModelResult{protoToolCall("call-1"), protoText("done"), protoToolCall("call-2")}}
	cfg1 := exampleStores(root, "process-1")
	cfg1.Clock = clock.Now
	p1 := buildHost(cfg1, map[run.ModelRef]loop.ModelInvoker{"m-1": model1}, tool)
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

	// Turn 1: Route starts the Turn; the model asks for the tool, which blocks.
	stage1 := tool.stage()
	in1, err := p1.Owner.Chatlog.Submit(ctx, s1.Handle().Writer(), "in-1", agentinput.Text("what is the weather?"))
	if err != nil {
		panic(err)
	}
	turn1Done := make(chan turn.TurnResponse, 1)
	go func() {
		resp, err := s1.Route(ctx, []run.AgentInput{in1})
		if err != nil {
			panic(err)
		}
		turn1Done <- resp.TurnResponse
	}()
	<-stage1.started

	// Steer: a second Route while turn-1 runs goes to Deliver (APP-RTE-1).
	in2, err := p1.Owner.Chatlog.Submit(ctx, s1.Handle().Writer(), "in-2", agentinput.Text("and tomorrow?"))
	if err != nil {
		panic(err)
	}
	steerDone := make(chan struct{})
	go func() {
		defer close(steerDone)
		// Deliver into the running Turn returns already_driving, not an error.
		if _, err := s1.Route(ctx, []run.AgentInput{in2}); err != nil {
			panic(err)
		}
	}()
	waitUntil(func() bool {
		surface, err := p1.TurnSurface(ctx, sid)
		return err == nil && len(surface.Turns["turn-1"].InputIDs) == 2
	})
	<-steerDone
	chat, err := p1.ChatlogSurface(ctx, sid)
	if err != nil {
		panic(err)
	}
	steered, _ := chat.Inputs.Get("in-2")
	fmt.Printf("steer: in-2 %s to turn-1 while its tool call executes\n", steered.Status)

	// Queue: in-3 is only submitted; nothing delivers it into the running Turn.
	if _, err := p1.Owner.Chatlog.Submit(ctx, s1.Handle().Writer(), "in-3", agentinput.Text("book a table")); err != nil {
		panic(err)
	}
	chat, _ = p1.ChatlogSurface(ctx, sid)
	fmt.Printf("queue: %d input pending while turn-1 runs\n", len(chat.SubmittedInputs()))

	close(stage1.release)
	resp1 := <-turn1Done
	fmt.Printf("turn-1: %s\n", resp1.Status)

	// Turn 2 opens from the backlog (APP-RTE-2); its tool call blocks and the
	// process dies while the call is Executing.
	stage2 := tool.stage()
	turn2Err := make(chan error, 1)
	go func() {
		_, _, err := s1.Drain(ctx)
		turn2Err <- err
	}()
	<-stage2.started
	fmt.Println("turn-2: started from the queued input; tool call is Executing; process 1 crashes")

	// ---- process 2: new store instances over the same directory --------------
	cfg2 := exampleStores(root, "process-2")
	cfg2.Ownership, cfg2.Clock = session.OpenOptions{Takeover: true}, clock.Now
	store2, ok := cfg2.Store.(*filestore.Store)
	if !ok {
		panic("example stores are file-backed")
	}
	p2 := buildHost(cfg2, map[run.ModelRef]loop.ModelInvoker{"m-1": &scriptedRequests{}}, tool)
	if _, err := p2.RegisterPreset("jsonl-agent", preset); err != nil {
		panic(err)
	}
	owned, err := p2.Owner.Open(ctx, sid)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: took over; %d executing target disposed\n", owned.Recovered)

	resp2, err := p2.Owner.Driver.Drive(ctx, owned.Writer(), "turn-2")
	if err != nil {
		panic(err)
	}
	fmt.Printf("turn-2: %s, disposition %s, attempt %d\n", resp2.Status, resp2.Disposition, resp2.Attempt)

	// The dead process's worker returns; owner.json fences its settlement.
	close(stage2.release)
	fmt.Printf("process 1: %v\n", errorsIsOwnershipLost(<-turn2Err))

	// The whole Session is one JSONL file: one commit per line, digest-chained.
	page, err := store2.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil {
		panic(err)
	}
	var events []session.Event
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
	// turn-2: completed, disposition finished, attempt 1
	// process 1: ownership lost
	// log.jsonl: 22 lines, chain verified over 35 events
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
func (t *stagedTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *stagedTool) Replay() run.ReplayPolicy                  { return run.ReplayUnknown }
func (t *stagedTool) Placement() run.ToolPlacement              { return run.PlacementProcess }
func (t *stagedTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *stagedTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
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
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}
