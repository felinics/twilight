package app_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	agentinput "github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// setup composes a Host over an in-memory store with one model and one tool,
// registers the preset and opens a Session whose new Turns are named t2, t3, ...
func setup(t *testing.T, model loop.ModelInvoker, tool *gateTool, opts app.SessionOptions) (*app.Application, turn.PresetRef, session.SessionID, *app.Session) {
	t.Helper()
	tools := []loop.ExecutableTool{}
	if tool != nil {
		tools = append(tools, tool)
	}
	h := newHost(t, app.Config{}, map[run.ModelRef]loop.ModelInvoker{"m-1": model}, tools...)
	const sid session.SessionID = "s-1"
	if err := h.CreateSession(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", tools, app.WithSystemPrompt("be brief")))
	if err != nil {
		t.Fatal(err)
	}
	opts.Preset = preset
	next := 2
	if opts.NewTurnID == nil {
		opts.NewTurnID = func() turn.TurnID { id := turn.TurnID("t" + string(rune('0'+next))); next++; return id }
	}
	s, err := h.OpenSession(context.Background(), sid, opts)
	if err != nil {
		t.Fatal(err)
	}
	return h, preset, sid, s
}

// An input delivered while a tool call is Executing queues on the Run, is
// delivered to the same Turn in the same commit, and reaches the model in the
// next request together with the tool result (TRN-DLV, RUN-LOP-8).
func TestDeliverMidTurnReachesNextModelRequest(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	h, preset, sid, s := setup(t, model, tool, app.SessionOptions{})

	first, err := h.Owner.Chatlog.Submit(ctx, s.Handle().Writer(), "in-1", agentinput.Text("what is the weather?"))
	if err != nil {
		t.Fatal(err)
	}
	ref1 := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	done := make(chan turn.TurnResponse, 1)
	go func() {
		resp, err := h.Owner.Turns.Start(ctx, s.Handle().Writer(), turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{first}, Preset: preset})
		if err == nil {
			// The Coordinator only commits; the host drives (DRV-1).
			var driven driver.DriveResult
			driven, err = h.Owner.Driver.Drive(ctx, s.Handle().Writer(), ref1.TurnID)
			resp = driven.TurnResponse
		}
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	<-tool.started

	second, err := h.Owner.Chatlog.Submit(ctx, s.Handle().Writer(), "in-2", agentinput.Text("and tomorrow?"))
	if err != nil {
		t.Fatal(err)
	}
	// Deliver commits AcceptInput + input_delivered without waiting for the
	// tool; the Run is already driven here, so the response reports
	// already_driving (or finished when the running driver settles first).
	deliverDone := make(chan driver.DriveResult, 1)
	go func() {
		resp, err := s.Route(ctx, []run.AgentInput{second})
		if err != nil {
			t.Error(err)
		}
		deliverDone <- resp
	}()
	// The Deliver commit lands while the tool runs; the Loop sees PendingInputs
	// at its next Load. Release the tool and let both drivers finish.
	waitFor(t, func() bool {
		surface, err := h.TurnSurface(ctx, sid)
		return err == nil && len(surface.Turns["t1"].InputIDs) == 2
	})
	close(tool.release)
	if resp := <-deliverDone; !resp.AlreadyDriving && resp.Disposition != turn.ResumeFinished {
		t.Fatalf("deliver = %+v, want already driving or finished", resp)
	}
	resp := <-done
	if resp.Status != turn.TurnCompleted {
		t.Fatalf("turn status = %s, want completed", resp.Status)
	}
	seen := model.requests()
	if len(seen) != 2 {
		t.Fatalf("model requests = %d, want 2", len(seen))
	}
	last := seen[1].Messages
	var users []string
	for _, msg := range last {
		if msg.Role == sdk.MessageRoleUser {
			users = append(users, msg.Content[0].(sdk.TextPart).Text)
		}
	}
	if len(users) != 2 || users[1] != "and tomorrow?" {
		t.Fatalf("second request user messages = %v", users)
	}
	if last[len(last)-2].Role != sdk.MessageRoleTool {
		t.Fatalf("tool result did not precede the delivered input: %+v", roles(last))
	}
	chat, err := h.ChatlogSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := chat.Inputs.Get("in-2"); got.Status != chatlog.InputDelivered || got.Input.TurnID != "t1" {
		t.Fatalf("in-2 = %+v, want delivered to t1", got)
	}
}

// Stop settles the Turn as stopped in the same commit as CancelRun; a later
// Route opens a new Turn whose prompt builder sees the stopped Turn's content.
func TestStopSettlesTurnAndNextSendStartsNewTurn(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	h, preset, sid, s := setup(t, model, tool, app.SessionOptions{})
	first, _ := h.Owner.Chatlog.Submit(ctx, s.Handle().Writer(), "in-1", agentinput.Text("hello"))
	ref1 := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := h.Owner.Turns.Start(ctx, s.Handle().Writer(), turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{first}, Preset: preset}); err == nil {
			_, _ = h.Owner.Driver.Drive(ctx, s.Handle().Writer(), ref1.TurnID)
		}
	}()
	<-tool.started

	resp, err := h.Owner.Turns.Stop(ctx, s.Handle().Writer(), turn.StopRequest{Ref: ref1, Reason: "user"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != turn.TurnStopped || resp.Disposition != turn.ResumeFinished || resp.End == nil {
		t.Fatalf("stop response = %+v", resp)
	}
	if _, stopped := resp.End.(run.RunStoppedEnd); !stopped {
		t.Fatalf("end = %#v, want RunStoppedEnd", resp.End)
	}
	close(tool.release)
	<-done

	// The abandoned worker's settlement was rejected; the Run is terminal.
	record, err := h.Owner.Runs.Record(ctx, sid, resp.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Snapshot.State.Status != run.RunStopped || len(record.Snapshot.State.Result.UncertainCalls) != 1 {
		t.Fatalf("stopped run = %+v", record.Snapshot.State.Result)
	}

	second, _ := h.Owner.Chatlog.Submit(ctx, s.Handle().Writer(), "in-2", agentinput.Text("again"))
	resp2, err := s.Route(ctx, []run.AgentInput{second})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Ref.TurnID != "t2" || resp2.Status != turn.TurnCompleted {
		t.Fatalf("second send = %+v", resp2)
	}
	// The new Turn's request carried the stopped Turn's assistant tool call and
	// its unknown tool_result (DEC-PMT-6), then the new input.
	seen := model.requests()
	last := seen[len(seen)-1].Messages
	if got := roles(last); len(got) != 5 || got[0] != "system" || got[1] != "user" || got[2] != "assistant" || got[3] != "tool" || got[4] != "user" {
		t.Fatalf("roles = %v", got)
	}
}

type approvalGateTool struct{ *gateTool }

func (*approvalGateTool) Ref() run.ToolRef { return "approve" }
func (*approvalGateTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "approve", Parameters: &jsonschema.Schema{Type: "object"}}
}
func (*approvalGateTool) ResponsePolicy() run.ResponsePolicy { return run.ApprovalRequired }
func (*approvalGateTool) Replay() run.ReplayPolicy           { return run.ReplayUnknown }
func (*approvalGateTool) Placement() run.ToolPlacement       { return run.PlacementProcess }

func TestStopCompletesToolHistoryForNextTurn(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	approval := &approvalGateTool{gateTool: tool}
	model := &scriptedRequests{answers: []sdk.ModelResult{{FinishReason: sdk.FinishReasonToolCalls,
		ToolCalls: []sdk.ToolCall{
			{ToolCallID: "c1", ToolName: "lookup", Input: sdk.ParseToolArguments(`{}`)},
			{ToolCallID: "c2", ToolName: "lookup", Input: sdk.ParseToolArguments(`{}`)},
			{ToolCallID: "c3", ToolName: "approve", Input: sdk.ParseToolArguments(`{}`)},
		}}}}
	h := newHost(t, app.Config{}, map[run.ModelRef]loop.ModelInvoker{"m-1": model}, tool, approval)
	preset, err := h.RegisterPreset("sequential", mustPreset("m-1", []loop.ExecutableTool{tool, approval},
		app.WithScheduling(run.ToolScheduling{Mode: run.ToolScheduleSequential})))
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "s-stop-mixed"
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset, NewTurnID: func() turn.TurnID { return "t2" }})
	if err != nil {
		t.Fatal(err)
	}
	input, err := h.Owner.Chatlog.Submit(ctx, s.Handle().Writer(), "in-1", agentinput.Text("hello"))
	if err != nil {
		t.Fatal(err)
	}
	ref := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	started, err := h.Owner.Turns.Start(ctx, s.Handle().Writer(), turn.StartRequest{Ref: ref, Inputs: []run.AgentInput{input}, Preset: preset})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = h.Owner.Driver.Drive(ctx, s.Handle().Writer(), ref.TurnID)
	}()
	t.Cleanup(func() { close(tool.release); <-done })
	<-tool.started
	snapshot, err := runState(h, sid, started.RunID)
	if err != nil {
		t.Fatal(err)
	}
	calls := snapshot.State.Current.(run.ToolStep).Calls
	if calls[0].Status != run.ToolExecuting || calls[1].Status != run.ToolPending || calls[2].Status != run.ToolWaiting {
		t.Fatalf("calls before stop = %+v", calls)
	}
	if _, err := h.Owner.Turns.Stop(ctx, s.Handle().Writer(), turn.StopRequest{Ref: ref}); err != nil {
		t.Fatal(err)
	}
	record, err := h.Owner.Runs.Record(ctx, sid, started.RunID)
	if err != nil {
		t.Fatal(err)
	}
	result := record.Snapshot.State.Result
	if len(result.UncertainCalls) != 1 || result.UncertainCalls[0] != calls[0].CallID {
		t.Fatalf("uncertain calls = %v", result.UncertainCalls)
	}
	input, err = h.Owner.Chatlog.Submit(ctx, s.Handle().Writer(), "in-2", agentinput.Text("continue"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Route(ctx, []run.AgentInput{input}); err != nil {
		t.Fatal(err)
	}
	requests := model.requests()
	if len(requests) != 2 {
		t.Fatalf("model requests = %d, want 2", len(requests))
	}
	toolResults := make(map[string]sdk.ToolResultPart)
	for _, msg := range requests[1].Messages {
		for _, part := range msg.Content {
			if result, ok := part.(sdk.ToolResultPart); ok {
				toolResults[result.ToolCallID] = result
			}
		}
	}
	if len(toolResults) != 3 {
		t.Fatalf("tool results = %+v, want one for every original call", toolResults)
	}
	for _, id := range []string{"c1", "c2", "c3"} {
		result, ok := toolResults[id]
		class := run.FailureCancelled
		if id == "c1" {
			class = run.FailureEffectUnknown
		}
		if !ok || !result.IsError || !strings.Contains(fmt.Sprint(result.Result), class) {
			t.Fatalf("tool result %s = %+v, want %s error", id, result, class)
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
