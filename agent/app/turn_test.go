package app_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/component/localagent"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/conversation"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// setup composes a Host over an in-memory store with one model and one tool,
// registers the preset and opens a Session whose new Turns are named t2, t3, ...
func setup(t *testing.T, model local.ModelInvoker, tool *gateTool, opts app.SessionOptions) (*localagent.Agent, preset.PresetRef, session.SessionID, *app.Session) {
	t.Helper()
	tools := []local.ExecutableTool{}
	if tool != nil {
		tools = append(tools, tool)
	}
	h := newHost(t, app.Config{}, map[run.ModelRef]local.ModelInvoker{"m-1": model}, tools...)
	const sid session.SessionID = "s-1"
	if err := h.CreateSession(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	pref, err := h.RegisterPreset("b1", mustPreset("m-1", tools, app.WithSystemPrompt("be brief")))
	if err != nil {
		t.Fatal(err)
	}
	opts.Preset = pref
	next := 1
	if opts.NewTurnID == nil {
		opts.NewTurnID = func() turn.TurnID { id := turn.TurnID("t" + string(rune('0'+next))); next++; return id }
	}
	s, err := h.OpenSession(context.Background(), sid, opts)
	if err != nil {
		t.Fatal(err)
	}
	return h, pref, sid, s
}

// An input delivered while a tool call is Executing queues on the Run, is
// delivered to the same Turn in the same commit, and reaches the model in the
// next request together with the tool result (TRN-DLV, RUN-LOP-8).
func TestDeliverMidTurnReachesNextModelRequest(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	h, _, sid, s := setup(t, model, tool, app.SessionOptions{})

	ref1, err := s.SubmitInput(ctx, "in-1", "what is the weather?")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan turn.TurnResult, 1)
	go func() {
		// SubmitInput committed the route and advances in the background;
		// Settle waits for the Turn to end, then its status is read.
		var resp turn.TurnResult
		_, err := s.Settle(ctx, ref1.TurnID)
		if err == nil {
			resp, err = h.TurnStatus(ctx, ref1)
		}
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	<-tool.started

	// Send commits AcceptInput + input_delivered without waiting for the
	// tool; the Run is already driven here, so the result reports
	// already_driving (or finished when the running driver settles first).
	deliverDone := make(chan app.Result, 1)
	go func() {
		results, err := s.Send(ctx, "and tomorrow?")
		if err != nil {
			t.Error(err)
		}
		deliverDone <- results[0]
	}()
	// The Deliver commit lands while the tool runs; the Loop sees PendingInputs
	// at its next Load. Release the tool and let both drivers finish.
	waitFor(t, func() bool {
		surface, err := h.TurnSurface(ctx, sid)
		return err == nil && len(surface.Turns["t1"].InputIDs) == 2
	})
	close(tool.release)
	if resp := <-deliverDone; resp.Standing == conversation.Blocked {
		t.Fatalf("deliver = %+v, want carried by the running step or finished", resp)
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
	if chat.Inputs.Len() != 2 {
		t.Fatalf("inputs = %d, want 2", chat.Inputs.Len())
	}
	chat.Inputs.Range(func(id chatlog.InputID, got chatlog.InputView) bool {
		if got.Status != chatlog.InputDelivered || got.Input.TurnID != "t1" {
			t.Fatalf("%s = %+v, want delivered to t1", id, got)
		}
		return true
	})
}

// Stop settles the Turn as stopped in the same commit as CancelRun; a later
// Route opens a new Turn whose prompt builder sees the stopped Turn's content.
func TestStopSettlesTurnAndNextSendStartsNewTurn(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	h, _, sid, s := setup(t, model, tool, app.SessionOptions{})
	ref1, err := s.SubmitInput(ctx, "in-1", "hello")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.Settle(ctx, ref1.TurnID)
	}()
	<-tool.started

	resp, ok, err := s.Stop(ctx, "user")
	if err != nil || !ok {
		t.Fatalf("stop = %+v ok=%v err=%v", resp, ok, err)
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
	record, err := h.RunRecord(ctx, sid, resp.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Snapshot.State.Status != run.RunStopped || len(record.Snapshot.State.Result.UncertainCalls) != 1 {
		t.Fatalf("stopped run = %+v", record.Snapshot.State.Result)
	}

	results, err := s.Send(ctx, "again")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].TurnID != "t2" || results[0].Status != turn.TurnCompleted {
		t.Fatalf("second send = %+v", results)
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
	h := newHost(t, app.Config{}, map[run.ModelRef]local.ModelInvoker{"m-1": model}, tool, approval)
	pref, err := h.RegisterPreset("sequential", mustPreset("m-1", []local.ExecutableTool{tool, approval},
		app.WithScheduling(run.ToolScheduling{Mode: run.ToolScheduleSequential})))
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "s-stop-mixed"
	next := 1
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: pref, NewTurnID: func() turn.TurnID { id := turn.TurnID("t" + string(rune('0'+next))); next++; return id }})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := s.SubmitInput(ctx, "in-1", "hello")
	if err != nil {
		t.Fatal(err)
	}
	started, err := h.TurnStatus(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.Settle(ctx, ref.TurnID)
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
	if _, ok, err := s.Stop(ctx, ""); err != nil || !ok {
		t.Fatalf("stop: ok=%v err=%v", ok, err)
	}
	record, err := h.RunRecord(ctx, sid, started.RunID)
	if err != nil {
		t.Fatal(err)
	}
	result := record.Snapshot.State.Result
	if len(result.UncertainCalls) != 1 || result.UncertainCalls[0] != calls[0].CallID {
		t.Fatalf("uncertain calls = %v", result.UncertainCalls)
	}
	if _, err := s.Send(ctx, "continue"); err != nil {
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
