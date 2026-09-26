package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// APP-SES-4 / OBS-1: Submit returns while the model is still blocked, and
// the Session's event stream carries the Turn's started and completed rows,
// decoded, in commit order. Send over the same Session keeps its blocking
// contract.
func TestSubmitReturnsAtOnceAndEventsReportTheTurn(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "s-events"
	gate := &gateModel{started: make(chan sdk.Request, 1), release: make(chan struct{})}
	h := newHost(t, app.Config{}, map[run.ModelRef]loop.ModelInvoker{"m-1": gate})
	presetRef, err := h.RegisterPreset("a1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: presetRef})
	if err != nil {
		t.Fatal(err)
	}
	events := s.Events(ctx)

	ref, err := s.Submit(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the background drive never reached the model")
	}
	// Submit returned before the model answered: the Turn is still active.
	status, err := s.Status(ctx)
	if err != nil || status.Active != ref.TurnID {
		t.Fatalf("status after Submit = %+v %v, want active %s", status, err, ref.TurnID)
	}
	close(gate.release)

	// The Turn completes through its Run's run_ended: attempt/started names the
	// Run of the Turn, run_ended(completed) of that Run is the settlement.
	var sawStarted, sawCompleted bool
	var runID run.RunID
	deadline := time.After(5 * time.Second)
	for !sawCompleted {
		select {
		case e := <-events:
			if e.Err != nil {
				t.Fatalf("host-level failure on the stream: %v", e.Err)
			}
			switch v := e.Value.(type) {
			case turn.StartedPayload:
				sawStarted = v.TurnID == ref.TurnID
			case attempt.StartedPayload:
				if turn.TurnID(v.TurnID) == ref.TurnID {
					runID = v.RunID
				}
			case runmod.Event:
				if ended, ok := v.Fact.(run.RunEnded); ok && v.RunID == runID {
					_, sawCompleted = ended.End.(run.RunCompletedEnd)
				}
			}
		case <-deadline:
			t.Fatalf("no completed event for %s (started seen: %v)", ref.TurnID, sawStarted)
		}
	}
	if !sawStarted {
		t.Fatal("the stream did not carry the turn's started row")
	}
	// The reply is an assistant entry projected from the model_step_completed
	// row.
	chat, err := h.ChatlogSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(chat.EntryOrder) == 0 || chat.EntryOrder[len(chat.EntryOrder)-1].Kind != chatlog.EntryAssistant {
		t.Fatalf("chatlog after settlement = %+v", chat.EntryOrder)
	}

	// The background drive may still be draining the backlog after the
	// completed row landed; a Send racing it would be absorbed as
	// already_driving (APP-SES-3). Wait for quiescence first.
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	// Send still blocks to a settled Result on the same Session.
	results, err := s.Send(ctx, "again")
	if err != nil || len(results) == 0 || results[0].Status != turn.TurnCompleted || results[0].Reply != "late" {
		t.Fatalf("Send = %+v %v", results, err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// A background drive that fails -- here the AgentPreset names a model the
// Executor cannot resolve, so Dispatch fails -- reports on the stream as a
// host-level Event and through Ports.Warn; it never enters the Session log.
func TestBackgroundDriveFailureIsReportedOnTheStream(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "s-events-fail"
	warned := make(chan error, 1)
	h := newHost(t, app.Config{Warn: func(err error) {
		select {
		case warned <- err:
		default:
		}
	}}, map[run.ModelRef]loop.ModelInvoker{"m-1": &scriptedRequests{}})
	presetRef, err := h.RegisterPreset("a1", mustPreset("m-missing", nil))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: presetRef})
	if err != nil {
		t.Fatal(err)
	}
	events := s.Events(ctx)
	if _, err := s.Submit(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case e := <-events:
			if e.Err != nil && e.Row.Type == "" {
				select {
				case <-warned:
				case <-time.After(time.Second):
					t.Fatal("Ports.Warn did not receive the failure")
				}
				if err := s.Close(ctx); err != nil {
					t.Fatal(err)
				}
				return
			}
		case <-deadline:
			t.Fatal("no host-level failure event")
		}
	}
}
