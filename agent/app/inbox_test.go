package app_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// resolveFails is an inbox whose Resolve fails while armed: the owner
// "crashes" after committing a command's effect and before recording it.
type resolveFails struct {
	inbox.Store
	armed bool
}

var errStoreDown = errors.New("inbox store unavailable")

func (f *resolveFails) Resolve(ctx context.Context, sid session.SessionID, seq uint64, r inbox.Result) error {
	if f.armed {
		return errStoreDown
	}
	return f.Store.Resolve(ctx, sid, seq, r)
}

// inboxHost builds an application with an inbox; the Session is created but
// not opened, so commands can be left for its next owner.
func inboxHost(t *testing.T, store inbox.Store, model loop.ModelInvoker, tools ...loop.ExecutableTool) (*app.Application, turn.PresetRef, session.SessionID) {
	t.Helper()
	h := newHost(t, app.Config{Inbox: store}, map[run.ModelRef]loop.ModelInvoker{"m-1": model}, tools...)
	const sid session.SessionID = "s-inbox"
	if err := h.CreateSession(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", tools, app.WithSystemPrompt("be brief")))
	if err != nil {
		t.Fatal(err)
	}
	return h, preset, sid
}

func enqueue(t *testing.T, h *app.Application, sid session.SessionID, id string, kind inbox.Kind, payload any) inbox.Entry {
	t.Helper()
	c, err := app.NewCommand(inbox.CommandID(id), kind, payload)
	if err != nil {
		t.Fatal(err)
	}
	e, err := h.Enqueue(context.Background(), sid, c)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func await(t *testing.T, h *app.Application, sid session.SessionID, id string) inbox.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := h.AwaitCommand(ctx, sid, inbox.CommandID(id))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A submit left in the inbox before anyone owns the Session is applied by
// the owner that opens it; the same command enqueued again answers the
// stored entry and starts nothing (APP-INB-1, APP-INB-3).
func TestInboxSubmitAppliedOnOpen(t *testing.T) {
	ctx := context.Background()
	store := sqlitetest.Open(t).Inbox()
	h, preset, sid := inboxHost(t, store, &scriptedRequests{})
	if sids, err := h.PendingSessions(ctx, 0); err != nil || len(sids) != 0 {
		t.Fatalf("pending sessions before enqueue = %v %v", sids, err)
	}
	enqueue(t, h, sid, "c1", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "hello"})
	if sids, err := h.PendingSessions(ctx, 0); err != nil || len(sids) != 1 || sids[0] != sid {
		t.Fatalf("pending sessions = %v %v, want the session with the command", sids, err)
	}
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset, InboxPoll: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	if r := await(t, h, sid, "c1"); r.Status != inbox.StatusApplied {
		t.Fatalf("submit result = %+v", r)
	}
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	surface, err := h.TurnSurface(ctx, sid)
	if err != nil || len(surface.Order) != 1 || surface.Turns[surface.Order[0]].Status != turn.TurnCompleted {
		t.Fatalf("turns after the inbox submit = %+v %v", surface, err)
	}
	chat, err := h.ChatlogSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if in, ok := chat.Inputs.Get("in-1"); !ok || in.Status != chatlog.InputDelivered {
		t.Fatalf("in-1 = %+v ok:%v, want delivered", in, ok)
	}
	// The retried command is the stored one.
	if e := enqueue(t, h, sid, "c1", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "hello"}); e.Pending() || e.Seq != 0 {
		t.Fatalf("replayed enqueue = %+v, want the resolved entry", e)
	}
	if sids, err := h.PendingSessions(ctx, 0); err != nil || len(sids) != 0 {
		t.Fatalf("pending sessions after apply = %v %v", sids, err)
	}
}

// Commands enqueued while this process holds the Session are applied on
// the wake-up, without waiting for the poll: a stop of the active Turn
// settles it; a stop with no active Turn, a stop naming another Turn and
// an unknown kind resolve rejected (APP-INB-2).
func TestInboxStopAndRejections(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	h, preset, sid := inboxHost(t, sqlitetest.Open(t).Inbox(), &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}, tool)
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset, InboxPoll: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	enqueue(t, h, sid, "idle-stop", app.CommandStop, app.StopCommand{Reason: "nothing runs"})
	if r := await(t, h, sid, "idle-stop"); r.Status != inbox.StatusRejected || !strings.Contains(r.Reason, "no active turn") {
		t.Fatalf("stop without an active turn = %+v", r)
	}
	enqueue(t, h, sid, "bad", inbox.Kind("dance"), nil)
	if r := await(t, h, sid, "bad"); r.Status != inbox.StatusRejected || !strings.Contains(r.Reason, "unknown command kind") {
		t.Fatalf("unknown kind = %+v", r)
	}
	enqueue(t, h, sid, "c1", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "look it up"})
	if r := await(t, h, sid, "c1"); r.Status != inbox.StatusApplied {
		t.Fatalf("submit = %+v", r)
	}
	<-tool.started
	status, err := s.Status(ctx)
	if err != nil || status.Active == "" {
		t.Fatalf("status mid-tool = %+v %v", status, err)
	}
	enqueue(t, h, sid, "stale", app.CommandStop, app.StopCommand{TurnID: "t-other", Reason: "stale client"})
	if r := await(t, h, sid, "stale"); r.Status != inbox.StatusRejected || !strings.Contains(r.Reason, "not the active turn") {
		t.Fatalf("stop naming another turn = %+v", r)
	}
	enqueue(t, h, sid, "stop", app.CommandStop, app.StopCommand{TurnID: status.Active, Reason: "user"})
	if r := await(t, h, sid, "stop"); r.Status != inbox.StatusApplied {
		t.Fatalf("stop = %+v", r)
	}
	close(tool.release)
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := h.Owner.Turns.Status(ctx, turn.TurnRef{SessionID: sid, TurnID: status.Active})
	if err != nil || resp.Status != turn.TurnStopped {
		t.Fatalf("stopped turn = %+v %v", resp, err)
	}
}

// The owner commits a command's effect and fails before resolving it: the
// command stays pending, the next pass finds the effect already committed
// and resolves it applied, and the Session saw the input once (APP-INB-2).
func TestInboxReplayAfterCrashBeforeResolve(t *testing.T) {
	ctx := context.Background()
	store := &resolveFails{Store: sqlitetest.Open(t).Inbox(), armed: true}
	h, preset, sid := inboxHost(t, store, &scriptedRequests{})
	enqueue(t, h, sid, "c1", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "hello"})
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset, InboxPoll: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.Pending(ctx, sid); err != nil || len(pending) != 1 {
		t.Fatalf("pending after the failed resolve = %+v %v, want the command still pending", pending, err)
	}
	store.armed = false
	if n, err := s.ApplyPending(ctx); err != nil || n != 1 {
		t.Fatalf("second pass = %d %v", n, err)
	}
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if r := await(t, h, sid, "c1"); r.Status != inbox.StatusApplied {
		t.Fatalf("result after replay = %+v", r)
	}
	surface, err := h.TurnSurface(ctx, sid)
	if err != nil || len(surface.Order) != 1 {
		t.Fatalf("turns after replay = %d %v, want one", len(surface.Order), err)
	}
	chat, err := h.ChatlogSurface(ctx, sid)
	if err != nil || chat.Inputs.Len() != 1 {
		t.Fatalf("inputs after replay = %+v %v, want one", chat, err)
	}
}

// Without Config.Inbox the inbox surface is unavailable, not silently a
// no-op.
func TestInboxRequiresStore(t *testing.T) {
	ctx := context.Background()
	h, _, sid := inboxHost(t, nil, &scriptedRequests{})
	if _, err := h.Enqueue(ctx, sid, inbox.Command{ID: "c1", Kind: app.CommandStop}); !errors.Is(err, app.ErrNoInbox) {
		t.Fatalf("enqueue without an inbox = %v", err)
	}
}
