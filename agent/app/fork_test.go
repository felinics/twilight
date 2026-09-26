package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// OWN-FRK-1/2: forking before a Turn yields a child whose conversation ends
// where the Turn's inputs were still undelivered. Resuming the child
// regenerates the Turn from the same input; withdrawing the input and sending
// another edits it. The parent is unchanged either way, and both children
// read the frozen bodies of the shared prefix.
func TestForkBeforeTurnRegeneratesAndEdits(t *testing.T) {
	ctx := context.Background()
	model := &scriptedRequests{answers: []sdk.ModelResult{
		{Text: "first answer", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}},
		{Text: "second answer", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}},
		{Text: "regenerated", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}},
		{Text: "edited answer", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}},
	}}
	store, content := filestoretest.Store(t), durableContent(t)
	h := newHost(t, app.Config{Store: store, Content: content, Ownership: session.OpenOptions{Takeover: true}}, map[run.ModelRef]loop.ModelInvoker{"m-1": model})
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	names := func(prefix string) func() turn.TurnID {
		n := 0
		return func() turn.TurnID { n++; return turn.TurnID(prefix + string(rune('0'+n))) }
	}
	open := func(sid session.SessionID, prefix string) *app.Session {
		s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset, NewTurnID: names(prefix)})
		if err != nil {
			t.Fatalf("open %s: %v", sid, err)
		}
		return s
	}
	parent := open("parent", "p")
	for _, text := range []string{"hello", "how are you"} {
		if results, err := parent.Send(ctx, text); err != nil || len(results) != 1 || results[0].Status != turn.TurnCompleted {
			t.Fatalf("send %q = %+v %v", text, results, err)
		}
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
	parentHead, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "parent"})

	// Regenerate: fork before the second Turn, open the child, resume it.
	header, err := h.ForkBeforeTurn(ctx, "parent", "p2", "regen")
	if err != nil {
		t.Fatal(err)
	}
	parentHeader, _ := h.Owner.Store.Header(ctx, "parent")
	if header.Parent == nil || header.Parent.Segment != parentHeader.ID {
		t.Fatalf("fork header = %+v, want an edge to the parent's segment", header)
	}
	regen := open("regen", "r")
	chat, err := h.ChatlogSurface(ctx, "regen")
	if err != nil {
		t.Fatal(err)
	}
	if pending := chat.SubmittedInputs(); len(pending) != 1 || chat.Assistants.Len() != 1 {
		t.Fatalf("child before resume: pending=%d assistants=%d, want the second input undelivered and one reply", len(pending), chat.Assistants.Len())
	}
	// No Turn is active at the fork point; the undelivered input is a backlog
	// Drain starts a new Turn from.
	resp, ok, err := regen.Drain(ctx)
	if err != nil || !ok || resp.Status != turn.TurnCompleted {
		t.Fatalf("regen drain = %+v ok=%v %v", resp, ok, err)
	}
	if reply := lastReply(t, h, "regen"); reply != "regenerated" {
		t.Fatalf("regenerated reply = %q", reply)
	}
	reqs := model.requests()
	if last := messageTexts(reqs[len(reqs)-1]); len(last) != 3 || last[0] != "user: hello" || last[1] != "assistant: first answer" || last[2] != "user: how are you" {
		t.Fatalf("regenerated request = %v", last)
	}
	if err := regen.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Edit: fork the same point, withdraw the original input, send another.
	if _, err := h.ForkBeforeTurn(ctx, "parent", "p2", "edit"); err != nil {
		t.Fatal(err)
	}
	edit := open("edit", "e")
	chat, _ = h.ChatlogSurface(ctx, "edit")
	pending := chat.SubmittedInputs()
	if err := h.Owner.Chatlog.Withdraw(ctx, edit.Handle().Writer(), run.InputID(pending[0].ID), "edited"); err != nil {
		t.Fatal(err)
	}
	if err := h.Owner.Chatlog.Withdraw(ctx, edit.Handle().Writer(), run.InputID(pending[0].ID), "edited"); err == nil {
		t.Fatal("withdrawing a withdrawn input succeeded")
	}
	results, err := edit.Send(ctx, "how is the weather")
	if err != nil || len(results) != 1 || results[0].Reply != "edited answer" {
		t.Fatalf("edit send = %+v %v", results, err)
	}
	reqs = model.requests()
	if last := messageTexts(reqs[len(reqs)-1]); len(last) != 3 || last[2] != "user: how is the weather" {
		t.Fatalf("edited request = %v", last)
	}
	chat, _ = h.ChatlogSurface(ctx, "edit")
	if v, _ := chat.Inputs.Get(pending[0].ID); v.Status != chatlog.InputWithdrawn {
		t.Fatalf("withdrawn input status = %s", v.Status)
	}
	if err := edit.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// The parent is untouched; each child holds only its own commits after
	// the shared prefix.
	after, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "parent"})
	if after.Head != parentHead.Head {
		t.Fatalf("parent head moved: %+v -> %+v", parentHead.Head, after.Head)
	}
	for _, sid := range []session.SessionID{"regen", "edit"} {
		page, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if len(page.Commits) <= int(header.Parent.Seq)+1 || page.Commits[header.Parent.Seq+1].Seq != header.Parent.Seq+1 {
			t.Fatalf("%s does not continue from the anchor", sid)
		}
	}
	// The children inherited the conversation, not the parent's execution:
	// the parent's Runs are unknown to a child (SES-FRK-5), while the
	// parent's own turn surface still settles them.
	parentTurns, err := turn.ReadSurface(ctx, h.Owner.Projections, "parent")
	if err != nil || len(parentTurns.Turns["p1"].Attempts) != 1 {
		t.Fatalf("parent turns = %+v %v", parentTurns.Turns, err)
	}
	p1Run := parentTurns.Turns["p1"].Attempts[0].RunID
	if _, err := h.Owner.Runs.Record(ctx, "parent", p1Run); err != nil {
		t.Fatalf("parent record of its own run: %v", err)
	}
	if _, err := h.Owner.Runs.Record(ctx, "regen", p1Run); !errors.Is(err, runtime.ErrRunNotFound) {
		t.Fatalf("child record of the parent's run = %v, want ErrRunNotFound", err)
	}
	childTurns, err := turn.ReadSurface(ctx, h.Owner.Projections, "regen")
	if err != nil || childTurns.Turns["p1"].Status != turn.TurnCompleted {
		t.Fatalf("child view of the inherited turn = %+v %v, want completed", childTurns.Turns["p1"], err)
	}
	// A Turn started by the first commit has no prefix; an unknown Turn is a
	// conflict; a Session cannot fork itself.
	if _, err := h.ForkBeforeTurn(ctx, "parent", "p9", "x"); err == nil {
		t.Fatal("fork before an unknown turn succeeded")
	}
	if _, err := h.Fork(ctx, app.ForkRequest{Parent: "parent", At: 0, Child: "parent"}); err == nil {
		t.Fatal("self fork succeeded")
	}
}

// lastReply materializes the text of the last assistant entry of a Session's
// context through the Host's content store.
func lastReply(t *testing.T, h *app.Application, sid session.SessionID) string {
	t.Helper()
	state, _, err := h.Projection(context.Background(), sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	entries := state.(chatlog.Context).Entries
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Kind == chatlog.EntryAssistant {
			m, err := chatlog.NewMaterializer(h.Content()).Entry(context.Background(), &entries[i])
			if err != nil {
				t.Fatal(err)
			}
			return m.Text()
		}
	}
	return ""
}

// OWN-FRK-1: a fork point inside an active Turn is refused -- the child
// would inherit a Turn whose execution belongs to the parent -- and the same
// parent forks once the Turn has settled.
func TestForkInsideActiveTurnIsRefused(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "s-fork-active"
	gate := &gateModel{started: make(chan sdk.Request, 1), release: make(chan struct{})}
	store := filestoretest.Store(t)
	h := newHost(t, app.Config{Store: store}, map[run.ModelRef]loop.ModelInvoker{"m-1": gate})
	presetRef, err := h.RegisterPreset("a1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: presetRef})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the background drive never reached the model")
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil || len(page.Commits) == 0 {
		t.Fatalf("parent commits = %d %v", len(page.Commits), err)
	}
	mid := page.Commits[len(page.Commits)-1].Seq
	if _, err := h.Fork(ctx, app.ForkRequest{Parent: sid, At: mid, Child: "mid"}); !session.IsCode(err, session.ErrInvalid) {
		t.Fatalf("fork inside an active turn = %v, want ErrInvalid", err)
	}
	if _, err := store.Header(ctx, "mid"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("refused fork left a root: %v", err)
	}
	close(gate.release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, err := s.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Active == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("turn still active: %+v", status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	page, _ = store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if _, err := h.Fork(ctx, app.ForkRequest{Parent: sid, At: page.Commits[len(page.Commits)-1].Seq, Child: "after"}); err != nil {
		t.Fatalf("fork at a quiescent point: %v", err)
	}
}
