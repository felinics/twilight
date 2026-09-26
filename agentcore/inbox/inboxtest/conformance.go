package inboxtest

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
)

// Factory builds a fresh, empty Store for one subtest.
type Factory func(t *testing.T) inbox.Store

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("commands", func(t *testing.T) { testCommands(t, factory(t)) })
}

func command(id, kind, payload string) inbox.Command {
	return inbox.Command{ID: inbox.CommandID(id), Kind: inbox.Kind(kind), Payload: run.MustParseCanonicalJSON(payload)}
}

// Enqueue is idempotent by CommandID and refuses a reused ID; Seq counts
// per Session in enqueue order; Pending and Sessions see only unresolved
// entries; Resolve closes an entry once.
func testCommands(t *testing.T, s inbox.Store) { //nolint:gocyclo // one scenario, checked step by step
	ctx := context.Background()
	const a, b session.SessionID = "s-a", "s-b"
	if pending, err := s.Pending(ctx, a); err != nil || len(pending) != 0 {
		t.Fatalf("pending of an empty inbox = %v %v", pending, err)
	}
	if sids, err := s.Sessions(ctx, 0); err != nil || len(sids) != 0 {
		t.Fatalf("sessions of an empty store = %v %v", sids, err)
	}
	if _, ok, err := s.Lookup(ctx, a, "c1"); err != nil || ok {
		t.Fatalf("lookup of an unknown command = ok:%v %v", ok, err)
	}
	e1, err := s.Enqueue(ctx, a, command("c1", "stop", `{"reason":"user"}`))
	if err != nil || e1.Seq != 0 || !e1.Pending() || e1.Command.ID != "c1" {
		t.Fatalf("first enqueue = %+v %v", e1, err)
	}
	again, err := s.Enqueue(ctx, a, command("c1", "stop", `{"reason":"user"}`))
	if err != nil || again.Seq != 0 || again.EnqueuedAtUnixMilli != e1.EnqueuedAtUnixMilli {
		t.Fatalf("replayed enqueue = %+v %v, want the stored entry", again, err)
	}
	if _, err := s.Enqueue(ctx, a, command("c1", "stop", `{"reason":"other"}`)); !errors.Is(err, inbox.ErrCommandConflict) {
		t.Fatalf("reused id with another payload = %v, want conflict", err)
	}
	if _, err := s.Enqueue(ctx, a, command("c1", "submit", `{"reason":"user"}`)); !errors.Is(err, inbox.ErrCommandConflict) {
		t.Fatalf("reused id with another kind = %v, want conflict", err)
	}
	e2, err := s.Enqueue(ctx, a, command("c2", "submit", `{"text":"hi"}`))
	if err != nil || e2.Seq != 1 {
		t.Fatalf("second enqueue = %+v %v", e2, err)
	}
	// Seq is per Session; the same CommandID in another Session is another
	// command.
	eb, err := s.Enqueue(ctx, b, command("c1", "submit", `{"text":"b"}`))
	if err != nil || eb.Seq != 0 {
		t.Fatalf("enqueue in another session = %+v %v", eb, err)
	}
	if pending, err := s.Pending(ctx, a); err != nil || len(pending) != 2 || pending[0].Seq != 0 || pending[1].Seq != 1 {
		t.Fatalf("pending = %+v %v", pending, err)
	}
	if sids, err := s.Sessions(ctx, 0); err != nil || len(sids) != 2 || sids[0] != a || sids[1] != b {
		t.Fatalf("sessions = %v %v", sids, err)
	}
	if sids, err := s.Sessions(ctx, 1); err != nil || len(sids) != 1 || sids[0] != a {
		t.Fatalf("sessions limited to one = %v %v", sids, err)
	}
	if err := s.Resolve(ctx, a, 0, inbox.Result{Status: inbox.StatusRejected, Reason: "no active turn"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Resolve(ctx, a, 0, inbox.Result{Status: inbox.StatusApplied}); !errors.Is(err, inbox.ErrNotPending) {
		t.Fatalf("resolving twice = %v, want not pending", err)
	}
	if err := s.Resolve(ctx, a, 7, inbox.Result{Status: inbox.StatusApplied}); !errors.Is(err, inbox.ErrNotPending) {
		t.Fatalf("resolving an unknown seq = %v, want not pending", err)
	}
	got, ok, err := s.Lookup(ctx, a, "c1")
	if err != nil || !ok || got.Pending() || got.Result.Status != inbox.StatusRejected || got.Result.Reason != "no active turn" || got.Result.ResolvedAtUnixMilli == 0 {
		t.Fatalf("resolved entry = %+v ok:%v %v", got, ok, err)
	}
	// The replayed Enqueue of a resolved command still answers the entry.
	if e, err := s.Enqueue(ctx, a, command("c1", "stop", `{"reason":"user"}`)); err != nil || e.Pending() {
		t.Fatalf("replayed enqueue after resolve = %+v %v", e, err)
	}
	if pending, err := s.Pending(ctx, a); err != nil || len(pending) != 1 || pending[0].Command.ID != "c2" {
		t.Fatalf("pending after resolve = %+v %v", pending, err)
	}
	if err := s.Resolve(ctx, a, 1, inbox.Result{Status: inbox.StatusApplied}); err != nil {
		t.Fatal(err)
	}
	if sids, err := s.Sessions(ctx, 0); err != nil || len(sids) != 1 || sids[0] != b {
		t.Fatalf("sessions after draining a = %v %v", sids, err)
	}
	// Seq keeps counting after resolutions.
	if e3, err := s.Enqueue(ctx, a, command("c3", "retry", `{}`)); err != nil || e3.Seq != 2 {
		t.Fatalf("enqueue after resolve = %+v %v", e3, err)
	}
}
