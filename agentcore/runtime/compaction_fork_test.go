package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/session/writer/writertest"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/sdk"
)

func (f *compactFixture) forkWriter(child session.SessionID) writer.Writer {
	f.t.Helper()
	page, err := f.k.Store.ReadCommits(f.ctx, session.CommitReadRequest{SessionID: compactSession})
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.k.Fork(f.ctx, sessionkernel.ForkRequest{Parent: compactSession, At: page.Head.Next - 1, Child: child}); err != nil {
		f.t.Fatal(err)
	}
	w, err := writertest.Open(f.t, f.ctx, f.k.Writers, child, session.OpenOptions{})
	if err != nil {
		f.t.Fatal(err)
	}
	return w
}

func (f *compactFixture) stateFor(w writer.Writer) compaction.State {
	f.t.Helper()
	state, err := compaction.Read(f.ctx, w.Projections(), w.SessionID())
	if err != nil {
		f.t.Fatal(err)
	}
	return state
}

func (f *compactFixture) assertOperation(w writer.Writer, id compaction.ID, status compaction.Status, acknowledged bool) compaction.Operation {
	f.t.Helper()
	state := f.stateFor(w)
	op, found := state.Operations[id]
	if !found || len(state.Operations) != 1 || op.Status != status || op.Acknowledged != acknowledged {
		f.t.Fatalf("Session %s operation: found=%v state=%+v, want %s acknowledged=%v", w.SessionID(), found, state, status, acknowledged)
	}
	return op
}

// Use a real filestore fork: the parent's start commit is visible to ledger
// lookup, even though the Own compaction stream leaves the child with no work.
func TestCompactionForkSameCallerIDHasIndependentProtocol(t *testing.T) {
	var calls atomic.Int32
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		calls.Add(1)
		return sdk.ModelResult{Text: compactSummary}, nil
	})
	port := newCompactPort(f.worker)
	x := f.execution(port, nil, true)
	f.history()
	const id compaction.ID = "same-caller-id"
	plan, request := f.plan(), compactRequest()
	if err := x.Compactions.Start(f.ctx, f.w, id, plan, &request); err != nil {
		t.Fatal(err)
	}
	child := f.forkWriter("pending-child")
	inherited, err := chatlog.ReadContext(f.ctx, child.Projections(), child.SessionID())
	if err != nil || !reflect.DeepEqual(inherited.Entries, f.context().Entries) {
		t.Fatalf("fork did not inherit Context: %+v, %v", inherited, err)
	}
	if state := f.stateFor(child); len(state.Operations) != 0 || state.Pending != "" || state.NeedsRecovery() {
		t.Fatalf("child inherited operation obligations: %+v", state)
	}
	for range 2 {
		if err := x.Compactions.Start(f.ctx, child, id, plan, &request); err != nil {
			t.Fatal(err)
		}
	}
	parentOp := f.assertOperation(f.w, id, compaction.Pending, false)
	childOp := f.assertOperation(child, id, compaction.Pending, false)
	parentKey, childKey := parentOp.Request.Key(compactSession), childOp.Request.Key(child.SessionID())
	if parentKey == childKey || parentKey.OwnerID != childKey.OwnerID || parentKey.Effect != childKey.Effect {
		t.Fatalf("caller identity must remain stable under different Session scopes: parent=%+v child=%+v", parentKey, childKey)
	}
	for _, w := range []writer.Writer{f.w, child} {
		result, err := x.Compactions.Advance(f.ctx, w)
		if err != nil || !result.Pending || result.Key != parentOp.Request.Key(w.SessionID()) {
			t.Fatalf("Session %s dispatch=%+v, %v", w.SessionID(), result, err)
		}
		if got := waitCompact(t, f.ctx, port.dispatched); got != result.Key {
			t.Fatalf("dispatched key=%+v, want %+v", got, result.Key)
		}
	}
	parentOut, childOut := f.awaitOutcome(port, parentKey), f.awaitOutcome(port, childKey)
	first, second := child, f.w
	firstOut, secondOut := childOut, parentOut
	if _, err := x.Compactions.Deliver(f.ctx, first, secondOut); err == nil {
		t.Fatal("accepted another Session's same-caller-ID outcome")
	}
	port.failACK.Store(true)
	if result, err := x.Compactions.Deliver(f.ctx, first, firstOut); err == nil || !strings.Contains(err.Error(), "injected ACK failure") || result.Pending {
		t.Fatalf("first settlement=%+v, %v", result, err)
	}
	f.assertOperation(first, id, compaction.Applied, false)
	f.assertOperation(second, id, compaction.Pending, false)
	if result, err := x.Compactions.Deliver(f.ctx, second, secondOut); err != nil || result.Pending {
		t.Fatalf("second settlement=%+v, %v", result, err)
	}
	f.assertOperation(second, id, compaction.Applied, true)
	f.assertOperation(first, id, compaction.Applied, false)
	if _, err := port.GetOutcome(f.ctx, secondOut.Key); !errors.Is(err, effect.ErrOutcomeCollected) {
		t.Fatalf("second outcome not collected: %v", err)
	}
	if _, err := port.GetOutcome(f.ctx, firstOut.Key); err != nil {
		t.Fatalf("second ACK collected first outcome: %v", err)
	}
	if _, err := x.Compactions.Deliver(f.ctx, first, firstOut); err != nil {
		t.Fatal(err)
	}
	parentOp = f.assertOperation(f.w, id, compaction.Applied, true)
	childOp = f.assertOperation(child, id, compaction.Applied, true)
	if parentOp.CompactionID == childOp.CompactionID || calls.Load() != 2 || port.ackCalls.Load() != 3 {
		t.Fatalf("independent work: parent=%+v child=%+v calls=%d ACKs=%d", parentOp, childOp, calls.Load(), port.ackCalls.Load())
	}
}

func TestCompactionForkInheritedSummaryDoesNotAliasChildArtifacts(t *testing.T) {
	var calls atomic.Int32
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		return sdk.ModelResult{Text: fmt.Sprintf("summary %d", calls.Add(1))}, nil
	})
	x := f.execution(f.worker, nil, true)
	f.history()
	const id compaction.ID = "reused-after-fork"
	f.accept(x, id)
	f.settleMaintenance(x)
	parentOp := f.assertOperation(f.w, id, compaction.Applied, true)
	parentSurface, err := chatlog.ReadSurface(f.ctx, f.w.Projections(), compactSession)
	if err != nil {
		t.Fatal(err)
	}
	parentCompaction, found := parentSurface.Compactions.Get(parentOp.CompactionID)
	if !found {
		t.Fatal("parent compaction missing")
	}
	parentSummary, found := parentSurface.Summaries.Get(parentCompaction.Compaction.SummaryID)
	if !found {
		t.Fatal("parent summary missing")
	}
	child := f.forkWriter("completed-child")
	if state := f.stateFor(child); len(state.Operations) != 0 || state.NeedsRecovery() {
		t.Fatalf("child inherited completed operation: %+v", state)
	}
	inherited, err := chatlog.ReadContext(f.ctx, child.Projections(), child.SessionID())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inherited.Entries, f.context().Entries) {
		t.Fatal("child did not inherit parent's summarized conversation")
	}
	plan, possible, err := chatlog.PlanCompaction(inherited.Entries, 1)
	if err != nil || !possible {
		t.Fatalf("child plan: possible=%v, %v", possible, err)
	}
	request := compactRequest()
	if err := x.Compactions.Start(f.ctx, child, id, plan, &request); err != nil {
		t.Fatal(err)
	}
	if _, err := f.advance(x.Compactions, child); err != nil {
		t.Fatal(err)
	}
	childOp := f.assertOperation(child, id, compaction.Applied, true)
	if childOp.CompactionID == parentOp.CompactionID {
		t.Fatal("child compaction aliases inherited parent compaction")
	}
	childSurface, err := chatlog.ReadSurface(f.ctx, child.Projections(), child.SessionID())
	if err != nil {
		t.Fatal(err)
	}
	childCompaction, found := childSurface.Compactions.Get(childOp.CompactionID)
	if !found || childCompaction.Compaction.SummaryID == parentSummary.ID {
		t.Fatal("child summary identity missing or aliases inherited summary")
	}
	childSummary, found := childSurface.Summaries.Get(childCompaction.Compaction.SummaryID)
	if !found || chatlog.PartsText(childSummary.Parts) != "summary 2" {
		t.Fatalf("child summary=%+v, found=%v", childSummary, found)
	}
	stillInherited, found := childSurface.Summaries.Get(parentSummary.ID)
	if !found || !reflect.DeepEqual(stillInherited, parentSummary) || childSurface.Summaries.Len() != 2 || childSurface.Compactions.Len() != 2 {
		t.Fatal("child overwrote inherited conversation artifacts")
	}
	f.assertOperation(f.w, id, compaction.Applied, true)
	f.counts(1, 1, 0)
	if !reflect.DeepEqual(parentSurface, mustCompactSurface(f, f.w)) || calls.Load() != 2 {
		t.Fatal("child changed parent's conversation or failed to execute its own summary")
	}
}

func mustCompactSurface(f *compactFixture, w writer.Writer) chatlog.Surface {
	f.t.Helper()
	surface, err := chatlog.ReadSurface(f.ctx, w.Projections(), w.SessionID())
	if err != nil {
		f.t.Fatal(err)
	}
	return surface
}

func TestTurnForkInheritsHistoryWithoutExecutionState(t *testing.T) {
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		return sdk.ModelResult{Text: "answer"}, nil
	})
	x := f.execution(f.worker, nil, true)
	parent := f.startTurn("parent-turn", false)
	if _, err := f.drive(x, f.w, parent.Ref.TurnID); err != nil {
		t.Fatal(err)
	}
	parentSurface, err := sessionstore.ReadSurface(f.ctx, f.w.Projections(), compactSession)
	if err != nil {
		t.Fatal(err)
	}
	view := parentSurface.Turns[parent.Ref.TurnID]
	if view.Status != turn.TurnCompleted || view.Preset != f.preset || len(view.InputIDs) != 1 {
		t.Fatalf("parent = %+v", view)
	}
	child := f.forkWriter("turn-child")
	inherited, err := sessionstore.ReadSurface(f.ctx, child.Projections(), child.SessionID())
	if err != nil || !reflect.DeepEqual(inherited, parentSurface) {
		t.Fatalf("inherited surface = %+v %v", inherited, err)
	}
	context, err := chatlog.ReadContext(f.ctx, child.Projections(), child.SessionID())
	if err != nil || !reflect.DeepEqual(context.Entries, f.context().Entries) {
		t.Fatalf("inherited context = %+v %v", context, err)
	}
	state, _, err := child.Projections().Load(f.ctx, child.SessionID(), sessionstore.MachineProjectionID, sessionstore.MachineProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	machine, ok := state.(sessionstore.Machine)
	if !ok || len(machine.Active) != 0 || len(machine.Positions) != 0 {
		t.Fatalf("child machine = %+v", state)
	}
	if _, err := f.k.TurnStore.Record(f.ctx, child.SessionID(), parent.Ref.TurnID); !errors.Is(err, store.ErrTurnNotFound) {
		t.Fatalf("inherited execution record = %v", err)
	}
	in, err := f.k.Chatlog.Submit(f.ctx, child, "child-input", turn.MustParseCanonicalJSON("{\"text\":\"child\"}"))
	if err != nil {
		t.Fatal(err)
	}
	next, err := f.k.Turns.Start(f.ctx, child, sessionkernel.StartRequest{Ref: sessionstore.TurnRef{SessionID: child.SessionID(), TurnID: "child-turn"}, Inputs: []turn.AgentInput{in}, Preset: f.preset})
	if err != nil || next.Status != turn.TurnActive {
		t.Fatalf("child start = %+v %v", next, err)
	}
	if _, err := f.drive(x, child, next.Ref.TurnID); err != nil {
		t.Fatal(err)
	}
	unchanged, err := sessionstore.ReadSurface(f.ctx, f.w.Projections(), compactSession)
	if err != nil || !reflect.DeepEqual(unchanged, parentSurface) {
		t.Fatalf("parent changed: %+v %v", unchanged, err)
	}
}
