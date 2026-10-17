package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
)

// Proof uses no Writer projection, command evaluation, or ownership operation.
// Only the bound scope is needed, and any other Writer access would panic.
type proofScope struct {
	writer.Writer
	sid session.SessionID
}

func (w proofScope) SessionID() session.SessionID { return w.sid }

func appendProofFacts(t *testing.T, h session.Handle, reg *module.Registry, turnID turn.TurnID, id ledger.CommitID, facts ...turn.Fact) ledger.Commit {
	t.Helper()
	events := make([]ledger.Event, len(facts))
	for i, fact := range facts {
		payload, err := reg.Encode(EventType(fact), Event{TurnID: turnID, Fact: fact})
		if err != nil {
			t.Fatal(err)
		}
		events[i] = ledger.Event{Type: EventType(fact), Payload: payload}
	}
	c, err := h.Append(context.Background(), ledger.Proposal{CommitID: id, Batches: []ledger.EventBatch{{Domain: Stream(turnID), Events: events}}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestEffectProofSurvivesTerminalRunAndFindsCancelDisposition(t *testing.T) {
	ctx := context.Background()
	durable := filestoretest.Store(t)
	reg, err := module.BuildRegistry(Module)
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "effect-proof"
	if _, err := durable.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	const turnID turn.TurnID = "run"
	const effectID turn.EffectID = "model-effect"
	prefix := []turn.Fact{turn.TurnStarted{Preset: ref.Ref{ID: "test", Digest: "sha256:test"}, TurnID: turnID},
		turn.ModelStepPrepared{StepID: "step", Model: "m", RequestDigest: "sha256:req"},
		turn.ModelStepStarted{StepID: "step", Effect: effectID}}
	appendProofFacts(t, h, reg, turnID, "prefix", prefix...)
	// Cross a stream page boundary before the disposition.
	for i := 0; i < 130; i++ {
		f := turn.InputAccepted{Input: turn.AgentInput{ID: turn.InputID(fmt.Sprintf("input-%d", i)), Digest: "sha256:input"}}
		appendProofFacts(t, h, reg, turnID, ledger.CommitID(fmt.Sprintf("input-%d", i)), f)
		prefix = append(prefix, f)
	}
	state := turn.MachineState{TurnID: turnID, Status: turn.TurnActive, Current: turn.ModelStep{RefValue: turn.StepRef{TurnID: turnID, ID: "step"}, Status: turn.ModelExecuting, Effect: effectID}}
	closing, err := schema.Machine().Decide(state, turn.CancelTurn{})
	if err != nil {
		t.Fatal(err)
	}
	cancelCommit := appendProofFacts(t, h, reg, turnID, "cancel-run-not-an-effect-command", closing...)
	// The actual machine projection removes this terminal Turn. That is not
	// absence of a durable disposition proof.
	machine := newMachine()
	for _, fact := range append(prefix, closing...) {
		machine, err = machine.Apply(module.DecodedEvent{Domain: Stream(turnID), Value: Event{TurnID: turnID, Fact: fact}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, active := machine.Active[turnID]; active {
		t.Fatal("terminal Run remained in projection")
	}
	// A fresh adapter has neither the command result nor a machine cache.
	adapter := &SessionTurnStore{cfg: Config{Store: durable, Registry: reg}}
	proof := adapter.Bind(proofScope{sid: sid}).(store.EffectDispositionReader)
	facts, found, err := proof.EffectDisposition(ctx, turnID, effectID)
	if err != nil || !found || len(facts) != 1 || store.DispositionEffect(facts[0]) != effectID {
		t.Fatalf("cancel proof=%+v found=%v err=%v", facts, found, err)
	}
	if _, ok := facts[0].(turn.ModelStepFailed); !ok {
		t.Fatalf("cancel proof=%T", facts[0])
	}
	for _, query := range []struct {
		run    turn.TurnID
		effect turn.EffectID
	}{{turnID, "other"}, {"other-run", effectID}} {
		if facts, found, err := proof.EffectDisposition(ctx, query.run, query.effect); err != nil || found || len(facts) != 0 {
			t.Fatalf("foreign query=%+v proof=%+v found=%v err=%v", query, facts, found, err)
		}
	}
	// Run is module.Own: an inherited parent settlement cannot authorize ACK
	// of a child's different AssignmentKey, even with identical IDs.
	const child session.SessionID = "child"
	if _, err := durable.Create(ctx, session.CreateRequest{SessionID: child, Fork: &session.ForkOrigin{Session: sid, Seq: cancelCommit.Seq}}); err != nil {
		t.Fatal(err)
	}
	childProof := adapter.Bind(proofScope{sid: child}).(store.EffectDispositionReader)
	if facts, found, err := childProof.EffectDisposition(ctx, turnID, effectID); err != nil || found || len(facts) != 0 {
		t.Fatalf("inherited effect was accepted as local proof: %+v found=%v err=%v", facts, found, err)
	}
}

func TestEffectProofFindsEveryToolInSingleCancelCommit(t *testing.T) {
	ctx := context.Background()
	durable := filestoretest.Store(t)
	reg, err := module.BuildRegistry(Module)
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "tools-proof"
	if _, err := durable.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	state := turn.MachineState{TurnID: "run", Status: turn.TurnActive, Current: turn.ToolStep{RefValue: turn.StepRef{TurnID: "run", ID: "tools"}, Calls: []turn.ToolCallState{
		{CallID: "call-1", Status: turn.ToolExecuting, Effect: "tool-1"},
		{CallID: "call-2", Status: turn.ToolExecuting, Effect: "tool-2"},
	}}}
	facts, err := schema.Machine().Decide(state, turn.CancelTurn{})
	if err != nil {
		t.Fatal(err)
	}
	appendProofFacts(t, h, reg, "run", "cancel-both-tools", facts...)
	adapter := &SessionTurnStore{cfg: Config{Store: durable, Registry: reg}}
	proof := adapter.Bind(proofScope{sid: sid}).(store.EffectDispositionReader)
	for _, id := range []turn.EffectID{"tool-1", "tool-2"} {
		facts, found, err := proof.EffectDisposition(ctx, "run", id)
		if err != nil || !found || len(facts) != 1 || store.DispositionEffect(facts[0]) != id {
			t.Fatalf("%s proof=%+v found=%v err=%v", id, facts, found, err)
		}
	}
}

type failedProofStore struct {
	session.Store
	err   error
	stall bool
}

func (s failedProofStore) ReadStream(context.Context, session.StreamReadRequest) (session.StreamPage, error) {
	return session.StreamPage{HasMore: s.stall}, s.err
}

func TestEffectProofReadFailureIsNotAbsence(t *testing.T) {
	boom := errors.New("ledger unavailable")
	for _, s := range []failedProofStore{{err: boom}, {stall: true}} {
		adapter := &SessionTurnStore{cfg: Config{Store: s}}
		proof := adapter.Bind(proofScope{sid: "session"}).(store.EffectDispositionReader)
		_, found, err := proof.EffectDisposition(context.Background(), "run", "effect")
		if err == nil || found || (s.err != nil && !errors.Is(err, boom)) {
			t.Fatalf("proof read found=%v err=%v", found, err)
		}
	}
}
