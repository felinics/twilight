package loop

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/store"
)

// These fakes expose only the operations ACK repair is allowed to use. An
// accidental replay, backend read, or command evaluation panics through the
// nil embedded ports instead of quietly passing the test.
type ackRepairExecutor struct {
	effect.ExecutionPort
	attachment effect.Attachment
	probeErr   error
	ackErr     error
	probes     int
	acks       int
	readable   bool
}

func (e *ackRepairExecutor) Abort(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	e.probes++
	return e.attachment, e.probeErr
}

func (e *ackRepairExecutor) Acknowledge(context.Context, effect.AssignmentKey) error {
	e.acks++
	if e.ackErr == nil {
		e.readable = false
	}
	return e.ackErr
}

type ackRepairStore struct {
	store.TurnStore
	effects   map[turn.EffectID][]turn.Fact
	proofErr  error
	lookups   int
	committed store.CommitResult
}

func (*ackRepairStore) Scope() turn.Scope { return "session" }
func (*ackRepairStore) Load(context.Context, turn.TurnID) (store.Snapshot, error) {
	return store.Snapshot{State: turn.MachineState{Status: turn.TurnCompleted}}, nil
}
func (s *ackRepairStore) EffectDisposition(_ context.Context, _ turn.TurnID, id turn.EffectID) ([]turn.Fact, bool, error) {
	s.lookups++
	facts, found := s.effects[id]
	return facts, found, s.proofErr
}
func (s *ackRepairStore) Commit(_ context.Context, _ store.CommitRequest) (store.CommitResult, error) {
	if s.effects == nil {
		s.effects = map[turn.EffectID][]turn.Fact{}
	}
	for _, fact := range s.committed.Facts {
		id := store.DispositionEffect(fact)
		if id != "" {
			s.effects[id] = append(s.effects[id], fact)
		}
	}
	return s.committed, nil
}

func ackKey() effect.AssignmentKey {
	return effect.AssignmentKey{Session: "session", OwnerKind: effect.OwnerTurn, OwnerID: string("run"), Effect: "effect"}
}

func TestAcknowledgeSettledRepairsDroppedDelivery(t *testing.T) {
	ctx := context.Background()
	key := ackKey()
	boom := errors.New("ACK response lost")
	exec := &ackRepairExecutor{attachment: effect.Attachment{State: effect.AttachmentTerminal, Execution: effect.ExecutionCompleted}, ackErr: boom, readable: true}
	st := &ackRepairStore{committed: store.CommitResult{Facts: []turn.Fact{turn.ModelStepCompleted{Effect: key.Effect}}}}
	l := &Loop{Executor: exec}
	ref := effectRef{turnID: turn.TurnID(key.OwnerID), id: key.Effect}
	if _, err := l.settle(ctx, st, nil, &ref, 0, turn.SubmitModelResult{}); err != nil {
		t.Fatal(err)
	}
	if exec.acks != 1 || !exec.readable {
		t.Fatalf("failed best-effort ACK: calls=%d readable=%v", exec.acks, exec.readable)
	}
	// A process restart loses the CommitResult and in-memory Loop. Only the
	// durable effect map survives. Deliver sees the terminal Turn and drops.
	st = &ackRepairStore{effects: st.effects}
	l = &Loop{Executor: exec, slots: map[turn.TurnID]*turnSlot{}}
	res, err := l.Deliver(ctx, st, effect.Outcome{Key: key}, nil)
	if err != nil || res.Disposition != LoopDropped || exec.acks != 1 {
		t.Fatalf("late delivery=%+v err=%v ACKs=%d", res, err, exec.acks)
	}
	exec.ackErr = nil
	for i := 0; i < 2; i++ {
		acked, err := l.AcknowledgeSettled(ctx, st, key)
		if err != nil || !acked {
			t.Fatalf("repair %d: ACK=%v err=%v", i, acked, err)
		}
	}
	if exec.readable || exec.acks != 3 {
		t.Fatalf("ACK repair did not collect safely: calls=%d readable=%v", exec.acks, exec.readable)
	}
}

func TestAcknowledgeSettledRequiresEffectProof(t *testing.T) {
	ctx := context.Background()
	key := ackKey()
	cases := map[string]map[turn.EffectID][]turn.Fact{
		"absent":                  {},
		"start is not settlement": {key.Effect: {turn.ModelStepStarted{Effect: key.Effect}}},
		"TurnEnded alone":         {key.Effect: {turn.TurnEnded{End: turn.TurnCompletedEnd{}}}},
		"another effect":          {key.Effect: {turn.ToolCallCompleted{Effect: "other"}}},
		"empty effect":            {key.Effect: {turn.ToolCallFailed{}}},
	}
	for name, effects := range cases {
		t.Run(name, func(t *testing.T) {
			exec := &ackRepairExecutor{attachment: effect.Attachment{State: effect.AttachmentTerminal, Execution: effect.ExecutionCompleted}, readable: true}
			l := &Loop{Executor: exec}
			acked, err := l.AcknowledgeSettled(ctx, &ackRepairStore{effects: effects}, key)
			if err != nil || acked || exec.acks != 0 || exec.probes != 0 || !exec.readable {
				t.Fatalf("unproved ACK=%v err=%v probes=%d ACKs=%d readable=%v", acked, err, exec.probes, exec.acks, exec.readable)
			}
		})
	}
}

func TestAcknowledgeSettledDispositionVariants(t *testing.T) {
	key := ackKey()
	for _, fact := range []turn.Fact{
		turn.ModelStepCompleted{Effect: key.Effect}, turn.ModelStepRejected{Effect: key.Effect}, turn.ModelStepFailed{Effect: key.Effect},
		turn.ToolCallCompleted{Effect: key.Effect}, turn.ToolCallFailed{Effect: key.Effect, Outcome: turn.ToolOutcomeUnknown},
		turn.ModelStepRecovered{Effect: key.Effect},
	} {
		st := &ackRepairStore{effects: map[turn.EffectID][]turn.Fact{key.Effect: {fact}}}
		exec := &ackRepairExecutor{attachment: effect.Attachment{State: effect.AttachmentTerminal, Execution: effect.ExecutionUnknown}, readable: true}
		acked, err := (&Loop{Executor: exec}).AcknowledgeSettled(context.Background(), st, key)
		if err != nil || !acked || exec.acks != 1 {
			t.Fatalf("%T ACK=%v err=%v calls=%d", fact, acked, err, exec.acks)
		}
	}
}

func TestRecoveryACKRequiresClosedExecution(t *testing.T) {
	key := ackKey()
	for _, tc := range []struct {
		name string
		att  effect.Attachment
		ack  bool
	}{
		{"running", effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning}, false},
		{"expired lease is not absence", effect.Attachment{State: effect.AttachmentOrphaned, Execution: effect.ExecutionDispatching, LeaseUntilUnixMilli: 1}, false},
		{"Abort fenced missing", effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}, false},
		{"missing without tombstone is not proof", effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, false},
		{"terminal label without terminal status", effect.Attachment{State: effect.AttachmentTerminal, Execution: effect.ExecutionRunning}, false},
		{"completed", effect.Attachment{State: effect.AttachmentTerminal, Execution: effect.ExecutionCompleted}, true},
		{"unknown", effect.Attachment{State: effect.AttachmentTerminal, Execution: effect.ExecutionUnknown}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &ackRepairStore{effects: map[turn.EffectID][]turn.Fact{key.Effect: {turn.ModelStepRecovered{Effect: key.Effect}}}}
			exec := &ackRepairExecutor{attachment: tc.att, readable: true}
			acked, err := (&Loop{Executor: exec}).AcknowledgeSettled(context.Background(), st, key)
			if err != nil || acked != tc.ack || exec.probes != 1 || (exec.acks == 1) != tc.ack || exec.readable == tc.ack {
				t.Fatalf("ACK=%v want=%v err=%v probes=%d ACKs=%d readable=%v", acked, tc.ack, err, exec.probes, exec.acks, exec.readable)
			}
		})
	}
}

func TestAcknowledgeSettledErrorsDoNotAuthorizeCollection(t *testing.T) {
	ctx := context.Background()
	key := ackKey()
	boom := errors.New("storage unavailable")
	for _, where := range []string{"proof", "probe", "ACK"} {
		t.Run(where, func(t *testing.T) {
			st := &ackRepairStore{effects: map[turn.EffectID][]turn.Fact{key.Effect: {turn.ToolCallCompleted{Effect: key.Effect}}}}
			exec := &ackRepairExecutor{attachment: effect.Attachment{State: effect.AttachmentTerminal, Execution: effect.ExecutionCompleted}, readable: true}
			switch where {
			case "proof":
				st.proofErr = boom
			case "probe":
				exec.probeErr = boom
			case "ACK":
				exec.ackErr = boom
			}
			acked, err := (&Loop{Executor: exec}).AcknowledgeSettled(ctx, st, key)
			if acked || !errors.Is(err, boom) || !exec.readable {
				t.Fatalf("ACK=%v err=%v readable=%v", acked, err, exec.readable)
			}
		})
	}
	st := &ackRepairStore{}
	exec := &ackRepairExecutor{}
	l := &Loop{Executor: exec}
	if _, err := l.AcknowledgeSettled(ctx, struct{ store.TurnStore }{st}, key); !errors.Is(err, store.ErrEffectProofUnsupported) {
		t.Fatalf("missing proof port=%v", err)
	}
	wrong := key
	wrong.Session = "other-session"
	if acked, err := l.AcknowledgeSettled(ctx, st, wrong); err == nil || acked || st.lookups != 0 {
		t.Fatalf("foreign scope ACK=%v err=%v lookups=%d", acked, err, st.lookups)
	}
}

func TestAcknowledgeSettledRejectsCompactionOwner(t *testing.T) {
	key := ackKey()
	key.OwnerKind = effect.OwnerCompaction
	st := &ackRepairStore{effects: map[turn.EffectID][]turn.Fact{key.Effect: {turn.ToolCallCompleted{Effect: key.Effect}}}}
	exec := &ackRepairExecutor{readable: true}
	acked, err := (&Loop{Executor: exec}).AcknowledgeSettled(context.Background(), st, key)
	if err == nil || acked || st.lookups != 0 || exec.probes != 0 || exec.acks != 0 || !exec.readable {
		t.Fatalf("foreign owner: ACK=%v err=%v lookups=%d probes=%d ACKs=%d", acked, err, st.lookups, exec.probes, exec.acks)
	}
}
