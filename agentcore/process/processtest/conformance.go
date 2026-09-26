package processtest

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// Factory builds a fresh, empty Store for one subtest.
type Factory func(t *testing.T) process.Store

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("ledger", func(t *testing.T) { testLedger(t, factory(t)) })
}

func commit(t *testing.T, seq process.CommitSeq, id process.CommitID, typ process.EventType, payload any) process.Commit {
	t.Helper()
	ev, err := ledger.NewEvent(typ, 1, payload)
	if err != nil {
		t.Fatal(err)
	}
	return process.Commit{Seq: seq, CommitID: id, Events: []process.Event{ev}}
}

// The dispatch ledger answers replays by identity, stale sequences and
// illegal steps with the kernel's sentinels, and fences a writer whose epoch
// is behind the highest that has written the key (RUN-EXE-15).
func testLedger(t *testing.T, store process.Store) {
	ctx := context.Background()
	k1 := effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e1"}
	if _, _, ok, err := store.Load(ctx, k1); err != nil || ok {
		t.Fatalf("load of an unknown key = ok:%v %v", ok, err)
	}
	if commits, head, err := store.Read(ctx, k1, 0); err != nil || len(commits) != 0 || head.Next != 0 {
		t.Fatalf("read of an unknown key = %d commits head:%+v %v", len(commits), head, err)
	}
	steps := []struct {
		name    string
		epoch   ledger.Epoch
		commit  process.Commit
		wantErr error
	}{
		{"first plan opens the ledger", 1, commit(t, 0, process.PlannedCommitID(k1, 1), process.EventDispatchPlanned, process.Planned{Attempt: 1}), nil},
		{"replayed plan is already applied", 1, commit(t, 0, process.PlannedCommitID(k1, 1), process.EventDispatchPlanned, process.Planned{Attempt: 1}), ledger.ErrAlreadyApplied},
		{"stale seq conflicts", 1, commit(t, 0, process.DispatchedCommitID(k1, 1), process.EventDispatched, process.Dispatched{Attempt: 1}), ledger.ErrConflict},
		{"plan while owed is a state conflict", 1, commit(t, 1, process.PlannedCommitID(k1, 2), process.EventDispatchPlanned, process.Planned{Attempt: 2}), ledger.ErrStateConflict},
		{"a later epoch commits", 2, commit(t, 1, process.DispatchedCommitID(k1, 1), process.EventDispatched, process.Dispatched{Attempt: 1}), nil},
		{"an earlier epoch is fenced", 1, commit(t, 2, process.GivenUpCommitID(k1), process.EventGivenUp, process.GivenUp{Reason: "x"}), ledger.ErrFenced},
		{"given up", 2, commit(t, 2, process.GivenUpCommitID(k1), process.EventGivenUp, process.GivenUp{Reason: "budget"}), nil},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			if err := store.Append(ctx, s.epoch, k1, s.commit); !errors.Is(err, s.wantErr) {
				t.Fatalf("append = %v, want %v", err, s.wantErr)
			}
		})
	}
	state, head, ok, err := store.Load(ctx, k1)
	if err != nil || !ok || head.Next != 3 || state.Planned != 1 || state.Dispatched != 1 || !state.GivenUp || state.Key != k1 {
		t.Fatalf("load = %+v head:%+v ok:%v %v", state, head, ok, err)
	}
	commits, head, err := store.Read(ctx, k1, 1)
	if err != nil || len(commits) != 2 || head.Next != 3 || commits[0].Seq != 1 {
		t.Fatalf("read from 1 = %d commits head:%+v %v", len(commits), head, err)
	}
	// Keys are independent ledgers: the fence of one is not the fence of
	// another.
	k2 := effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e2"}
	if err := store.Append(ctx, 1, k2, commit(t, 0, process.PlannedCommitID(k2, 1), process.EventDispatchPlanned, process.Planned{Attempt: 1})); err != nil {
		t.Fatalf("append to a second key under epoch 1 = %v", err)
	}
}
