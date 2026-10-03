package redispatch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/redispatch"
)

var key = effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e1"}

type step = struct {
	typ     ledger.EventType
	payload any
}

func commit(t *testing.T, seq ledger.CommitSeq, s step) ledger.Commit {
	t.Helper()
	ev, err := ledger.NewEvent(s.typ, 1, s.payload)
	if err != nil {
		t.Fatal(err)
	}
	return ledger.Commit{Seq: seq, CommitID: ledger.CommitID("c" + string(rune('0'+seq))), Batches: []ledger.EventBatch{{Events: []ledger.Event{ev}}}}
}

func TestFold(t *testing.T) {
	cases := []struct {
		name    string
		steps   []step
		want    redispatch.State
		wantErr error
	}{
		{"planned then dispatched", []step{{redispatch.EventDispatchPlanned, redispatch.Planned{Attempt: 1}}, {redispatch.EventDispatched, redispatch.Dispatched{Attempt: 1}}, {redispatch.EventDispatchPlanned, redispatch.Planned{Attempt: 2}}},
			redispatch.State{Planned: 2, Dispatched: 1}, nil},
		{"given up with an attempt owed", []step{{redispatch.EventDispatchPlanned, redispatch.Planned{Attempt: 1}}, {redispatch.EventGivenUp, redispatch.GivenUp{Reason: "budget"}}},
			redispatch.State{Planned: 1, GivenUp: true, Reason: "budget"}, nil},
		{"given up without an attempt", []step{{redispatch.EventGivenUp, redispatch.GivenUp{Reason: "rejected"}}}, redispatch.State{GivenUp: true, Reason: "rejected"}, nil},
		{"plan out of order", []step{{redispatch.EventDispatchPlanned, redispatch.Planned{Attempt: 2}}}, redispatch.State{}, ledger.ErrStateConflict},
		{"plan while one is owed", []step{{redispatch.EventDispatchPlanned, redispatch.Planned{Attempt: 1}}, {redispatch.EventDispatchPlanned, redispatch.Planned{Attempt: 2}}}, redispatch.State{}, ledger.ErrStateConflict},
		{"dispatched without a plan", []step{{redispatch.EventDispatched, redispatch.Dispatched{Attempt: 1}}}, redispatch.State{}, ledger.ErrStateConflict},
		{"dispatched twice", []step{{redispatch.EventDispatchPlanned, redispatch.Planned{Attempt: 1}}, {redispatch.EventDispatched, redispatch.Dispatched{Attempt: 1}}, {redispatch.EventDispatched, redispatch.Dispatched{Attempt: 1}}}, redispatch.State{}, ledger.ErrStateConflict},
		{"nothing after given up", []step{{redispatch.EventGivenUp, redispatch.GivenUp{}}, {redispatch.EventDispatchPlanned, redispatch.Planned{Attempt: 1}}}, redispatch.State{}, ledger.ErrStateConflict},
		{"unknown event", []step{{"other", nil}}, redispatch.State{}, ledger.ErrStateConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var state redispatch.State
			var err error
			for i, s := range tc.steps {
				c := commit(t, ledger.CommitSeq(i), s)
				if state, err = redispatch.Fold(state, &c); err != nil {
					break
				}
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("fold = %v, want %v", err, tc.wantErr)
			}
			if err == nil && state != tc.want {
				t.Fatalf("state = %+v, want %+v", state, tc.want)
			}
		})
	}
}

// memStore is an in-memory redispatch.Store for the helper tests.
type memStore struct {
	commits map[effect.AssignmentKey][]ledger.Commit
	epoch   map[effect.AssignmentKey]ledger.Epoch
}

func newMemStore() *memStore {
	return &memStore{commits: map[effect.AssignmentKey][]ledger.Commit{}, epoch: map[effect.AssignmentKey]ledger.Epoch{}}
}

func (m *memStore) Load(_ context.Context, k effect.AssignmentKey) (redispatch.State, ledger.Head, bool, error) {
	cs := m.commits[k]
	state := redispatch.State{Key: k}
	for i := range cs {
		var err error
		if state, err = redispatch.Fold(state, &cs[i]); err != nil {
			return redispatch.State{}, ledger.Head{}, false, err
		}
	}
	return state, ledger.Head{Next: ledger.CommitSeq(len(cs))}, len(cs) > 0, nil
}

func (m *memStore) Read(_ context.Context, k effect.AssignmentKey, from ledger.CommitSeq) ([]ledger.Commit, ledger.Head, error) {
	cs := m.commits[k]
	if int(from) > len(cs) {
		from = ledger.CommitSeq(len(cs))
	}
	return cs[from:], ledger.Head{Next: ledger.CommitSeq(len(cs))}, nil
}

func (m *memStore) Append(ctx context.Context, epoch ledger.Epoch, k effect.AssignmentKey, c ledger.Commit) error {
	for _, have := range m.commits[k] {
		if have.CommitID == c.CommitID {
			return ledger.ErrAlreadyApplied
		}
	}
	if epoch < m.epoch[k] {
		return ledger.ErrFenced
	}
	state, head, _, err := m.Load(ctx, k)
	if err != nil {
		return err
	}
	if c.Seq != head.Next {
		return ledger.ErrConflict
	}
	if _, err := redispatch.Fold(state, &c); err != nil {
		return err
	}
	m.epoch[k] = epoch
	m.commits[k] = append(m.commits[k], c)
	return nil
}

func TestPlanDispatchAndGiveUp(t *testing.T) {
	ctx := context.Background()
	s := newMemStore()
	// Plan returns the owed attempt until it is marked dispatched.
	for range 2 {
		if n, err := redispatch.Plan(ctx, s, 1, key, 1); err != nil || n != 1 {
			t.Fatalf("plan = %d %v, want the owed attempt 1", n, err)
		}
	}
	if err := redispatch.MarkDispatched(ctx, s, 1, key, 1, 1); err != nil {
		t.Fatal(err)
	}
	// Marking twice is a no-op; the next Plan is a new attempt.
	if err := redispatch.MarkDispatched(ctx, s, 1, key, 1, 1); err != nil {
		t.Fatal(err)
	}
	if n, err := redispatch.Plan(ctx, s, 1, key, 1); err != nil || n != 2 {
		t.Fatalf("plan = %d %v, want 2", n, err)
	}
	if err := redispatch.GiveUp(ctx, s, 1, key, "budget", 1); err != nil {
		t.Fatal(err)
	}
	// Giving up twice is a no-op; a stale epoch is fenced.
	if err := redispatch.GiveUp(ctx, s, 1, key, "again", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := redispatch.Plan(ctx, s, 0, effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e2"}, 1); err != nil {
		t.Fatal(err)
	}
	if err := redispatch.MarkDispatched(ctx, s, 0, key, 2, 1); !errors.Is(err, ledger.ErrFenced) && !errors.Is(err, ledger.ErrStateConflict) {
		t.Fatalf("dispatched under a stale epoch after given up = %v", err)
	}
	state, head, ok, err := s.Load(ctx, key)
	if err != nil || !ok || head.Next != 4 || state.Planned != 2 || state.Dispatched != 1 || state.Pending() != 2 || !state.GivenUp || state.Reason != "budget" {
		t.Fatalf("state = %+v head %+v ok:%v %v", state, head, ok, err)
	}
}
