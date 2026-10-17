package reconcile

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/redispatch"
)

// attempts is an in-memory dispatch ledger.
type attempts struct {
	commits map[effect.AssignmentKey][]ledger.Commit
	epoch   map[effect.AssignmentKey]ledger.Epoch
}

func newAttempts() *attempts {
	return &attempts{commits: map[effect.AssignmentKey][]ledger.Commit{}, epoch: map[effect.AssignmentKey]ledger.Epoch{}}
}

func (a *attempts) Load(_ context.Context, k effect.AssignmentKey) (redispatch.State, ledger.Head, bool, error) {
	cs := a.commits[k]
	state := redispatch.State{Key: k}
	for i := range cs {
		var err error
		if state, err = redispatch.Fold(state, &cs[i]); err != nil {
			return redispatch.State{}, ledger.Head{}, false, err
		}
	}
	return state, ledger.Head{Next: ledger.CommitSeq(len(cs))}, len(cs) > 0, nil
}

func (a *attempts) Read(_ context.Context, k effect.AssignmentKey, _ ledger.CommitSeq) ([]ledger.Commit, ledger.Head, error) {
	return a.commits[k], ledger.Head{Next: ledger.CommitSeq(len(a.commits[k]))}, nil
}

func (a *attempts) Append(ctx context.Context, epoch ledger.Epoch, k effect.AssignmentKey, c ledger.Commit) error {
	for _, have := range a.commits[k] {
		if have.CommitID == c.CommitID {
			return ledger.ErrAlreadyApplied
		}
	}
	if epoch < a.epoch[k] {
		return ledger.ErrFenced
	}
	state, head, _, err := a.Load(ctx, k)
	if err != nil {
		return err
	}
	if c.Seq != head.Next {
		return ledger.ErrConflict
	}
	if _, err := redispatch.Fold(state, &c); err != nil {
		return err
	}
	a.epoch[k] = epoch
	a.commits[k] = append(a.commits[k], c)
	return nil
}

// an effect the executor holds nothing for is handed over again
// within the redispatch budget. Each attempt is planned before the Dispatch
// and marked dispatched after it; an attempt left planned (a crash or a
// refusal between the two) is made again and costs no budget. A refusal the
// executor may lift leaves the target Executing for the next reconciliation;
// the budget's end and a definite rejection dispose.
func TestPlanRedispatchesMissingWithinBudget(t *testing.T) {
	ctx := context.Background()
	key := effect.AssignmentKey{Session: "s", OwnerKind: effect.OwnerTurn, OwnerID: string("r1"), Effect: "c1"}
	cases := []struct {
		name       string
		redispatch error
		prior      int  // attempts already dispatched
		pending    bool // one more attempt planned but not dispatched
		givenUp    bool
		want       Verdict
		planned    int
		dispatched int
		gaveUp     bool
		calls      int
	}{
		{"first redispatch", nil, 0, false, false, Redispatch, 1, 1, false, 1},
		{"refusal within budget", effect.ErrDispatchRetryable, 1, false, false, Defer, 2, 1, false, 1},
		{"lost response", effect.ErrDispatchUnknown, 0, false, false, Defer, 1, 0, false, 1},
		{"owed attempt is made again, not a new one", nil, 0, true, false, Redispatch, 1, 1, false, 1},
		{"owed attempt at the budget is still made", nil, 1, true, false, Redispatch, 2, 2, false, 1},
		{"budget exhausted", nil, 2, false, false, Dispose, 2, 2, true, 0},
		{"already given up", nil, 0, false, true, Dispose, 0, 0, true, 0},
		{"definite rejection", errors.New("run no longer executes the effect"), 0, false, false, Dispose, 1, 0, true, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newAttempts()
			for i := 1; i <= tc.prior; i++ {
				n, err := redispatch.Plan(ctx, store, 1, key, 1)
				if err != nil {
					t.Fatal(err)
				}
				if err := redispatch.MarkDispatched(ctx, store, 1, key, n, 1); err != nil {
					t.Fatal(err)
				}
			}
			if tc.pending {
				if _, err := redispatch.Plan(ctx, store, 1, key, 1); err != nil {
					t.Fatal(err)
				}
			}
			if tc.givenUp {
				if err := redispatch.GiveUp(ctx, store, 1, key, "before", 1); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			r := &Reconciler{Executions: &fakePort{state: effect.AttachmentMissing}, Missing: RedispatchMissing, Attempts: store, Epoch: 1, MaxRedispatches: 2,
				Redispatch: func(_ context.Context, k effect.AssignmentKey) error {
					calls++
					if k != key {
						t.Fatalf("redispatch of %+v", k)
					}
					return tc.redispatch
				}}
			decisions, err := r.Plan(ctx, "s", executingModel("c1"))
			if err != nil || len(decisions) != 1 {
				t.Fatalf("plan = %+v %v", decisions, err)
			}
			d := decisions[0]
			wantRetry := tc.calls > 0 && errors.Is(tc.redispatch, effect.ErrDispatchRetryable)
			if d.Verdict != tc.want || (d.Recovery != nil) != (tc.want == Dispose) || calls != tc.calls || d.Retry != wantRetry {
				t.Fatalf("decision = %+v calls=%d, want %s calls=%d retry=%v", d, calls, tc.want, tc.calls, wantRetry)
			}
			state, _, _, err := store.Load(ctx, key)
			if err != nil || state.Planned != tc.planned || state.Dispatched != tc.dispatched || state.GivenUp != tc.gaveUp {
				t.Fatalf("ledger = %+v %v, want planned=%d dispatched=%d givenUp=%v", state, err, tc.planned, tc.dispatched, tc.gaveUp)
			}
		})
	}
}

// A disconnected recovery pass leaves a durable pending attempt, not a
// background retry. A new activation explicitly plans again and reuses it.
func TestRedispatchPassCanResumePendingAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	key := effect.AssignmentKey{Session: "s", OwnerKind: effect.OwnerTurn, OwnerID: string("r1"), Effect: "c1"}
	attemptStore := newAttempts()
	port := &fakePort{state: effect.AttachmentMissing}
	calls := 0
	first := &Reconciler{Executions: port, Missing: RedispatchMissing, Attempts: attemptStore, Epoch: 1, MaxRedispatches: 1,
		Redispatch: func(context.Context, effect.AssignmentKey) error { calls++; return effect.ErrDispatchRetryable }}
	ds, err := first.Plan(ctx, "s", executingModel("c1"))
	if err != nil || len(ds) != 1 || ds[0].Verdict != Defer || !ds[0].Retry || calls != 1 {
		t.Fatalf("first pass=%+v err=%v calls=%d", ds, err, calls)
	}
	cancel()
	if _, err := first.Plan(ctx, "s", executingModel("c1")); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("disconnected pass err=%v calls=%d", err, calls)
	}
	state, _, _, err := attemptStore.Load(context.Background(), key)
	if err != nil || state.Planned != 1 || state.Dispatched != 0 || state.Pending() != 1 || state.GivenUp {
		t.Fatalf("pending ledger=%+v err=%v", state, err)
	}
	second := &Reconciler{Executions: port, Missing: RedispatchMissing, Attempts: attemptStore, Epoch: 2, MaxRedispatches: 1,
		Redispatch: func(context.Context, effect.AssignmentKey) error { calls++; return nil }}
	ds, err = second.Plan(context.Background(), "s", executingModel("c1"))
	state, _, _, loadErr := attemptStore.Load(context.Background(), key)
	if err != nil || loadErr != nil || len(ds) != 1 || ds[0].Verdict != Redispatch || ds[0].Retry || calls != 2 || state.Planned != 1 || state.Dispatched != 1 || state.GivenUp {
		t.Fatalf("resumed pass=%+v err=%v ledger=%+v loadErr=%v calls=%d", ds, err, state, loadErr, calls)
	}
	// The accepted execution is observed in another finite pass. No attempt
	// is charged and no Outcome is consumed implicitly.
	port.state = effect.AttachmentActive
	ds, err = second.Plan(context.Background(), "s", executingModel("c1"))
	if err != nil || len(ds) != 1 || ds[0].Verdict != Keep || calls != 2 || port.reads != 0 {
		t.Fatalf("accepted pass=%+v err=%v calls=%d reads=%d", ds, err, calls, port.reads)
	}
}

// The missing policy is explicit: RedispatchMissing without its ports is a
// configuration error, and ports alone never select redispatch.
func TestMissingPolicyIsExplicit(t *testing.T) {
	ctx := context.Background()
	calls := 0
	redispatch := func(context.Context, effect.AssignmentKey) error { calls++; return nil }
	r := &Reconciler{Executions: &fakePort{state: effect.AttachmentMissing}, Missing: RedispatchMissing, Redispatch: redispatch}
	if _, err := r.Plan(ctx, "s", executingModel("c1")); !errors.Is(err, ErrMissingPolicyPorts) {
		t.Fatalf("redispatch without a dispatch ledger = %v, want ErrMissingPolicyPorts", err)
	}
	r = &Reconciler{Executions: &fakePort{state: effect.AttachmentMissing}, Redispatch: redispatch, Attempts: newAttempts()}
	decisions, err := r.Plan(ctx, "s", executingModel("c1"))
	if err != nil || len(decisions) != 1 || decisions[0].Verdict != Dispose || calls != 0 {
		t.Fatalf("ports without the policy: %+v %v calls=%d, want dispose without a redispatch", decisions, err, calls)
	}
}
