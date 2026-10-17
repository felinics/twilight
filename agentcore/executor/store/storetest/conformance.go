package storetest

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/executor/protocol"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

// Fixture is one adapter under test. Store is a handle over a fresh, empty
// store; Reopen returns a second handle over the same store, standing for a
// second process, nil meaning the adapter has no notion of a handle and
// Store is shared. Advance moves the clock leases are judged by.
type Fixture struct {
	Store   executionstore.Store
	Reopen  func(t *testing.T) executionstore.Store
	Advance func(time.Duration)
}

// Factory builds a fresh Fixture for one subtest.
type Factory func(t *testing.T) Fixture

// Seeder is the test affordance every adapter offers next to the Store: a
// ledger written directly in the shape executionstore.SeedCommits plans.
type Seeder interface {
	Seed(context.Context, executionstore.Execution) error
}

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("ledger", func(t *testing.T) { testLedger(t, factory(t)) })
	t.Run("seed_aborted", func(t *testing.T) { testSeedAborted(t, factory(t)) })
	t.Run("directory", func(t *testing.T) { testDirectory(t, factory(t)) })
	for _, mode := range []string{"never_claimed", "live_foreign", "expired"} {
		t.Run("controller_cancel/"+mode, func(t *testing.T) { testControllerCancel(t, factory(t), mode) })
	}
}

func assignment(id turn.EffectID) effect.Assignment {
	return effect.Assignment{Session: "s", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), StepID: "step", Effect: id,
		Body: effect.ModelAssignment{Model: "m", RequestDigest: "sha256:req"}}
}

func accept(t *testing.T, s executionstore.Store, a effect.Assignment) {
	t.Helper()
	ev, err := ledger.NewEvent(executionstore.EventExecutionAccepted, 0, executionstore.Accepted{Assignment: a})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), executionstore.Lease{}, a.Key(), ledger.Commit{CommitID: executionstore.AcceptCommitID(a.Key()), Batches: []ledger.EventBatch{{Events: []ledger.Event{ev}}}}); err != nil {
		t.Fatal(err)
	}
}

func step(s executionstore.Store, lease executionstore.Lease, seq ledger.CommitSeq, typ ledger.EventType, payload any) error {
	ev, err := ledger.NewEvent(typ, 0, payload)
	if err != nil {
		return err
	}
	return s.Append(context.Background(), lease, lease.Key, ledger.Commit{Seq: seq, CommitID: executionstore.DeriveCommitID(lease.Key, "test", string(typ)+"/"+strconv.FormatUint(uint64(seq), 10)), Batches: []ledger.EventBatch{{Events: []ledger.Event{ev}}}})
}

// One execution ledger, two handles: the second handle stands for a second
// process. Commits appended through one are read through the other, leases
// fence across them, and the state machine, identity and lease checks
// answer with the store's sentinel errors.
func testLedger(t *testing.T, f Fixture) { //nolint:gocyclo // one scenario, checked step by step
	ctx := context.Background()
	a := f.Store
	b := a
	if f.Reopen != nil {
		b = f.Reopen(t)
	}
	asg := assignment("e1")
	key := asg.Key()
	accept(t, a, asg)
	// A replayed acceptance is recognised by its identity and not written
	// again; the Worker tells two Assignments apart by reading the ledger.
	ev, _ := ledger.NewEvent(executionstore.EventExecutionAccepted, 0, executionstore.Accepted{Assignment: asg})
	if err := b.Append(ctx, executionstore.Lease{}, key, ledger.Commit{CommitID: executionstore.AcceptCommitID(key), Batches: []ledger.EventBatch{{Events: []ledger.Event{ev}}}}); !errors.Is(err, executionstore.ErrAlreadyApplied) {
		t.Fatalf("replayed acceptance through the other handle = %v, want already applied", err)
	}
	if state, _, ok, err := b.Load(ctx, key); err != nil || !ok || state.Assignment.Key() != key {
		t.Fatalf("load after replay = %+v ok:%v %v", state, ok, err)
	}
	if _, _, ok, err := b.Load(ctx, assignment("absent").Key()); err != nil || ok {
		t.Fatalf("load of an unknown key = ok:%v %v, want a proven absence", ok, err)
	}
	// A fenced event without a lease is refused before any lease exists.
	if err := step(a, executionstore.Lease{Key: key}, 1, executionstore.EventExecutionStarted, nil); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("fenced event without a lease = %v, want lease lost", err)
	}
	// worker-a acquires through handle a; worker-b cannot while the lease
	// lives, and a's fenced commits go through.
	held, ok, err := a.Acquire(ctx, key, "worker-a", time.Minute)
	if err != nil || !ok || held.Epoch != 1 || held.Owner != "worker-a" || held.Key != key {
		t.Fatalf("acquire = %+v ok:%v %v", held, ok, err)
	}
	if _, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire under a live foreign lease = ok:%v %v", ok, err)
	}
	// The holder acquiring again keeps its Epoch and records nothing.
	if again, ok, err := a.Acquire(ctx, key, "worker-a", time.Minute); err != nil || !ok || again.Epoch != 1 {
		t.Fatalf("re-acquire by the holder = %+v ok:%v %v", again, ok, err)
	}
	if _, head, _, err := a.Load(ctx, key); err != nil || head.Next != 2 {
		t.Fatalf("head after acquire = %+v %v, want the acceptance and one claim", head, err)
	}
	if err := step(a, held, 2, executionstore.EventExecutionStarted, nil); err != nil {
		t.Fatal(err)
	}
	if err := step(a, held, 2, executionstore.EventExecutionStarted, nil); !errors.Is(err, executionstore.ErrAlreadyApplied) {
		t.Fatalf("replayed commit = %v, want already applied", err)
	}
	if err := step(a, held, 2, executionstore.EventCancelRequested, nil); !errors.Is(err, executionstore.ErrConflict) {
		t.Fatalf("another commit at a taken seq = %v, want sequence conflict", err)
	}
	if err := step(a, held, 3, executionstore.EventExecutionStarted, nil); !errors.Is(err, executionstore.ErrStateConflict) {
		t.Fatalf("illegal step = %v, want state conflict", err)
	}
	foreign := executionstore.Lease{Key: key, Owner: "worker-b", Epoch: 1}
	if err := step(b, foreign, 3, executionstore.EventExecutionRunning, nil); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("commit by a non-owner = %v, want lease lost", err)
	}
	if lease, ok, err := b.LeaseOf(ctx, key); err != nil || !ok || lease.Owner != "worker-a" || lease.Epoch != 1 {
		t.Fatalf("lease read through the other handle = %+v %v %v", lease, ok, err)
	}
	if _, _, err := b.LeaseOf(ctx, assignment("absent").Key()); !errors.Is(err, effect.ErrExecutionNotFound) {
		t.Fatalf("lease of an unknown key = %v, want not found", err)
	}
	// Renew moves the expiry; after it the lease outlives the original ttl.
	f.Advance(45 * time.Second)
	if err := a.Renew(ctx, held, time.Minute); err != nil {
		t.Fatalf("renew = %v", err)
	}
	f.Advance(30 * time.Second)
	if _, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire under a renewed lease = ok:%v %v", ok, err)
	}
	// The lease expires: worker-b takes over with a higher Epoch, fencing a.
	f.Advance(2 * time.Minute)
	taken, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute)
	if err != nil || !ok || taken.Epoch != 2 || taken.Owner != "worker-b" {
		t.Fatalf("takeover = %+v ok:%v %v", taken, ok, err)
	}
	if err := a.Renew(ctx, held, time.Minute); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("renew by the fenced owner = %v, want lease lost", err)
	}
	if err := step(a, held, 4, executionstore.EventExecutionRunning, nil); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("fenced commit = %v, want lease lost", err)
	}
	if err := step(b, taken, 4, executionstore.EventExecutionRunning, nil); err != nil {
		t.Fatal(err)
	}
	got, head, ok, err := a.Load(ctx, key)
	if err != nil || !ok || got.State != effect.ExecutionRunning || got.Lease.Owner != "worker-b" || got.Lease.Epoch != 2 || head.Next != 5 {
		t.Fatalf("fold through the first handle = %+v head %+v ok:%v %v", got, head, ok, err)
	}
	// Settlement ends the execution: leases are refused, and only the
	// acknowledgement may follow.
	out := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key}
	if err := step(b, taken, 5, executionstore.EventExecutionSettled, executionstore.Settled{State: effect.ExecutionCompleted, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire of a settled execution = ok:%v %v", ok, err)
	}
	if err := b.Renew(ctx, taken, time.Minute); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("renew of a settled execution = %v, want lease lost", err)
	}
	if _, _, err := b.Acquire(ctx, assignment("nope").Key(), "worker-b", time.Minute); !errors.Is(err, effect.ErrExecutionNotFound) {
		t.Fatalf("acquire of an unknown key = %v, want not found", err)
	}
	if err := b.Renew(ctx, executionstore.Lease{Key: assignment("nope").Key(), Owner: "worker-b", Epoch: 1}, time.Minute); !errors.Is(err, effect.ErrExecutionNotFound) {
		t.Fatalf("renew of an unknown key = %v, want not found", err)
	}
	if err := step(a, executionstore.Lease{Key: key}, 6, executionstore.EventOutcomeAcknowledged, nil); err != nil {
		t.Fatal(err)
	}
	got, _, _, err = b.Load(ctx, key)
	if err != nil || !got.Acknowledged || got.Outcome == nil || got.Assignment.Body == nil || got.State != effect.ExecutionCompleted {
		t.Fatalf("acknowledged fold = %+v %v", got, err)
	}
	// Read serves a catch-up from any position with the ledger's head.
	tail, head, err := a.Read(ctx, key, 4)
	if err != nil || len(tail) != 3 || tail[0].Seq != 4 || head.Next != 7 {
		t.Fatalf("read from 4 = %d commits head %+v %v", len(tail), head, err)
	}
	if tail, head, err := a.Read(ctx, key, 7); err != nil || len(tail) != 0 || head.Next != 7 {
		t.Fatalf("read from the head = %d commits head %+v %v, want none with the ledger's head", len(tail), head, err)
	}
	if tail, head, err := a.Read(ctx, assignment("absent").Key(), 0); err != nil || len(tail) != 0 || head.Next != 0 {
		t.Fatalf("read of an unknown key = %d commits head %+v %v, want an empty ledger", len(tail), head, err)
	}
	accept(t, b, assignment("e2"))
	if owned, err := a.ListOwned(ctx, "nobody"); err != nil || len(owned) != 0 {
		t.Fatalf("ListOwned(nobody) = %d %v, want none", len(owned), err)
	}
	if owned, err := a.ListOwned(ctx, "worker-b"); err != nil || len(owned) != 1 || owned[0] != key {
		t.Fatalf("ListOwned(worker-b) = %+v %v, want the leased key", owned, err)
	}
}

// A controller command is state-machine constrained but unfenced. It does
// not create/renew a lease and cannot grant authority to execute: even a
// subsequent legitimate claimant may only reconcile cancellation.
func testControllerCancel(t *testing.T, f Fixture, mode string) {
	ctx := context.Background()
	asg := assignment("cancel")
	key := asg.Key()
	accept(t, f.Store, asg)
	var held executionstore.Lease
	if mode != "never_claimed" {
		var err error
		held, _, err = f.Store.Acquire(ctx, key, "worker", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
	}
	if mode == "expired" {
		f.Advance(2 * time.Minute)
	}
	before, _, _, _ := f.Store.Load(ctx, key)
	_, head, _, _ := f.Store.Load(ctx, key)
	if err := step(f.Store, executionstore.Lease{Key: key}, head.Next, executionstore.EventCancelRequested, nil); err != nil {
		t.Fatalf("unfenced controller cancellation = %v", err)
	}
	state, head, _, err := f.Store.Load(ctx, key)
	if err != nil || state.State != effect.ExecutionCancelRequested || state.Lease != before.Lease {
		t.Fatalf("cancel changed execution authority: %+v, %v", state, err)
	}
	if err := step(f.Store, executionstore.Lease{Key: key}, head.Next, executionstore.EventExecutionStarted, nil); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("controller Start = %v, want lease lost", err)
	}
	if mode != "live_foreign" {
		held, _, err = f.Store.Acquire(ctx, key, "observer", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, head, _, _ = f.Store.Load(ctx, key)
	for _, typ := range []ledger.EventType{executionstore.EventExecutionStarted, executionstore.EventExecutionRunning, executionstore.EventExecutionRestarted, executionstore.EventExecutionBound} {
		var payload any
		if typ == executionstore.EventExecutionRestarted {
			payload = executionstore.Restarted{Ref: "new"}
		}
		if typ == executionstore.EventExecutionBound {
			payload = executionstore.Bound{Ref: executionstore.ExecutionRef{Provider: "p", Ref: "new"}}
		}
		if err := step(f.Store, held, head.Next, typ, payload); !errors.Is(err, executionstore.ErrStateConflict) {
			t.Fatalf("%s after cancellation = %v, want state conflict", typ, err)
		}
	}
	out := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, Cancelled: true}
	if err := step(f.Store, executionstore.Lease{Key: key}, head.Next, executionstore.EventExecutionSettled, executionstore.Settled{State: effect.ExecutionCancelled, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	_, head, _, _ = f.Store.Load(ctx, key)
	if err := step(f.Store, executionstore.Lease{Key: key}, head.Next, executionstore.EventCancelRequested, nil); !errors.Is(err, executionstore.ErrStateConflict) {
		t.Fatalf("cancel after terminal = %v, want state conflict", err)
	}
}

// A seeded aborted key has the ledger a real Abort leaves: the
// tombstone alone at Seq 0 under the abort identity, so a Dispatch's
// acceptance is already applied against it and no lease exists. A seeded
// live key carries the lease it was given.
func testSeedAborted(t *testing.T, f Fixture) {
	ctx := context.Background()
	s, ok := f.Store.(Seeder)
	if !ok {
		t.Fatalf("%T offers no Seed; every adapter seeds through executionstore.SeedCommits", f.Store)
	}
	asg := assignment("aborted")
	key := asg.Key()
	if err := s.Seed(ctx, executionstore.Execution{ExecutionState: executionstore.ExecutionState{Assignment: asg, State: effect.ExecutionAborted}, Lease: executionstore.Lease{Owner: "ignored", Epoch: 3}}); err != nil {
		t.Fatal(err)
	}
	state, head, ok, err := f.Store.Load(ctx, key)
	if err != nil || !ok || !state.Aborted() || state.Outcome != nil || state.Assignment.Effect != "" || head.Next != 1 {
		t.Fatalf("seeded aborted key = %+v head=%+v ok:%v %v, want a lone tombstone", state, head, ok, err)
	}
	ev, _ := ledger.NewEvent(executionstore.EventExecutionAccepted, 0, executionstore.Accepted{Assignment: asg})
	if err := f.Store.Append(ctx, executionstore.Lease{}, key, ledger.Commit{Seq: 0, CommitID: executionstore.AcceptCommitID(key), Batches: []ledger.EventBatch{{Events: []ledger.Event{ev}}}}); !errors.Is(err, executionstore.ErrConflict) {
		t.Fatalf("acceptance against the tombstone = %v, want the Seq 0 conflict a live Abort produces", err)
	}
	if owned, err := f.Store.ListOwned(ctx, "ignored"); err != nil || len(owned) != 0 {
		t.Fatalf("ListOwned for a tombstone = %+v %v, want none", owned, err)
	}
	live := assignment("live")
	lease := executionstore.Lease{Key: live.Key(), Owner: "worker-a", Epoch: 2, UntilUnixMilli: 1 << 60}
	if err := s.Seed(ctx, executionstore.Execution{ExecutionState: executionstore.ExecutionState{Assignment: live, State: effect.ExecutionRunning}, Lease: lease}); err != nil {
		t.Fatal(err)
	}
	if err := s.Seed(ctx, executionstore.Execution{ExecutionState: executionstore.ExecutionState{Assignment: live, State: effect.ExecutionRunning}}); err == nil {
		t.Fatal("seeding a key twice succeeded")
	}
	got, _, ok, err := f.Store.Load(ctx, live.Key())
	if err != nil || !ok || got.State != effect.ExecutionRunning || got.Lease != lease {
		t.Fatalf("seeded live key = %+v ok:%v %v, want running under %+v", got, ok, err, lease)
	}
	if err := step(f.Store, lease, 4, executionstore.EventCancelRequested, nil); err != nil {
		t.Fatalf("commit under the seeded lease = %v", err)
	}
	if owned, err := f.Store.ListOwned(ctx, "worker-a"); err != nil || len(owned) != 1 || owned[0] != live.Key() {
		t.Fatalf("ListOwned for the seeded lease = %+v %v", owned, err)
	}
}
