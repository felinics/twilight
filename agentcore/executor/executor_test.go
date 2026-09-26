package executor_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
)

type testBackend struct {
	mu       sync.Mutex
	calls    int
	last     effect.Assignment
	outcomes map[effect.AssignmentKey]chan effect.Outcome
}

func newTestBackend() *testBackend {
	return &testBackend{outcomes: make(map[effect.AssignmentKey]chan effect.Outcome)}
}

func (b *testBackend) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (b *testBackend) Dispatch(_ context.Context, a effect.Assignment) error {
	b.mu.Lock()
	b.calls++
	b.last = a
	if b.outcomes[a.Key()] == nil {
		b.outcomes[a.Key()] = make(chan effect.Outcome, 1)
	}
	ch := b.outcomes[a.Key()]
	b.mu.Unlock()
	go func() {
		ch <- effect.Outcome{Key: a.Key(), Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "ok"}}}
	}()
	return nil
}
func (b *testBackend) Attach(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outcomes[key] == nil {
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning, BackendAttached: true}, nil
}
func (b *testBackend) Abort(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	att, err := b.Attach(ctx, key)
	if err == nil && att.State == effect.AttachmentMissing {
		att = effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}
	}
	return att, err
}
func (b *testBackend) GetStatus(_ context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outcomes[key] == nil {
		return effect.ExecutionNotFound, effect.ErrExecutionNotFound
	}
	return effect.ExecutionRunning, nil
}
func (b *testBackend) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	b.mu.Lock()
	ch := b.outcomes[key]
	b.mu.Unlock()
	if ch == nil {
		return effect.Outcome{}, effect.ErrExecutionNotFound
	}
	select {
	case out := <-ch:
		return out, nil
	case <-ctx.Done():
		return effect.Outcome{}, ctx.Err()
	}
}
func (b *testBackend) Cancel(context.Context, effect.AssignmentKey) error { return nil }

// routes serves every Assignment from one Port-shaped fake under the "test"
// provider.
func routes(p effect.ExecutionPort) []executor.Route {
	return []executor.Route{executor.Default("test", executor.PortBackend(p))}
}

// refBackend implements the Backend contract directly and counts its
// lifecycle calls; the Ref it prepares is fixed.
type refBackend struct {
	*testBackend
	mu       sync.Mutex
	prepared int
	started  int
	restarts int
	startRef string
	// attach, when set, scripts Attach's answer instead of the Port's.
	attach effect.AttachmentState
}

func (b *refBackend) setAttach(state effect.AttachmentState) {
	b.mu.Lock()
	b.attach = state
	b.mu.Unlock()
}

func (b *refBackend) Prepare(context.Context, effect.Assignment) (string, error) {
	b.mu.Lock()
	b.prepared++
	b.mu.Unlock()
	return "execution-1", nil
}

func (b *refBackend) Start(ctx context.Context, ref string, a effect.Assignment) error {
	b.mu.Lock()
	b.started++
	b.startRef = ref
	b.mu.Unlock()
	return b.Dispatch(ctx, a)
}

func (b *refBackend) Restart(context.Context, string, effect.Assignment) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.restarts++
	return fmt.Sprintf("execution-%d", b.restarts+1), nil
}

func (b *refBackend) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	b.mu.Lock()
	scripted := b.attach
	b.mu.Unlock()
	if scripted != "" {
		return effect.Attachment{State: scripted}, nil
	}
	return b.testBackend.Attach(ctx, b.lastKey())
}

func (b *refBackend) Status(ctx context.Context, ref string) (effect.ExecutionStatus, error) {
	return b.GetStatus(ctx, b.lastKey())
}

func (b *refBackend) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	return b.GetOutcome(ctx, b.lastKey())
}

func (b *refBackend) Cancel(ctx context.Context, ref string) error {
	return b.testBackend.Cancel(ctx, b.lastKey())
}

func (b *testBackend) lastKey() effect.AssignmentKey {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.last.Key()
}

// awaitOutcome is the tests' blocking read: GetOutcome is a plain query, so
// a test that wants the eventual Outcome waits with a Watcher over the port,
// which uses its settlement stream and a short read interval otherwise.
func awaitOutcome(ctx context.Context, port effect.ExecutionPort, key effect.AssignmentKey) (effect.Outcome, error) {
	w := &effect.Watcher{Port: port, Poll: 5 * time.Millisecond}
	defer w.Close()
	return w.Await(ctx, key)
}

func testAssignment() effect.Assignment {
	request := model.ModelRequest{Model: "m"}
	digest, err := schema.Canonical().DigestRequest(request)
	if err != nil {
		panic(err)
	}
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", Effect: "effect",
		Body: effect.ModelAssignment{Model: "m", Request: &request, RequestDigest: digest}}
}

func TestWorkerIdempotentAndOutcome(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, storetest.NewMap(nil), routes(backend))
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("backend calls = %d, want 1", calls)
	}
	out, err := awaitOutcome(ctx, worker, a.Key())
	if err != nil || modelText(out) != "ok" {
		t.Fatalf("outcome = %+v, %v", out, err)
	}
	out2, err := awaitOutcome(ctx, worker, a.Key())
	if err != nil || modelText(out2) != "ok" {
		t.Fatalf("replayed outcome = %+v, %v", out2, err)
	}
}

// A replay of the same Assignment against an execution that has started
// leaves it to its lease holder, live or expired: recovery of a started
// execution is RecoverExecution's, asked for by the Owner that observed it
// orphaned (RUN-EXE-3).
func TestWorkerDispatchReplayPreservesExistingExecution(t *testing.T) {
	for _, state := range []effect.ExecutionStatus{effect.ExecutionDispatching, effect.ExecutionRunning, effect.ExecutionCancelRequested} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			records := storetest.NewMap(nil)
			a := testAssignment()
			r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: state}, Lease: store.Lease{Owner: "expired-worker", Epoch: 4, UntilUnixMilli: 1}}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := newTestBackend()
			worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "new-worker"})
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.Dispatch(ctx, a); err != nil {
				t.Fatal(err)
			}
			got, _, _, err := records.Load(ctx, a.Key())
			if err != nil || got.State != r.State || got.Lease.Owner != r.Lease.Owner || got.Lease.Epoch != r.Lease.Epoch {
				t.Fatalf("replay changed execution: %+v, %v", got, err)
			}
			backend.mu.Lock()
			calls := backend.calls
			backend.mu.Unlock()
			if calls != 0 {
				t.Fatalf("replay dispatched %d backend calls", calls)
			}
		})
	}
}

// A replay of the same Assignment against an acceptance that never started
// and that no live lease holds continues it: the Dispatch that wrote the
// acceptance failed before Start, and the replay is that request again
// (RUN-EXE-3). Under a live lease the replay starts nothing; the holder is
// about to.
func TestWorkerDispatchReplayContinuesUnstartedAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lease store.Lease
		runs  bool
	}{
		{"no lease", store.Lease{}, true},
		{"expired lease", store.Lease{Owner: "dead-worker", Epoch: 4, UntilUnixMilli: 1}, true},
		{"live lease", store.Lease{Owner: "live-worker", Epoch: 4, UntilUnixMilli: time.Now().Add(time.Hour).UnixMilli()}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			records := storetest.NewMap(nil)
			a := testAssignment()
			if err := records.Seed(ctx, store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionAccepted}, Lease: tc.lease}); err != nil {
				t.Fatal(err)
			}
			backend := newTestBackend()
			worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "new-worker"})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if err := worker.Dispatch(ctx, a); err != nil {
				t.Fatal(err)
			}
			if !tc.runs {
				got, _, _, err := records.Load(ctx, a.Key())
				if err != nil || got.State != effect.ExecutionAccepted || got.Lease.Owner != tc.lease.Owner {
					t.Fatalf("replay under a live lease changed execution: %+v, %v", got, err)
				}
				return
			}
			readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			out, err := awaitOutcome(readCtx, worker, a.Key())
			if err != nil || modelText(out) != "ok" {
				t.Fatalf("continued outcome = %+v, %v", out, err)
			}
			backend.mu.Lock()
			calls := backend.calls
			backend.mu.Unlock()
			if calls != 1 {
				t.Fatalf("replay dispatched %d backend calls, want 1", calls)
			}
		})
	}
}

type uncertainDispatchBackend struct {
	*testBackend
	ready chan struct{}
}

func (b *uncertainDispatchBackend) Dispatch(_ context.Context, a effect.Assignment) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.last = a
	return effect.ErrDispatchUnknown
}

func (b *uncertainDispatchBackend) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	select {
	case <-b.ready:
		return effect.Outcome{Key: key, Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "accepted before response was lost"}}}, nil
	case <-ctx.Done():
		return effect.Outcome{}, ctx.Err()
	}
}

func TestWorkerUncertainDispatchPreservesExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	records := storetest.NewMap(nil)
	backend := &uncertainDispatchBackend{testBackend: newTestBackend(), ready: make(chan struct{})}
	defer close(backend.ready)
	worker, err := executor.NewWorker(ctx, records, routes(backend))
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(ctx, a); !errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("dispatch error = %v", err)
	}
	r, _, _, err := records.Load(ctx, a.Key())
	if err != nil || r.State != effect.ExecutionDispatching || r.Outcome != nil {
		t.Fatalf("uncertain dispatch changed execution: %+v, %v", r, err)
	}
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatalf("acceptance replay = %v", err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("backend calls = %d, want 1", calls)
	}
	select {
	case backend.ready <- struct{}{}:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	out, err := awaitOutcome(ctx, worker, a.Key())
	if err != nil || modelText(out) != "accepted before response was lost" {
		t.Fatalf("eventual outcome = %+v, %v", out, err)
	}
}

// holdBackend accepts a Dispatch and never produces its Outcome.
type holdBackend struct{ *testBackend }

func (b *holdBackend) Dispatch(_ context.Context, a effect.Assignment) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outcomes[a.Key()] == nil {
		b.outcomes[a.Key()] = make(chan effect.Outcome, 1)
	}
	return nil
}

// Close stops every watcher and heartbeat, and returns while a backend
// execution is still running; the record keeps its lease for another
// incarnation.
func TestWorkerCloseStopsGoroutines(t *testing.T) {
	ctx := context.Background()
	records := storetest.NewMap(nil)
	backend := &holdBackend{newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", executor.PortBackend(backend))},
		executor.WorkerOptions{ID: "worker-c", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { worker.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return with a watcher in flight")
	}
	r, _, ok, err := records.Load(ctx, a.Key())
	if err != nil || !ok || r.State.Terminal() {
		t.Fatalf("record after close = %+v ok=%v %v, want a live non-terminal record", r, ok, err)
	}
}

// A record whose ExecutionRef names a provider this Worker has no Backend
// for is left untouched: RecoverExecution and GetStatus report ErrUnknownProvider and
// no backend is called (RUN-EXE-10).
func TestWorkerRecoverExecutionRefusesUnknownProvider(t *testing.T) {
	ctx := context.Background()
	records := storetest.NewMap(nil)
	a := testAssignment()
	r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionRunning, ExecutionRef: store.ExecutionRef{Provider: "elsewhere", Ref: "existing-job"}}, Lease: store.Lease{Owner: "expired-worker", Epoch: 3, UntilUnixMilli: 1}}
	if err := records.Seed(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "new-worker"})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RecoverExecution(ctx, a.Key()); !errors.Is(err, executor.ErrUnknownProvider) {
		t.Fatalf("takeover = %v, want ErrUnknownProvider", err)
	}
	if _, err := worker.GetStatus(ctx, a.Key()); !errors.Is(err, executor.ErrUnknownProvider) {
		t.Fatalf("status = %v, want ErrUnknownProvider", err)
	}
	got, _, _, err := records.Load(ctx, a.Key())
	if err != nil || got.Lease.Owner != r.Lease.Owner || got.Lease.Epoch != r.Lease.Epoch || got.ExecutionRef != r.ExecutionRef {
		t.Fatalf("unknown-provider takeover changed the record: %+v, %v", got, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 0 {
		t.Fatalf("unknown-provider takeover dispatched %d calls", calls)
	}
}

// Dispatch selects the Backend once, prepares the Ref and persists it with the
// record before Start; Start receives the persisted Ref (RUN-EXE-9/10).
func TestWorkerPersistsExecutionRefBeforeStart(t *testing.T) {
	ctx := context.Background()
	records := storetest.NewMap(nil)
	backend := &refBackend{testBackend: newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)},
		executor.WorkerOptions{ID: "worker-a", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	record, _, ok, err := records.Load(ctx, a.Key())
	if err != nil || !ok {
		t.Fatalf("record = %+v, ok=%v, err=%v", record, ok, err)
	}
	if record.ExecutionRef != (store.ExecutionRef{Provider: "ref", Ref: "execution-1"}) {
		t.Fatalf("execution ref = %+v", record.ExecutionRef)
	}
	backend.mu.Lock()
	prepared, started, startRef := backend.prepared, backend.started, backend.startRef
	backend.mu.Unlock()
	if prepared != 1 || started != 1 || startRef != "execution-1" {
		t.Fatalf("backend calls = prepared:%d started:%d ref:%q, want 1/1/execution-1", prepared, started, startRef)
	}
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	prepared, started = backend.prepared, backend.started
	backend.mu.Unlock()
	// A replay is answered from the ledger: no second Prepare, no second Start.
	if prepared != 1 || started != 1 {
		t.Fatalf("replay prepared %d times and started %d times, want neither a second prepare nor a second start", prepared, started)
	}
}

func TestExecutionStoreFencesRecoverExecution(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := storetest.NewMap(func() time.Time { return now })
	a := testAssignment()
	openLedger(t, records, a)
	first, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired || first.Epoch != 1 {
		t.Fatalf("first acquire = %+v, acquired=%v, err=%v", first, acquired, err)
	}
	now = base.Add(500 * time.Millisecond)
	if _, acquired, err := records.Acquire(ctx, a.Key(), "worker-b", time.Second); err != nil || acquired {
		t.Fatalf("live lease acquire = acquired=%v, err=%v; want rejected", acquired, err)
	}
	now = base.Add(2 * time.Second)
	second, acquired, err := records.Acquire(ctx, a.Key(), "worker-b", time.Second)
	if err != nil || !acquired || second.Epoch != 2 {
		t.Fatalf("takeover = %+v, acquired=%v, err=%v", second, acquired, err)
	}
	if err := appendStep(records, first, 3, store.EventExecutionStarted); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale commit = %v, want ErrLeaseLost", err)
	}
	// The ledger recorded both claims, in order, under their epochs.
	commits, head, err := records.Read(ctx, a.Key(), 0)
	if err != nil || head.Next != 3 || len(commits) != 3 {
		t.Fatalf("ledger = %d commits head %+v %v, want accept + two claims", len(commits), head, err)
	}
	for i, want := range []store.EventType{store.EventExecutionAccepted, store.EventExecutionClaimed, store.EventExecutionClaimed} {
		if commits[i].Events[0].Type != want {
			t.Fatalf("commit %d = %s, want %s", i, commits[i].Events[0].Type, want)
		}
	}
}

// openLedger writes the acceptance commit a Dispatch would.
func openLedger(t *testing.T, records store.Store, a effect.Assignment) {
	t.Helper()
	ev, err := store.NewEvent(store.EventExecutionAccepted, 0, store.Accepted{Assignment: a})
	if err != nil {
		t.Fatal(err)
	}
	if err := records.Append(context.Background(), store.Lease{}, a.Key(), store.Commit{CommitID: store.AcceptCommitID(a.Key()), Events: []store.Event{ev}}); err != nil {
		t.Fatal(err)
	}
}

// appendStep commits one state-machine event at seq under lease.
func appendStep(records store.Store, lease store.Lease, seq store.CommitSeq, typ store.EventType) error {
	ev, err := store.NewEvent(typ, 0, nil)
	if err != nil {
		return err
	}
	return records.Append(context.Background(), lease, lease.Key, store.Commit{Seq: seq, CommitID: store.DeriveCommitID(lease.Key, "test", fmt.Sprintf("%s/%d", typ, seq)), Events: []store.Event{ev}})
}

func TestExecutionStoreRequiresDispatchingBarrier(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := storetest.NewMap(func() time.Time { return now })
	a := testAssignment()
	openLedger(t, records, a)
	claimed, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire = %+v, acquired=%v, err=%v", claimed, acquired, err)
	}
	if err := appendStep(records, claimed, 2, store.EventExecutionRunning); !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("accepted to running = %v, want ErrStateConflict", err)
	}
	if err := appendStep(records, claimed, 2, store.EventExecutionStarted); err != nil {
		t.Fatalf("accepted to dispatching = %v", err)
	}
	if err := appendStep(records, claimed, 2, store.EventExecutionStarted); !errors.Is(err, store.ErrAlreadyApplied) {
		t.Fatalf("replayed commit = %v, want ErrAlreadyApplied", err)
	}
	if err := appendStep(records, claimed, 2, store.EventCancelRequested); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("another commit at a taken seq = %v, want ErrConflict", err)
	}
	now = base.Add(2 * time.Second)
	if err := appendStep(records, claimed, 3, store.EventExecutionRunning); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expired transition = %v, want ErrLeaseLost", err)
	}
	// An unfenced append may not carry a fenced event.
	if err := appendStep(records, store.Lease{Key: a.Key()}, 3, store.EventExecutionRunning); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("unfenced fenced event = %v, want ErrLeaseLost", err)
	}
}

func TestRecordStoreSurvivesWorkerRecreation(t *testing.T) {
	ctx := context.Background()
	fs := storetest.NewMap(nil)
	a := testAssignment()
	first, err := executor.NewWorker(ctx, fs, routes(newTestBackend()))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := awaitOutcome(ctx, first, a.Key()); err != nil {
		t.Fatal(err)
	}
	second, err := executor.NewWorker(ctx, fs, routes(newTestBackend()))
	if err != nil {
		t.Fatal(err)
	}
	out, err := awaitOutcome(ctx, second, a.Key())
	if _, ok := out.ModelResult(); err != nil || !ok {
		t.Fatalf("recreated worker outcome = %+v, %v", out, err)
	}
}

func testToolAssignment() effect.Assignment {
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", CallID: "call-1", Effect: "effect",
		Body: effect.ToolAssignment{ToolRef: "gate", DefinitionDigest: "d", Arguments: run.MustParseCanonicalJSON(`{}`), Policy: run.DirectExecution}}
}

// Dispose is the control plane's give-up path: the record settles Unknown
// regardless of owner or lease, so the Owner's next read disposes the Run
// target; a terminal record is left alone and a key with no record is not
// found.
func TestWorkerDisposeSettlesUnknown(t *testing.T) {
	completedEnv := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Unknown: false}
	rows := []struct {
		name    string
		record  *store.Execution // nil means the key was never written
		wantErr error
	}{
		{"expired foreign owner", &store.Execution{ExecutionState: store.ExecutionState{State: effect.ExecutionRunning}, Lease: store.Lease{Owner: "dead-worker", Epoch: 3, UntilUnixMilli: 1}}, nil},
		{"live foreign owner", &store.Execution{ExecutionState: store.ExecutionState{State: effect.ExecutionDispatching}, Lease: store.Lease{Owner: "live-worker", Epoch: 3, UntilUnixMilli: time.Now().Add(time.Hour).UnixMilli()}}, nil},
		{"already terminal", &store.Execution{ExecutionState: store.ExecutionState{State: effect.ExecutionCompleted, Outcome: &completedEnv}, Lease: store.Lease{Owner: "dead-worker", Epoch: 3, UntilUnixMilli: 1}}, nil},
		{"missing record", nil, effect.ErrExecutionNotFound},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ctx := context.Background()
			records := storetest.NewMap(nil)
			a := testAssignment()
			key := a.Key()
			if row.record != nil {
				r := *row.record
				r.Assignment = a
				if err := records.Seed(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			worker, err := executor.NewWorker(ctx, records, routes(newTestBackend()), executor.WorkerOptions{ID: "worker-a"})
			if err != nil {
				t.Fatal(err)
			}
			err = worker.Dispose(ctx, key)
			if !errors.Is(err, row.wantErr) {
				t.Fatalf("dispose = %v, want %v", err, row.wantErr)
			}
			if row.record == nil {
				return
			}
			got, _, _, err := records.Load(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if row.record.State == effect.ExecutionCompleted {
				if got.State != effect.ExecutionCompleted || got.Outcome == nil || got.Outcome.Unknown {
					t.Fatalf("terminal record changed: %+v", got)
				}
				return
			}
			if got.State != effect.ExecutionUnknown || got.Outcome == nil || !got.Outcome.Unknown {
				t.Fatalf("record after dispose = %+v", got)
			}
		})
	}
}

// The Port adapter proves nothing about a Ref it cannot read as a key: that
// is an error, not a missing execution.
func TestPortBackendAttachRejectsForeignRef(t *testing.T) {
	_, err := executor.PortBackend(newTestBackend()).Attach(context.Background(), "not-a-key")
	if err == nil {
		t.Fatal("foreign ref answered instead of failing")
	}
}

// Attach classifies by the record's lease in the shared store, not by which
// Worker answers: a live lease held by another incarnation is active, an
// expired one orphaned (RUN-EXE-3).
func TestWorkerAttachClassifiesByLease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_000_000, 0)
	cases := []struct {
		name  string
		owner string
		epoch store.Epoch
		lease int64
		want  effect.AttachmentState
	}{
		{"another incarnation, live lease", "other-worker", 3, now.Add(time.Minute).UnixMilli(), effect.AttachmentActive},
		{"another incarnation, expired lease", "other-worker", 3, now.Add(-time.Minute).UnixMilli(), effect.AttachmentOrphaned},
		{"never acquired", "", 0, 0, effect.AttachmentOrphaned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records := storetest.NewMap(func() time.Time { return now })
			a := testAssignment()
			r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionRunning, ExecutionRef: store.ExecutionRef{Provider: "elsewhere", Ref: "job"}}, Lease: store.Lease{Owner: tc.owner, Epoch: tc.epoch, UntilUnixMilli: tc.lease}}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			worker, err := executor.NewWorker(ctx, records, routes(newTestBackend()), executor.WorkerOptions{ID: "this-worker", Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			got, err := worker.Attach(ctx, a.Key())
			if err != nil || got.State != tc.want {
				t.Fatalf("attach = %+v, %v, want %s", got, err, tc.want)
			}
		})
	}
}

// failingCreateStore refuses every Append: the ledger store is unavailable.
type failingCreateStore struct{ store.Store }

func (failingCreateStore) Append(context.Context, store.Lease, effect.AssignmentKey, store.Commit) error {
	return errors.New("store unavailable")
}

// A Dispatch the Worker cannot record is a retryable refusal; a definite
// rejection of the Assignment is a plain error; neither is an unknown
// outcome (RUN-EXE-3). The HTTP binding's status mapping is tested with it.
func TestDispatchRefusalClassification(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		records    store.Store
		assignment effect.Assignment
		retryable  bool
	}{
		{"record store unavailable", failingCreateStore{storetest.NewMap(nil)}, testAssignment(), true},
		{"assignment without body", storetest.NewMap(nil), effect.Assignment{Session: "s", RunID: "r", Effect: "e"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker, err := executor.NewWorker(ctx, tc.records, routes(newTestBackend()))
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			err = worker.Dispatch(ctx, tc.assignment)
			if err == nil || errors.Is(err, effect.ErrDispatchUnknown) || errors.Is(err, effect.ErrDispatchRetryable) != tc.retryable {
				t.Fatalf("dispatch = %v, want retryable=%v and not unknown", err, tc.retryable)
			}
		})
	}
}

// Acknowledge marks a settled record and collects it at once: the payload
// and Outcome go, the key, digest, state and ExecutionRef stay, so the record
// still answers Attach with terminal, GetOutcome with collected, and a
// Dispatch of the same key starts nothing. An unacknowledged record keeps
// its Outcome readable, and an execution in flight cannot be acknowledged
// (RUN-EXE-13).
func TestWorkerAcknowledgeCollects(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(3_000_000, 0)
	rows := []struct {
		name          string
		state         effect.ExecutionStatus
		acknowledge   bool
		wantAckErr    error
		wantCollected bool
	}{
		{"acknowledged", effect.ExecutionCompleted, true, nil, true},
		{"unacknowledged", effect.ExecutionCompleted, false, nil, false},
		{"executing", effect.ExecutionRunning, true, store.ErrStateConflict, false},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			records := storetest.NewMap(func() time.Time { return now })
			a := testAssignment()
			r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: row.state, ExecutionRef: store.ExecutionRef{Provider: "test", Ref: "job"}}, Lease: store.Lease{Owner: "worker-a", Epoch: 1, UntilUnixMilli: now.Add(time.Hour).UnixMilli()}}
			if row.state.Terminal() {
				env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: a.Key()}
				r.Outcome = &env
			}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := newTestBackend()
			worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "worker-a", Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if row.acknowledge {
				err := worker.Acknowledge(ctx, a.Key())
				if !errors.Is(err, row.wantAckErr) {
					t.Fatalf("acknowledge = %v, want %v", err, row.wantAckErr)
				}
				if err == nil {
					if err := worker.Acknowledge(ctx, a.Key()); err != nil {
						t.Fatalf("repeated acknowledge = %v", err)
					}
				}
			}
			got, _, _, err := records.Load(ctx, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			if !row.wantCollected {
				if got.Acknowledged || got.Assignment.Body == nil || (row.state.Terminal() && got.Outcome == nil) {
					t.Fatalf("record was acknowledged: %+v", got)
				}
				return
			}
			// The fold keeps its facts; acknowledgement is one more of them.
			if !got.Acknowledged || got.Assignment.Body == nil || got.Outcome == nil || got.Assignment.Key() != a.Key() || got.State != row.state {
				t.Fatalf("acknowledged record = %+v", got)
			}
			if att, err := worker.Attach(ctx, a.Key()); err != nil || att.State != effect.AttachmentTerminal {
				t.Fatalf("attach of collected = %+v %v", att, err)
			}
			if _, err := awaitOutcome(ctx, worker, a.Key()); !errors.Is(err, effect.ErrOutcomeCollected) || !errors.Is(err, effect.ErrOutcomeUnavailable) {
				t.Fatalf("outcome of collected = %v", err)
			}
			if err := worker.Dispatch(ctx, a); err != nil {
				t.Fatalf("dispatch replay of a collected key = %v", err)
			}
			backend.mu.Lock()
			calls := backend.calls
			backend.mu.Unlock()
			if calls != 0 {
				t.Fatalf("dispatch replay of a collected key executed %d times", calls)
			}
		})
	}
}

// RUN-EXE-16: Abort and the acceptance a Dispatch writes contend for the
// first commit of a key's ledger; exactly one stands. A key aborted first
// rejects every later Dispatch and answers aborted everywhere; a key
// dispatched first is reported live by Abort and left alone.
func TestAbortAndDispatchAreMutuallyExclusive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	records := storetest.NewMap(nil)
	backend := &refBackend{testBackend: newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", backend)})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	t.Run("abort first", func(t *testing.T) {
		a := testAssignment()
		a.Effect = "aborted-first"
		closed, err := worker.Abort(ctx, a.Key())
		if err != nil || closed.State != effect.AttachmentAborted {
			t.Fatalf("abort = %+v %v", closed, err)
		}
		if err := worker.Dispatch(ctx, a); !errors.Is(err, effect.ErrExecutionAborted) || errors.Is(err, effect.ErrDispatchRetryable) || errors.Is(err, effect.ErrDispatchUnknown) {
			t.Fatalf("dispatch after abort = %v, want a definite ErrExecutionAborted", err)
		}
		if backend.prepared != 0 {
			t.Fatalf("backend prepared %d executions for an aborted key", backend.prepared)
		}
		again, err := worker.Abort(ctx, a.Key())
		if err != nil || again.State != effect.AttachmentAborted {
			t.Fatalf("second abort = %+v %v", again, err)
		}
		if att, err := worker.Attach(ctx, a.Key()); err != nil || att.State != effect.AttachmentAborted || att.Execution != effect.ExecutionAborted {
			t.Fatalf("attach = %+v %v", att, err)
		}
		if _, err := awaitOutcome(ctx, worker, a.Key()); !errors.Is(err, effect.ErrOutcomeUnavailable) {
			t.Fatalf("outcome of an aborted key = %v, want ErrOutcomeUnavailable", err)
		}
		if err := worker.Acknowledge(ctx, a.Key()); err != nil {
			t.Fatalf("acknowledge of an aborted key = %v", err)
		}
		if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
			t.Fatalf("recover of an aborted key = %v", err)
		}
	})
	t.Run("dispatch first", func(t *testing.T) {
		a := testAssignment()
		a.Effect = "dispatched-first"
		if err := worker.Dispatch(ctx, a); err != nil {
			t.Fatal(err)
		}
		live, err := worker.Abort(ctx, a.Key())
		if err != nil || live.State == effect.AttachmentAborted || live.State == effect.AttachmentMissing {
			t.Fatalf("abort after dispatch = %+v %v, want the live attachment", live, err)
		}
		if out, err := awaitOutcome(ctx, worker, a.Key()); err != nil || out.Key != a.Key() {
			t.Fatalf("outcome after a lost abort = %+v %v", out, err)
		}
	})
}

// GetOutcome is a read: before settlement it answers ErrOutcomeNotReady at
// once. The Worker's settlement stream carries the notice of every key it
// settles under its hub's epoch, a subscriber that reconnects with its last
// sequence gets what it missed, and Close ends the stream (RUN-EXE-17).
func TestWorkerGetOutcomeIsAReadAndSettlementsNotify(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backend := &holdBackend{newTestBackend()}
	hub := executor.NewSettlementHub("worker-a/1", 0)
	worker, err := executor.NewWorker(ctx, storetest.NewMap(nil), []executor.Route{executor.Default("test", executor.PortBackend(backend))},
		executor.WorkerOptions{ID: "worker-a", Settlements: hub})
	if err != nil {
		t.Fatal(err)
	}
	a, b := testAssignment(), testAssignment()
	b.Effect = "second"
	for _, asg := range []effect.Assignment{a, b} {
		if err := worker.Dispatch(ctx, asg); err != nil {
			t.Fatal(err)
		}
	}
	began := time.Now()
	if _, err := worker.GetOutcome(ctx, a.Key()); !errors.Is(err, effect.ErrOutcomeNotReady) {
		t.Fatalf("read of an unsettled execution = %v, want ErrOutcomeNotReady", err)
	}
	if waited := time.Since(began); waited > time.Second {
		t.Fatalf("read waited %v; a read does not wait", waited)
	}
	// A subscriber under the hub's epoch from sequence 0 sees a's
	// settlement whether it connected before or after the notice was
	// recorded, then stops reading.
	seen := make(chan effect.Settlement, 4)
	streamDone := make(chan error, 1)
	go func() {
		streamDone <- worker.Settlements(ctx, hub.Epoch(), 0, func(s effect.Settlement) bool {
			if s.Key == (effect.AssignmentKey{}) {
				return true // the stream's announcement of its epoch and head
			}
			seen <- s
			return s.Key != a.Key()
		})
	}()
	settle := func(asg effect.Assignment, text string) {
		backend.mu.Lock()
		backend.outcomes[asg.Key()] <- effect.Outcome{Key: asg.Key(), Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: text}}}
		backend.mu.Unlock()
	}
	settle(a, "first")
	var first effect.Settlement
	select {
	case first = <-seen:
	case <-ctx.Done():
		t.Fatal("no settlement notice for a")
	}
	if first.Key != a.Key() || first.Sequence == 0 || first.Epoch != hub.Epoch() {
		t.Fatalf("settlement = %+v", first)
	}
	if err := <-streamDone; err != nil {
		t.Fatalf("stream ended with %v after fn stopped it", err)
	}
	if out, err := worker.GetOutcome(ctx, a.Key()); err != nil || modelText(out) != "first" {
		t.Fatalf("read after the notice = %+v, %v", out, err)
	}
	// b settles while nobody listens; resubscribing after a's sequence in
	// the same epoch delivers it, and Close ends the stream with nil.
	settle(b, "second")
	if _, err := awaitOutcome(ctx, worker, b.Key()); err != nil {
		t.Fatalf("b did not settle: %v", err)
	}
	var missed []effect.Settlement
	streamDone = make(chan error, 1)
	go func() {
		streamDone <- worker.Settlements(ctx, first.Epoch, first.Sequence, func(s effect.Settlement) bool {
			if s.Key != (effect.AssignmentKey{}) {
				missed = append(missed, s)
			}
			return true
		})
	}()
	time.Sleep(50 * time.Millisecond)
	worker.Close()
	if err := <-streamDone; err != nil {
		t.Fatalf("stream after Close = %v, want a clean end", err)
	}
	if len(missed) != 1 || missed[0].Key != b.Key() || missed[0].Sequence != first.Sequence+1 {
		t.Fatalf("resubscription after %d = %+v, want b's notice alone", first.Sequence, missed)
	}
}
