package executor_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/sdk"
)

// The tests of one execution's supervision (supervise.go): lease and
// heartbeat, attach and replay on recovery, retry, observing and settling.
// The fakes they share with the port tests live in executor_test.go.

type temporarilyUnreadableBackend struct {
	*testBackend
	failed  chan struct{}
	ready   chan struct{}
	once    sync.Once
	unknown bool
}

func (b *temporarilyUnreadableBackend) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	select {
	case <-b.ready:
		if b.unknown {
			return effect.Outcome{Key: key, Result: effect.Unknown{Message: "execution explicitly abandoned"}}, nil
		}
		return b.testBackend.GetOutcome(ctx, key)
	default:
		b.once.Do(func() { close(b.failed) })
		return effect.Outcome{}, errors.New("temporary outcome transport failure")
	}
}

func TestWorkerOutcomeReadFailurePreservesExecution(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint("explicit_unknown=", unknown), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			records := storetest.NewMap(nil)
			backend := &temporarilyUnreadableBackend{testBackend: newTestBackend(), failed: make(chan struct{}), ready: make(chan struct{}), unknown: unknown}
			worker, err := executor.NewWorker(ctx, records, routes(backend))
			if err != nil {
				t.Fatal(err)
			}
			a := testAssignment()
			if err := worker.Dispatch(ctx, a); err != nil {
				t.Fatal(err)
			}
			select {
			case <-backend.failed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			r, _, _, err := records.Load(ctx, a.Key())
			if err != nil || r.State != effect.ExecutionRunning || r.Outcome != nil {
				t.Fatalf("read error changed execution: %+v, %v", r, err)
			}
			close(backend.ready)
			out, err := awaitOutcome(ctx, worker, a.Key())
			if _, isUnknown := out.Result.(effect.Unknown); err != nil || isUnknown != unknown || (!unknown && modelText(out) != "ok") {
				t.Fatalf("eventual outcome = %+v, %v", out, err)
			}
		})
	}
}

// Adopting a model record whose execution the backend no longer finds
// restarts it as a new generation: the previous Ref moves to Superseded and
// Start receives the new one (RUN-EXE-9).
func TestWorkerRestartSupersedesRef(t *testing.T) {
	ctx := context.Background()
	records := storetest.NewMap(nil)
	a := testAssignment()
	r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionRunning, ExecutionRef: store.ExecutionRef{Provider: "ref", Ref: "execution-1"}}, Lease: store.Lease{Owner: "dead-worker", Epoch: 2, UntilUnixMilli: 1}}
	if err := records.Seed(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := &refBackend{testBackend: newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)}, executor.WorkerOptions{ID: "worker-b", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	got, _, _, err := records.Load(ctx, a.Key())
	if err != nil {
		t.Fatal(err)
	}
	if got.ExecutionRef.Ref != "execution-2" || len(got.Superseded) != 1 || got.Superseded[0].Ref != "execution-1" {
		t.Fatalf("restarted record = ref %q superseded %+v, want execution-2 over [execution-1]", got.ExecutionRef.Ref, got.Superseded)
	}
	backend.mu.Lock()
	prepared, restarts, startRef := backend.prepared, backend.restarts, backend.startRef
	backend.mu.Unlock()
	if prepared != 0 || restarts != 1 || startRef != "execution-2" {
		t.Fatalf("backend calls = prepared:%d restarts:%d start ref:%q, want 0/1/execution-2", prepared, restarts, startRef)
	}
}

func TestWorkerReclaimsExpiredAssignment(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := storetest.NewMap(func() time.Time { return now })
	a := testAssignment()
	openLedger(t, records, a)
	claimed, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired {
		t.Fatalf("initial acquire = %+v, acquired=%v", claimed, acquired)
	}
	if err := appendStep(records, claimed, 2, store.EventExecutionStarted); err != nil {
		t.Fatalf("mark dispatching = %v", err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	now = base.Add(2 * time.Second)
	if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	out, err := awaitOutcome(readCtx, worker, a.Key())
	if err != nil || modelText(out) != "ok" {
		t.Fatalf("recovered outcome = %+v, %v", out, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	var request *model.ModelRequest
	if modelAssignment, ok := backend.last.Model(); ok {
		request = modelAssignment.Request
	}
	backend.mu.Unlock()
	if calls != 1 || request == nil {
		t.Fatalf("recovery dispatch calls=%d request=%v, want one inline request", calls, request)
	}
}

func TestWorkerRecoverExecutionAdoptsExpiredLease(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base.Add(2 * time.Second)
	records := storetest.NewMap(func() time.Time { return now })
	a := testAssignment()
	r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionRunning}, Lease: store.Lease{Owner: "dead-worker", Epoch: 4, UntilUnixMilli: base.Add(time.Second).UnixMilli()}}
	if err := records.Seed(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	out, err := awaitOutcome(readCtx, worker, a.Key())
	if err != nil || modelText(out) != "ok" {
		t.Fatalf("adopted outcome = %+v, %v", out, err)
	}
	got, _, _, err := records.Load(ctx, a.Key())
	if err != nil || got.Lease.Owner != "worker-b" {
		t.Fatalf("adopted record = %+v, %v", got, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("backend calls = %d, want 1", calls)
	}
}

// A record under another Worker's live lease is not this Worker's to
// recover: RecoverExecution leaves it untouched and dispatches nothing.
func TestWorkerRecoverExecutionLeavesLiveLease(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := storetest.NewMap(func() time.Time { return now })
	a := testAssignment()
	r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionRunning}, Lease: store.Lease{Owner: "worker-a", Epoch: 4, UntilUnixMilli: base.Add(time.Second).UnixMilli()}}
	if err := records.Seed(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatalf("recover of a live lease = %v, want a no-op", err)
	}
	got, _, _, err := records.Load(ctx, a.Key())
	if err != nil || got.Lease.Owner != "worker-a" || got.Lease.Epoch != 4 {
		t.Fatalf("live record changed: %+v, %v", got, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 0 {
		t.Fatalf("recovery dispatched %d backend calls", calls)
	}
}

// Adoption never re-dispatches an unbound tool whose prior execution may have
// crossed the effect boundary (TRN-DUR-4); it settles Unknown instead. A
// record that never dispatched (Accepted) still executes on adoption, and
// model assignments stay replayable (TestWorkerReconcileAdoptsExpiredLease).
func TestWorkerAdoptionOfUnattachableToolSettlesUnknown(t *testing.T) {
	rows := []struct {
		state       effect.ExecutionStatus
		wantCalls   int
		wantUnknown bool
	}{
		{effect.ExecutionRunning, 0, true},
		{effect.ExecutionDispatching, 0, true},
		{effect.ExecutionAccepted, 1, false},
	}
	base := time.Unix(100, 0)
	now := base.Add(2 * time.Second)
	for _, row := range rows {
		t.Run(string(row.state), func(t *testing.T) {
			ctx := context.Background()
			records := storetest.NewMap(func() time.Time { return now })
			a := testToolAssignment()
			r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: row.state}, Lease: store.Lease{Owner: "dead-worker", Epoch: 2, UntilUnixMilli: base.Add(time.Second).UnixMilli()}}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := newTestBackend()
			worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
				ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			// The accepted row dispatches to the backend on a goroutine; the
			// worker is closed before the database and its directory are.
			defer worker.Close()
			if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			got, _, _, err := records.Load(ctx, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			if row.wantUnknown {
				if got.State != effect.ExecutionUnknown || got.Outcome == nil || !got.Outcome.Unknown {
					t.Fatalf("adopted record = %+v, want Unknown settle", got)
				}
			} else if got.Lease.Owner != "worker-b" {
				t.Fatalf("adopted record owner = %q, want worker-b", got.Lease.Owner)
			}
			backend.mu.Lock()
			calls := backend.calls
			backend.mu.Unlock()
			if calls != row.wantCalls {
				t.Fatalf("backend calls = %d, want %d", calls, row.wantCalls)
			}
		})
	}
}

type flakyRenewStore struct {
	store.Store
	mu       sync.Mutex
	deadline time.Time
	renewals int
}

func (s *flakyRenewStore) Renew(ctx context.Context, lease store.Lease, ttl time.Duration) error {
	s.mu.Lock()
	s.renewals++
	failing := time.Now().Before(s.deadline)
	s.mu.Unlock()
	if failing {
		return errors.New("store temporarily unavailable")
	}
	return s.Store.Renew(ctx, lease, ttl)
}

// A transient Renew failure must not stop lease maintenance; the heartbeat
// retries and the record stays owned. Without the retry the heartbeat exited
// on the first error and the lease stayed lost once the failure outlasted it.
func TestWorkerHeartbeatRetriesTransientRenewErrors(t *testing.T) {
	records := &flakyRenewStore{Store: storetest.NewMap(nil), deadline: time.Now().Add(1100 * time.Millisecond)}
	backend := &uncertainDispatchBackend{testBackend: newTestBackend(), ready: make(chan struct{})}
	defer close(backend.ready)
	worker, err := executor.NewWorker(context.Background(), records, routes(backend), executor.WorkerOptions{ID: "worker-a", LeaseDuration: 800 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(context.Background(), a); err == nil || !errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("dispatch = %v, want ErrDispatchUnknown", err)
	}
	// The injected failure outlasts one 800ms lease; only a retrying
	// heartbeat re-establishes the lease after the store recovers.
	time.Sleep(time.Until(records.deadline) + 400*time.Millisecond)
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, _, ok, err := records.Load(context.Background(), a.Key())
		if err != nil || !ok {
			t.Fatalf("record = %+v ok=%v err=%v", r, ok, err)
		}
		if r.Lease.Owner == "worker-a" && r.Lease.UntilUnixMilli > time.Now().UnixMilli() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease lost after transient Renew failures; heartbeat did not retry")
		}
		time.Sleep(50 * time.Millisecond)
	}
	records.mu.Lock()
	renewals := records.renewals
	records.mu.Unlock()
	if renewals < 4 {
		t.Fatalf("renewals = %d, want at least 4 (failures retried)", renewals)
	}
}

// A takeover whose backend cannot confirm the execution (orphaned) holds the
// lease and asks again instead of restarting: nothing is re-dispatched until
// the backend proves the execution missing, and a backend that then observes
// it hands the Worker the original Outcome (RUN-EXE-3, TRN-DUR-4).
func TestWorkerRecoverExecutionWaitsForUnconfirmedBackend(t *testing.T) {
	rows := []struct {
		name         string
		assignment   effect.Assignment
		resolve      effect.AttachmentState
		wantRestarts int
		wantState    effect.ExecutionStatus
	}{
		{"model, backend later proves missing", testAssignment(), effect.AttachmentMissing, 1, effect.ExecutionCompleted},
		{"tool, backend later proves missing", testToolAssignment(), effect.AttachmentMissing, 0, effect.ExecutionUnknown},
		{"model, backend later observes it", testAssignment(), effect.AttachmentActive, 0, effect.ExecutionRunning},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ctx := context.Background()
			records := storetest.NewMap(nil)
			a := row.assignment
			r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionRunning, ExecutionRef: store.ExecutionRef{Provider: "ref", Ref: "execution-1"}}, Lease: store.Lease{Owner: "dead-worker", Epoch: 2, UntilUnixMilli: 1}}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := &refBackend{testBackend: newTestBackend(), attach: effect.AttachmentOrphaned}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)}, executor.WorkerOptions{ID: "worker-b", LeaseDuration: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			// Undecided: the record is held under this Worker's lease, still
			// Running, and the backend has been neither restarted nor started.
			held, _, _, err := records.Load(ctx, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			restarts, started := backend.restarts, backend.started
			backend.mu.Unlock()
			if held.Lease.Owner != "worker-b" || held.State != effect.ExecutionRunning || restarts != 0 || started != 0 {
				t.Fatalf("held record = %+v, backend restarts=%d started=%d; want the lease held and nothing dispatched", held, restarts, started)
			}
			// The holder itself reports what its backend can confirm: nothing
			// yet, so orphaned; the Reconciler defers and awaits the Outcome.
			if att, err := worker.Attach(ctx, a.Key()); err != nil || att.State != effect.AttachmentOrphaned || att.Owner != "worker-b" {
				t.Fatalf("attach while undecided = %+v %v, want orphaned under this Worker's lease", att, err)
			}
			backend.setAttach(row.resolve)
			deadline := time.Now().Add(5 * time.Second)
			for {
				got, _, _, err := records.Load(ctx, a.Key())
				if err != nil {
					t.Fatal(err)
				}
				backend.mu.Lock()
				restarts = backend.restarts
				backend.mu.Unlock()
				if got.State == row.wantState && restarts == row.wantRestarts && (row.resolve != effect.AttachmentActive || got.Lease.Owner == "worker-b") {
					if row.wantRestarts == 1 && (got.ExecutionRef.Ref != "execution-2" || len(got.Superseded) != 1) {
						t.Fatalf("restarted record = %+v", got)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("record after the backend answered %s = %+v, restarts=%d; want %s/%d", row.resolve, got, restarts, row.wantState, row.wantRestarts)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
}

// modelText is the text of a ModelSucceeded outcome, "" for anything else.
func modelText(out effect.Outcome) string {
	r, ok := out.ModelResult()
	if !ok {
		return ""
	}
	return r.Text
}

// A lost tool execution is re-dispatched by adoption only when the Replay
// policy its Assignment carries is allowed; a forbidden or unjudged tool is
// settled Unknown, the message naming the declaration, and never started
// again (RUN-EXE-9, TRN-DUR-4).
func TestWorkerAdoptsToolByReplayDeclaration(t *testing.T) {
	cases := []struct {
		name     string
		policy   run.ReplayPolicy
		replayed bool
	}{
		{"allowed", run.ReplayAllowed, true},
		{"forbidden", run.ReplayForbidden, false},
		{"unknown", run.ReplayUnknown, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			records := storetest.NewMap(nil)
			a := testToolAssignment()
			body, _ := a.Tool()
			body.Replay = tc.policy
			a.Body = body
			r := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionRunning, ExecutionRef: store.ExecutionRef{Provider: "ref", Ref: "execution-1"}}, Lease: store.Lease{Owner: "dead-worker", Epoch: 2, UntilUnixMilli: 1}}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := &refBackend{testBackend: newTestBackend()}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)}, executor.WorkerOptions{ID: "worker-b", LeaseDuration: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			got, _, _, err := records.Load(ctx, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			started, startRef := backend.started, backend.startRef
			backend.mu.Unlock()
			if tc.replayed {
				if got.ExecutionRef.Ref != "execution-2" || len(got.Superseded) != 1 || started != 1 || startRef != "execution-2" {
					t.Fatalf("replayed tool: record ref %q superseded %d started %d at %q", got.ExecutionRef.Ref, len(got.Superseded), started, startRef)
				}
				return
			}
			if got.State != effect.ExecutionUnknown || got.Outcome == nil || !got.Outcome.Unknown || got.Outcome.Error == nil ||
				got.Outcome.Error.Code != "adopted_without_replay" || !strings.Contains(got.Outcome.Error.Message, "declares replay "+tc.policy.String()) {
				t.Fatalf("unreplayable tool: state %s outcome %+v", got.State, got.Outcome)
			}
			if started != 0 || got.ExecutionRef.Ref != "execution-1" || len(got.Superseded) != 0 {
				t.Fatalf("unreplayable tool was re-dispatched: started %d ref %q superseded %d", started, got.ExecutionRef.Ref, len(got.Superseded))
			}
		})
	}
}

// transientBackend fails the first executions of an effect with a transient
// provider failure and succeeds afterwards; each Restart is a new Ref.
type transientBackend struct {
	*testBackend
	mu        sync.Mutex
	failures  int
	starts    []string
	code      effect.FailureCode
	toolRetry run.RetryDisposition
}

func (b *transientBackend) Prepare(context.Context, effect.Assignment) (string, error) {
	return "exec-1", nil
}
func (b *transientBackend) Restart(_ context.Context, previous string, _ effect.Assignment) (string, error) {
	return previous + "'", nil
}
func (b *transientBackend) Start(_ context.Context, ref string, a effect.Assignment) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.last = a
	b.starts = append(b.starts, ref)
	var result effect.OutcomeResult = effect.ModelSucceeded{Result: sdk.ModelResult{Text: "ok"}}
	if a.Kind() == effect.AssignmentTool {
		result = effect.ToolExecutionSucceeded{}
	}
	if len(b.starts) <= b.failures {
		if a.Kind() == effect.AssignmentTool {
			result = effect.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureUnavailable, Message: "try again"}, Retry: b.toolRetry}
		} else {
			result = effect.ModelFailed{Code: b.code, Message: "try again"}
		}
	}
	if b.outcomes[a.Key()] == nil {
		b.outcomes[a.Key()] = make(chan effect.Outcome, 8)
	}
	b.outcomes[a.Key()] <- effect.Outcome{Key: a.Key(), Result: result}
	return nil
}
func (b *transientBackend) Attach(ctx context.Context, _ string) (effect.Attachment, error) {
	return b.testBackend.Attach(ctx, b.lastKey())
}
func (b *transientBackend) Status(ctx context.Context, _ string) (effect.ExecutionStatus, error) {
	return b.GetStatus(ctx, b.lastKey())
}
func (b *transientBackend) Outcome(ctx context.Context, _ string) (effect.Outcome, error) {
	return b.GetOutcome(ctx, b.lastKey())
}
func (b *transientBackend) Cancel(ctx context.Context, _ string) error {
	return b.testBackend.Cancel(ctx, b.lastKey())
}

// A Known failure that declares itself retryable is re-dispatched through
// Restart within the Worker's budget, the earlier Refs kept in Superseded:
// a model failure by the disposition the effect layer derived from its
// code, a tool failure by the disposition the tool gave it. A failure that
// does not, or an exhausted budget, settles the failure (RUN-EXE-11).
func TestWorkerRetriesRetryableFailures(t *testing.T) {
	cases := []struct {
		name       string
		assignment effect.Assignment
		failures   int
		code       effect.FailureCode
		toolRetry  run.RetryDisposition
		budget     int
		wantStarts int
		wantOK     bool
	}{
		{"model retried until success", testAssignment(), 2, effect.FailureRateLimited, 0, 3, 3, true},
		{"model budget exhausted", testAssignment(), 5, effect.FailureProviderUnavailable, 0, 2, 2, false},
		{"model definite failure", testAssignment(), 1, effect.FailureAuthentication, 0, 3, 1, false},
		{"model retries disabled", testAssignment(), 1, effect.FailureConnection, 0, 0, 1, false},
		{"tool failure declared retryable", testToolAssignment(), 1, "", run.RetryAllowed, 3, 2, true},
		{"tool failure declared never", testToolAssignment(), 1, "", run.RetryNever, 3, 1, false},
		{"tool failure unjudged", testToolAssignment(), 1, "", run.RetryUnknown, 3, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			records := storetest.NewMap(nil)
			backend := &transientBackend{testBackend: newTestBackend(), failures: tc.failures, code: tc.code, toolRetry: tc.toolRetry}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("t", backend)},
				executor.WorkerOptions{ID: "w", Retry: executor.RetryBudget{MaxAttempts: tc.budget, Backoff: time.Millisecond}})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if err := worker.Dispatch(ctx, tc.assignment); err != nil {
				t.Fatal(err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			var out effect.Outcome
			for {
				out, err = awaitOutcome(waitCtx, worker, tc.assignment.Key())
				if err == nil {
					break
				}
				if waitCtx.Err() != nil {
					t.Fatalf("outcome: %v", err)
				}
				time.Sleep(time.Millisecond)
			}
			var ok bool
			switch out.Result.(type) {
			case effect.ModelSucceeded, effect.ToolExecutionSucceeded:
				ok = true
			}
			if ok != tc.wantOK {
				t.Fatalf("outcome = %#v, want success=%v", out.Result, tc.wantOK)
			}
			backend.mu.Lock()
			starts := len(backend.starts)
			backend.mu.Unlock()
			got, _, _, _ := records.Load(ctx, tc.assignment.Key())
			if starts != tc.wantStarts || len(got.Superseded) != tc.wantStarts-1 {
				t.Fatalf("starts = %d superseded = %d, want %d executions", starts, len(got.Superseded), tc.wantStarts)
			}
		})
	}
}

// gateStore lets a test hold the Worker between the acceptance it writes
// and the execution_started it commits next, so a controller's Dispose can
// land in that window.
type gateStore struct {
	store.Store
	mu      sync.Mutex
	armed   bool
	reached chan struct{}
	release chan struct{}
}

func (s *gateStore) Append(ctx context.Context, lease store.Lease, key effect.AssignmentKey, c store.Commit) error {
	if lease.Epoch > 0 && len(c.Events) == 1 && c.Events[0].Type == store.EventExecutionStarted {
		s.mu.Lock()
		armed := s.armed
		s.armed = false
		s.mu.Unlock()
		if armed {
			close(s.reached)
			<-s.release
		}
	}
	return s.Store.Append(ctx, lease, key, c)
}

// A Dispose that settles the execution between its acceptance and the
// execution_started the Worker commits next wins: the Worker's step is a
// state conflict, Backend.Start is never called, and the ledger keeps the
// Dispose's Unknown settlement (RUN-EXE-16, TRN-DUR-4).
func TestWorkerStartYieldsToSettlementBeforeStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	records := &gateStore{Store: storetest.NewMap(nil), armed: true, reached: make(chan struct{}), release: make(chan struct{})}
	backend := &refBackend{testBackend: newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", backend)}, executor.WorkerOptions{ID: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	a := testAssignment()
	dispatched := make(chan error, 1)
	go func() { dispatched <- worker.Dispatch(ctx, a) }()
	select {
	case <-records.reached:
	case <-ctx.Done():
		t.Fatal("dispatch never reached execution_started")
	}
	if err := worker.Dispose(ctx, a.Key()); err != nil {
		t.Fatalf("dispose = %v", err)
	}
	close(records.release)
	select {
	case err := <-dispatched:
		if err != nil {
			t.Fatalf("dispatch after a racing dispose = %v, want nil: the ledger decided", err)
		}
	case <-ctx.Done():
		t.Fatal("dispatch did not return")
	}
	backend.mu.Lock()
	started := backend.started
	backend.mu.Unlock()
	if started != 0 {
		t.Fatalf("backend started %d executions after the key was disposed", started)
	}
	got, _, _, err := records.Load(ctx, a.Key())
	if err != nil || got.State != effect.ExecutionUnknown || got.Outcome == nil || !got.Outcome.Unknown {
		t.Fatalf("ledger after the race = %+v, %v; want the Dispose's Unknown settlement", got, err)
	}
}
