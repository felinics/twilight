package executor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

// RPC hooks run outside the mutex, so a test can commit cancellation while
// an old Outcome or a Restart response is in flight.
type controlledBackend struct {
	*refBackend
	lock            sync.Mutex
	starts, cancels []string
	restarts        int
	start           func(context.Context, string, int) error
	outcome         func(context.Context, string) (effect.Outcome, error)
	restart         func(context.Context) error
	cancelErr       error
}

func newControlledBackend() *controlledBackend {
	return &controlledBackend{refBackend: &refBackend{testBackend: newTestBackend(), attach: effect.AttachmentActive}}
}
func (b *controlledBackend) Start(ctx context.Context, ref string, _ effect.Assignment) error {
	b.lock.Lock()
	b.starts = append(b.starts, ref)
	n := len(b.starts)
	b.lock.Unlock()
	if b.start != nil {
		return b.start(ctx, ref, n)
	}
	return nil
}
func (b *controlledBackend) Restart(ctx context.Context, ref string, _ effect.Assignment) (string, error) {
	b.lock.Lock()
	b.restarts++
	b.lock.Unlock()
	if b.restart != nil {
		if err := b.restart(ctx); err != nil {
			return "", err
		}
	}
	return ref + "-retry", nil
}
func (b *controlledBackend) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	if b.outcome != nil {
		return b.outcome(ctx, ref)
	}
	return effect.Outcome{}, effect.ErrOutcomeNotReady
}
func (b *controlledBackend) Cancel(_ context.Context, ref string) error {
	b.lock.Lock()
	b.cancels = append(b.cancels, ref)
	b.lock.Unlock()
	return b.cancelErr
}
func (b *controlledBackend) counts() (int, int, int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return len(b.starts), b.restarts, len(b.cancels)
}
func waitTest(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for test boundary")
	}
}

func TestWorkerCancelOrphanNeverStarts(t *testing.T) {
	for _, mode := range []string{"unbound", "unbound_recovery", "never_claimed", "expired", "orphaned", "missing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			records := storetest.NewMap(nil)
			a := testAssignment()
			s := store.Execution{ExecutionState: store.ExecutionState{Assignment: a, State: effect.ExecutionAccepted}}
			if mode == "unbound_recovery" {
				s.State = effect.ExecutionCancelRequested
			}
			if mode != "unbound" && mode != "unbound_recovery" {
				s.ExecutionRef = executor.ExecutionRef{Provider: "test", Ref: "execution-1"}
			}
			if mode == "expired" || mode == "orphaned" {
				s.State = effect.ExecutionRunning
				s.Lease = store.Lease{Owner: "dead", Epoch: 7, UntilUnixMilli: 1}
			}
			if err := records.Seed(ctx, s); err != nil {
				t.Fatal(err)
			}
			backend := newControlledBackend()
			backend.outcome = func(context.Context, string) (effect.Outcome, error) {
				return effect.Outcome{Result: effect.Cancelled{}}, nil
			}
			if mode == "orphaned" {
				backend.setAttach(effect.AttachmentOrphaned)
			}
			if mode == "missing" {
				backend.setAttach(effect.AttachmentMissing)
				backend.cancelErr = effect.ErrExecutionNotFound
			}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", backend)})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if mode != "unbound_recovery" {
				if err := worker.Cancel(ctx, a.Key()); err != nil {
					t.Fatal(err)
				}
			}
			cancelled, _, _, _ := records.Load(ctx, a.Key())
			if cancelled.Lease != s.Lease && mode == "unbound" {
				t.Fatalf("Cancel acquired lease: %+v", cancelled.Lease)
			}
			if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			out, err := awaitOutcome(waitCtx, worker, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "orphaned" || mode == "missing" {
				if _, ok := out.Result.(effect.Unknown); !ok {
					t.Fatalf("orphan result = %#v", out.Result)
				}
			} else if _, ok := out.Result.(effect.Cancelled); !ok {
				t.Fatalf("cancel result = %#v", out.Result)
			}
			starts, restarts, _ := backend.counts()
			backend.mu.Lock()
			prepared := backend.prepared
			backend.mu.Unlock()
			if starts != 0 || restarts != 0 || prepared != 0 {
				t.Fatalf("cancel prepared/started/restarted = %d/%d/%d", prepared, starts, restarts)
			}
		})
	}
}

func TestWorkerCancelPreventsNewRetry(t *testing.T) {
	for _, boundary := range []string{"outcome", "backoff", "restart"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			base := storetest.NewMap(nil)
			backend := newControlledBackend()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			backend.outcome = func(ctx context.Context, _ string) (effect.Outcome, error) {
				if boundary == "outcome" {
					once.Do(func() { close(entered) })
					select {
					case <-release:
					case <-ctx.Done():
						return effect.Outcome{}, ctx.Err()
					}
				}
				return effect.Outcome{Result: effect.ModelFailed{Code: effect.FailureRateLimited}}, nil
			}
			var records store.Store = base
			if boundary == "backoff" {
				// The waitOwned read after retry selection is the beginning of
				// the backoff. Return its snapshot only after Cancel commits.
				records = &backoffGateStore{Store: base, entered: entered, release: release}
			}
			if boundary == "restart" {
				backend.restart = func(ctx context.Context) error {
					once.Do(func() { close(entered) })
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", backend)}, executor.WorkerOptions{
				Retry: executor.RetryBudget{MaxAttempts: 3, Backoff: 20 * time.Millisecond},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if gate, ok := records.(*backoffGateStore); ok {
				backend.outcome = func(context.Context, string) (effect.Outcome, error) {
					gate.mu.Lock()
					gate.armed = true
					gate.mu.Unlock()
					return effect.Outcome{Result: effect.ModelFailed{Code: effect.FailureRateLimited}}, nil
				}
			}
			a := testAssignment()
			if err := worker.Dispatch(ctx, a); err != nil {
				t.Fatal(err)
			}
			waitTest(t, entered)
			if err := worker.Cancel(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			close(release)
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if _, err := awaitOutcome(waitCtx, worker, a.Key()); err != nil {
				t.Fatal(err)
			}
			starts, restarts, _ := backend.counts()
			wantRestarts := 0
			if boundary == "restart" {
				wantRestarts = 1
			}
			got, _, _, _ := base.Load(ctx, a.Key())
			if starts != 1 || restarts != wantRestarts || len(got.Superseded) != 0 || got.ExecutionRef.Ref != "execution-1" {
				t.Fatalf("cancel lost to retry: starts=%d restarts=%d fold=%+v", starts, restarts, got)
			}
		})
	}
}

// Cancellation that wins after a restart commit keeps the fresh binding
// observable but must still prevent its Start RPC.
func TestWorkerCancelBetweenRestartCommitAndStart(t *testing.T) {
	ctx := context.Background()
	base := storetest.NewMap(nil)
	records := &restartCommitGate{Store: base, entered: make(chan struct{}), release: make(chan struct{})}
	backend := newControlledBackend()
	backend.outcome = func(_ context.Context, ref string) (effect.Outcome, error) {
		if ref == "execution-1" {
			return effect.Outcome{Result: effect.ModelFailed{Code: effect.FailureRateLimited}}, nil
		}
		return effect.Outcome{Result: effect.Cancelled{}}, nil
	}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", backend)}, executor.WorkerOptions{Retry: executor.RetryBudget{MaxAttempts: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	a := testAssignment()
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	waitTest(t, records.entered)
	if err := worker.Cancel(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	close(records.release)
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := awaitOutcome(waitCtx, worker, a.Key())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := out.Result.(effect.Cancelled); !ok {
		t.Fatalf("outcome = %#v", out.Result)
	}
	state, _, _, _ := base.Load(ctx, a.Key())
	starts, restarts, cancels := backend.counts()
	if starts != 1 || restarts != 1 || cancels < 2 || state.ExecutionRef.Ref != "execution-1-retry" || len(state.Superseded) != 1 {
		t.Fatalf("cancelled committed retry: starts=%d restarts=%d cancels=%d fold=%+v", starts, restarts, cancels, state)
	}
}

type restartCommitGate struct {
	store.Store
	entered, release chan struct{}
}

func (s *restartCommitGate) Append(ctx context.Context, lease store.Lease, key effect.AssignmentKey, c ledger.Commit) error {
	err := s.Store.Append(ctx, lease, key, c)
	if err == nil && c.Batches[0].Events[0].Type == store.EventExecutionRestarted {
		close(s.entered)
		<-s.release
	}
	return err
}

// A request already sent is not undone by ending its RPC context. The
// durable binding survives and the watcher repeats physical cancellation.
func TestWorkerCancelledStartRemainsObservable(t *testing.T) {
	ctx := context.Background()
	records := storetest.NewMap(nil)
	backend := newControlledBackend()
	entered, release := make(chan struct{}), make(chan struct{})
	backend.start = func(ctx context.Context, _ string, _ int) error {
		close(entered)
		select {
		case <-release:
			return context.Canceled
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	observed := make(chan struct{})
	var once sync.Once
	backend.outcome = func(context.Context, string) (effect.Outcome, error) {
		once.Do(func() { close(observed) })
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", backend)})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	a := testAssignment()
	dispatched := make(chan error, 1)
	go func() { dispatched <- worker.Dispatch(ctx, a) }()
	waitTest(t, entered)
	if err := worker.Cancel(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-dispatched:
		if !errors.Is(err, effect.ErrDispatchUnknown) {
			t.Fatalf("cancelled RPC = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Dispatch did not exit")
	}
	waitTest(t, observed)
	state, _, _, _ := records.Load(ctx, a.Key())
	starts, restarts, cancels := backend.counts()
	if state.State != effect.ExecutionCancelRequested || state.Outcome != nil || state.ExecutionRef.Ref != "execution-1" || starts != 1 || restarts != 0 || cancels < 2 {
		t.Fatalf("cancelled RPC discarded uncertainty: starts=%d restarts=%d cancels=%d fold=%+v", starts, restarts, cancels, state)
	}
}

type backoffGateStore struct {
	store.Store
	mu               sync.Mutex
	armed            bool
	reads            int
	entered, release chan struct{}
}

func (s *backoffGateStore) Load(ctx context.Context, key effect.AssignmentKey) (store.Execution, ledger.Head, bool, error) {
	state, head, ok, err := s.Store.Load(ctx, key)
	s.mu.Lock()
	gate := false
	if s.armed {
		s.reads++
		if s.reads == 2 {
			gate = true
			s.armed = false
		}
	}
	s.mu.Unlock()
	if gate {
		close(s.entered)
		<-s.release
	}
	return state, head, ok, err
}

func TestWorkerBoundedStart(t *testing.T) {
	for _, retry := range []bool{false, true} {
		for _, stop := range []string{"timeout", "caller", "close"} {
			if retry && stop == "caller" {
				continue
			} // retries belong to the Worker lifecycle
			t.Run(map[bool]string{false: "initial/", true: "retry/"}[retry]+stop, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				records := storetest.NewMap(nil)
				backend := newControlledBackend()
				entered, exited := make(chan struct{}), make(chan struct{})
				freshObserved := make(chan struct{})
				var observedOnce sync.Once
				backend.start = func(ctx context.Context, _ string, n int) error {
					if retry && n == 1 {
						return nil
					}
					close(entered)
					<-ctx.Done()
					close(exited)
					return ctx.Err()
				}
				if retry {
					backend.outcome = func(_ context.Context, ref string) (effect.Outcome, error) {
						if ref == "execution-1" {
							return effect.Outcome{Result: effect.ModelFailed{Code: effect.FailureRateLimited}}, nil
						}
						observedOnce.Do(func() { close(freshObserved) })
						return effect.Outcome{}, effect.ErrOutcomeNotReady
					}
				}
				timeout := time.Hour
				if stop == "timeout" {
					timeout = 30 * time.Millisecond
				}
				worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", backend)}, executor.WorkerOptions{
					StartTimeout: timeout, Retry: executor.RetryBudget{MaxAttempts: 2},
				})
				if err != nil {
					t.Fatal(err)
				}
				defer worker.Close()
				a := testAssignment()
				dispatched := make(chan error, 1)
				go func() { dispatched <- worker.Dispatch(ctx, a) }()
				waitTest(t, entered)
				closed := make(chan struct{})
				if stop == "caller" {
					cancel()
				}
				if stop == "close" {
					go func() { worker.Close(); close(closed) }()
				}
				waitTest(t, exited)
				if retry && stop != "close" {
					waitTest(t, freshObserved)
				}
				if stop == "close" {
					waitTest(t, closed)
				}
				select {
				case err := <-dispatched:
					if !retry && !errors.Is(err, effect.ErrDispatchUnknown) {
						t.Fatalf("Start cancellation = %v", err)
					}
					if retry && err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("Dispatch did not exit")
				}
				state, _, _, _ := records.Load(context.Background(), a.Key())
				if state.State != effect.ExecutionDispatching || state.Outcome != nil {
					t.Fatalf("uncertain RPC fabricated a failure: %+v", state)
				}
				if err := worker.Dispatch(context.Background(), a); err != nil {
					t.Fatal(err)
				}
				if err := worker.RecoverExecution(context.Background(), a.Key()); err != nil {
					t.Fatal(err)
				}
				starts, restarts, _ := backend.counts()
				want := 1
				if retry {
					want = 2
				}
				if starts != want || restarts != want-1 {
					t.Fatalf("uncertain RPC duplicated effect: starts=%d restarts=%d", starts, restarts)
				}
			})
		}
	}
}
