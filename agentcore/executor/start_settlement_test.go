package executor_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

// Unlike the map fixture, a database rejects operations on expired contexts.
type startSettlementStore struct {
	store.Store
	preparation context.Context
	failWrites  int32
	writes      atomic.Int32
}

func (s *startSettlementStore) Acquire(ctx context.Context, key effect.AssignmentKey, owner string, ttl time.Duration) (store.Lease, bool, error) {
	lease, ok, err := s.Store.Acquire(ctx, key, owner, ttl)
	if err != nil || !ok {
		return lease, ok, err
	}
	s.preparation = ctx
	deadline, _ := ctx.Deadline()
	timer := time.NewTimer(time.Until(deadline) / 2)
	defer timer.Stop()
	select {
	case <-timer.C:
		return lease, ok, nil
	case <-ctx.Done():
		return lease, ok, ctx.Err()
	}
}

func (s *startSettlementStore) Load(ctx context.Context, key effect.AssignmentKey) (store.Execution, ledger.Head, bool, error) {
	if err := ctx.Err(); err != nil {
		return store.Execution{}, ledger.Head{}, false, err
	}
	return s.Store.Load(ctx, key)
}

func (s *startSettlementStore) Append(ctx context.Context, lease store.Lease, key effect.AssignmentKey, c ledger.Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.Batches[0].Events[0].Type == store.EventExecutionSettled && s.writes.Add(1) <= s.failWrites {
		return errors.New("temporary settlement write failure")
	}
	return s.Store.Append(ctx, lease, key, c)
}

func TestWorkerPersistsStartResultAfterPreparationTimeout(t *testing.T) {
	rejected := errors.New("backend explicitly rejected execution")
	for _, tc := range []struct {
		name       string
		startErr   error
		failWrites int32
	}{
		{name: "started"},
		{name: "rejected", startErr: rejected},
		{name: "rejected_with_transient_store_failure", startErr: rejected, failWrites: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := &startSettlementStore{Store: storetest.NewMap(nil), failWrites: tc.failWrites}
			backend := newControlledBackend()
			backend.start = func(rpc context.Context, _ string, _ int) error {
				// Preparation expires first; the independently bounded RPC is live.
				<-records.preparation.Done()
				if err := rpc.Err(); err != nil {
					t.Errorf("Start RPC expired before returning its definitive result: %v", err)
					return err
				}
				return tc.startErr
			}
			worker, err := executor.NewWorker(context.Background(), records, []executor.Route{executor.Default("test", backend)}, executor.WorkerOptions{StartTimeout: 200 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			a := testToolAssignment()
			dispatchErr := worker.Dispatch(context.Background(), a)
			if tc.startErr == nil {
				if dispatchErr != nil {
					t.Fatal(dispatchErr)
				}
			} else {
				if tc.failWrites == 0 && !errors.Is(dispatchErr, rejected) {
					t.Fatalf("Dispatch = %v, want backend rejection", dispatchErr)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if _, err := awaitOutcome(ctx, worker, a.Key()); err != nil {
					t.Fatal(err)
				}
			}
			state, _, _, err := records.Load(context.Background(), a.Key())
			if err != nil {
				t.Fatal(err)
			}
			if tc.startErr == nil {
				if state.State != effect.ExecutionRunning {
					t.Fatalf("state = %s, want running", state.State)
				}
			} else if state.State != effect.ExecutionFailed || state.Outcome == nil || state.Outcome.Error == nil || state.Outcome.Error.Code != "dispatch_failed" || state.Outcome.Error.Message != rejected.Error() {
				t.Fatalf("backend rejection lost: state=%s outcome=%+v", state.State, state.Outcome)
			}
			starts, restarts, _ := backend.counts()
			if starts != 1 || restarts != 0 {
				t.Fatalf("starts/restarts = %d/%d, want 1/0", starts, restarts)
			}
		})
	}
}
