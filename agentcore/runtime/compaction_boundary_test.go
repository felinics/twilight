package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/sdk"
)

type compactUnknownDispatch struct{ *executor.Worker }

func (p *compactUnknownDispatch) Dispatch(ctx context.Context, assignment effect.Assignment) error {
	if err := p.Worker.Dispatch(ctx, assignment); err != nil {
		return err
	}
	return effect.ErrDispatchUnknown
}

func TestCompactionUnknownDispatchKeepsAcceptedIdentity(t *testing.T) {
	var calls atomic.Int32
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		calls.Add(1)
		return sdk.ModelResult{Text: compactSummary}, nil
	})
	old := f.execution(&compactUnknownDispatch{Worker: f.worker}, nil, true)
	f.history()
	f.accept(old, "unknown-dispatch")
	accepted := f.state().Operations["unknown-dispatch"]
	if result, err := old.Compactions.Advance(f.ctx, f.w); !errors.Is(err, effect.ErrDispatchUnknown) || !result.Pending || result.Key != accepted.Request.Key(compactSession) {
		t.Fatalf("Advance=%+v, %v", result, err)
	}
	if op := f.state().Operations["unknown-dispatch"]; op.Status != compaction.Pending || op.Acknowledged {
		t.Fatalf("uncertain dispatch rejected: %+v", op)
	}
	f.reopenWriter()
	fresh := f.execution(f.worker, nil, false)
	f.settleMaintenance(fresh)
	op := f.state().Operations["unknown-dispatch"]
	if op.Status != compaction.Applied || !op.Acknowledged || !reflect.DeepEqual(op.Request, accepted.Request) || calls.Load() != 1 {
		t.Fatalf("recovered=%+v calls=%d", op, calls.Load())
	}
	f.counts(1, 1, 0)
}

func TestCompactionObsoleteBeforeDispatchClosesExecutionKey(t *testing.T) {
	var calls atomic.Int32
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		calls.Add(1)
		return sdk.ModelResult{Text: compactSummary}, nil
	})
	x := f.execution(f.worker, nil, true)
	f.history()
	f.accept(x, "obsolete")
	accepted := f.state().Operations["obsolete"]
	f.startTurn("new-context", true)
	before := f.context().Entries
	if _, err := x.Compactions.Advance(f.ctx, f.w); err != nil {
		t.Fatal(err)
	}
	op := f.state().Operations["obsolete"]
	if op.Status != compaction.Stale || !op.Acknowledged || calls.Load() != 0 {
		t.Fatalf("obsolete=%+v calls=%d", op, calls.Load())
	}
	attachment, err := f.worker.Attach(f.ctx, accepted.Request.Key(compactSession))
	if err != nil || attachment.State != effect.AttachmentAborted {
		t.Fatalf("late dispatch not fenced: %+v, %v", attachment, err)
	}
	if !reflect.DeepEqual(before, f.context().Entries) {
		t.Fatal("obsolete operation replaced context")
	}
	f.counts(0, 0, 0)
}

func TestCompactionFailureAttemptsAreBoundedAcrossOwners(t *testing.T) {
	var calls atomic.Int32
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		calls.Add(1)
		return sdk.ModelResult{}, errors.New("summary temporarily unavailable")
	})
	x := f.execution(f.worker, &compactPolicy{limit: 80}, true)
	f.history()
	current := f.startTurn("bounded", false)
	for i := 0; i < driver.DefaultCompactionAttempts; i++ {
		if _, err := f.drive(x, f.w, current.Ref.TurnID); !errors.Is(err, driver.ErrSummaryFailed) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	f.reopenWriter()
	replacement := f.execution(f.worker, &compactPolicy{limit: 80}, true)
	if _, err := f.drive(replacement, f.w, current.Ref.TurnID); !errors.Is(err, driver.ErrCompactionNoProgress) {
		t.Fatalf("attempt limit after replacement: %v", err)
	}
	if calls.Load() != int32(driver.DefaultCompactionAttempts) || len(f.state().Operations) != driver.DefaultCompactionAttempts {
		t.Fatalf("unbounded attempts: calls=%d state=%+v", calls.Load(), f.state())
	}
	f.counts(0, 0, 0)
}
