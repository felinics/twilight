package runtime_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/sdk"
)

// Inject both a rejected Session append and a response lost after its durable
// append. The caller must recover from facts, not the returned error alone.
type compactFaultWriter struct {
	writer.Writer
	event   ledger.EventType
	after   bool
	failure error
}

func (w *compactFaultWriter) Commit(ctx context.Context, fn writer.CommitFn) (writer.CommitResult, error) {
	matched := false
	result, err := w.Writer.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		group, err := fn(v)
		if err != nil || group == nil {
			return group, err
		}
		for _, batch := range group.Batches {
			for _, event := range batch.Events {
				matched = matched || event.Type == w.event
			}
		}
		if matched && !w.after {
			return nil, w.failure
		}
		return group, nil
	})
	if err == nil && matched && w.after && result.Outcome == writer.CommitApplied {
		return result, w.failure
	}
	return result, err
}

func TestCompactionIntentCommitBoundaries(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-intent", true: "accepted-response-lost"}[after], func(t *testing.T) {
			var calls atomic.Int32
			f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
				calls.Add(1)
				return sdk.ModelResult{Text: compactSummary}, nil
			})
			x := f.execution(f.worker, nil, true)
			f.history()
			request, plan := compactRequest(), f.plan()
			injected := errors.New("intent append boundary")
			w := &compactFaultWriter{Writer: f.w, event: compaction.TypeRequested, after: after, failure: injected}
			if err := x.Compactions.Start(f.ctx, w, "intent-boundary", plan, &request); !errors.Is(err, injected) {
				t.Fatalf("Start=%v", err)
			}
			if pending := f.state().Pending != ""; pending != after {
				t.Fatalf("intent durability=%v, want %v", pending, after)
			}
			if calls.Load() != 0 {
				t.Fatal("Start crossed the effect boundary")
			}
			if !after {
				if result, err := x.Compactions.Advance(f.ctx, f.w); err != nil || result.Pending || result.Progress {
					t.Fatalf("Advance without intent=%+v, %v", result, err)
				}
			}
			f.reopenWriter()
			// A retry after a lost accepted response must compare equal, not
			// mint a second intent/effect or bypass the quiescence guard.
			if err := x.Compactions.Start(f.ctx, f.w, "intent-boundary", plan, &request); err != nil {
				t.Fatal(err)
			}
			f.settleMaintenance(x)
			if calls.Load() != 1 || len(f.state().Operations) != 1 {
				t.Fatalf("calls=%d state=%+v", calls.Load(), f.state())
			}
		})
	}
}

func TestCompactionACKRetryDoesNotRegenerateOrRecommit(t *testing.T) {
	for _, stage := range []string{"executor-ack", "ack-fact"} {
		t.Run(stage, func(t *testing.T) {
			var calls atomic.Int32
			f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
				calls.Add(1)
				return sdk.ModelResult{Text: compactSummary}, nil
			})
			port := newCompactPort(f.worker)
			x := f.execution(port, nil, true)
			f.history()
			f.accept(x, "ack-retry")
			injected := errors.New("injected ACK failure")
			w := f.w
			if stage == "executor-ack" {
				port.failACK.Store(true)
			} else {
				w = &compactFaultWriter{Writer: f.w, event: compaction.TypeAcknowledged, failure: injected}
			}
			result, err := f.advance(x.Compactions, w)
			if err == nil || !strings.Contains(err.Error(), injected.Error()) || result.Pending || !result.Progress || (stage == "ack-fact" && !errors.Is(err, injected)) {
				t.Fatalf("ACK failure result=%+v, error=%v", result, err)
			}
			op := f.state().Operations["ack-retry"]
			if op.Status != compaction.Applied || op.Acknowledged || port.ackCalls.Load() != 1 {
				t.Fatalf("operation after failed ACK=%+v calls=%d", op, port.ackCalls.Load())
			}
			if stage == "ack-fact" {
				if _, err := f.worker.GetOutcome(f.ctx, op.Request.Key(compactSession)); !errors.Is(err, effect.ErrOutcomeCollected) {
					t.Fatalf("executor outcome was not collected: %v", err)
				}
			}
			f.counts(1, 1, 0)
			f.reopenWriter()
			x = f.execution(port, nil, false)
			f.settleMaintenance(x)
			if _, err := x.Compactions.Advance(f.ctx, f.w); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 || port.ackCalls.Load() != 2 || !f.state().Operations["ack-retry"].Acknowledged {
				t.Fatalf("calls=%d ACKs=%d state=%+v", calls.Load(), port.ackCalls.Load(), f.state())
			}
			if needed, err := x.Compactions.NeedsRecovery(f.ctx, f.w); err != nil || needed {
				t.Fatalf("NeedsRecovery=%v, %v", needed, err)
			}
			f.counts(1, 1, 0)
			f.assertAtomicFinish()
		})
	}
}

type compactInspectPort struct {
	*executor.Worker
	attachment effect.AttachmentState
	abortState effect.AttachmentState
	outcome    effect.Outcome
	readErr    error
	dispatches int
	reads      int
	recoveries int
}

func (p *compactInspectPort) Attach(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: p.attachment}, nil
}
func (p *compactInspectPort) Abort(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: p.abortState}, nil
}
func (p *compactInspectPort) Dispatch(context.Context, effect.Assignment) error {
	p.dispatches++
	return nil
}
func (p *compactInspectPort) GetOutcome(context.Context, effect.AssignmentKey) (effect.Outcome, error) {
	p.reads++
	return p.outcome, p.readErr
}
func (p *compactInspectPort) RecoverExecution(context.Context, effect.AssignmentKey) error {
	p.recoveries++
	return nil
}

// The inspection stub has no executor record to collect. Durable ACK facts
// still go through the real Writer.
func (*compactInspectPort) Acknowledge(context.Context, effect.AssignmentKey) error { return nil }

func TestCompactionFiniteInspectionDoesNotBlindReplay(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		attachment                                effect.AttachmentState
		readErr                                   error
		wantDispatches, wantReads, wantRecoveries int
	}{
		{"dispatch-returns-without-reading", effect.AttachmentMissing, nil, 1, 0, 0},
		{"orphaned", effect.AttachmentOrphaned, nil, 0, 0, 1},
		{"terminal-unavailable", effect.AttachmentTerminal, effect.ErrOutcomeUnavailable, 0, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
				t.Error("inspection must not invoke model")
				return sdk.ModelResult{}, nil
			})
			port := &compactInspectPort{Worker: f.worker, attachment: tc.attachment, readErr: tc.readErr}
			x := f.execution(port, nil, true)
			f.history()
			f.accept(x, "inspect")
			result, err := x.Compactions.Advance(f.ctx, f.w)
			if tc.readErr != nil {
				if !errors.Is(err, tc.readErr) {
					t.Fatalf("Advance=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !result.Pending || port.dispatches != tc.wantDispatches || port.reads != tc.wantReads || port.recoveries != tc.wantRecoveries {
				t.Fatalf("Advance=%+v dispatch/read/recover=%d/%d/%d", result, port.dispatches, port.reads, port.recoveries)
			}
			if op := f.state().Operations["inspect"]; op.Status != compaction.Pending || op.Acknowledged {
				t.Fatalf("inspection disposed uncertain work: %+v", op)
			}
		})
	}
}

func TestCompactionMissingAbortRaceKeepsIntent(t *testing.T) {
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		t.Error("obsolete operation was redispatched")
		return sdk.ModelResult{}, nil
	})
	port := &compactInspectPort{Worker: f.worker, attachment: effect.AttachmentMissing, abortState: effect.AttachmentActive}
	x := f.execution(port, nil, true)
	f.history()
	f.accept(x, "abort-race")
	f.startTurn("changed-context", true)
	result, err := x.Compactions.Advance(f.ctx, f.w)
	if err != nil || !result.Pending || port.dispatches != 0 || port.reads != 0 {
		t.Fatalf("Abort race=%+v, %v, dispatch/read=%d/%d", result, err, port.dispatches, port.reads)
	}
	if op := f.state().Operations["abort-race"]; op.Status != compaction.Pending || op.Acknowledged {
		t.Fatalf("Abort losing to acceptance disposed intent: %+v", op)
	}
}

func TestCompactionAdvanceRejectsMismatchedOutcome(t *testing.T) {
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		return sdk.ModelResult{Text: compactSummary}, nil
	})
	port := &compactInspectPort{Worker: f.worker, attachment: effect.AttachmentTerminal}
	x := f.execution(port, nil, true)
	f.history()
	f.accept(x, "identity-check")
	// GetOutcome must answer for exactly the key queried, even when the
	// returned key could otherwise identify an accepted compaction.
	if result, err := x.Compactions.Advance(f.ctx, f.w); err == nil || !result.Pending || result.Progress {
		t.Fatalf("mismatched outcome=%+v, %v", result, err)
	}
	if op := f.state().Operations["identity-check"]; op.Status != compaction.Pending || op.Acknowledged {
		t.Fatalf("mismatched outcome changed intent: %+v", op)
	}
	f.counts(0, 0, 0)
}

func TestCompactionTerminalUnknownAndAbortedAreNotReplayed(t *testing.T) {
	for _, aborted := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "aborted"}[aborted], func(t *testing.T) {
			f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
				t.Error("terminal execution was replayed")
				return sdk.ModelResult{}, nil
			})
			port := &compactInspectPort{Worker: f.worker, attachment: effect.AttachmentTerminal}
			if aborted {
				port.attachment = effect.AttachmentAborted
			}
			x := f.execution(port, nil, true)
			f.history()
			f.accept(x, "terminal")
			op := f.state().Operations["terminal"]
			port.outcome = effect.Outcome{Key: op.Request.Key(compactSession), Result: effect.Unknown{Message: "executor lost execution"}}
			if result, err := x.Compactions.Advance(f.ctx, f.w); !errors.Is(err, driver.ErrSummaryFailed) || result.Pending || !result.Progress {
				t.Fatalf("Advance=%+v, %v", result, err)
			}
			if port.dispatches != 0 || !f.state().Operations["terminal"].Acknowledged {
				t.Fatal("terminal execution was replayed or left unacknowledged")
			}
			f.counts(0, 0, 0)
		})
	}
}
