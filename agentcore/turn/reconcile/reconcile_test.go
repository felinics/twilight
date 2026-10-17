package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/testutil/sdkconv"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/plan"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/sdk"
)

func frozenModel(r sdk.ModelResult) model.ModelResult {
	frozen, err := sdkconv.FreezeModelResult(r)
	if err != nil {
		panic(err)
	}
	return frozen
}

// fakePort records all work. Outcomes are read only by explicit host calls,
// never by Plan, a subscription, or a background recovery task.
type fakePort struct {
	mu       sync.Mutex
	state    effect.AttachmentState
	asked    []effect.AssignmentKey
	aborted  []effect.AssignmentKey
	outcome  func(context.Context, effect.AssignmentKey) (effect.Outcome, error)
	reads    int
	attachEr error
	accepted effect.AttachmentState
}

func (*fakePort) Validate(context.Context, effect.Assignment) (*turn.ToolFailure, error) {
	return nil, nil
}
func (*fakePort) Dispatch(context.Context, effect.Assignment) error { return nil }
func (p *fakePort) Attach(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked = append(p.asked, key)
	if p.attachEr != nil {
		return effect.Attachment{}, p.attachEr
	}
	return effect.Attachment{State: p.state}, nil
}
func (p *fakePort) Abort(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.aborted = append(p.aborted, key)
	if p.accepted != "" {
		return effect.Attachment{State: p.accepted}, nil
	}
	if p.state == effect.AttachmentMissing || p.state == effect.AttachmentAborted {
		return effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}, nil
	}
	return effect.Attachment{State: p.state}, nil
}
func (*fakePort) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionRunning, nil
}
func (p *fakePort) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	p.mu.Lock()
	p.reads++
	outcome := p.outcome
	p.mu.Unlock()
	if outcome == nil {
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
	return outcome(ctx, key)
}
func (*fakePort) Cancel(context.Context, effect.AssignmentKey) error { return nil }
func (*fakePort) Settlements(context.Context, string, uint64, func(effect.Settlement) bool) error {
	panic("a recovery pass must not subscribe or leave background work")
}

func executingModel(eff turn.EffectID) *store.Snapshot {
	return &store.Snapshot{State: turn.MachineState{TurnID: "r1", Status: turn.TurnActive,
		Current: turn.ModelStep{RefValue: turn.StepRef{TurnID: "r1", ID: "s1"}, Model: "m", RequestDigest: "sha256:req", Status: turn.ModelExecuting, Effect: eff}}}
}

func TestPlanVerdicts(t *testing.T) {
	for _, tc := range []struct {
		state    effect.AttachmentState
		want     Verdict
		observed effect.AttachmentState
		disposes bool
	}{
		{effect.AttachmentActive, Keep, effect.AttachmentActive, false},
		{effect.AttachmentTerminal, Keep, effect.AttachmentTerminal, false},
		{effect.AttachmentOrphaned, Defer, effect.AttachmentOrphaned, false},
		{effect.AttachmentMissing, Dispose, effect.AttachmentAborted, true},
		{effect.AttachmentAborted, Dispose, effect.AttachmentAborted, true},
	} {
		port := &fakePort{state: tc.state}
		decisions, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1"))
		if err != nil || len(decisions) != 1 {
			t.Fatalf("%s: plan=%+v %v", tc.state, decisions, err)
		}
		d := decisions[0]
		if d.Verdict != tc.want || (d.Recovery != nil) != tc.disposes || d.Observed != tc.observed || d.Retry {
			t.Fatalf("%s: decision=%+v want=%s disposes=%v", tc.state, d, tc.want, tc.disposes)
		}
		if len(port.asked) != 1 || port.asked[0] != (effect.AssignmentKey{Session: "s", OwnerKind: effect.OwnerTurn, OwnerID: string("r1"), Effect: "c1"}) || port.reads != 0 {
			t.Fatalf("%s: asked=%+v reads=%d", tc.state, port.asked, port.reads)
		}
	}
	if _, err := (&Reconciler{Executions: &fakePort{state: "weird"}}).Plan(context.Background(), "s", executingModel("c1")); err == nil {
		t.Fatal("unknown attachment state accepted")
	}
	if _, err := (&Reconciler{}).Plan(context.Background(), "s", executingModel("c1")); !errors.Is(err, ErrNoExecutionPort) {
		t.Fatalf("no executor: %v", err)
	}
	port := &fakePort{state: effect.AttachmentActive}
	if _, err := (&Reconciler{Abandon: true, Executions: port}).Plan(context.Background(), "s", executingModel("c1")); !errors.Is(err, ErrAbandonWithExecutor) || len(port.asked) != 0 || len(port.aborted) != 0 {
		t.Fatalf("Abandon beside executor: %v asked=%d aborted=%d", err, len(port.asked), len(port.aborted))
	}
	decisions, err := (&Reconciler{Abandon: true}).Plan(context.Background(), "s", executingModel("c1"))
	if err != nil || len(decisions) != 1 || decisions[0].Verdict != Dispose || decisions[0].Recovery == nil {
		t.Fatalf("Abandon: %+v %v", decisions, err)
	}
	if _, ok := decisions[0].Recovery.Command.(turn.RecoverModelExecution); !ok {
		t.Fatalf("model disposal=%T", decisions[0].Recovery.Command)
	}
	if _, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("")); !errors.Is(err, ErrTargetWithoutEffect) {
		t.Fatalf("target without effect: %v", err)
	}
}

func TestAssignmentFromTarget(t *testing.T) {
	targets := plan.RecoveryTargets(&executingModel("c1").State)
	if len(targets) != 1 {
		t.Fatalf("targets=%d", len(targets))
	}
	a := AssignmentFromTarget("s", targets[0])
	if model, ok := a.Model(); !ok || model.Request != nil || model.RequestDigest != "sha256:req" {
		t.Fatalf("assignment=%+v", a)
	}
	if a.Key() != (effect.AssignmentKey{Session: "s", OwnerKind: effect.OwnerTurn, OwnerID: string("r1"), Effect: "c1"}) {
		t.Fatalf("key=%+v", a.Key())
	}
}

// A host explicitly retries a failed read after another finite pass. Plan
// never fabricates an Outcome, owns a read retry, or retains a registration.
func TestKeptOutcomeReadRetries(t *testing.T) {
	ctx := context.Background()
	ready := false
	boom := errors.New("temporary transport error")
	result := frozenModel(sdk.ModelResult{Text: "eventual"})
	port := &fakePort{state: effect.AttachmentActive, outcome: func(_ context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
		if !ready {
			return effect.Outcome{}, boom
		}
		return effect.Outcome{Key: key, Result: effect.ModelSucceeded{Result: result}}, nil
	}}
	key := effect.AssignmentKey{Session: "s", OwnerKind: effect.OwnerTurn, OwnerID: string("r1"), Effect: "c1"}
	for pass := 0; pass < 2; pass++ {
		r := &Reconciler{Executions: port}
		decisions, err := r.Plan(ctx, "s", executingModel("c1"))
		if err != nil || len(decisions) != 1 || decisions[0].Verdict != Keep || port.reads != pass {
			t.Fatalf("pass=%d decisions=%+v err=%v reads=%d", pass, decisions, err, port.reads)
		}
		out, err := port.GetOutcome(ctx, key)
		if pass == 0 {
			if !errors.Is(err, boom) || out.Result != nil {
				t.Fatalf("failed read fabricated Outcome: %+v %v", out, err)
			}
			ready = true
		} else if m, ok := out.ModelResult(); err != nil || !ok || m.Text != "eventual" {
			t.Fatalf("explicit read=%+v %v", out, err)
		}
	}
}

func TestRecoveryPassCanDisconnectAndResume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	port := &recoveringPort{fakePort: &fakePort{state: effect.AttachmentOrphaned}}
	first := &Reconciler{Executions: port}
	if ds, err := first.Plan(ctx, "s", executingModel("c1")); err != nil || len(ds) != 1 || ds[0].Verdict != Defer {
		t.Fatalf("first pass=%+v %v", ds, err)
	}
	cancel()
	if _, err := first.Plan(ctx, "s", executingModel("c1")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled pass=%v", err)
	}
	if len(port.asked) != 1 || len(port.recovered) != 1 || port.reads != 0 {
		t.Fatalf("disconnected pass retained work: asks=%d recoveries=%d reads=%d", len(port.asked), len(port.recovered), port.reads)
	}
	// A new activation has no lifetime, watcher, or registration to rebuild.
	port.state = effect.AttachmentTerminal
	port.outcome = func(_ context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
		return effect.Outcome{Key: key, Result: effect.ModelSucceeded{Result: frozenModel(sdk.ModelResult{Text: "late"})}}, nil
	}
	second := &Reconciler{Executions: port}
	if ds, err := second.Plan(context.Background(), "s", executingModel("c1")); err != nil || len(ds) != 1 || ds[0].Verdict != Keep || port.reads != 0 {
		t.Fatalf("resumed pass=%+v %v reads=%d", ds, err, port.reads)
	}
	out, err := port.GetOutcome(context.Background(), effect.AssignmentKey{Session: "s", OwnerKind: effect.OwnerTurn, OwnerID: string("r1"), Effect: "c1"})
	if m, ok := out.ModelResult(); err != nil || !ok || m.Text != "late" || port.reads != 1 {
		t.Fatalf("resumed explicit read=%+v %v reads=%d", out, err, port.reads)
	}
}

// Read error taxonomy belongs to the effect reader/watcher, not reconciliation.
// Even a definitive read failure cannot silently become a recovery disposition.
func TestOutcomeReadErrorsDoNotBecomePlanDisposal(t *testing.T) {
	for _, readErr := range []error{effect.ErrExecutionNotFound, effect.ErrOutcomeUnavailable, errors.New("transport failure")} {
		port := &fakePort{state: effect.AttachmentActive, outcome: func(context.Context, effect.AssignmentKey) (effect.Outcome, error) { return effect.Outcome{}, readErr }}
		r := &Reconciler{Executions: port}
		for pass := 0; pass < 2; pass++ {
			ds, err := r.Plan(context.Background(), "s", executingModel("c1"))
			if err != nil || len(ds) != 1 || ds[0].Verdict != Keep || ds[0].Recovery != nil || port.reads != pass || len(port.aborted) != 0 {
				t.Fatalf("readErr=%v pass=%d decisions=%+v err=%v reads=%d aborts=%d", readErr, pass, ds, err, port.reads, len(port.aborted))
			}
			if _, err := port.GetOutcome(context.Background(), effect.AssignmentKey{}); !errors.Is(err, readErr) {
				t.Fatalf("explicit read=%v want=%v", err, readErr)
			}
		}
	}
}

type recoveringPort struct {
	*fakePort
	recovered []effect.AssignmentKey
}

func (p *recoveringPort) RecoverExecution(_ context.Context, key effect.AssignmentKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recovered = append(p.recovered, key)
	return nil
}

func TestPlanAsksRecovererForOrphans(t *testing.T) {
	for _, tc := range []struct {
		state         effect.AttachmentState
		wantRecovered int
	}{
		{effect.AttachmentOrphaned, 1}, {effect.AttachmentActive, 0}, {effect.AttachmentMissing, 0},
	} {
		port := &recoveringPort{fakePort: &fakePort{state: tc.state}}
		if _, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1")); err != nil {
			t.Fatalf("%s: %v", tc.state, err)
		}
		if len(port.recovered) != tc.wantRecovered || port.reads != 0 {
			t.Fatalf("%s: recovered=%v reads=%d", tc.state, port.recovered, port.reads)
		}
	}
	if _, err := (&Reconciler{Executions: &fakePort{state: effect.AttachmentOrphaned}}).Plan(context.Background(), "s", executingModel("c1")); err != nil {
		t.Fatal(err)
	}
}

// Orphan detection is a later explicit pass, not a periodic probe installed
// by the earlier Keep. A successful recovery is observed on another pass.
func TestRepeatedPlanRecoversNewOrphan(t *testing.T) {
	port := &recoveringPort{fakePort: &fakePort{state: effect.AttachmentActive}}
	r := &Reconciler{Executions: port}
	for pass, state := range []effect.AttachmentState{effect.AttachmentActive, effect.AttachmentOrphaned, effect.AttachmentActive, effect.AttachmentTerminal} {
		port.state = state
		ds, err := r.Plan(context.Background(), "s", executingModel("c1"))
		want := Keep
		if state == effect.AttachmentOrphaned {
			want = Defer
		}
		if err != nil || len(ds) != 1 || ds[0].Verdict != want || port.reads != 0 {
			t.Fatalf("pass=%d decisions=%+v err=%v reads=%d", pass, ds, err, port.reads)
		}
	}
	if len(port.asked) != 4 || len(port.recovered) != 1 {
		t.Fatalf("asks=%d recoveries=%d", len(port.asked), len(port.recovered))
	}
}

func TestPlanClosesMissingBeforeDisposing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		accepted effect.AttachmentState
		want     Verdict
		disposes bool
	}{
		{"tombstone stands", "", Dispose, true},
		{"acceptance won and continues", effect.AttachmentActive, Keep, false},
		{"acceptance won without a lease", effect.AttachmentOrphaned, Defer, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			port := &fakePort{state: effect.AttachmentMissing, accepted: tc.accepted}
			ds, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1"))
			if err != nil || len(ds) != 1 {
				t.Fatalf("plan=%+v %v", ds, err)
			}
			if d := ds[0]; d.Verdict != tc.want || (d.Recovery != nil) != tc.disposes || len(port.aborted) != 1 {
				t.Fatalf("decision=%+v aborts=%d want=%s dispose=%v", d, len(port.aborted), tc.want, tc.disposes)
			}
		})
	}
	for _, state := range []effect.AttachmentState{effect.AttachmentActive, effect.AttachmentOrphaned, effect.AttachmentTerminal} {
		port := &fakePort{state: state}
		if _, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1")); err != nil || len(port.aborted) != 0 {
			t.Fatalf("%s: aborts=%d err=%v", state, len(port.aborted), err)
		}
	}
}

func TestPlanTransportFailureIsNotAbsence(t *testing.T) {
	boom := errors.New("executor unreachable")
	port := &fakePort{state: effect.AttachmentMissing, attachEr: boom}
	if ds, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1")); !errors.Is(err, boom) || len(ds) != 0 || len(port.aborted) != 0 {
		t.Fatalf("plan=%+v err=%v aborts=%d", ds, err, len(port.aborted))
	}
}
