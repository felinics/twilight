package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/store"
)

// fakePort is an execution store whose Attach answers a fixed state and
// whose GetOutcome is scripted per test.
type fakePort struct {
	mu       sync.Mutex
	state    effect.AttachmentState
	asked    []effect.AssignmentKey
	aborted  []effect.AssignmentKey
	outcome  func(context.Context, effect.AssignmentKey) (effect.Outcome, error)
	attachEr error
	// accepted, when set, is what Abort finds instead of a missing key: an
	// acceptance that reached the ledger first.
	accepted effect.AttachmentState
}

func (p *fakePort) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (p *fakePort) Dispatch(context.Context, effect.Assignment) error { return nil }
func (p *fakePort) Attach(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	p.mu.Lock()
	p.asked = append(p.asked, key)
	p.mu.Unlock()
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
func (p *fakePort) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionRunning, nil
}
func (p *fakePort) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	if p.outcome == nil {
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
	return p.outcome(ctx, key)
}
func (p *fakePort) Cancel(context.Context, effect.AssignmentKey) error { return nil }

func executingModel(eff run.EffectID) *store.Snapshot {
	return &store.Snapshot{State: run.MachineState{
		RunID: "r1", Status: run.RunActive,
		Current: run.ModelStep{RefValue: run.StepRef{RunID: "r1", ID: "s1"}, Model: "m", RequestDigest: "sha256:req", Status: run.ModelExecuting, Effect: eff},
	}}
}

// Each executor observation maps to exactly one verdict; only missing and
// aborted produce a recovery command (missing is closed to aborted first,
// RUN-EXE-16), and only an unknown state is an error.
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
		r := &Reconciler{Executions: port}
		decisions, err := r.Plan(context.Background(), "s", executingModel("c1"))
		if err != nil || len(decisions) != 1 {
			t.Fatalf("%s: plan = %+v %v", tc.state, decisions, err)
		}
		d := decisions[0]
		if d.Verdict != tc.want || (d.Recovery != nil) != tc.disposes || d.Observed != tc.observed {
			t.Fatalf("%s: decision = %+v, want %s disposes=%v", tc.state, d, tc.want, tc.disposes)
		}
		if len(port.asked) != 1 || port.asked[0] != (effect.AssignmentKey{Session: "s", RunID: "r1", Effect: "c1"}) {
			t.Fatalf("%s: asked = %+v", tc.state, port.asked)
		}
	}
	if _, err := (&Reconciler{Executions: &fakePort{state: "weird"}}).Plan(context.Background(), "s", executingModel("c1")); err == nil {
		t.Fatal("unknown attachment state accepted")
	}
	// No executor to ask proves nothing: an error, not a disposal. Only an
	// explicit Abandon disposes without asking, and only for a scope with no
	// executor at all: beside a port it is a configuration error, so the
	// Abort of RUN-EXE-16 cannot be skipped while an executor exists.
	if _, err := (&Reconciler{}).Plan(context.Background(), "s", executingModel("c1")); !errors.Is(err, ErrNoExecutionPort) {
		t.Fatalf("plan without executor = %v, want ErrNoExecutionPort", err)
	}
	port := &fakePort{state: effect.AttachmentActive}
	if _, err := (&Reconciler{Abandon: true, Executions: port}).Plan(context.Background(), "s", executingModel("c1")); !errors.Is(err, ErrAbandonWithExecutor) || len(port.asked) != 0 || len(port.aborted) != 0 {
		t.Fatalf("plan with Abandon beside an executor = %v asked=%d aborted=%d, want ErrAbandonWithExecutor and no calls", err, len(port.asked), len(port.aborted))
	}
	decisions, err := (&Reconciler{Abandon: true}).Plan(context.Background(), "s", executingModel("c1"))
	if err != nil || len(decisions) != 1 || decisions[0].Verdict != Dispose || decisions[0].Recovery == nil {
		t.Fatalf("plan with Abandon = %+v %v", decisions, err)
	}
	if _, ok := decisions[0].Recovery.Command.(run.RecoverModelExecution); !ok {
		t.Fatalf("model disposal = %T", decisions[0].Recovery.Command)
	}
	// A target whose start fact recorded no effect cannot be asked about.
	if _, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("")); !errors.Is(err, ErrTargetWithoutEffect) {
		t.Fatalf("plan of a target without effect = %v, want ErrTargetWithoutEffect", err)
	}
}

// AssignmentFromTarget carries the digest-level description of the target
// and never an inline request body (RUN-EXE-7).
func TestAssignmentFromTarget(t *testing.T) {
	targets := plan.RecoveryTargets(&executingModel("c1").State)
	if len(targets) != 1 {
		t.Fatalf("targets = %d", len(targets))
	}
	a := AssignmentFromTarget("s", targets[0])
	if model, ok := a.Model(); !ok || model.Request != nil || model.RequestDigest != "sha256:req" {
		t.Fatalf("assignment = %+v", a)
	}
	if a.Key() != (effect.AssignmentKey{Session: "s", RunID: "r1", Effect: "c1"}) {
		t.Fatalf("key = %+v", a.Key())
	}
}

// recoveringPort is a fakePort that can also take records back.
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

// An orphaned target is deferred, and a port that can recover is asked to
// take the record back once, at Plan time; a port that cannot is only
// observed. Keep and Dispose never ask (RUN-CMT-7, RUN-EXE-6).
func TestPlanAsksRecovererForOrphans(t *testing.T) {
	for _, tc := range []struct {
		state         effect.AttachmentState
		wantRecovered int
	}{
		{effect.AttachmentOrphaned, 1},
		{effect.AttachmentActive, 0},
		{effect.AttachmentMissing, 0},
	} {
		port := &recoveringPort{fakePort: &fakePort{state: tc.state}}
		if _, err := (&Reconciler{Executions: port, Recover: port}).Plan(context.Background(), "s", executingModel("c1")); err != nil {
			t.Fatalf("%s: plan: %v", tc.state, err)
		}
		if len(port.recovered) != tc.wantRecovered {
			t.Fatalf("%s: recovered = %v, want %d", tc.state, port.recovered, tc.wantRecovered)
		}
	}
	plain := &fakePort{state: effect.AttachmentOrphaned}
	if _, err := (&Reconciler{Executions: plain}).Plan(context.Background(), "s", executingModel("c1")); err != nil {
		t.Fatalf("plain port: %v", err)
	}
}
