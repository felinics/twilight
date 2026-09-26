package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/sdk"
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

// watching is the Watcher a test Reconciler waits with: a fast poll, since
// fakePort offers no settlement stream, and closed with the test.
func watching(t *testing.T, port effect.ExecutionPort) *effect.Watcher {
	t.Helper()
	w := &effect.Watcher{Port: port, Poll: 5 * time.Millisecond, Reconnect: 5 * time.Millisecond}
	t.Cleanup(w.Close)
	return w
}

func executingModel(eff run.EffectID) *runtime.Snapshot {
	return &runtime.Snapshot{State: run.MachineState{
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
	// Deliver without a Lifetime would keep targets with nothing reading
	// their Outcome and nothing saying so: a configuration error, refused
	// before the executor is asked.
	port = &fakePort{state: effect.AttachmentActive}
	if _, err := (&Reconciler{Executions: port, Deliver: func(effect.Outcome) {}}).Plan(context.Background(), "s", executingModel("c1")); !errors.Is(err, ErrDeliverWithoutLifetime) || len(port.asked) != 0 {
		t.Fatalf("plan with Deliver and no Lifetime = %v asked=%d, want ErrDeliverWithoutLifetime and no calls", err, len(port.asked))
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

// A kept target's Outcome read is retried by the Watcher on a transport
// error and never fabricated; the real one is delivered when it arrives.
func TestKeptOutcomeReadRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	failed := make(chan struct{})
	ready := make(chan struct{})
	var once sync.Once
	result := sdk.ModelResult{Text: "eventual"}
	port := &fakePort{state: effect.AttachmentActive, outcome: func(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
		select {
		case <-ready:
			return effect.Outcome{Key: key, Result: effect.ModelSucceeded{Result: result}}, nil
		default:
			once.Do(func() { close(failed) })
			return effect.Outcome{}, errors.New("temporary transport error")
		}
	}}
	delivered := make(chan effect.Outcome, 1)
	r := &Reconciler{Executions: port, Lifetime: ctx, Watcher: watching(t, port), Deliver: func(out effect.Outcome) { delivered <- out }}
	if _, err := r.Plan(ctx, "s", executingModel("c1")); err != nil {
		t.Fatal(err)
	}
	<-failed
	select {
	case out := <-delivered:
		t.Fatalf("read failure fabricated outcome: %+v", out)
	default:
	}
	close(ready)
	select {
	case out := <-delivered:
		if r, ok := out.ModelResult(); !ok || r.Text != "eventual" {
			t.Fatalf("delivered = %+v", out)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// A kept target's registration ends with the reconciler's Lifetime: an
// Outcome that becomes readable afterwards is not delivered.
func TestLifetimeStopsOutcomeWatcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lifetime, stop := context.WithCancel(ctx)
	var mu sync.Mutex
	ready := false
	reads := make(chan struct{}, 64)
	port := &fakePort{state: effect.AttachmentOrphaned, outcome: func(context.Context, effect.AssignmentKey) (effect.Outcome, error) {
		select {
		case reads <- struct{}{}:
		default:
		}
		mu.Lock()
		defer mu.Unlock()
		if !ready {
			return effect.Outcome{}, effect.ErrOutcomeNotReady
		}
		return effect.Outcome{Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "late"}}}, nil
	}}
	delivered := make(chan effect.Outcome, 1)
	r := &Reconciler{Executions: port, Lifetime: lifetime, Watcher: watching(t, port), Deliver: func(out effect.Outcome) { delivered <- out }}
	decisions, err := r.Plan(ctx, "s", executingModel("c1"))
	if err != nil || decisions[0].Verdict != Defer {
		t.Fatalf("plan = %+v %v", decisions, err)
	}
	<-reads
	stop()
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	ready = true
	mu.Unlock()
	select {
	case out := <-delivered:
		t.Fatalf("delivered %+v after the Lifetime ended", out)
	case <-time.After(50 * time.Millisecond):
	}
}

// A read the executor answers definitively (no record for the key, no
// Outcome ever) drops the registration and reports through Fail after one
// read; a read that fails otherwise is retried by the Watcher and neither
// fabricates an Outcome nor reports.
func TestOutcomeReadErrorTaxonomy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cases := []struct {
		name       string
		err        error
		definitive bool
	}{
		{"execution not found is definitive", effect.ErrExecutionNotFound, true},
		{"outcome unavailable is definitive", effect.ErrOutcomeUnavailable, true},
		{"transport failures are retried", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			reads := 0
			port := &fakePort{state: effect.AttachmentActive, outcome: func(context.Context, effect.AssignmentKey) (effect.Outcome, error) {
				mu.Lock()
				reads++
				mu.Unlock()
				return effect.Outcome{}, tc.err
			}}
			failed := make(chan error, 1)
			r := &Reconciler{Executions: port, Lifetime: ctx, Watcher: watching(t, port),
				Deliver: func(out effect.Outcome) { t.Errorf("delivered %+v", out) },
				Fail:    func(_ effect.AssignmentKey, err error) { failed <- err }}
			if _, err := r.Plan(ctx, "s", executingModel("c1")); err != nil {
				t.Fatal(err)
			}
			if !tc.definitive {
				time.Sleep(50 * time.Millisecond)
				mu.Lock()
				n := reads
				mu.Unlock()
				if n < 2 {
					t.Fatalf("transient failure read %d times, want retries", n)
				}
				select {
				case err := <-failed:
					t.Fatalf("transient failure reported %v", err)
				default:
				}
				return
			}
			select {
			case err := <-failed:
				if !errors.Is(err, tc.err) {
					t.Fatalf("fail = %v, want %v", err, tc.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			time.Sleep(30 * time.Millisecond)
			mu.Lock()
			n := reads
			mu.Unlock()
			if n != 1 {
				t.Fatalf("definitive error read %d times, want 1", n)
			}
		})
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
		if _, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1")); err != nil {
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

// A kept target still waiting is re-attached at OrphanProbe intervals: a
// record that has become orphaned is recovered once per episode, and the
// Outcome the recovery produces is delivered.
func TestKeptOutcomeProbeRecoversOrphan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	ready := false
	port := &recoveringPort{fakePort: &fakePort{state: effect.AttachmentActive}}
	port.outcome = func(context.Context, effect.AssignmentKey) (effect.Outcome, error) {
		mu.Lock()
		defer mu.Unlock()
		if !ready {
			return effect.Outcome{}, effect.ErrOutcomeNotReady
		}
		return effect.Outcome{Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "recovered"}}}, nil
	}
	delivered := make(chan effect.Outcome, 1)
	r := &Reconciler{Executions: port, Lifetime: ctx, Watcher: watching(t, port), OrphanProbe: 10 * time.Millisecond, Deliver: func(out effect.Outcome) { delivered <- out }}
	if _, err := r.Plan(ctx, "s", executingModel("c1")); err != nil {
		t.Fatal(err)
	}
	// The Worker dies: the record reads as orphaned from now on.
	port.mu.Lock()
	port.state = effect.AttachmentOrphaned
	port.mu.Unlock()
	deadline := time.Now().Add(8 * time.Second)
	for {
		port.mu.Lock()
		n := len(port.recovered)
		port.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery requests = %d, want 1", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Recovery took the record back and finished it.
	mu.Lock()
	ready = true
	mu.Unlock()
	select {
	case out := <-delivered:
		if m, ok := out.Result.(effect.ModelSucceeded); !ok || m.Result.Text != "recovered" {
			t.Fatalf("delivered %+v", out)
		}
	case <-ctx.Done():
		t.Fatal("outcome was not delivered after recovery")
	}
	port.mu.Lock()
	n := len(port.recovered)
	port.mu.Unlock()
	if n != 1 {
		t.Fatalf("recovery requests = %d, want exactly 1", n)
	}
}

// RUN-EXE-16: a missing effect is closed on the executor before the Run
// disposes it, so a Dispatch that arrives later starts nothing; when an
// acceptance reached the executor first, Abort reports it and the target is
// kept or deferred instead of disposed.
func TestPlanClosesMissingBeforeDisposing(t *testing.T) {
	cases := []struct {
		name     string
		accepted effect.AttachmentState // what Abort finds when the acceptance won
		want     Verdict
		disposes bool
	}{
		{"tombstone stands", "", Dispose, true},
		{"acceptance won and runs", effect.AttachmentActive, Keep, false},
		{"acceptance won without a lease", effect.AttachmentOrphaned, Defer, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			port := &fakePort{state: effect.AttachmentMissing, accepted: tc.accepted}
			decisions, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1"))
			if err != nil || len(decisions) != 1 {
				t.Fatalf("plan = %+v %v", decisions, err)
			}
			d := decisions[0]
			if d.Verdict != tc.want || (d.Recovery != nil) != tc.disposes || len(port.aborted) != 1 {
				t.Fatalf("decision = %+v aborted=%d, want %s disposes=%v after one Abort", d, len(port.aborted), tc.want, tc.disposes)
			}
		})
	}
	// Anything the executor still holds is never aborted.
	for _, state := range []effect.AttachmentState{effect.AttachmentActive, effect.AttachmentOrphaned, effect.AttachmentTerminal} {
		port := &fakePort{state: state}
		if _, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1")); err != nil || len(port.aborted) != 0 {
			t.Fatalf("%s: aborted=%d %v, want no Abort", state, len(port.aborted), err)
		}
	}
}
