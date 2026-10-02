package watch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// fakePort is an execution store whose Attach answers a settable state,
// whose GetOutcome is scripted per test and that records every recovery
// it is asked for.
type fakePort struct {
	mu        sync.Mutex
	state     effect.AttachmentState
	outcome   func(effect.AssignmentKey) (effect.Outcome, error)
	recovered []effect.AssignmentKey
}

func (p *fakePort) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (p *fakePort) Dispatch(context.Context, effect.Assignment) error { return nil }
func (p *fakePort) Attach(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return effect.Attachment{State: p.state}, nil
}
func (p *fakePort) Abort(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}, nil
}
func (p *fakePort) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionRunning, nil
}
func (p *fakePort) GetOutcome(_ context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	return p.outcome(key)
}
func (p *fakePort) Cancel(context.Context, effect.AssignmentKey) error { return nil }
func (p *fakePort) RecoverExecution(_ context.Context, key effect.AssignmentKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recovered = append(p.recovered, key)
	return nil
}

func (p *fakePort) setState(s effect.AttachmentState) {
	p.mu.Lock()
	p.state = s
	p.mu.Unlock()
}

func (p *fakePort) recoveries() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.recovered)
}

var key = effect.AssignmentKey{Session: "s", RunID: "r1", Effect: "c1"}

// fast is a Watcher over port that polls quickly, since fakePort offers no
// settlement stream, and is closed with the test.
func fast(t *testing.T, port *fakePort, probe time.Duration) *Watcher {
	t.Helper()
	w := &Watcher{Port: port, Recover: port, Poll: 5 * time.Millisecond, Reconnect: 5 * time.Millisecond, Probe: probe}
	t.Cleanup(w.Close)
	return w
}

// A read that fails with a transport error is retried and never
// fabricated; the real Outcome is delivered when it arrives.
func TestReadRetriesTransportErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	failed := make(chan struct{})
	ready := make(chan struct{})
	var once sync.Once
	port := &fakePort{state: effect.AttachmentActive, outcome: func(key effect.AssignmentKey) (effect.Outcome, error) {
		select {
		case <-ready:
			return effect.Outcome{Key: key, Result: effect.Unknown{Message: "eventual"}}, nil
		default:
			once.Do(func() { close(failed) })
			return effect.Outcome{}, errors.New("temporary transport error")
		}
	}}
	delivered := make(chan effect.Outcome, 1)
	fast(t, port, -1).Watch(ctx, key, func(out effect.Outcome) { delivered <- out }, nil)
	<-failed
	select {
	case out := <-delivered:
		t.Fatalf("read failure fabricated outcome: %+v", out)
	default:
	}
	close(ready)
	select {
	case out := <-delivered:
		if u, ok := out.Result.(effect.Unknown); !ok || u.Message != "eventual" || out.Key != key {
			t.Fatalf("delivered = %+v", out)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// A dropped registration is not delivered: an Outcome that becomes
// readable after cancel is nobody's.
func TestCancelDropsRegistration(t *testing.T) {
	var mu sync.Mutex
	ready := false
	reads := make(chan struct{}, 64)
	port := &fakePort{state: effect.AttachmentActive, outcome: func(key effect.AssignmentKey) (effect.Outcome, error) {
		select {
		case reads <- struct{}{}:
		default:
		}
		mu.Lock()
		defer mu.Unlock()
		if !ready {
			return effect.Outcome{}, effect.ErrOutcomeNotReady
		}
		return effect.Outcome{Key: key, Result: effect.Unknown{Message: "late"}}, nil
	}}
	delivered := make(chan effect.Outcome, 1)
	cancel := fast(t, port, -1).Watch(context.Background(), key, func(out effect.Outcome) { delivered <- out }, nil)
	<-reads
	cancel()
	mu.Lock()
	ready = true
	mu.Unlock()
	select {
	case out := <-delivered:
		t.Fatalf("delivered %+v after the registration was dropped", out)
	case <-time.After(50 * time.Millisecond):
	}
}

// A read the port answers definitively (no record for the key, no Outcome
// ever) drops the registration and reports through fail after one read; a
// read that fails otherwise is retried and neither fabricates an Outcome
// nor reports.
func TestReadErrorTaxonomy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		definitive bool
	}{
		{"execution not found is definitive", effect.ErrExecutionNotFound, true},
		{"outcome unavailable is definitive", effect.ErrOutcomeUnavailable, true},
		{"transport failures are retried", errors.New("boom"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var mu sync.Mutex
			reads := 0
			port := &fakePort{state: effect.AttachmentActive, outcome: func(effect.AssignmentKey) (effect.Outcome, error) {
				mu.Lock()
				reads++
				mu.Unlock()
				return effect.Outcome{}, tc.err
			}}
			failed := make(chan error, 1)
			fast(t, port, -1).Watch(ctx, key,
				func(out effect.Outcome) { t.Errorf("delivered %+v", out) },
				func(err error) { failed <- err })
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

// A key still waiting is re-attached by the probe: a record that has become
// orphaned is handed to Recover once per episode, and the Outcome the
// recovery produces is delivered.
func TestProbeRecoversOrphanOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	ready := false
	port := &fakePort{state: effect.AttachmentActive, outcome: func(key effect.AssignmentKey) (effect.Outcome, error) {
		mu.Lock()
		defer mu.Unlock()
		if !ready {
			return effect.Outcome{}, effect.ErrOutcomeNotReady
		}
		return effect.Outcome{Key: key, Result: effect.Unknown{Message: "recovered"}}, nil
	}}
	delivered := make(chan effect.Outcome, 1)
	fast(t, port, 10*time.Millisecond).Watch(ctx, key, func(out effect.Outcome) { delivered <- out }, nil)
	// The Worker dies: the record reads as orphaned from now on.
	port.setState(effect.AttachmentOrphaned)
	deadline := time.Now().Add(8 * time.Second)
	for port.recoveries() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("recovery requests = %d, want 1", port.recoveries())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Recovery took the record back and finished it.
	mu.Lock()
	ready = true
	mu.Unlock()
	select {
	case out := <-delivered:
		if u, ok := out.Result.(effect.Unknown); !ok || u.Message != "recovered" {
			t.Fatalf("delivered %+v", out)
		}
	case <-ctx.Done():
		t.Fatal("outcome was not delivered after recovery")
	}
	if n := port.recoveries(); n != 1 {
		t.Fatalf("recovery requests = %d, want exactly 1", n)
	}
}
