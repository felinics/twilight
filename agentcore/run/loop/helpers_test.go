package loop

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/prompt"
	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/effect/watch"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/run/sessionstore/sessionstoretest"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"testing"
	"time"
)

const (
	testModel   ModelRef          = "m-1"
	testSession session.SessionID = "s-1"
	testScope   Scope             = Scope(testSession)
)

func cj(raw string) jsonstable.Value { return jsonstable.MustParse(raw) }

// inputDigest names an input body: the Run stores only the digest.
func inputDigest(raw string) Digest { return jsonstable.DigestBytes([]byte(raw)) }

// testStack is the minimal Session stack a Loop test drives: kernel Memory
// Store, the run module, one owner process (Writers) and a Runtime.
type testStack struct {
	store    session.Stores
	registry *module.Registry
	bindings artifact.BindingStore
	ledger   artifact.RetentionLedger
	writers  writer.Writers
	runtime  *sessionstore.SessionRunStore
	now      func() time.Time
}

func newTestStack(t testing.TB, now func() time.Time) *testStack {
	t.Helper()
	if now == nil {
		now = time.Now
	}
	store := filestoretest.Store(t)
	registry, err := module.BuildRegistry(sessionstore.Module)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), session.CreateRequest{SessionID: testSession}); err != nil {
		t.Fatal(err)
	}
	s := &testStack{store: store, registry: registry, now: now}
	s.open(t)
	return s
}

// open starts a new owner process over the same store, superseding a previous
// one that is still open.
func (s *testStack) open(t testing.TB) {
	t.Helper()
	if s.bindings == nil {
		s.bindings, s.ledger = artifacttest.Stores(t)
	}
	s.writers = writer.NewWriters(s.store, s.registry, writer.Admission{Bindings: s.bindings, Ledger: s.ledger}, session.OpenOptions{Takeover: true}, writer.WritersConfig{})
	rt, err := sessionstore.NewSessionRunStore(sessionstore.Config{Registry: s.registry, Store: s.store, Frozen: sessionstoretest.Frozen(t, s.bindings), Now: s.now})
	if err != nil {
		t.Fatal(err)
	}
	s.runtime = rt
}

// writer is the owner's Writer for testSession: the capability every
// command of the drive takes.
func (s *testStack) writer(t testing.TB) writer.Writer {
	t.Helper()
	w, err := s.writers.Writer(context.Background(), testSession)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// createRun appends the Start group of one Run with a seed input (RUN-NEW-1).
func (s *testStack) createRun(t testing.TB, runID RunID, inputs ...AgentInput) {
	t.Helper()
	newRun, err := BuildNewRun(runID, "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := schema.Machine().CreateGroup(newRun, inputs)
	if err != nil {
		t.Fatal(err)
	}
	runEvents := make([]writer.TypedEvent, 0, len(facts))
	for _, f := range facts {
		runEvents = append(runEvents, writer.TypedEvent{Type: sessionstore.EventType(f), Value: sessionstore.Event{RunID: runID, Fact: f}})
	}
	group := &writer.SemanticGroup{CommitID: ledger.CommitID("create/" + string(runID)),
		Batches: []writer.TypedBatch{{Domain: sessionstore.Stream(runID), Events: runEvents}}}
	w, err := s.writers.Writer(context.Background(), testSession)
	if err != nil {
		t.Fatal(err)
	}
	res, err := w.Commit(context.Background(), func(writer.View) (*writer.SemanticGroup, error) { return group, nil })
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != writer.CommitApplied {
		t.Fatalf("create run: %s %s", res.Outcome, res.Detail)
	}
}

// newTestRuntime is a run store holding "run-1" seeded with one input.
func newTestRuntime(t testing.TB) (*sessionstore.SessionRunStore, writer.Writer) {
	t.Helper()
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{"q":"hi"}`)})
	return stack.runtime, stack.writer(t)
}

func loopRuntime(t *testing.T) (*sessionstore.SessionRunStore, writer.Writer) {
	t.Helper()
	return newTestRuntime(t)
}

// recordFacts returns every committed fact of runID in stream order.
func recordFacts(t testing.TB, rt *sessionstore.SessionRunStore, runID RunID) []Fact {
	t.Helper()
	record, err := rt.Record(context.Background(), testSession, runID)
	if err != nil {
		t.Fatal(err)
	}
	return record.Facts
}

func loadState(t testing.TB, rt *sessionstore.SessionRunStore, w writer.Writer, runID RunID) store.Snapshot {
	t.Helper()
	snap, err := rt.Bind(w).Load(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

// newLoop builds a Loop over a local.LocalExecutor for tests; the executor no
// longer reads frozen bodies (RUN-EXE-7), so the runtime is not wired in.
func newLoop(t testing.TB, models local.ModelCatalog, tools local.ToolCatalog, settings Settings, streaming bool) (*Loop, error) {
	if models == nil {
		return nil, errors.New("agent: loop: nil model catalog")
	}
	if tools == nil {
		return nil, errors.New("agent: loop: nil tool catalog")
	}
	hub := executor.NewProgressHub(0)
	backend, err := local.NewLocalExecutor(models, tools, hub, streaming)
	if err != nil {
		return nil, err
	}
	exec, err := executor.NewWorker(context.Background(), storetest.NewMap(nil), []executor.Route{executor.Default("local", backend)}, executor.WorkerOptions{Progress: hub})
	if err != nil {
		return nil, err
	}
	return New(effect.PortsOf(exec), settings)
}

// awaiting is the Watcher a test waits for Outcomes with over the Loop's
// port: the port's settlement stream when it offers one, a fast poll
// otherwise, and no orphan probing. It closes with the test.
func awaiting(t testing.TB, l *Loop) *watch.Watcher {
	t.Helper()
	w := &watch.Watcher{Port: l.Ports.Execution, Settlements: l.Ports.Settlements, Poll: 5 * time.Millisecond, Reconnect: 5 * time.Millisecond, Probe: -1}
	t.Cleanup(w.Close)
	return w
}

// settle steps one Run the way a host does, on the caller's goroutine:
// Advance, await the Outcome of each effect it dispatched, Deliver it, and
// again, until the Run finishes, waits, or a step fails. Each step is the
// Loop's own; the wait is the Watcher's.
func settle(ctx context.Context, l *Loop, watcher *watch.Watcher, rt store.RunStore, builder prompt.Builder, runID RunID) (LoopResult, error) {
	for {
		res, err := l.Advance(ctx, rt, builder, runID)
		if err != nil || res.Disposition != LoopDispatched {
			return res, err
		}
		for _, key := range res.Dispatched {
			out, err := watcher.Await(ctx, key)
			if err != nil {
				return LoopResult{}, err
			}
			if res, err = l.Deliver(ctx, rt, out); err != nil || res.Disposition == LoopFinished {
				return res, err
			}
		}
	}
}

// recoverRuns runs the takeover disposition of rec over every active Run of
// the Session w owns and returns the accepted recovery commands.
func recoverRuns(ctx context.Context, t testing.TB, rt *sessionstore.SessionRunStore, w writer.Writer, rec *reconcile.Reconciler) (int, error) {
	t.Helper()
	snapshots, err := rt.ActiveRuns(ctx, w)
	if err != nil {
		return 0, err
	}
	return rec.ReconcileAll(ctx, rt.Bind(w), snapshots)
}
