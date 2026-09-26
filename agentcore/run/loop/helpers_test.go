package loop

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/run/runmodtest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"testing"
	"time"
)

const (
	testModel   ModelRef          = "m-1"
	testSession session.SessionID = "s-1"
	testScope   Scope             = Scope(testSession)
)

func cj(raw string) CanonicalJSON { return MustParseCanonicalJSON(raw) }

// inputDigest names an input body: the Run stores only the digest.
func inputDigest(raw string) Digest { return es.DigestBytes([]byte(raw)) }

// testStack is the minimal Session stack a Loop test drives: kernel Memory
// Store, the run module, one owner process (Writers) and a Runtime.
type testStack struct {
	store    session.Store
	registry *extension.Registry
	bindings artifact.BindingStore
	ledger   artifact.RetentionLedger
	writers  writer.Writers
	runtime  *runmod.SessionRunStore
	now      func() time.Time
}

func newTestStack(t testing.TB, now func() time.Time) *testStack {
	t.Helper()
	if now == nil {
		now = time.Now
	}
	store := filestoretest.Store(t)
	registry, err := extension.BuildRegistry(runmod.Module)
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
	rt, err := runmod.NewSessionRunStore(runmod.Config{Registry: s.registry, Store: s.store, Frozen: runmodtest.Frozen(t, s.bindings), Now: s.now})
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
		runEvents = append(runEvents, writer.TypedEvent{Type: runmod.EventType(f), Value: runmod.Event{RunID: runID, Fact: f}})
	}
	group := &writer.SemanticGroup{CommitID: session.CommitID("create/" + string(runID)),
		Batches: []writer.TypedBatch{{Stream: runmod.Stream(runID), Events: runEvents}}}
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
func newTestRuntime(t testing.TB) (*runmod.SessionRunStore, writer.Writer) {
	t.Helper()
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{"q":"hi"}`)})
	return stack.runtime, stack.writer(t)
}

func loopRuntime(t *testing.T) (*runmod.SessionRunStore, writer.Writer) {
	t.Helper()
	return newTestRuntime(t)
}

// recordFacts returns every committed fact of runID in stream order.
func recordFacts(t testing.TB, rt *runmod.SessionRunStore, runID RunID) []Fact {
	t.Helper()
	record, err := rt.Record(context.Background(), testSession, runID)
	if err != nil {
		t.Fatal(err)
	}
	return record.Facts
}

func loadState(t testing.TB, rt *runmod.SessionRunStore, w writer.Writer, runID RunID) runtime.Snapshot {
	t.Helper()
	snap, err := rt.Bind(w).Load(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

// newLoop builds a Loop over a LocalExecutor for tests; the executor no
// longer reads frozen bodies (RUN-EXE-7), so the runtime is not wired in.
func newLoop(t testing.TB, sink EventSink, models ModelCatalog, tools ToolCatalog, builder PromptBuilder, settings Settings, streaming bool) (*Loop, error) {
	if models == nil {
		return nil, errors.New("agent: loop: nil model catalog")
	}
	if tools == nil {
		return nil, errors.New("agent: loop: nil tool catalog")
	}
	hub := executor.NewProgressHub(0)
	backend, err := NewLocalExecutor(models, tools, hub, streaming)
	if err != nil {
		return nil, err
	}
	exec, err := executor.NewWorker(context.Background(), storetest.NewMap(nil), []executor.Route{executor.Default("local", backend)}, executor.WorkerOptions{Progress: hub})
	if err != nil {
		return nil, err
	}
	return New(exec, builder, settings)
}
