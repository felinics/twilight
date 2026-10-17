package loop

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/session/writer/writertest"
	"github.com/felinics/twilight/agentcore/testutil/local"
	. "github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/sessionstore/sessionstoretest"
	"github.com/felinics/twilight/agentcore/turn/store"
	"testing"
	"time"
)

const (
	testModel   ModelRef          = "m-1"
	testSession session.SessionID = "s-1"
	testScope   Scope             = Scope(testSession)
)

func cj(raw string) CanonicalJSON { return MustParseCanonicalJSON(raw) }

// inputDigest names an input body: the Turn stores only the digest.
func inputDigest(raw string) Digest { return jsonstable.DigestBytes([]byte(raw)) }

// testStack is the minimal Session stack a Loop test drives: kernel Memory
// Store, the Turn module, one owner process (Writers) and a Runtime.
type testStack struct {
	store    session.Stores
	registry *module.Registry
	bindings artifact.BindingStore
	ledger   artifact.RetentionLedger
	writers  *writer.Factory
	w        writer.Writer
	runtime  *sessionstore.SessionTurnStore
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
	s.writers = &writer.Factory{Store: s.store, Registry: s.registry, Admission: writer.Admission{Bindings: s.bindings, Ledger: s.ledger}}
	s.w = nil
	rt, err := sessionstore.NewSessionTurnStore(sessionstore.Config{Registry: s.registry, Store: s.store, Frozen: sessionstoretest.Frozen(t, s.bindings), Now: s.now})
	if err != nil {
		t.Fatal(err)
	}
	s.runtime = rt
}

// writer is the owner's Writer for testSession: the capability every
// command of the drive takes.
func (s *testStack) writer(t testing.TB) writer.Writer {
	t.Helper()
	if s.w != nil {
		return s.w
	}
	w, err := writertest.Open(t, context.Background(), s.writers, testSession, session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	s.w = w
	return w
}

// createTurn appends the Start group of one Turn with a seed input.
func (s *testStack) createTurn(t testing.TB, turnID TurnID, inputs ...AgentInput) {
	t.Helper()
	newTurn, err := BuildNewTurn(turnID, "", ref.Ref{ID: "test", Digest: "sha256:test"})
	if err != nil {
		t.Fatal(err)
	}
	facts, err := schema.Machine().CreateGroup(newTurn, inputs)
	if err != nil {
		t.Fatal(err)
	}
	runEvents := make([]writer.TypedEvent, 0, len(facts))
	for _, f := range facts {
		runEvents = append(runEvents, writer.TypedEvent{Type: sessionstore.EventType(f), Value: sessionstore.Event{TurnID: turnID, Fact: f}})
	}
	group := &writer.SemanticGroup{CommitID: ledger.CommitID("create/" + string(turnID)),
		Batches: []writer.TypedBatch{{Domain: sessionstore.Stream(turnID), Events: runEvents}}}
	w := s.writer(t)
	res, err := w.Commit(context.Background(), func(writer.View) (*writer.SemanticGroup, error) { return group, nil })
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != writer.CommitApplied {
		t.Fatalf("create run: %s %s", res.Outcome, res.Detail)
	}
}

// newTestRuntime is a Turn store holding "run-1" seeded with one input.
func newTestRuntime(t testing.TB) (*sessionstore.SessionTurnStore, writer.Writer) {
	t.Helper()
	stack := newTestStack(t, nil)
	stack.createTurn(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{"q":"hi"}`)})
	return stack.runtime, stack.writer(t)
}

func loopRuntime(t *testing.T) (*sessionstore.SessionTurnStore, writer.Writer) {
	t.Helper()
	return newTestRuntime(t)
}

// recordFacts returns every committed fact of turnID in stream order.
func recordFacts(t testing.TB, rt *sessionstore.SessionTurnStore, turnID TurnID) []Fact {
	t.Helper()
	record, err := rt.Record(context.Background(), testSession, turnID)
	if err != nil {
		t.Fatal(err)
	}
	return record.Facts
}

func loadState(t testing.TB, rt *sessionstore.SessionTurnStore, w writer.Writer, turnID TurnID) store.Snapshot {
	t.Helper()
	snap, err := rt.Bind(w).Load(context.Background(), turnID)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

// newLoop builds a Loop over a local.LocalExecutor for tests; the executor no
// longer reads frozen bodies, so the runtime is not wired in.
func newLoop(t testing.TB, sink EventSink, models local.ModelCatalog, tools local.ToolCatalog, builder prompt.Builder, settings Settings, streaming bool) (*Loop, error) {
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
	return New(exec, builder, settings)
}
