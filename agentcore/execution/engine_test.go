package execution

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/run/sessionstore/sessionstoretest"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// scriptedPort is an ExecutionPort a test scripts: every dispatched key
// gets an Outcome channel the test fills, Attach answers the attachment the
// test set, and Settlements tells the Watcher of every Outcome handed back
// so it is read at once.
type scriptedPort struct {
	mu         sync.Mutex
	attachment effect.AttachmentState
	dispatched []effect.Assignment
	outcomes   map[effect.AssignmentKey]chan effect.Outcome
	settled    map[effect.AssignmentKey]effect.Outcome
	acked      []effect.AssignmentKey
	cancelled  []effect.AssignmentKey
	notices    chan effect.AssignmentKey
}

func newScriptedPort(attachment effect.AttachmentState) *scriptedPort {
	return &scriptedPort{attachment: attachment, outcomes: map[effect.AssignmentKey]chan effect.Outcome{},
		settled: map[effect.AssignmentKey]effect.Outcome{}, notices: make(chan effect.AssignmentKey, 16)}
}

func (p *scriptedPort) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (p *scriptedPort) Dispatch(_ context.Context, a effect.Assignment) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dispatched = append(p.dispatched, a)
	if _, ok := p.outcomes[a.Key()]; !ok {
		p.outcomes[a.Key()] = make(chan effect.Outcome, 1)
	}
	return nil
}
func (p *scriptedPort) Attach(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return effect.Attachment{State: p.attachment, Execution: effect.ExecutionRunning, BackendAttached: p.attachment == effect.AttachmentActive}, nil
}
func (p *scriptedPort) Abort(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	return p.Attach(ctx, key)
}
func (p *scriptedPort) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionRunning, nil
}
func (p *scriptedPort) GetOutcome(_ context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if out, ok := p.settled[key]; ok {
		return out, nil
	}
	ch, ok := p.outcomes[key]
	if !ok {
		return effect.Outcome{}, effect.ErrExecutionNotFound
	}
	select {
	case out := <-ch:
		p.settled[key] = out
		return out, nil
	default:
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
}
func (p *scriptedPort) Cancel(_ context.Context, key effect.AssignmentKey) error {
	p.mu.Lock()
	p.cancelled = append(p.cancelled, key)
	p.mu.Unlock()
	return nil
}
func (p *scriptedPort) Acknowledge(_ context.Context, key effect.AssignmentKey) error {
	p.mu.Lock()
	p.acked = append(p.acked, key)
	p.mu.Unlock()
	return nil
}

// Settlements forwards the keys complete hands back.
func (p *scriptedPort) Settlements(ctx context.Context, _ string, _ uint64, fn func(effect.Settlement) bool) error {
	seq := uint64(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case key := <-p.notices:
			seq++
			if !fn(effect.Settlement{Key: key, Epoch: "e", Sequence: seq}) {
				return nil
			}
		}
	}
}

// complete makes key's Outcome readable and notices the Watcher.
func (p *scriptedPort) complete(key effect.AssignmentKey, out effect.Outcome) {
	p.mu.Lock()
	ch, ok := p.outcomes[key]
	if !ok {
		ch = make(chan effect.Outcome, 1)
		p.outcomes[key] = ch
	}
	p.mu.Unlock()
	out.Key = key
	ch <- out
	p.notices <- key
}

func (p *scriptedPort) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.dispatched)
}

// staticBuilder is the one prompt builder the tests register: one user
// message under the preset's model.
type staticBuilder struct{ ap preset.AgentPreset }

func (b staticBuilder) Build(_ context.Context, in run.PromptInput) (prompt.Prompt, error) {
	ids := make([]run.InputID, len(in.Inputs))
	for i, x := range in.Inputs {
		ids[i] = x.ID
	}
	req := model.ModelRequest{Model: string(b.ap.Model), Messages: []model.Message{{Role: model.MessageRoleUser,
		Content: []model.MessagePart{{Type: model.MessagePartTypeText, Text: "go"}}}}}
	return prompt.Prompt{Model: b.ap.Model, Request: req, InputIDs: ids}, nil
}

const builderRef preset.PromptBuilderRef = "test/static"

func catalog(t *testing.T) *prompt.Catalog {
	t.Helper()
	c, err := prompt.NewCatalog(map[prompt.BuilderRef]prompt.BuilderFactory{
		builderRef: func(ap preset.AgentPreset, _ prompt.Sources) prompt.Builder { return staticBuilder{ap} },
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// sessionSide is the Session side one Engine test needs: a Store, Writers,
// the run adapter and the Turn protocol's committer over them.
type sessionSide struct {
	store   session.Stores
	writers writer.Writers
	runs    *sessionstore.SessionRunStore
	proj    session.ProjectionReader
	turns   *turn.Coordinator
	chat    *chatlog.Commands
	// presets is the registry the engine under test resolves from.
	presets preset.Registry
}

func newSessionSide(t *testing.T) *sessionSide {
	t.Helper()
	registry, err := module.BuildRegistry(chatlog.Module, sessionstore.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	store := filestoretest.Store(t)
	bindings, retention := artifacttest.Stores(t)
	writers := writer.NewWriters(store, registry, writer.Admission{Bindings: bindings, Ledger: retention}, session.OpenOptions{Takeover: true}, writer.WritersConfig{})
	runs, err := sessionstore.NewSessionRunStore(sessionstore.Config{Registry: registry, Store: store, Frozen: sessionstoretest.Frozen(t, bindings)})
	if err != nil {
		t.Fatal(err)
	}
	proj := session.NewProjectionReader(store, registry, nil)
	t.Cleanup(func() { _ = writer.CloseWriters(context.Background(), writers) })
	return &sessionSide{store: store, writers: writers, runs: runs, proj: proj,
		turns: &turn.Coordinator{Projections: proj, Runs: runs}, chat: &chatlog.Commands{Now: time.Now}}
}

func (s *sessionSide) writer(t *testing.T, sid session.SessionID) writer.Writer {
	t.Helper()
	w, err := s.writers.Writer(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// startTurn creates sid, submits one input and starts a Turn under the
// preset registered in s; the Run is Open and undriven. It returns the
// Writer, the Turn and what a drive of the Turn needs: its Run and preset.
func (s *sessionSide) startTurn(t *testing.T, sid session.SessionID) (writer.Writer, turn.TurnRef, run.RunID, preset.PresetRef) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	ref, err := s.presets.Register("p", preset.AgentPreset{Model: "m-1", PromptBuilder: builderRef})
	if err != nil {
		t.Fatal(err)
	}
	w := s.writer(t, sid)
	in, err := s.chat.Submit(ctx, w, "in-1", jsonstable.MustParse(`{"text":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	tref := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	started, err := s.turns.Start(ctx, w, turn.StartRequest{Ref: tref, Inputs: []run.AgentInput{in}, Preset: ref})
	if err != nil {
		t.Fatal(err)
	}
	return w, tref, started.RunID, ref
}

func newEngine(t *testing.T, cfg Config, s *sessionSide) *engine {
	t.Helper()
	if cfg.Executor.Execution == nil {
		cfg.Executor = effect.PortsOf(newScriptedPort(effect.AttachmentMissing))
	}
	if cfg.PromptBuilders == nil {
		cfg.PromptBuilders = catalog(t)
	}
	if cfg.Presets == nil {
		cfg.Presets = preset.NewMemory()
	}
	src := Sources{}
	if s != nil {
		s.presets = cfg.Presets
		src = Sources{Runs: s.runs, Projections: s.proj}
	}
	x, err := New(cfg, src)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.Close)
	return x.(*engine)
}

// A component failure reaches the transient stream exactly once: through
// the configured Fail callback, which owns that delivery, or directly when
// no callback is configured.
func TestComponentFailureReportedOnce(t *testing.T) {
	const sid session.SessionID = "s-fail"
	boom := errors.New("boom")
	for _, tc := range []struct {
		name         string
		withCallback bool
		wantCalls    int
		wantEvents   int
	}{
		{"callback owns delivery", true, 1, 0},
		{"no callback publishes directly", false, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			cfg := Config{Progress: observe.NewProgresses()}
			if tc.withCallback {
				cfg.Fail = func(session.SessionID, error) { calls++ }
			}
			x := newEngine(t, cfg, nil)
			events := cfg.Progress.Subscribe(ctx, sid)
			x.recovery.report(sid, boom)
			got := 0
			timeout := time.After(100 * time.Millisecond)
		drain:
			for {
				select {
				case e := <-events:
					if !errors.Is(e.Err, boom) {
						t.Fatalf("event = %+v, want Err boom", e)
					}
					got++
				case <-timeout:
					break drain
				}
			}
			if calls != tc.wantCalls || got != tc.wantEvents {
				t.Fatalf("callback calls = %d, progress events = %d; want %d and %d", calls, got, tc.wantCalls, tc.wantEvents)
			}
		})
	}
}

// The redispatch budget and the dispatch re-offer policy of the config reach
// the components that apply them.
func TestPoliciesReachComponents(t *testing.T) {
	policy := loop.DispatchPolicy{Retries: 5, Backoff: 7 * time.Millisecond}
	x := newEngine(t, Config{MaxRedispatches: 7, Dispatch: policy}, nil)
	if x.recovery.maxRedispatches != 7 {
		t.Fatalf("recovery.maxRedispatches = %d, want 7", x.recovery.maxRedispatches)
	}
	if x.recovery.loop.Settings.Dispatch != policy {
		t.Fatalf("loop.Settings.Dispatch = %+v, want %+v", x.recovery.loop.Settings.Dispatch, policy)
	}
}

// A reattached Outcome is settled and reported through Notify; the Engine
// does not drive the Run on. The Run's model step is Executing when the
// Engine takes the Session over, the executor still holds the attempt, its
// Outcome arrives, and the Run stands settled at the model step's completion
// with nothing dispatched after it until the host drives.
func TestReattachedOutcomeNotifiesWithoutDriving(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sid session.SessionID = "s-notify"
	s := newSessionSide(t)
	port := newScriptedPort(effect.AttachmentActive)
	notified := make(chan session.SessionID, 4)
	x := newEngine(t, Config{Executor: effect.PortsOf(port), Notify: func(id session.SessionID) { notified <- id }}, s)
	w, tref, _, _ := s.startTurn(t, sid)

	// One Advance dispatches the model effect and returns: the state an
	// owner that died mid-flight leaves, an Executing step with its attempt
	// on the executor. The same Loop instance the Engine drives with is used,
	// so its already-driving guard sees exactly what a takeover sees.
	surface, err := turn.ReadSurface(ctx, s.proj, sid)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := x.driver.builders.For(surface.Turns[tref.TurnID].Preset)
	if err != nil {
		t.Fatal(err)
	}
	res, err := x.driver.loop.Advance(ctx, s.runs.Bind(w), builder, surface.Turns[tref.TurnID].RunID)
	if err != nil || len(res.Dispatched) != 1 {
		t.Fatalf("advance = %+v %v, want one dispatch", res, err)
	}

	// Takeover: the executor still holds the attempt (Active), so the target
	// is kept and its Outcome awaited.
	if n, err := x.Takeover(ctx, w); err != nil || n != 0 {
		t.Fatalf("takeover = %d %v, want 0 dispositions", n, err)
	}
	a := port.dispatched[0]
	port.complete(a.Key(), effect.Outcome{Result: effect.ModelSucceeded{Result: model.ModelResult{Text: "done", FinishReason: model.FinishReasonStop}}})

	select {
	case got := <-notified:
		if got != sid {
			t.Fatalf("notified %s, want %s", got, sid)
		}
	case <-ctx.Done():
		t.Fatal("no Notify after the reattached Outcome settled")
	}
	// Nothing was dispatched after the settlement: the Engine did not drive.
	time.Sleep(50 * time.Millisecond)
	if n := port.count(); n != 1 {
		t.Fatalf("dispatches after settlement = %d, want 1 (the engine drove the Run itself)", n)
	}
	rec, err := s.runs.Record(ctx, sid, turn.DeriveRunID(sid, tref.TurnID))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Snapshot.State.Status != run.RunCompleted {
		t.Fatalf("run after settlement = %v, want completed", rec.Snapshot.State.Status)
	}
	select {
	case got := <-notified:
		t.Fatalf("second Notify %s", got)
	default:
	}
}

// Drive steps the Run once and returns with the effect it dispatched in
// flight; the Outcome settles in the background and reaches the host as
// Notify, and the next Drive finds the Run finished without dispatching
// again.
func TestDriveAwaitsOutcomeAndNotifies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sid session.SessionID = "s-drive"
	s := newSessionSide(t)
	port := newScriptedPort(effect.AttachmentActive)
	notified := make(chan session.SessionID, 4)
	x := newEngine(t, Config{Executor: effect.PortsOf(port), Notify: func(id session.SessionID) { notified <- id }}, s)
	w, _, runID, ref := s.startTurn(t, sid)

	step, err := x.Drive(ctx, w, runID, ref)
	if err != nil || step.Dispatched != 1 || step.InFlight != 1 || step.Finished {
		t.Fatalf("first drive = %+v %v, want one effect dispatched and in flight", step, err)
	}
	a := port.dispatched[0]
	port.complete(a.Key(), effect.Outcome{Result: effect.ModelSucceeded{Result: model.ModelResult{Text: "done", FinishReason: model.FinishReasonStop}}})
	select {
	case got := <-notified:
		if got != sid {
			t.Fatalf("notified %s, want %s", got, sid)
		}
	case <-ctx.Done():
		t.Fatal("no Notify after the Outcome settled")
	}
	step, err = x.Drive(ctx, w, runID, ref)
	if err != nil || !step.Finished {
		t.Fatalf("drive after the settlement = %+v %v, want finished", step, err)
	}
	if n := port.count(); n != 1 {
		t.Fatalf("dispatches = %d, want 1", n)
	}
	port.mu.Lock()
	acked := append([]effect.AssignmentKey(nil), port.acked...)
	port.mu.Unlock()
	if len(acked) != 1 || acked[0] != a.Key() {
		t.Fatalf("acknowledged = %v, want the settled key", acked)
	}
}

// Detach ends what the Session awaits here: an effect dispatched and not
// yet settled is cancelled at the executor, and its Outcome, arriving
// afterwards, is not delivered.
func TestDetachCancelsTheEffectsInFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sid session.SessionID = "s-detach"
	s := newSessionSide(t)
	port := newScriptedPort(effect.AttachmentActive)
	notified := make(chan session.SessionID, 4)
	x := newEngine(t, Config{Executor: effect.PortsOf(port), Notify: func(id session.SessionID) { notified <- id }}, s)
	w, tref, runID, ref := s.startTurn(t, sid)
	if step, err := x.Drive(ctx, w, runID, ref); err != nil || step.InFlight != 1 {
		t.Fatalf("drive = %+v %v, want one effect in flight", step, err)
	}
	key := port.dispatched[0].Key()
	x.Detach(sid)
	port.mu.Lock()
	cancelled := append([]effect.AssignmentKey(nil), port.cancelled...)
	port.mu.Unlock()
	if len(cancelled) != 1 || cancelled[0] != key {
		t.Fatalf("cancelled = %v, want %v", cancelled, key)
	}
	port.complete(key, effect.Outcome{Result: effect.Cancelled{Message: "detached"}})
	select {
	case got := <-notified:
		t.Fatalf("Notify %s after Detach", got)
	case <-time.After(100 * time.Millisecond):
	}
	rec, err := s.runs.Record(ctx, sid, turn.DeriveRunID(sid, tref.TurnID))
	if err != nil {
		t.Fatal(err)
	}
	if ms, ok := rec.Snapshot.State.Current.(run.ModelStep); !ok || ms.Status != run.ModelExecuting {
		t.Fatalf("run after detach = %+v, want the model step still executing for the next owner", rec.Snapshot.State.Current)
	}
}
