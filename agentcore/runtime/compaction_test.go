package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/executor"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/session/writer/writertest"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/testutil/local"
	"github.com/felinics/twilight/agentcore/testutil/sdkconv"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

const compactSession session.SessionID = "compaction-integration"
const compactModel turn.ModelRef = "test-model"
const compactSummary = "short summary"

// These tests assemble only core ports: no reference-agent builder or test API
// is required. The executor record store survives Worker replacements.
type compactFixture struct {
	t        *testing.T
	ctx      context.Context
	k        *sessionkernel.Kernel
	w        writer.Writer
	worker   *executor.Worker
	records  executionstore.Store
	backend  *local.LocalExecutor
	builders *prompt.Catalog
	preset   preset.PresetRef
}

type compactInvoker func(context.Context, sdk.Request) (sdk.ModelResult, error)

func (f compactInvoker) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
	return f(ctx, req)
}

func newCompactFixture(t *testing.T, inv compactInvoker, tools ...local.ExecutableTool) *compactFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	bindings, retention := artifacttest.Stores(t)
	k, err := sessionkernel.New(sessionkernel.Ports{
		Store: filestoretest.Store(t), Content: filestoretest.Content(t, sessionstore.FrozenAuthority),
		Artifacts: sessionkernel.Artifacts{Bindings: bindings, Ledger: retention},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := k.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	catalog, err := local.NewCatalog(map[turn.ModelRef]local.ModelInvoker{compactModel: inv}, tools...)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := local.NewLocalExecutor(catalog, catalog, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	builders, err := prompt.NewCatalog(map[prompt.BuilderRef]prompt.PromptBuilderFactory{
		"context": func(ap preset.AgentPreset, src prompt.Sources) prompt.Builder {
			return compactBuilder{ap: ap, src: src}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	f := &compactFixture{t: t, ctx: ctx, k: k, records: storetest.NewMap(nil), backend: backend, builders: builders}
	f.newWorker()
	if err := k.CreateSession(ctx, compactSession, nil); err != nil {
		t.Fatal(err)
	}
	f.acquire()
	return f
}

func (f *compactFixture) newWorker() {
	f.t.Helper()
	w, err := executor.NewWorker(f.ctx, f.records, []executor.Route{local.Route(f.backend)})
	if err != nil {
		f.t.Fatal(err)
	}
	f.worker = w
	f.t.Cleanup(w.Close)
}

func (f *compactFixture) execution(port effect.ExecutionPort, policy driver.ContextPolicy, frozen bool) *rt.Execution {
	f.t.Helper()
	src := rt.ExecutionSources{TurnStore: f.k.TurnStore, Projections: f.k.Projections, Content: f.k.Content}
	if frozen {
		src.Frozen = f.k.Frozen
	}
	x, err := rt.NewExecution(rt.ExecutionConfig{Executor: port, PromptBuilders: f.builders, ContextPolicy: policy}, src)
	if err != nil {
		f.t.Fatal(err)
	}
	f.preset, err = x.Presets.Register("integration", preset.AgentPreset{
		Model: compactModel, PromptBuilder: "context", SystemPrompt: strings.Repeat("instruction ", 8),
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return x
}

func (f *compactFixture) acquire() {
	f.t.Helper()
	w, err := writertest.Open(f.t, f.ctx, f.k.Writers, compactSession, session.OpenOptions{})
	if err != nil {
		f.t.Fatal(err)
	}
	f.w = w
}

func (f *compactFixture) reopenWriter() {
	f.t.Helper()
	if err := f.w.Close(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	f.acquire()
}

// Only tests wait. Production Step/Advance always give ownership back to the
// host instead of holding a Writer while an executor completes work.
func (f *compactFixture) drive(x *rt.Execution, w writer.Writer, id turn.TurnID) (driver.StepResult, error) {
	f.t.Helper()
	for {
		result, err := x.Driver.Step(f.ctx, w, id)
		if err != nil || result.Disposition == driver.Finished {
			return result, err
		}
		if result.Disposition == driver.Waiting {
			if err := compactPoll(f.ctx); err != nil {
				return result, err
			}
		}
	}
}

func compactPoll(ctx context.Context) error {
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (f *compactFixture) advance(c *driver.Compactions, w writer.Writer) (driver.MaintenanceResult, error) {
	f.t.Helper()
	for {
		result, err := c.Advance(f.ctx, w)
		if err != nil || !result.Pending {
			return result, err
		}
		if err := compactPoll(f.ctx); err != nil {
			return result, err
		}
	}
}

func (f *compactFixture) awaitOutcome(port effect.ExecutionPort, key effect.AssignmentKey) effect.Outcome {
	f.t.Helper()
	for {
		out, err := port.GetOutcome(f.ctx, key)
		if err == nil {
			return out
		}
		if !errors.Is(err, effect.ErrOutcomeNotReady) {
			f.t.Fatal(err)
		}
		if err := compactPoll(f.ctx); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *compactFixture) startTurn(id string, stop bool) sessionkernel.TurnResult {
	f.t.Helper()
	body, err := json.Marshal(map[string]string{"text": id + strings.Repeat("x", 100)})
	if err != nil {
		f.t.Fatal(err)
	}
	in, err := f.k.Chatlog.Submit(f.ctx, f.w, turn.InputID(id), turn.MustParseCanonicalJSON(string(body)))
	if err != nil {
		f.t.Fatal(err)
	}
	ref := sessionstore.TurnRef{SessionID: compactSession, TurnID: turn.TurnID(id)}
	result, err := f.k.Turns.Start(f.ctx, f.w, sessionkernel.StartRequest{Ref: ref, Inputs: []turn.AgentInput{in}, Preset: f.preset})
	if err != nil {
		f.t.Fatal(err)
	}
	if stop {
		if _, err := f.k.Turns.Stop(f.ctx, f.w, sessionkernel.StopRequest{Ref: ref, Reason: "history only"}); err != nil {
			f.t.Fatal(err)
		}
	}
	return result
}

func (f *compactFixture) history() {
	f.t.Helper()
	for i := 0; i < 3; i++ {
		f.startTurn(fmt.Sprintf("history-%d", i), true)
	}
}

func (f *compactFixture) context() chatlog.Context {
	f.t.Helper()
	c, err := chatlog.ReadContext(f.ctx, f.w.Projections(), compactSession)
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

func (f *compactFixture) plan() chatlog.CompactionPlan {
	f.t.Helper()
	p, possible, err := chatlog.PlanCompaction(f.context().Entries, 1)
	if err != nil || !possible {
		f.t.Fatalf("plan: possible=%v error=%v", possible, err)
	}
	return p
}

func (f *compactFixture) state() compaction.State {
	f.t.Helper()
	s, err := compaction.Read(f.ctx, f.w.Projections(), compactSession)
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}

func (f *compactFixture) counts(summaries, compactions, assistants int) {
	f.t.Helper()
	s, err := chatlog.ReadSurface(f.ctx, f.w.Projections(), compactSession)
	if err != nil {
		f.t.Fatal(err)
	}
	if s.Summaries.Len() != summaries || s.Compactions.Len() != compactions || s.Assistants.Len() != assistants {
		f.t.Fatalf("summaries/compactions/assistants = %d/%d/%d, want %d/%d/%d", s.Summaries.Len(), s.Compactions.Len(), s.Assistants.Len(), summaries, compactions, assistants)
	}
}

func compactRequest() driver.SummaryRequest {
	return driver.SummaryRequest{Model: compactModel, Request: model.ModelRequest{Model: string(compactModel), System: "summary"}}
}

func (f *compactFixture) accept(x *rt.Execution, id compaction.ID) {
	f.t.Helper()
	r := compactRequest()
	if err := x.Compactions.Start(f.ctx, f.w, id, f.plan(), &r); err != nil {
		f.t.Fatal(err)
	}
}

func waitCompact[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-ctx.Done():
		t.Fatalf("waiting for compaction boundary: %v", ctx.Err())
	}
	var zero T
	return zero
}

// Build from the real Context and materialize it, including summaries. Inputs
// already delivered to this preparation must still be listed as consumed IDs.
type compactBuilder struct {
	ap  preset.AgentPreset
	src prompt.Sources
}

func (b compactBuilder) Build(ctx context.Context, in prompt.Input) (prompt.Prompt, error) {
	c, err := chatlog.ReadContext(ctx, b.src.Projections, session.SessionID(in.Scope))
	if err != nil {
		return prompt.Prompt{}, err
	}
	entries, err := chatlog.NewMaterializer(b.src.Content).Entries(ctx, c.Entries)
	if err != nil {
		return prompt.Prompt{}, err
	}
	request := model.ModelRequest{Model: string(b.ap.Model), System: b.ap.SystemPrompt}
	pairs := make([]chatlog.EntryDigestPair, len(entries))
	for i := range entries {
		e := &entries[i]
		text := e.Text()
		if e.Entry.Input != nil {
			text = e.Entry.Input.Content.String()
		}
		request.Messages = append(request.Messages, model.Message{Role: model.MessageRoleUser, Content: []model.MessagePart{{Type: model.MessagePartTypeText, Text: text}}})
		pairs[i] = e.Entry.Pair()
	}
	digest, err := chatlog.DigestBaseContext(pairs)
	if err != nil {
		return prompt.Prompt{}, err
	}
	ids := make([]turn.InputID, len(in.Inputs))
	for i := range in.Inputs {
		ids[i] = in.Inputs[i].ID
	}
	specs, definitions, err := prompt.ToolSpecs(b.ap.Tools)
	if err != nil {
		return prompt.Prompt{}, err
	}
	request.Tools = definitions
	return prompt.Prompt{Model: b.ap.Model, Request: request, InputIDs: ids, Token: turn.PromptToken(digest), Tools: specs}, nil
}

type compactPolicy struct {
	limit      int
	candidates []model.ModelRequest
	builds     int
	entryCount int
}

func (p *compactPolicy) Decide(_ context.Context, _ prompt.Input, candidate *prompt.Prompt) (driver.ContextDecision, error) {
	p.candidates = append(p.candidates, candidate.Request)
	// A deterministic character estimator keeps the budget small and tests
	// that policy receives the whole request, not just historical entries.
	n := len(candidate.Request.System)
	for _, msg := range candidate.Request.Messages {
		for _, part := range msg.Content {
			n += len(part.Text)
		}
	}
	exceeded, err := (prompt.TokenBudget{ContextWindow: p.limit + 20, OutputReserve: 10, SafetyMargin: 10}).Exceeded(n)
	return driver.ContextDecision{Compact: exceeded, RetainEntries: 1}, err
}

func (p *compactPolicy) BuildSummary(_ context.Context, c *driver.CompactionContext) (driver.SummaryRequest, error) {
	p.builds++
	p.entryCount = len(c.Entries)
	r := compactRequest()
	for i := 0; i < len(c.Entries)-len(c.Plan.Retained); i++ {
		r.Request.Messages = append(r.Request.Messages, model.Message{Role: model.MessageRoleUser, Content: []model.MessagePart{{Type: model.MessagePartTypeText, Text: c.Entries[i].Entry.Input.Content.String()}}})
	}
	return r, nil
}

func TestCompactionCandidateRebuildAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name       string
		limit      int
		noProgress bool
	}{
		{"fits after summary", 450, false}, {"retained suffix still too large", 80, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var summaries, answers atomic.Int32
			answered := make(chan sdk.Request, 2)
			f := newCompactFixture(t, func(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
				if req.System == "summary" {
					summaries.Add(1)
					return sdk.ModelResult{Text: compactSummary}, nil
				}
				answers.Add(1)
				answered <- req
				return sdk.ModelResult{Text: "normal answer"}, nil
			})
			policy := &compactPolicy{limit: tc.limit}
			x := f.execution(f.worker, policy, true)
			f.history()
			current := f.startTurn("current", false)
			_, err := f.drive(x, f.w, current.Ref.TurnID)
			if tc.noProgress {
				if !errors.Is(err, driver.ErrCompactionNoProgress) {
					t.Fatalf("Step = %v, want ErrCompactionNoProgress", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if summaries.Load() != 1 || policy.builds != 1 || policy.entryCount != 4 || len(policy.candidates) != 2 {
				t.Fatalf("summary calls=%d builds=%d entries=%d candidates=%d", summaries.Load(), policy.builds, policy.entryCount, len(policy.candidates))
			}
			before, after := policy.candidates[0], policy.candidates[1]
			if before.System == "" || len(before.Messages) != 4 || len(after.Messages) != 2 || after.Messages[0].Content[0].Text != compactSummary || !strings.Contains(after.Messages[1].Content[0].Text, "current") {
				t.Fatalf("candidate was not rebuilt from summary plus retained input: before=%+v after=%+v", before, after)
			}
			wantAnswers := 1
			if tc.noProgress {
				wantAnswers = 0
			}
			if answers.Load() != int32(wantAnswers) {
				t.Fatalf("normal calls=%d, want %d", answers.Load(), wantAnswers)
			}
			if !tc.noProgress {
				// History alone fits: it is the complete candidate, including
				// the instruction and current input, that exceeds this budget.
				historySize := 0
				for _, message := range before.Messages[:3] {
					historySize += len(message.Content[0].Text)
				}
				if historySize > tc.limit {
					t.Fatalf("fixture history alone exceeds the budget: %d > %d", historySize, tc.limit)
				}
				actual := waitCompact(t, f.ctx, answered)
				if len(actual.Messages) != 2 || actual.Messages[0].Content[0].(sdk.TextPart).Text != compactSummary {
					t.Fatalf("model received an uncompressed request: %+v", actual)
				}
			}
			f.counts(1, 1, wantAnswers)
			state := f.state()
			if len(state.Operations) != 1 || state.Pending != "" {
				t.Fatalf("state=%+v", state)
			}
			for _, op := range state.Operations {
				if op.Status != compaction.Applied || !op.Acknowledged {
					t.Fatalf("operation=%+v", op)
				}
				if _, err := f.k.TurnStore.Record(f.ctx, compactSession, turn.TurnID(op.Request.Key(compactSession).OwnerID)); !errors.Is(err, store.ErrTurnNotFound) {
					t.Fatalf("summary created a normal Run: %v", err)
				}
			}
			if tc.noProgress {
				// The same preparation remains protected on an explicit retry.
				if _, err := f.drive(x, f.w, current.Ref.TurnID); !errors.Is(err, driver.ErrCompactionNoProgress) {
					t.Fatalf("repeat Step=%v", err)
				}
				if summaries.Load() != 1 {
					t.Fatal("no-progress preparation summarized twice")
				}
				// The boundary guard is durable, not an Execution-local counter.
				f.reopenWriter()
				freshPolicy := &compactPolicy{limit: tc.limit}
				replacement := f.execution(f.worker, freshPolicy, true)
				if _, err := f.drive(replacement, f.w, current.Ref.TurnID); !errors.Is(err, driver.ErrCompactionNoProgress) {
					t.Fatalf("Step after owner replacement=%v", err)
				}
				if summaries.Load() != 1 || freshPolicy.builds != 0 {
					t.Fatal("owner replacement forgot no-progress preparation")
				}
			}
		})
	}
}

// Embedding the Worker preserves the real settlement stream and recovery
// capabilities while allowing observations/failure at the ACK boundary.
type compactPort struct {
	*executor.Worker
	attached   chan effect.AssignmentKey
	dispatched chan effect.AssignmentKey
	ackCalls   atomic.Int32
	failACK    atomic.Bool
}

func newCompactPort(w *executor.Worker) *compactPort {
	return &compactPort{Worker: w, attached: make(chan effect.AssignmentKey, 16), dispatched: make(chan effect.AssignmentKey, 16)}
}
func (p *compactPort) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	result, err := p.Worker.Attach(ctx, key)
	select {
	case p.attached <- key:
	default:
	}
	return result, err
}
func (p *compactPort) Dispatch(ctx context.Context, a effect.Assignment) error {
	select {
	case p.dispatched <- a.Key():
	default:
	}
	return p.Worker.Dispatch(ctx, a)
}
func (p *compactPort) Acknowledge(ctx context.Context, key effect.AssignmentKey) error {
	p.ackCalls.Add(1)
	if p.failACK.CompareAndSwap(true, false) {
		return errors.New("injected ACK failure")
	}
	return p.Worker.Acknowledge(ctx, key)
}

// Commit notification is after the underlying durable append, not a poll of
// projections. Rejecting the finish group models a crash after Outcome but
// before any of Summary, CompactionCreated or Finished can become visible.
type compactWriter struct {
	writer.Writer
	acked          chan struct{}
	modelCompleted chan struct{}
	failFinish     error
}

func (w *compactWriter) Commit(ctx context.Context, fn writer.CommitFn) (writer.CommitResult, error) {
	ack := false
	completed := false
	result, err := w.Writer.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		group, err := fn(v)
		if err != nil || group == nil {
			return group, err
		}
		for _, batch := range group.Batches {
			for _, event := range batch.Events {
				if event.Type == compaction.TypeFinished && w.failFinish != nil {
					return nil, w.failFinish
				}
				if event.Type == compaction.TypeAcknowledged {
					ack = true
				}
				if event.Type == sessionstore.Type("model_step_completed") {
					completed = true
				}
			}
		}
		return group, nil
	})
	if err == nil && ack && result.Outcome == writer.CommitApplied && w.acked != nil {
		w.acked <- struct{}{}
	}
	if err == nil && completed && result.Outcome == writer.CommitApplied && w.modelCompleted != nil {
		w.modelCompleted <- struct{}{}
	}
	return result, err
}

func (f *compactFixture) settleMaintenance(x *rt.Execution) {
	f.t.Helper()
	if _, err := f.advance(x.Compactions, f.w); err != nil {
		f.t.Fatal(err)
	}
}

func TestCompactionAdvanceRecoversUndispatchedIdleSession(t *testing.T) {
	var calls atomic.Int32
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		calls.Add(1)
		return sdk.ModelResult{Text: compactSummary}, nil
	})
	oldPort := newCompactPort(f.worker)
	old := f.execution(oldPort, nil, true)
	f.history()
	f.accept(old, "accepted")
	if calls.Load() != 0 || len(oldPort.dispatched) != 0 {
		t.Fatal("Start dispatched a model")
	}
	before := f.state().Operations["accepted"]
	oldEpoch := f.w.Epoch()
	f.reopenWriter()
	if f.w.Epoch() <= oldEpoch {
		t.Fatal("owner generation did not advance")
	}
	// Neither policy nor Frozen is supplied to the replacement Execution.
	port := newCompactPort(f.worker)
	x := f.execution(port, nil, false)
	f.settleMaintenance(x)
	op := f.state().Operations["accepted"]
	if op.Status != compaction.Applied || !op.Acknowledged || !reflect.DeepEqual(op.Request, before.Request) {
		t.Fatalf("recovered operation=%+v", op)
	}
	if key := waitCompact(t, f.ctx, port.dispatched); key != before.Request.Key(compactSession) {
		t.Fatalf("dispatch key=%+v", key)
	}
	if calls.Load() != 1 {
		t.Fatalf("summary calls=%d", calls.Load())
	}
	f.counts(1, 1, 0)
	if _, err := x.Compactions.Advance(f.ctx, f.w); err != nil {
		t.Fatal(err)
	}
	f.counts(1, 1, 0)
}

func TestCompactionInFlightOwnerHandoff(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	var calls atomic.Int32
	f := newCompactFixture(t, func(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
		calls.Add(1)
		started <- struct{}{}
		select {
		case <-release:
			return sdk.ModelResult{Text: compactSummary}, nil
		case <-ctx.Done():
			return sdk.ModelResult{}, ctx.Err()
		}
	})
	// Ensure even a failing assertion cannot strand the local model goroutine.
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	old := f.execution(f.worker, nil, true)
	f.history()
	f.accept(old, "in-flight")
	accepted := f.state().Operations["in-flight"]
	key := accepted.Request.Key(compactSession)
	result, err := old.Compactions.Advance(f.ctx, f.w)
	if err != nil || !result.Pending || result.Key != key {
		t.Fatalf("dispatch=%+v, %v", result, err)
	}
	waitCompact(t, f.ctx, started)
	if op := f.state().Operations["in-flight"]; op.Status != compaction.Pending || op.Acknowledged {
		t.Fatalf("dispatch settled operation=%+v", op)
	}
	// Advance has returned; releasing ownership never cancels physical work.
	f.reopenWriter()
	port := newCompactPort(f.worker)
	x := f.execution(port, nil, false)
	result, err = x.Compactions.Advance(f.ctx, f.w)
	if err != nil || !result.Pending {
		t.Fatalf("reattach=%+v, %v", result, err)
	}
	if got := waitCompact(t, f.ctx, port.attached); got != key {
		t.Fatalf("replacement attached=%+v, want %+v", got, key)
	}
	close(release)
	f.settleMaintenance(x)
	if calls.Load() != 1 || len(port.dispatched) != 0 {
		t.Fatalf("handoff redispatched: calls=%d dispatches=%d", calls.Load(), len(port.dispatched))
	}
	if op := f.state().Operations["in-flight"]; op.Status != compaction.Applied || !op.Acknowledged {
		t.Fatalf("operation=%+v", op)
	}
	f.counts(1, 1, 0)
}

func TestCompactionSuccessfulOutcomeBeforeCommitSurvivesWorkerRestart(t *testing.T) {
	var calls atomic.Int32
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		calls.Add(1)
		return sdk.ModelResult{Text: compactSummary}, nil
	})
	port := newCompactPort(f.worker)
	old := f.execution(port, nil, true)
	f.history()
	f.accept(old, "before-commit")
	op := f.state().Operations["before-commit"]
	injected := errors.New("session append unavailable")
	w := &compactWriter{Writer: f.w, failFinish: injected}
	if _, err := f.advance(old.Compactions, w); !errors.Is(err, injected) {
		t.Fatalf("Advance=%v", err)
	}
	outcome, err := f.worker.GetOutcome(f.ctx, op.Request.Key(compactSession))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := outcome.Result.(effect.ModelSucceeded); !ok {
		t.Fatalf("outcome=%+v", outcome)
	}
	if port.ackCalls.Load() != 0 || f.state().Operations["before-commit"].Status != compaction.Pending {
		t.Fatal("uncommitted outcome acknowledged or intent lost")
	}
	f.counts(0, 0, 0)
	f.worker.Close()
	f.reopenWriter()
	f.newWorker() // same executor record store, different Worker incarnation
	newPort := newCompactPort(f.worker)
	x := f.execution(newPort, nil, false)
	f.settleMaintenance(x)
	if got := waitCompact(t, f.ctx, newPort.attached); got != op.Request.Key(compactSession) {
		t.Fatalf("new key=%+v", got)
	}
	if calls.Load() != 1 || len(newPort.dispatched) != 0 {
		t.Fatalf("terminal summary redone: calls=%d dispatches=%d", calls.Load(), len(newPort.dispatched))
	}
	f.counts(1, 1, 0)
	f.assertAtomicFinish()
}

func (f *compactFixture) assertAtomicFinish() {
	f.t.Helper()
	page, err := f.k.Store.ReadCommits(f.ctx, session.CommitReadRequest{SessionID: compactSession})
	if err != nil {
		f.t.Fatal(err)
	}
	found := false
	for _, commit := range page.Commits {
		types := map[ledger.EventType]bool{}
		for _, batch := range commit.Batches {
			for _, event := range batch.Events {
				types[event.Type] = true
			}
		}
		if types[chatlog.TypeSummary] || types[chatlog.TypeCompactionCreated] {
			if found || !types[chatlog.TypeSummary] || !types[chatlog.TypeCompactionCreated] || !types[compaction.TypeFinished] {
				f.t.Fatalf("non-atomic or duplicate finish: %+v", commit)
			}
			found = true
		}
	}
	if !found {
		f.t.Fatal("no atomic summary/compaction/finished commit")
	}
}

func TestCompactionContextChangeMakesInflightSummaryStale(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	f := newCompactFixture(t, func(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
		started <- struct{}{}
		select {
		case <-release:
			return sdk.ModelResult{Text: compactSummary}, nil
		case <-ctx.Done():
			return sdk.ModelResult{}, ctx.Err()
		}
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	x := f.execution(f.worker, nil, true)
	f.history()
	f.accept(x, "stale")
	if result, err := x.Compactions.Advance(f.ctx, f.w); err != nil || !result.Pending {
		t.Fatalf("Advance=%+v, %v", result, err)
	}
	waitCompact(t, f.ctx, started)
	// Delivering input (not merely submitting it) changes the real Context.
	f.startTurn("concurrent-input", true)
	changed := f.context().Entries
	close(release)
	f.settleMaintenance(x)
	op := f.state().Operations["stale"]
	if op.Status != compaction.Stale || op.CompactionID != "" || !op.Acknowledged || op.Code != "context_changed" {
		t.Fatalf("operation=%+v", op)
	}
	if !reflect.DeepEqual(f.context().Entries, changed) {
		t.Fatal("stale summary overwrote the changed Context")
	}
	f.counts(0, 0, 0)
}

func TestCompactionIdentityAndPendingGuard(t *testing.T) {
	var calls atomic.Int32
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		calls.Add(1)
		return sdk.ModelResult{Text: compactSummary}, nil
	})
	x := f.execution(f.worker, nil, true)
	f.history()
	plan, request := f.plan(), compactRequest()
	if err := x.Compactions.Start(f.ctx, f.w, "identity", plan, &request); err != nil {
		t.Fatal(err)
	}
	if err := x.Compactions.Start(f.ctx, f.w, "identity", plan, &request); err != nil {
		t.Fatalf("identical Start retry=%v", err)
	}
	changedRequest := compactRequest()
	changedRequest.Request.System = "different summary instruction"
	if err := x.Compactions.Start(f.ctx, f.w, "identity", plan, &changedRequest); !errors.Is(err, compaction.ErrConflict) {
		t.Fatalf("different request=%v", err)
	}
	changedPlan := plan
	changedPlan.Retained = nil
	if err := x.Compactions.Start(f.ctx, f.w, "identity", changedPlan, &request); !errors.Is(err, compaction.ErrConflict) {
		t.Fatalf("different plan=%v", err)
	}
	if err := x.Compactions.Start(f.ctx, f.w, "other", plan, &request); !errors.Is(err, compaction.ErrPending) {
		t.Fatalf("second pending operation=%v", err)
	}
	if len(f.state().Operations) != 1 || calls.Load() != 0 {
		t.Fatal("conflicting Start mutated intent or dispatched")
	}
	current := f.startTurn("guarded", false)
	bound := f.k.TurnStore.Bind(f.w)
	snap, err := bound.Load(f.ctx, current.Ref.TurnID)
	if err != nil {
		t.Fatal(err)
	}
	r := model.ModelRequest{Model: string(compactModel), System: "normal"}
	digest, err := schema.Canonical().DigestRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	id := schema.Identity().DeriveModelRequestCommandID(current.Ref.TurnID, snap.Position)
	cmd := turn.PrepareModelRequest{StepID: schema.Identity().DeriveModelStepID(current.Ref.TurnID, id), Model: compactModel, Request: r, RequestDigest: digest, InputIDs: []turn.InputID{"guarded"}}
	env, err := schema.Wire().Envelope(current.Ref.TurnID, id, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bound.Commit(f.ctx, store.CommitRequest{Base: snap.Position, Command: env}); !errors.Is(err, compaction.ErrPending) {
		t.Fatalf("Prepare while pending=%v", err)
	}
	after, err := bound.Load(f.ctx, current.Ref.TurnID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Position != snap.Position {
		t.Fatal("pending guard advanced Run")
	}
}

func TestCompactionSummaryFailureIsNotConversation(t *testing.T) {
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		return sdk.ModelResult{}, errors.New("summary provider failed")
	})
	x := f.execution(f.worker, nil, true)
	f.history()
	before := f.context().Entries
	f.accept(x, "failure")
	if _, err := f.advance(x.Compactions, f.w); !errors.Is(err, driver.ErrSummaryFailed) {
		t.Fatalf("Advance=%v, want ErrSummaryFailed", err)
	}
	op := f.state().Operations["failure"]
	if op.Status != compaction.Failed || !op.Acknowledged || op.CompactionID != "" || op.Code == "" {
		t.Fatalf("operation=%+v", op)
	}
	if !reflect.DeepEqual(before, f.context().Entries) {
		t.Fatal("failed summary changed Context")
	}
	f.counts(0, 0, 0)
}

type compactGateTool struct {
	started chan struct{}
	release chan struct{}
}

func (*compactGateTool) Ref() turn.ToolRef { return "lookup" }
func (*compactGateTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: &jsonschema.Schema{Type: "object"}}
}
func (*compactGateTool) ResponsePolicy() turn.ResponsePolicy        { return turn.DirectExecution }
func (*compactGateTool) Replay() turn.ReplayPolicy                  { return turn.ReplayAllowed }
func (*compactGateTool) Placement() turn.ToolPlacement              { return turn.PlacementProcess }
func (*compactGateTool) ValidateArguments(turn.CanonicalJSON) error { return nil }
func (g *compactGateTool) Execute(ctx context.Context, _ local.ToolExecutionRequest) local.ToolExecutionOutcome {
	g.started <- struct{}{}
	select {
	case <-g.release:
		body, _ := json.Marshal(strings.Repeat("tool-result-", 100))
		return local.ToolExecutionSucceeded{Result: turn.ToolExecutionResult{Output: turn.MustParseCanonicalJSON(string(body))}}
	case <-ctx.Done():
		return local.ToolExecutionFailed{Failure: turn.ToolFailure{Class: "cancelled", Message: ctx.Err().Error()}}
	}
}

func TestCompactionStartsAfterReattachedToolOutcome(t *testing.T) {
	tool := &compactGateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-tool.release:
		default:
			close(tool.release)
		}
	})
	var normal, summaries atomic.Int32
	f := newCompactFixture(t, func(_ context.Context, request sdk.Request) (sdk.ModelResult, error) {
		if request.System == "summary" {
			summaries.Add(1)
			return sdk.ModelResult{Text: compactSummary}, nil
		}
		if normal.Add(1) == 1 {
			return sdk.ModelResult{ToolCalls: []sdk.ToolCall{{ToolCallID: "call-1", ToolName: "lookup", Input: sdk.ToolArguments{JSON: json.RawMessage(`{}`)}}}}, nil
		}
		return sdk.ModelResult{Text: "after recovery"}, nil
	}, tool)
	old := f.execution(f.worker, nil, true)
	definition, err := sdkconv.FreezeToolDefinition(tool.Definition())
	if err != nil {
		t.Fatal(err)
	}
	ap := preset.AgentPreset{Model: compactModel, PromptBuilder: "context", Tools: []preset.ToolContract{{Ref: tool.Ref(), Definition: definition, Policy: tool.ResponsePolicy(), Replay: tool.Replay(), Placement: tool.Placement()}}}
	f.preset, err = old.Presets.Register("tool-preset", ap)
	if err != nil {
		t.Fatal(err)
	}
	current := f.startTurn("tool-turn", false)
	lp, err := old.Driver.Loops.For(f.preset)
	if err != nil {
		t.Fatal(err)
	}
	bound := f.k.TurnStore.Bind(f.w)
	modelCall, err := lp.Advance(f.ctx, bound, current.Ref.TurnID, nil)
	if err != nil || len(modelCall.Dispatched) != 1 {
		t.Fatalf("model dispatch=%+v, %v", modelCall, err)
	}
	out := f.awaitOutcome(f.worker, modelCall.Dispatched[0])
	if _, err := lp.Deliver(f.ctx, bound, out, nil); err != nil {
		t.Fatal(err)
	}
	toolCall, err := lp.Advance(f.ctx, bound, current.Ref.TurnID, nil)
	if err != nil || len(toolCall.Dispatched) != 1 {
		t.Fatalf("tool dispatch=%+v, %v", toolCall, err)
	}
	waitCompact(t, f.ctx, tool.started)
	f.reopenWriter()
	policy := &driver.BudgetPolicy{
		Budget: func(context.Context, turn.ModelRef) (prompt.TokenBudget, error) {
			return prompt.TokenBudget{ContextWindow: 300, OutputReserve: 30}, nil
		},
		Estimate: func(_ context.Context, request *model.ModelRequest) (int, error) {
			n := len(request.System)
			for _, message := range request.Messages {
				for _, part := range message.Content {
					n += len(part.Text)
				}
			}
			return n, nil
		},
		Summary: func(context.Context, *driver.CompactionContext) (driver.SummaryRequest, error) {
			return compactRequest(), nil
		},
	}
	x := f.execution(f.worker, policy, true)
	if _, err := x.Presets.Register("tool-preset", ap); err != nil {
		t.Fatal(err)
	}
	observed := &compactWriter{Writer: f.w, modelCompleted: make(chan struct{}, 1)}
	if result, err := x.Driver.Step(f.ctx, observed, current.Ref.TurnID); err != nil || result.Disposition != driver.Waiting {
		t.Fatalf("Step=%+v, %v", result, err)
	}
	if len(f.state().Operations) != 0 {
		t.Fatal("maintenance existed before the recovered tool outcome")
	}
	close(tool.release)
	if _, err := f.drive(x, observed, current.Ref.TurnID); err != nil {
		t.Fatal(err)
	}
	waitCompact(t, f.ctx, observed.modelCompleted)
	if summaries.Load() != 1 || normal.Load() != 2 {
		t.Fatalf("summary/normal calls=%d/%d", summaries.Load(), normal.Load())
	}
	f.counts(1, 1, 2)
	if state := f.state(); state.Pending != "" || len(state.Operations) != 1 {
		t.Fatalf("maintenance was stranded: %+v", state)
	}
}
