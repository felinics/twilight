package app_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/component/localagent"
	"github.com/felinics/twilight/agent/sdkconv"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// recordingExecutor stands in for a remote worker: it holds no ModelInvoker
// and no ExecutableTool, records every Assignment it is handed and answers
// each model assignment with a scripted Outcome from another goroutine.
type recordingExecutor struct {
	mu       sync.Mutex
	assigned []effect.Assignment
	reply    string
	outcomes map[effect.AssignmentKey]chan effect.Outcome
	settled  map[effect.AssignmentKey]effect.Outcome
}

// frozenModel stands in for the backend boundary: the effect protocol
// carries only frozen model results.
func frozenModel(r sdk.ModelResult) model.ModelResult {
	frozen, err := sdkconv.FreezeModelResult(r)
	if err != nil {
		panic(err)
	}
	return frozen
}

func (e *recordingExecutor) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}

func (e *recordingExecutor) Dispatch(_ context.Context, a effect.Assignment) error {
	e.mu.Lock()
	if e.outcomes == nil {
		e.outcomes = make(map[effect.AssignmentKey]chan effect.Outcome)
	}
	e.assigned = append(e.assigned, a)
	e.outcomes[a.Key()] = make(chan effect.Outcome, 1)
	ch := e.outcomes[a.Key()]
	reply := e.reply
	e.mu.Unlock()
	go func() {
		ch <- effect.Outcome{Key: a.Key(), Result: effect.ModelSucceeded{Result: frozenModel(sdk.ModelResult{Text: reply, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}})}}
	}()
	return nil
}

func (e *recordingExecutor) Attach(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
}

func (e *recordingExecutor) Abort(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}, nil
}

func (e *recordingExecutor) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionRunning, nil
}

// GetOutcome is a read (effect.ExecutionPort): a key whose scripted reply
// has not landed yet is ErrOutcomeNotReady, a settled key answers the same
// Outcome on every read.
func (e *recordingExecutor) GetOutcome(_ context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if out, ok := e.settled[key]; ok {
		return out, nil
	}
	ch := e.outcomes[key]
	if ch == nil {
		return effect.Outcome{}, effect.ErrExecutionNotFound
	}
	select {
	case out := <-ch:
		if e.settled == nil {
			e.settled = map[effect.AssignmentKey]effect.Outcome{}
		}
		e.settled[key] = out
		return out, nil
	default:
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
}

// Settlements is effect.SettlementPort: the fake scans its channels and
// notices every key that settled, so the Owner's Watcher does not wait for
// its poll.
func (e *recordingExecutor) Settlements(ctx context.Context, _ string, after uint64, fn func(effect.Settlement) bool) error {
	seq := after
	noticed := map[effect.AssignmentKey]bool{}
	for {
		e.mu.Lock()
		var ready []effect.AssignmentKey
		for key, ch := range e.outcomes {
			if noticed[key] {
				continue
			}
			if _, done := e.settled[key]; done {
				ready = append(ready, key)
				continue
			}
			select {
			case out := <-ch:
				if e.settled == nil {
					e.settled = map[effect.AssignmentKey]effect.Outcome{}
				}
				e.settled[key] = out
				ready = append(ready, key)
			default:
			}
		}
		e.mu.Unlock()
		for _, key := range ready {
			noticed[key] = true
			seq++
			if !fn(effect.Settlement{Key: key, Epoch: "fake", Sequence: seq}) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (e *recordingExecutor) Cancel(context.Context, effect.AssignmentKey) error { return nil }

func (e *recordingExecutor) assignments() []effect.Assignment {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]effect.Assignment(nil), e.assigned...)
}

// The Owner side needs no effect implementation (OWN-PRT-2): a Host built
// over an Executor that is only a recorder registers an AgentPreset, starts a Turn,
// dispatches the model Assignment with the frozen request's digest, and
// settles the Outcome the executor sends back. Every model client and tool
// lives on the executor's side of the port.
func TestAuthorityRunsWithoutEffectImplementations(t *testing.T) {
	ctx := context.Background()
	exec := &recordingExecutor{reply: "hello from the executor"}
	content := durableContent(t)
	cfg := durablePorts(t, app.Config{Sessions: app.SessionPorts{Content: content}})
	cfg.Port = exec
	h, err := localagent.Compose(cfg)
	if err != nil {
		t.Fatal(err)
	}
	fz, err := sessionstore.FrozenValues(content, cfg.Sessions.Artifacts.Bindings)
	if err != nil {
		t.Fatal(err)
	}
	presetRef, err := h.RegisterPreset("remote", mustPreset("m-remote", nil, app.WithSystemPrompt("be brief")))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, "s-authority", app.SessionOptions{Preset: presetRef})
	if err != nil {
		t.Fatal(err)
	}
	results, err := s.Send(ctx, "what is the weather?")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != turn.TurnCompleted || results[0].Reply != "hello from the executor" {
		t.Fatalf("results = %+v", results)
	}

	assigned := exec.assignments()
	if len(assigned) != 1 {
		t.Fatalf("assignments = %d, want 1 model assignment", len(assigned))
	}
	a := assigned[0]
	modelAssignment, isModel := a.Model()
	if !isModel || modelAssignment.Model != "m-remote" || a.Effect == "" || a.RunID == "" || a.StepID == "" {
		t.Fatalf("assignment = %+v", a)
	}
	// The body the executor would fetch is in the shared frozen store under
	// the digest the Assignment carries (RUN-WIR-4).
	raw, ok, err := fz.Get(ctx, modelAssignment.RequestDigest)
	if err != nil || !ok || len(raw) == 0 {
		t.Fatalf("frozen request %s: ok=%v err=%v", modelAssignment.RequestDigest, ok, err)
	}
	req, err := frozen.DecodeRequest(raw, modelAssignment.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "m-remote" || len(req.Messages) == 0 {
		t.Fatalf("frozen request = %+v", req)
	}
}

// Older Turns keep resolving the preset they recorded (PST-2).
func TestPresetVersionsRemainAvailable(t *testing.T) {
	presets := preset.NewMemory()
	ap := mustPreset("m-1", nil, app.WithSystemPrompt("original"))
	ap.Tools = []preset.ToolContract{{Ref: "tool", Definition: model.ToolDefinition{
		Name: "tool", Parameters: jsonstable.MustParse(`{}`), CacheControl: &model.CacheControl{Type: "ephemeral"},
	}}}
	ref, err := presets.Register("p", ap)
	if err != nil {
		t.Fatal(err)
	}
	ap.SystemPrompt = "updated"
	ap.Tools[0].Definition.Name = "updated_tool"
	ap.Tools[0].Definition.CacheControl.Type = "updated"
	newRef, err := presets.Register("p", ap)
	if err != nil {
		t.Fatal(err)
	}
	if newRef == ref {
		t.Fatal("changed preset fields reused the old preset ref")
	}
	old, err := presets.Resolve(ref)
	if err != nil || old.SystemPrompt != "original" || old.Tools[0].Definition.Name != "tool" || old.Tools[0].Definition.CacheControl.Type != "ephemeral" {
		t.Fatalf("original preset = %+v, %v", old, err)
	}
	old.Tools[0].Definition.Name = "mutated_read"
	old.Tools[0].Definition.CacheControl.Type = "mutated_read"
	again, err := presets.Resolve(ref)
	if err != nil || again.Tools[0].Definition.Name != "tool" || again.Tools[0].Definition.CacheControl.Type != "ephemeral" {
		t.Fatalf("resolve leaked mutable preset state: %+v, %v", again, err)
	}
	current, err := presets.Resolve(newRef)
	if err != nil || current.SystemPrompt != "updated" {
		t.Fatalf("updated preset = %+v, %v", current, err)
	}
	if _, err := presets.Resolve(preset.PresetRef{ID: "missing", Digest: ref.Digest}); err == nil {
		t.Fatal("unknown preset resolved")
	}
	if _, err := presets.Resolve(preset.PresetRef{ID: ref.ID, Digest: "unknown"}); err == nil {
		t.Fatal("unknown digest resolved")
	}
}
