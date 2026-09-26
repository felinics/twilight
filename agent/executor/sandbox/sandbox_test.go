package sandbox_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/environment/local"
	"github.com/felinics/twilight/agent/executor/sandbox"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
)

type fixture struct {
	t        *testing.T
	ctx      context.Context
	root     string
	store    *workspacetest.Map
	provider *local.Provider
	backend  *sandbox.Backend
	worker   *executor.Worker
	ws       workspace.Workspace
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	store := &workspacetest.Map{}
	root := filepath.Join(t.TempDir(), "envs")
	provider, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := sandbox.New(sandbox.Options{Workspaces: store, Provider: provider, Backend: local.Backend, Tools: tools.Default()})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := executor.NewWorker(ctx, storetest.NewMap(nil), []executor.Route{sandbox.Route(backend)}, executor.WorkerOptions{ID: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	ws := workspace.Workspace{ID: "ws-1", Project: "repo"}
	if err := store.Create(ctx, ws); err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, ctx: ctx, root: root, store: store, provider: provider, backend: backend, worker: worker, ws: ws}
}

func (f *fixture) assignment(tool tools.Tool, effectID run.EffectID, args string, target *run.TargetRef) effect.Assignment {
	f.t.Helper()
	def, err := sdkconv.FreezeToolDefinition(tool.Definition())
	if err != nil {
		f.t.Fatal(err)
	}
	digest, err := schema.Canonical().DigestToolDefinition(def)
	if err != nil {
		f.t.Fatal(err)
	}
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", CallID: run.CallID("call-" + string(effectID)), Effect: effectID, Target: target,
		Body: effect.ToolAssignment{ToolRef: tool.Ref(), DefinitionDigest: digest, Arguments: run.MustParseCanonicalJSON(args),
			Policy: tool.ResponsePolicy(), Replay: tool.Replay(), Placement: run.PlacementWorkspace}}
}

func (f *fixture) outcome(a effect.Assignment) effect.Outcome {
	f.t.Helper()
	if err := f.worker.Dispatch(f.ctx, a); err != nil {
		f.t.Fatalf("dispatch = %v", err)
	}
	w := &effect.Watcher{Port: f.worker, Poll: 5 * time.Millisecond}
	defer w.Close()
	out, err := w.Await(f.ctx, a.Key())
	if err != nil {
		f.t.Fatalf("await = %v", err)
	}
	return out
}

func toolOutput(t *testing.T, out effect.Outcome) string {
	t.Helper()
	ok, is := out.Result.(effect.ToolExecutionSucceeded)
	if !is {
		t.Fatalf("outcome = %#v, want success", out.Result)
	}
	return ok.Result.Output.String()
}

// Every call of a Session lands in the same workspace: the first call
// materializes the environment and records the RuntimeBinding, later calls
// attach the same one, and the files one call writes another reads.
func TestCallsShareTheWorkspaceEnvironment(t *testing.T) {
	f := newFixture(t)
	target := &run.TargetRef{Kind: workspace.TargetKind, ID: string(f.ws.ID)}
	if got := toolOutput(t, f.outcome(f.assignment(tools.WriteFile{}, "e1", `{"path":"notes.txt","content":"hi"}`, target))); !strings.Contains(got, `"bytes":2`) {
		t.Fatalf("write = %s", got)
	}
	ws, err := f.store.Get(f.ctx, f.ws.ID)
	if err != nil || ws.Runtime == nil || ws.Runtime.Backend != local.Backend || ws.Runtime.Generation != 1 {
		t.Fatalf("runtime after the first call = %+v %v", ws.Runtime, err)
	}
	if got := toolOutput(t, f.outcome(f.assignment(tools.Shell{}, "e2", `{"command":"cat notes.txt"}`, target))); !strings.Contains(got, `"stdout":"hi"`) {
		t.Fatalf("shell = %s", got)
	}
	if again, _ := f.store.Get(f.ctx, f.ws.ID); *again.Runtime != *ws.Runtime {
		t.Fatalf("runtime changed between calls: %+v -> %+v", ws.Runtime, again.Runtime)
	}
	// A new backend over the same store attaches the recorded environment
	// instead of materializing again.
	backend2, err := sandbox.New(sandbox.Options{Workspaces: f.store, Provider: f.provider, Backend: local.Backend, Tools: tools.Default()})
	if err != nil {
		t.Fatal(err)
	}
	worker2, err := executor.NewWorker(f.ctx, storetest.NewMap(nil), []executor.Route{sandbox.Route(backend2)}, executor.WorkerOptions{ID: "worker-b"})
	if err != nil {
		t.Fatal(err)
	}
	defer worker2.Close()
	f.worker = worker2
	if got := toolOutput(t, f.outcome(f.assignment(tools.ReadFile{}, "e3", `{"path":"notes.txt"}`, target))); !strings.Contains(got, `"content":"hi"`) {
		t.Fatalf("read through the second backend = %s", got)
	}
	if again, _ := f.store.Get(f.ctx, f.ws.ID); again.Runtime.Generation != 1 {
		t.Fatalf("second backend materialized again: %+v", again.Runtime)
	}
}

// A workspace whose recorded environment is gone is materialized again
// under the next generation; a workspace the store does not know is a
// definite failure of the call.
func TestLostEnvironmentIsRematerialized(t *testing.T) {
	f := newFixture(t)
	if err := f.store.UpdateRuntime(f.ctx, f.ws.ID, 0, workspace.RuntimeBinding{Backend: local.Backend, EnvironmentRef: "env-gone", Generation: 4}); err != nil {
		t.Fatal(err)
	}
	target := &run.TargetRef{Kind: workspace.TargetKind, ID: string(f.ws.ID)}
	// The first call after the rebuild tells the model; the next does not.
	if got := toolOutput(t, f.outcome(f.assignment(tools.ListDir{}, "e1", `{}`, target))); !strings.Contains(got, `"entries":[]`) || !strings.Contains(got, `"workspaceRematerialized":true`) {
		t.Fatalf("list in the rematerialized workspace = %s", got)
	}
	if got := toolOutput(t, f.outcome(f.assignment(tools.ListDir{}, "e1b", `{}`, target))); strings.Contains(got, "workspaceRematerialized") {
		t.Fatalf("second call still marked = %s", got)
	}
	ws, _ := f.store.Get(f.ctx, f.ws.ID)
	if ws.Runtime.Generation != 5 || ws.Runtime.EnvironmentRef == "env-gone" {
		t.Fatalf("runtime after rematerialization = %+v", ws.Runtime)
	}
	out := f.outcome(f.assignment(tools.ListDir{}, "e2", `{}`, &run.TargetRef{Kind: workspace.TargetKind, ID: "ws-unknown"}))
	failed, is := out.Result.(effect.ToolExecutionFailed)
	if !is || failed.Failure.Class != run.FailureNotFound || failed.Retry != run.RetryNever {
		t.Fatalf("unknown workspace = %#v", out.Result)
	}
}

// Snapshot copies the workspace's environment, records the Snapshot with
// its parent and makes it the workspace's latest; a workspace with no
// environment has nothing to snapshot (APP-WSP-7).
func TestSnapshotRecordsTheLatest(t *testing.T) {
	f := newFixture(t)
	if _, err := f.backend.Snapshot(f.ctx, f.ws.ID); !errors.Is(err, sandbox.ErrNothingToSnapshot) {
		t.Fatalf("snapshot before any environment = %v", err)
	}
	if _, err := f.backend.Snapshot(f.ctx, "ws-unknown"); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("snapshot of an unknown workspace = %v", err)
	}
	target := &run.TargetRef{Kind: workspace.TargetKind, ID: string(f.ws.ID)}
	toolOutput(t, f.outcome(f.assignment(tools.WriteFile{}, "e1", `{"path":"f.txt","content":"v1"}`, target)))
	first, err := f.backend.Snapshot(f.ctx, f.ws.ID)
	if err != nil || first.Workspace != f.ws.ID || first.Backend != local.Backend || first.StateRef == "" || first.Parent != nil {
		t.Fatalf("first snapshot = %+v %v", first, err)
	}
	toolOutput(t, f.outcome(f.assignment(tools.WriteFile{}, "e2", `{"path":"f.txt","content":"v2"}`, target)))
	second, err := f.backend.Snapshot(f.ctx, f.ws.ID)
	if err != nil || second.Parent == nil || *second.Parent != first.Ref || second.StateRef == first.StateRef {
		t.Fatalf("second snapshot = %+v %v", second, err)
	}
	ws, _ := f.store.Get(f.ctx, f.ws.ID)
	if ws.Snapshot == nil || *ws.Snapshot != second.Ref {
		t.Fatalf("workspace latest snapshot = %v, want %s", ws.Snapshot, second.Ref)
	}
	if stored, err := f.store.GetSnapshot(f.ctx, first.Ref); err != nil || stored != first {
		t.Fatalf("stored first snapshot = %+v %v", stored, err)
	}
	// A workspace forked from the first snapshot reads v1.
	forked, err := f.store.Fork(f.ctx, workspace.Fork{Source: f.ws.ID, Destination: "ws-fork", Snapshot: first.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if got := toolOutput(t, f.outcome(f.assignment(tools.ReadFile{}, "e3", `{"path":"f.txt"}`, &run.TargetRef{Kind: workspace.TargetKind, ID: string(forked.ID)}))); !strings.Contains(got, `"content":"v1"`) {
		t.Fatalf("read in the forked workspace = %s", got)
	}
}

// An environment that disappears under a cached attachment fails the call
// that hits it, is dropped from the cache, and the next call rebuilds the
// workspace under the next generation and says so.
func TestLostCachedEnvironmentIsRebuiltOnTheNextCall(t *testing.T) {
	f := newFixture(t)
	target := &run.TargetRef{Kind: workspace.TargetKind, ID: string(f.ws.ID)}
	toolOutput(t, f.outcome(f.assignment(tools.WriteFile{}, "e1", `{"path":"f.txt","content":"v1"}`, target)))
	ws, _ := f.store.Get(f.ctx, f.ws.ID)
	if err := os.RemoveAll(filepath.Join(f.root, string(ws.Runtime.EnvironmentRef))); err != nil {
		t.Fatal(err)
	}
	out := f.outcome(f.assignment(tools.Shell{}, "e2", `{"command":"true"}`, target))
	if _, failed := out.Result.(effect.ToolExecutionFailed); !failed {
		t.Fatalf("call on a vanished environment = %#v, want a failure", out.Result)
	}
	got := toolOutput(t, f.outcome(f.assignment(tools.ListDir{}, "e3", `{}`, target)))
	if !strings.Contains(got, `"workspaceRematerialized":true`) {
		t.Fatalf("call after the loss = %s, want a rebuilt environment marked", got)
	}
	if after, _ := f.store.Get(f.ctx, f.ws.ID); after.Runtime.Generation != 2 || after.Runtime.EnvironmentRef == ws.Runtime.EnvironmentRef {
		t.Fatalf("runtime after the rebuild = %+v", after.Runtime)
	}
}

// Validate refuses before any effect what the backend cannot serve: a
// workspace tool without a target, a process-placed tool and a model call.
func TestValidateRefusals(t *testing.T) {
	f := newFixture(t)
	noTarget := f.assignment(tools.Shell{}, "e1", `{"command":"true"}`, nil)
	failure, err := f.backend.Validate(f.ctx, noTarget)
	if err != nil || failure == nil || failure.Class != run.FailureInvalidInput || !strings.Contains(failure.Message, "bound to none") {
		t.Fatalf("validate without a target = %+v %v", failure, err)
	}
	process := f.assignment(tools.Shell{}, "e2", `{"command":"true"}`, &run.TargetRef{Kind: workspace.TargetKind, ID: "ws-1"})
	body := process.Body.(effect.ToolAssignment)
	body.Placement = run.PlacementProcess
	process.Body = body
	if failure, err := f.backend.Validate(f.ctx, process); err != nil || failure == nil || !strings.Contains(failure.Message, "placed in the process") {
		t.Fatalf("validate a process tool = %+v %v", failure, err)
	}
	if !sandbox.Route(f.backend).Match(noTarget) || sandbox.Route(f.backend).Match(process) {
		t.Fatal("route matches by placement, not by target")
	}
	model := effect.Assignment{Session: "s", RunID: "r", StepID: "step", Effect: "m", Body: effect.ModelAssignment{Model: "m", RequestDigest: "sha256:x"}}
	if failure, err := f.backend.Validate(f.ctx, model); err != nil || failure == nil || failure.Class != run.FailureProvider {
		t.Fatalf("validate a model call = %+v %v", failure, err)
	}
	// Start refuses the same way, so nothing reaches an environment.
	ref, _ := f.backend.Prepare(f.ctx, noTarget)
	if err := f.backend.Start(f.ctx, ref, noTarget); err == nil || errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("start without a target = %v, want a definite rejection", err)
	}
	// PublicTools carry the workspace placement into presets.
	public, err := sandbox.PublicTools(tools.Default())
	if err != nil || len(public) != 4 || public[0].Placement != run.PlacementWorkspace || public[0].Ref != tools.ShellRef {
		t.Fatalf("public tools = %+v %v", public, err)
	}
	_ = environment.ErrNotFound
}
