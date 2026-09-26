// Package sandbox is the workspace backend of the Worker (CLD-TOL-1): an
// executor.ExecutionBackend that runs workspace-placed tools inside the
// Environment a Session's Workspace is materialized in. It composes the
// kernel's in-process executor with an environment manager: every tool
// call's Assignment carries a workspace target (RUN-LOP-9), the manager
// resolves it to an attached Environment through the workspace Store and
// the environment Provider, and the tool runs there. Routing by the tool's
// Placement declaration sends only workspace-placed calls here.
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/notice"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// Provider is the backend's name in a Worker's Route table and in records.
const Provider = "sandbox"

// Options compose a Backend.
type Options struct {
	// Workspaces holds the Workspace records and their RuntimeBindings.
	Workspaces workspace.Store
	// Provider materializes and attaches environments.
	Provider environment.Provider
	// Backend is the Provider's identity written into RuntimeBindings.
	Backend environment.Backend
	// Tools are the workspace-placed tools this backend serves.
	Tools []tools.Tool
	// Progress receives the tools' progress frames; nil discards them.
	Progress effect.ProgressSink
}

// Backend is the workspace backend.
type Backend struct {
	inner *loop.LocalExecutor
	envs  *manager
}

var (
	_ executor.ExecutionBackend = (*Backend)(nil)
	_ notice.Source             = (*Backend)(nil)
	_ workspace.Snapshotter     = (*Backend)(nil)
)

// New composes a Backend.
func New(opts Options) (*Backend, error) {
	if opts.Workspaces == nil || opts.Provider == nil {
		return nil, errors.New("sandbox: a workspace store and an environment provider are required")
	}
	if opts.Backend == "" {
		return nil, errors.New("sandbox: the provider's backend identity is required")
	}
	envs := &manager{store: opts.Workspaces, provider: opts.Provider, backend: opts.Backend, attached: make(map[workspace.ID]environment.Environment)}
	catalog := toolCatalog{}
	for _, t := range opts.Tools {
		if t == nil || t.Ref() == "" {
			return nil, errors.New("sandbox: tools require a ref")
		}
		if _, dup := catalog[t.Ref()]; dup {
			return nil, fmt.Errorf("sandbox: duplicate tool %s", t.Ref())
		}
		catalog[t.Ref()] = &adapter{tool: t, envs: envs}
	}
	inner, err := loop.NewLocalExecutor(noModels{}, catalog, opts.Progress, false)
	if err != nil {
		return nil, err
	}
	return &Backend{inner: inner, envs: envs}, nil
}

// Route is the Worker route that sends workspace-placed tool calls to b:
// this Backend in its process, or a backendhttp.Client to a tool backend.
func Route(b executor.ExecutionBackend) executor.Route {
	return executor.Route{Provider: Provider, Backend: b, Match: executor.MatchTool(run.PlacementWorkspace)}
}

// PublicTools are the preset entries of workspace tools: frozen definition,
// policies and the workspace placement.
func PublicTools(ts []tools.Tool) ([]turn.PublicTool, error) {
	out := make([]turn.PublicTool, 0, len(ts))
	for _, t := range ts {
		def, err := sdkconv.FreezeToolDefinition(t.Definition())
		if err != nil {
			return nil, err
		}
		out = append(out, turn.PublicTool{Ref: t.Ref(), Definition: def, Policy: t.ResponsePolicy(), Replay: t.Replay(), Placement: run.PlacementWorkspace})
	}
	return out, nil
}

// Validate refuses what this backend cannot serve before any effect: a model
// call, a process-placed tool (a routing error), a workspace tool whose
// Assignment carries no workspace target (the Session is bound to none);
// the in-process executor then checks the tool's declarations.
func (b *Backend) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	t, ok := a.Tool()
	if !ok {
		return &run.ToolFailure{Class: run.FailureProvider, Message: "the workspace backend serves tool calls only"}, nil
	}
	if t.Placement != run.PlacementWorkspace {
		return &run.ToolFailure{Class: run.FailureInvalidInput, Message: fmt.Sprintf("tool %s is placed in the process, not in a workspace", t.ToolRef)}, nil
	}
	if a.Target == nil || a.Target.Kind != workspace.TargetKind || a.Target.ID == "" {
		return &run.ToolFailure{Class: run.FailureInvalidInput, Message: fmt.Sprintf("tool %s runs in a workspace but the session is bound to none", t.ToolRef)}, nil
	}
	return b.inner.Validate(ctx, a)
}

func (b *Backend) Prepare(ctx context.Context, a effect.Assignment) (string, error) {
	return b.inner.Prepare(ctx, a)
}

func (b *Backend) Start(ctx context.Context, ref string, a effect.Assignment) error {
	if failure, err := b.Validate(ctx, a); err != nil {
		return err
	} else if failure != nil {
		return fmt.Errorf("%w: %s: %s", loop.ErrExecutorRejected, failure.Class, failure.Message)
	}
	return b.inner.Start(ctx, ref, a)
}

func (b *Backend) Restart(ctx context.Context, previous string, a effect.Assignment) (string, error) {
	return b.inner.Restart(ctx, previous, a)
}
func (b *Backend) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	return b.inner.Attach(ctx, ref)
}
func (b *Backend) Status(ctx context.Context, ref string) (effect.ExecutionStatus, error) {
	return b.inner.Status(ctx, ref)
}
func (b *Backend) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	return b.inner.Outcome(ctx, ref)
}
func (b *Backend) Cancel(ctx context.Context, ref string) error { return b.inner.Cancel(ctx, ref) }

// Settled is notice.Source: the in-process executor's settlement notices.
func (b *Backend) Settled(ctx context.Context, epoch string, after uint64, fn func(notice.Ref) bool) error {
	return b.inner.Settled(ctx, epoch, after, fn)
}

// Close detaches every environment this process attached; the workspaces
// keep their RuntimeBindings.
func (b *Backend) Close(ctx context.Context) error { return b.envs.close(ctx) }

// Snapshot takes a Snapshot of the Workspace's current environment through
// the provider's Snapshotter capability, records it in the store and makes
// it the Workspace's latest (APP-WSP-7). A Workspace never materialized has
// nothing to snapshot (ErrNothingToSnapshot); a provider without the
// capability is environment.ErrUnsupported.
func (b *Backend) Snapshot(ctx context.Context, id workspace.ID) (workspace.Snapshot, error) {
	return b.envs.snapshot(ctx, id)
}

// ErrNothingToSnapshot is workspace.ErrNothingToSnapshot.
var ErrNothingToSnapshot = workspace.ErrNothingToSnapshot

// --- tool adapter -------------------------------------------------------------

type noModels struct{}

func (noModels) ResolveModel(ref run.ModelRef) (loop.ModelInvoker, error) {
	return nil, fmt.Errorf("sandbox: no model %s: the workspace backend serves tool calls only", ref)
}

type toolCatalog map[run.ToolRef]loop.ExecutableTool

func (c toolCatalog) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	t, ok := c[ref]
	if !ok {
		return nil, fmt.Errorf("sandbox: unknown workspace tool %s", ref)
	}
	return t, nil
}

// adapter presents a workspace tool to the in-process executor and resolves
// the environment when the call runs.
type adapter struct {
	tool tools.Tool
	envs *manager
}

var _ loop.ExecutableTool = (*adapter)(nil)

func (a *adapter) Ref() run.ToolRef                            { return a.tool.Ref() }
func (a *adapter) Definition() sdk.ToolDefinition              { return a.tool.Definition() }
func (a *adapter) ResponsePolicy() run.ResponsePolicy          { return a.tool.ResponsePolicy() }
func (a *adapter) Replay() run.ReplayPolicy                    { return a.tool.Replay() }
func (a *adapter) Placement() run.ToolPlacement                { return run.PlacementWorkspace }
func (a *adapter) ValidateArguments(v run.CanonicalJSON) error { return a.tool.ValidateArguments(v) }

func (a *adapter) Execute(ctx context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome { //nolint:gocritic // hugeParam: loop.ExecutableTool.Execute takes the request by value
	if req.Target == nil || req.Target.Kind != workspace.TargetKind || req.Target.ID == "" {
		return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureInvalidInput, Message: "workspace tool call without a workspace target"}, Retry: run.RetryNever}
	}
	env, rematerialized, err := a.envs.attach(ctx, workspace.ID(req.Target.ID))
	if err != nil {
		switch {
		case errors.Is(err, workspace.ErrNotFound):
			return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureNotFound, Message: err.Error()}, Retry: run.RetryNever}
		case errors.Is(err, environment.ErrUnsupported):
			return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureUnavailable, Message: err.Error()}, Retry: run.RetryNever}
		default:
			return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureUnavailable, Message: err.Error()}, Retry: run.RetryAllowed}
		}
	}
	out := a.tool.Run(ctx, env, &req)
	if rematerialized {
		out = markRematerialized(out)
	}
	if failed, is := out.(loop.ToolExecutionFailed); is && environmentMayBeGone(failed.Failure.Class) {
		// The tool is not run again here (its Replay declaration decides
		// that); the cache is checked so the next call finds a live
		// environment or rebuilds one.
		a.envs.invalidate(ctx, workspace.ID(req.Target.ID), env)
	}
	return out
}

// environmentMayBeGone are the failure classes a lost environment shows up
// as.
func environmentMayBeGone(class string) bool {
	switch class {
	case run.FailureUnavailable, run.FailureExecution, run.FailureInternal:
		return true
	default:
		return false
	}
}

// markRematerialized tells the model, on the first successful call after
// the workspace's environment was rebuilt, that files written since the
// last snapshot are gone (APP-WSP-7).
func markRematerialized(out loop.ToolExecutionOutcome) loop.ToolExecutionOutcome {
	ok, is := out.(loop.ToolExecutionSucceeded)
	if !is {
		return out
	}
	var fields map[string]any
	if err := ok.Result.Output.Decode(&fields); err != nil || fields == nil {
		return out
	}
	fields["workspaceRematerialized"] = true
	marked, err := run.CanonicalJSONFromValue(fields)
	if err != nil {
		return out
	}
	ok.Result.Output = marked
	return ok
}

// --- environment manager ------------------------------------------------------

// manager resolves a Workspace to an attached Environment: the one the
// Workspace's RuntimeBinding names, or a new materialization recorded with a
// conditional write on the binding generation, so two replicas that
// materialize the same workspace at once agree on one environment.
type manager struct {
	store    workspace.Store
	provider environment.Provider
	backend  environment.Backend

	// mu guards the maps; each workspace's resolution runs under its own
	// lock, so one provider call blocks no other workspace.
	mu       sync.Mutex
	attached map[workspace.ID]environment.Environment
	locks    map[workspace.ID]*sync.Mutex
	// rebuilt marks workspaces whose recorded environment was lost and
	// rebuilt by this process; the next successful call reports it.
	rebuilt map[workspace.ID]bool
}

func (m *manager) lock(id workspace.ID) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks == nil {
		m.locks = make(map[workspace.ID]*sync.Mutex)
	}
	l, ok := m.locks[id]
	if !ok {
		l = &sync.Mutex{}
		m.locks[id] = l
	}
	return l
}

// attach returns the workspace's environment and whether this call is the
// first after the environment was rebuilt from a lost one.
func (m *manager) attach(ctx context.Context, id workspace.ID) (env environment.Environment, rematerialized bool, err error) {
	l := m.lock(id)
	l.Lock()
	defer l.Unlock()
	m.mu.Lock()
	env, ok := m.attached[id]
	m.mu.Unlock()
	if !ok {
		env, err = m.resolve(ctx, id)
		if err != nil {
			return nil, false, err
		}
		m.mu.Lock()
		m.attached[id] = env
		m.mu.Unlock()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rebuilt[id] {
		delete(m.rebuilt, id)
		return env, true, nil
	}
	return env, false, nil
}

// invalidate drops the cached environment of id when the provider no
// longer has it, so the next call materializes the workspace again and
// reports it; an environment the provider still has stays cached.
func (m *manager) invalidate(ctx context.Context, id workspace.ID, env environment.Environment) {
	if _, err := m.provider.Attach(ctx, env.Ref()); err == nil || !errors.Is(err, environment.ErrNotFound) {
		return
	}
	l := m.lock(id)
	l.Lock()
	defer l.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if cached, ok := m.attached[id]; ok && cached.Ref() == env.Ref() {
		delete(m.attached, id)
	}
}

func (m *manager) resolve(ctx context.Context, id workspace.ID) (environment.Environment, error) {
	ws, err := m.store.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("workspace %s: %w", id, err)
	}
	var expected uint64
	if ws.Runtime != nil {
		expected = ws.Runtime.Generation
		if ws.Runtime.Backend == m.backend {
			env, err := m.provider.Attach(ctx, ws.Runtime.EnvironmentRef)
			if err == nil {
				return env, nil
			}
			if !errors.Is(err, environment.ErrNotFound) {
				return nil, fmt.Errorf("workspace %s: attach %s: %w", id, ws.Runtime.EnvironmentRef, err)
			}
		}
		// The recorded environment is gone or belongs to another provider:
		// the workspace is materialized again under the next generation, and
		// the next call says so.
		m.mu.Lock()
		if m.rebuilt == nil {
			m.rebuilt = make(map[workspace.ID]bool)
		}
		m.rebuilt[id] = true
		m.mu.Unlock()
	}
	return m.materialize(ctx, &ws, expected)
}

// snapshot is Backend.Snapshot.
func (m *manager) snapshot(ctx context.Context, id workspace.ID) (workspace.Snapshot, error) {
	ws, err := m.store.Get(ctx, id)
	if err != nil {
		return workspace.Snapshot{}, fmt.Errorf("workspace %s: %w", id, err)
	}
	if ws.Runtime == nil {
		return workspace.Snapshot{}, ErrNothingToSnapshot
	}
	env, _, err := m.attach(ctx, id)
	if err != nil {
		return workspace.Snapshot{}, err
	}
	snapshotter, ok := env.(environment.Snapshotter)
	if !ok {
		return workspace.Snapshot{}, fmt.Errorf("%w: environment %s takes no snapshots", environment.ErrUnsupported, env.Ref())
	}
	state, err := snapshotter.Snapshot(ctx)
	if err != nil {
		return workspace.Snapshot{}, fmt.Errorf("workspace %s: snapshot: %w", id, err)
	}
	snap := workspace.Snapshot{Ref: workspace.NewSnapshotRef(), Workspace: id, Backend: m.backend, StateRef: state, Parent: ws.Snapshot}
	if err := m.store.PutSnapshot(ctx, snap); err != nil {
		return workspace.Snapshot{}, err
	}
	// The latest snapshot is a partial write: a concurrent UpdateRuntime by
	// another replica is left where it is; two concurrent snapshots leave
	// the later write as the latest.
	if err := m.store.UpdateSnapshot(ctx, id, snap.Ref); err != nil {
		return workspace.Snapshot{}, err
	}
	return snap, nil
}

// materialize creates the workspace's next environment and records it;
// when another replica recorded a binding first, its environment is
// attached instead and the one created here is closed.
func (m *manager) materialize(ctx context.Context, ws *workspace.Workspace, expected uint64) (environment.Environment, error) {
	var env environment.Environment
	var err error
	if ws.Snapshot != nil {
		snap, serr := m.store.GetSnapshot(ctx, *ws.Snapshot)
		if serr != nil {
			return nil, fmt.Errorf("workspace %s: snapshot %s: %w", ws.ID, *ws.Snapshot, serr)
		}
		env, err = m.provider.Restore(ctx, environment.RestoreSpec{State: snap.StateRef, Destination: environment.Spec{Subject: string(ws.ID), Base: string(ws.Base)}})
	} else {
		env, err = m.provider.Create(ctx, environment.Spec{Subject: string(ws.ID), Base: string(ws.Base)})
	}
	if err != nil {
		return nil, fmt.Errorf("workspace %s: materialize: %w", ws.ID, err)
	}
	binding := environment.Binding{Backend: m.backend, EnvironmentRef: env.Ref(), Generation: expected + 1}
	if err := m.store.UpdateRuntime(ctx, ws.ID, expected, binding); err != nil {
		_ = env.Close(ctx)
		if !errors.Is(err, workspace.ErrGenerationConflict) {
			return nil, fmt.Errorf("workspace %s: record runtime: %w", ws.ID, err)
		}
		current, gerr := m.store.Get(ctx, ws.ID)
		if gerr != nil || current.Runtime == nil || current.Runtime.Backend != m.backend {
			return nil, fmt.Errorf("workspace %s: another replica materialized it under a binding this backend cannot attach", ws.ID)
		}
		return m.provider.Attach(ctx, current.Runtime.EnvironmentRef)
	}
	return env, nil
}

func (m *manager) close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var first error
	for id, env := range m.attached {
		if err := env.Close(ctx); err != nil && first == nil {
			first = err
		}
		delete(m.attached, id)
	}
	return first
}
