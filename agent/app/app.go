// Package app is the reference agent's application layer over agentcore: it composes an
// Authority from deployment choices (Build), registers presets, and offers
// the conversation policies a product needs on top of an owned Session --
// replies, workspace binding and snapshots, automatic compaction, the event
// stream and the subagent effect. Input admission, routing and driving are
// the runtime's; the core packages remain independent of this package.
package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agent/prompt"
	"github.com/felinics/twilight/agent/spawn"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"sync"
)

// Preset is an authority-side decision identity to register during Build.
type Preset struct {
	ID    preset.PresetID
	Value preset.AgentPreset
}

// Config contains the product assembly: the Session kernel, the execution
// side and the conversation policies. Deployment topology -- which process
// runs the model and tool backends, whether an execution Worker owns the
// records here -- is the composition root's choice; the root fills
// Execution.Executor with the port it decided on and closes whatever it
// composed around it. The Config deliberately contains concrete ports
// rather than a file format: YAML, environment variables and command-line
// flags can be decoded into this type by an outer deployment package later.
type Config struct {
	// Kernel is the session kernel's assembly: the Session store, the
	// content and artifact stores, the module extensions, the projection
	// cache and the other ports and policies sessionkernel.New composes.
	Kernel sessionkernel.Ports
	// Execution is the execution side's assembly: the effect port, the
	// decision catalog, the preset registry, the takeover policy and the
	// other drive-chain policies runtime.NewExecution composes over the
	// Kernel. New fills the Planner, the Responders and Fail.
	Execution rt.ExecutionConfig

	Presets []Preset
	// Warn receives failures of background work; nil discards them.
	Warn func(error)
	// Spawn enables the subagent tool (SPN); nil leaves it unavailable.
	Spawn *spawn.Options
	// Inbox is the durable command inbox of the Sessions (APP-INB-1): the
	// way a caller that does not hold a Session reaches its owner. Nil
	// leaves Enqueue and ApplyPending unavailable (ErrNoInbox); like every
	// store it is durable (OWN-PRT-3).
	Inbox inbox.Store
	// Activation holds Sessions for active work only and releases them when
	// quiescent (APP-ACT); nil keeps every Session open until Close. It
	// requires Inbox.
	Activation *Activation
	// Workspaces enables the workspace layer (APP-WSP): the Session binding
	// module, its target resolver, the workspace backend of the composed
	// Worker and the prompt's workspace preface. Nil leaves every
	// workspace-placed tool call without a target.
	Workspaces *WorkspaceConfig
}

// WorkspaceConfig composes the workspace layer's product side (APP-WSP-3).
type WorkspaceConfig struct {
	// Store holds the Workspace records and RuntimeBindings (required).
	Store workspace.Store
	// Snapshots takes the Snapshots (APP-WSP-7): the composition root that
	// runs the sandbox backend composes it and leaves this nil; a process
	// without the backend (the cloud owner) hands in the tool backend's
	// client (agent/workspace/http.Client).
	Snapshots workspace.Snapshotter
	// SnapshotAfterTurn takes a Snapshot of a Session's bound Workspace
	// after every quiescent settlement and records it on the
	// Session (APP-WSP-7), so a fork at a Turn boundary can restore the
	// files as they were. It needs Snapshots.
	SnapshotAfterTurn bool
}

// CompactorSystemPrompt is kept here for deterministic model test doubles and
// applications that need to recognize the built-in compaction request.
const CompactorSystemPrompt = compaction.CompactorSystemPrompt

// Application is the product assembly: the Session kernel, the execution
// side over it and the ownership authority -- plus the application's own
// services, the preset table, the event stream and the spawn effect. The
// Worker and the other deployment components around the effect port close
// at the composition root, after this.
type Application struct {
	// Owner holds the Sessions this process owns; Kernel is their durable
	// state and Execution is what advances them.
	Owner     *owner.Owner
	Kernel    *sessionkernel.Kernel
	Execution *rt.Execution
	spawn     *spawn.Responder
	// warn receives failures of background work.
	warn       func(error)
	inbox      inbox.Store
	activation *Activation
	bg         context.Context
	bgCancel   context.CancelFunc
	loops      sync.WaitGroup
	releases   group
	actMu      sync.Mutex
	activating map[session.SessionID]chan struct{}
	// workspaces is the workspace layer's configuration, nil when absent.
	workspaces *WorkspaceConfig
	// snapshots is where snapshots are taken, composed by the root.
	snapshots workspace.Snapshotter
	// bindings writes the Session's workspace binding facts.
	bindings workspace.Commands
	// resolved is closed and replaced at each inbox resolution of this
	// process; AwaitCommand waits on it (APP-INB-1).
	resolvedMu sync.Mutex
	resolved   chan struct{}

	mu   sync.RWMutex
	refs map[preset.PresetID]preset.PresetRef
	// sessions are the Sessions this process has open, by id: the Planner
	// finds a Session's compaction policy here while its Run is driven
	// (APP-CKP-1).
	sessions map[session.SessionID]*Session
}

// BeforePrepare is driver.Planner (RUN-LOP-10, APP-CKP-1): between two steps
// of a Run, while it is Open, the Session's automatic compaction policy runs
// against the context the next model request will read. Failures reach
// CompactWarn and never stop the drive.
func (app *Application) BeforePrepare(ctx context.Context, w writer.Writer, _ decision.Input) error {
	app.mu.RLock()
	s := app.sessions[w.SessionID()]
	app.mu.RUnlock()
	if s == nil || s.opts.CompactAfterEntries <= 0 {
		return nil
	}
	s.maybeCompact(ctx)
	return nil
}

// Opened returns the Session this process holds open under sid, if any: the
// command face wakes and closes Sessions through it (APP-INB-3).
func (app *Application) Opened(sid session.SessionID) (*Session, bool) {
	app.mu.RLock()
	defer app.mu.RUnlock()
	s, ok := app.sessions[sid]
	return s, ok
}

// Lease is the Session's current writer lease, read without ownership
// what a gateway routes by and a controller judges expiry by.
func (app *Application) Lease(ctx context.Context, sid session.SessionID) (session.Lease, bool, error) {
	return app.Kernel.Store.LeaseOf(ctx, sid)
}

func (app *Application) track(s *Session) {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.sessions[s.sid] = s
}

func (app *Application) untrack(s *Session) {
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.sessions[s.sid] == s {
		delete(app.sessions, s.sid)
	}
}

// New assembles the application from its product dependencies. The effect
// port -- the deployment decision -- arrives in c.Execution.Executor; the
// root that composed it (a local agent component, a cloud owner service)
// closes it and whatever it stands on after this Application.
func New(c Config) (*Application, error) { //nolint:gocritic // hugeParam: Config is a by-value options struct read once
	if c.Execution.Executor == nil {
		return nil, errors.New("app: an effect port is required (Config.Execution.Executor); composing it is the deployment root's act")
	}
	warn := c.Warn
	if warn == nil {
		warn = func(error) {}
	}
	app := &Application{warn: warn, inbox: c.Inbox, workspaces: c.Workspaces, bindings: workspace.Commands{Now: c.Kernel.Clock},
		refs: make(map[preset.PresetID]preset.PresetRef, len(c.Presets)), sessions: make(map[session.SessionID]*Session),
		activating: make(map[session.SessionID]chan struct{})}
	app.bg, app.bgCancel = context.WithCancel(context.Background())
	// Build is transactional: a failure after a resource with a lifetime
	// was created releases what was created, in reverse order.
	built := false
	defer func() {
		if !built {
			app.rollback()
		}
	}()
	if c.Activation != nil {
		if c.Inbox == nil {
			return nil, errors.New("app: Activation requires an inbox Store")
		}
		act := *c.Activation
		app.activation = &act
	}
	// The subagent tool is answered by a Responder on the Driver (SPN-1,
	// DRV-4), not executed: it drives children through the Authority, so it
	// is bound once the Owner exists. No effect route is involved.
	if c.Spawn != nil {
		app.spawn = spawn.NewResponder(*c.Spawn)
	}
	// The workspace layer (APP-WSP-3): the binding module joins the
	// registry, the resolver answers RUN-LOP-9 from the binding projection,
	// and the prompt gets its workspace preface.
	var resolver *workspace.Resolver
	if c.Workspaces != nil {
		if c.Workspaces.Store == nil {
			return nil, errors.New("app: Workspaces requires a workspace Store")
		}
		c.Kernel.Modules = append([]module.ModuleDescriptor{workspace.Module}, c.Kernel.Modules...)
		if c.Execution.TargetResolver == nil {
			resolver = &workspace.Resolver{}
			c.Execution.TargetResolver = resolver
		}
		if c.Execution.Decisions == nil {
			c.Execution.Decisions = prompt.CatalogWith(prompt.WorkspacePreface)
		}
		app.snapshots = c.Workspaces.Snapshots
		if c.Workspaces.SnapshotAfterTurn && app.snapshots == nil {
			return nil, errors.New("app: Workspaces.SnapshotAfterTurn requires a Snapshotter")
		}
	}
	if c.Execution.Decisions == nil {
		c.Execution.Decisions = prompt.DefaultCatalog()
	}
	kernel, err := sessionkernel.New(c.Kernel)
	if err != nil {
		return nil, err
	}
	app.Kernel = kernel
	// The subagent tool waits for an external response the Responder gives
	// (SPN-1, DRV-4); the Responder opens children through the Owner, which
	// it is bound to once the Owner exists, before any Respond can run.
	var responders map[run.ToolRef]driver.Responder
	if app.spawn != nil {
		responders = map[run.ToolRef]driver.Responder{c.Spawn.ToolRef(): app.spawn}
	}
	// The Sessions' compaction policy runs between the steps of a Turn
	// (APP-CKP-1, RUN-LOP-10); provisional observations of effects in
	// flight reach the transient stream (OBS-1).
	c.Execution.Planner = app
	c.Execution.Responders = responders
	c.Execution.Fail = app.fail
	exec, err := rt.NewExecution(c.Execution, rt.ExecutionSources{
		Runs: kernel.Runs, Projections: kernel.Projections, Content: kernel.Content,
	})
	if err != nil {
		return nil, err
	}
	app.Execution = exec
	app.Owner = owner.New(kernel, exec)
	if resolver != nil {
		resolver.Projections = kernel.Projections
	}
	if app.spawn != nil {
		app.spawn.Bind(app.Owner, kernel)
	}
	for i := range c.Presets {
		p := &c.Presets[i]
		if p.ID == "" {
			return nil, errors.New("app: preset requires an id")
		}
		if _, err := app.RegisterPreset(p.ID, p.Value); err != nil {
			return nil, fmt.Errorf("app: register preset %q: %w", p.ID, err)
		}
	}
	if app.activation != nil && app.activation.Scan > 0 {
		app.loops.Add(1)
		go app.scanLoop()
	}
	built = true
	return app, nil
}

// rollback releases what a failed New created, in reverse order of
// creation: the execution side and the Kernel (its Writers). Nothing was
// opened yet, so there is no ownership to release; the effect port and
// whatever the root composed around it are the root's to release.
func (app *Application) rollback() {
	app.bgCancel()
	if app.Execution != nil {
		app.Execution.Close()
	}
	if app.Kernel != nil {
		_ = app.Kernel.Close(context.Background())
	}
}

// fail reports a failure of the application's own background work (a
// settlement that errored, a snapshot that failed) to Warn and, as an
// Event, to the Session's subscribers (OBS-1). The Execution's components
// report through their own Fail callback, which NewExecution wires to the
// same stream.
func (app *Application) fail(sid session.SessionID, err error) {
	app.warn(err)
	app.Execution.Progress.Failed(sid, err)
}

// RegisterPreset adds or replaces a decision identity after Build.
func (app *Application) RegisterPreset(id preset.PresetID, p preset.AgentPreset) (preset.PresetRef, error) {
	ref, err := app.Execution.Presets.Register(id, p)
	if err != nil {
		return preset.PresetRef{}, err
	}
	app.mu.Lock()
	app.refs[id] = ref
	app.mu.Unlock()
	return ref, nil
}

// PresetRef returns the digest-checked reference for a registered preset.
func (app *Application) PresetRef(id preset.PresetID) (preset.PresetRef, error) {
	app.mu.RLock()
	ref, ok := app.refs[id]
	app.mu.RUnlock()
	if !ok {
		return preset.PresetRef{}, fmt.Errorf("app: unknown preset %q", id)
	}
	return ref, nil
}

// Events subscribes to one Session's observation stream from this moment
// on (OBS-1): the Kernel's committed stream merged with the Execution's
// transient progress and failures. Committed events keep their commit
// order; transient items may interleave and may be lost.
func (app *Application) Events(ctx context.Context, sid session.SessionID) <-chan Event {
	return mergeEvents(ctx, app.Kernel.Bus.Subscribe(ctx, sid), app.Execution.Progress.Subscribe(ctx, sid))
}

// EventsFrom is the catch-up form of Events: the Session's committed events
// from CommitSeq from, then the live stream, with the transient stream live
// from now on. A client that keeps the last Position it handled resumes
// here after a disconnect without a gap.
func (app *Application) EventsFrom(ctx context.Context, sid session.SessionID, from ledger.CommitSeq) (<-chan Event, error) {
	committed, err := app.Kernel.Bus.SubscribeFrom(ctx, sid, from)
	if err != nil {
		return nil, err
	}
	return mergeEvents(ctx, committed, app.Execution.Progress.Subscribe(ctx, sid)), nil
}

// mergeEvents forwards both observation streams into one channel until ctx
// ends; each source closes independently and drops out of the merge.
func mergeEvents(ctx context.Context, committed, transient <-chan Event) <-chan Event {
	out := make(chan Event, 64)
	go func() {
		defer close(out)
		for committed != nil || transient != nil {
			select {
			case e, ok := <-committed:
				if !ok {
					committed = nil
					continue
				}
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			case e, ok := <-transient:
				if !ok {
					transient = nil
					continue
				}
				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Close cancels the spawn effect's child drives, stops recovery listeners
// and releases every Session this application owns.
func (app *Application) Close(ctx context.Context) error {
	if app.spawn != nil {
		app.spawn.Close()
	}
	// The activation scan stops first, so it opens nothing more; then the
	// Sessions this process holds close: their background drives, appliers
	// and snapshots end before the Writers they commit through.
	app.bgCancel()
	app.loops.Wait()
	app.mu.RLock()
	open := make([]*Session, 0, len(app.sessions))
	for _, s := range app.sessions {
		open = append(open, s)
	}
	app.mu.RUnlock()
	var err error
	for _, s := range open {
		if cerr := s.Close(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}
	if werr := app.releases.wait(ctx); werr != nil && err == nil {
		err = werr
	}
	// Ownership is released first, then the execution side, then the
	// Writers (RUN-EXE-8, SPN-4). Records keep their leases until they
	// expire and the next incarnation adopts them. The effect port and the
	// deployment components around it close at the outer root, after this.
	if oerr := app.Owner.Close(ctx); oerr != nil && err == nil {
		err = oerr
	}
	if app.Execution != nil {
		app.Execution.Close()
	}
	if app.Kernel != nil {
		if cerr := app.Kernel.Close(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// Event is one item of a Session's event stream (OBS-1).
type Event = observe.Event

// ForkRequest forks a Session at one commit of its ledger (OWN-FRK-1).
type ForkRequest = sessionkernel.ForkRequest
