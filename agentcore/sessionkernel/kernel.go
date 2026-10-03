// Package sessionkernel is the Session-side semantic mechanism: the durable
// fact layer one Session's meaning lives in -- the Store and its Writers,
// the module Registry and the projections folded from it, the Run module's
// Session adapter, the Turn protocol's committer, the chatlog's commands,
// the content store behind the frozen bodies and the fork/collect lifecycle
// over the Store. These components exist to keep Session durable semantic
// state: they read it, write it atomically and project it. Everything that
// advances an agent from that state -- the drive chain, the settlement
// subscription, the execution port and the decision identities -- lives
// outside, in the runtime's Execution; which process hosts which side is a
// deployment choice.
package sessionkernel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/history"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Artifacts groups the artifact ports (OWN-PRT-3): the binding index the
// Writers resolve against and the retention ledger their claims live in.
// Both are required and durable; the facts of a Session name frozen bodies
// through them, so they must survive every restart the facts survive. There
// is no memory implementation of either.
type Artifacts struct {
	Bindings artifact.BindingStore
	Ledger   artifact.RetentionLedger
}

// Ports are the roles a Kernel is composed from. Every field is an interface
// or a core value. The Store and the Content store are required and durable
// (OWN-PRT-3); the other nil fields take the documented defaults.
type Ports struct {
	// Store is the Session kernel (required).
	Store session.Stores
	// Content is the cas ContentStore the frozen bodies live in under
	// sessionstore.FrozenAuthority (RUN-WIR-4). The run store writes them;
	// the materializer reads them for prompts, replies and transcripts.
	// Required.
	Content artifact.ContentStore
	// Artifacts are the binding store and retention ledger (required).
	Artifacts Artifacts
	// Observers are notified of every group the Writers apply (EXT-WRT-7)
	// besides the Kernel's own event stream.
	Observers []writer.CommitObserver
	// Registry is the module registry every Writer, projection and event
	// stream of this Kernel decodes through. When nil, New builds one from
	// the first-party modules and Modules; a host that needs the Registry
	// before the Kernel exists builds it with NewRegistry and passes it here.
	Registry *module.Registry
	// Modules are application modules registered after the first-party
	// ones (EXT-APP); ignored when Registry is given.
	Modules []module.ModuleDescriptor
	// Clock stamps event times; nil selects time.Now.
	Clock func() time.Time
	// Cache stores folded projection states; nil asks the Store for a durable
	// cache and falls back to an in-memory one (APP-MEM-2).
	Cache session.ProjectionCache
	// CacheEvery bounds how far a cached projection may fall behind the head;
	// zero takes module.DefaultCacheEvery (EXT-PRJ-7).
	CacheEvery ledger.CommitSeq
	// Ownership configures how Writers open Sessions.
	Ownership session.OpenOptions
}

// Kernel is the assembled Session-side semantic mechanism. Exported fields
// are the services a host drives Sessions with and reads them through; none
// is a product facade.
type Kernel struct {
	Store     session.Stores
	Writers   writer.Writers
	Registry  *module.Registry
	Admission writer.Admission
	// Runs is the Run module's Session adapter: the Run core's store bound
	// per Writer, Run reads by SessionID and the Run Parts of Turn units.
	Runs *sessionstore.SessionRunStore
	// Turns commits the Turn protocol and reads Turn status.
	Turns *Coordinator
	// Bus is the committed event stream: a CommitObserver on the Writers
	// (OBS-1), carrying the applied groups decoded, in commit order. It
	// carries facts only; transient observations are not part of it.
	Bus    *observe.Bus
	Frozen frozen.Store
	// Projections reads every projection through the Session's Writer.
	Projections session.ProjectionReader
	// Content materializes the frozen bodies projections name (CHT-MAT-1).
	Content chatlog.ContentResolver
	// Chatlog commits the chatlog's own facts (APP-INP-1, APP-CKP-1).
	Chatlog *chatlog.Commands
	// History answers fork-boundary questions.
	History history.History
	Clock   func() time.Time
}

// New composes a Kernel from its ports.
func New(p Ports) (*Kernel, error) { //nolint:gocritic // hugeParam: Ports is a by-value options struct read once
	if p.Store == nil {
		return nil, errors.New("sessionkernel: a session Store is required")
	}
	if p.Content == nil {
		return nil, errors.New("sessionkernel: a content Store is required (OWN-PRT-3)")
	}
	if p.Artifacts.Bindings == nil || p.Artifacts.Ledger == nil {
		return nil, errors.New("sessionkernel: a binding store and a retention ledger are required (OWN-PRT-3)")
	}
	store := p.Store
	registry := p.Registry
	if registry == nil {
		var err error
		if registry, err = NewRegistry(p.Modules); err != nil {
			return nil, err
		}
	}
	// The event stream observes every group the Writers apply (EXT-WRT-7)
	// and decodes through the Registry, so both exist before the Writers do.
	bus := observe.NewBus(registry, store)
	observers := append([]writer.CommitObserver{bus}, p.Observers...)
	bindings, retention := p.Artifacts.Bindings, p.Artifacts.Ledger
	// A projection cache lets a reopened Session start folding instead of
	// refolding the whole log (EXT-PRJ-3). An adapter that can store entries
	// durably provides its own; otherwise they live as long as the process.
	cache := p.Cache
	if cache == nil {
		if provider, ok := store.(session.ProjectionCacheProvider); ok {
			cache = provider.ProjectionCache()
		} else {
			cache = session.NewMemoryProjectionCache()
		}
	}
	// Frozen bodies live in the content store and are admitted through the
	// same binding store the Writers resolve against (RUN-WIR-4).
	fz, err := sessionstore.FrozenValues(p.Content, bindings)
	if err != nil {
		return nil, err
	}
	now := p.Clock
	if now == nil {
		now = time.Now
	}
	admission := writer.Admission{Bindings: bindings, Ledger: retention}
	writers := writer.NewWriters(store, registry, admission, p.Ownership,
		writer.WritersConfig{Cache: cache, CachePolicy: sessionstore.WriterCachePolicy(p.CacheEvery), Observers: observers})
	runs, err := sessionstore.NewSessionRunStore(sessionstore.Config{Registry: registry, Store: store, Frozen: fz, Cache: cache, Now: now})
	if err != nil {
		return nil, err
	}
	// The read model folds from the Store through the cache: reading a Session
	// takes no ownership (OWN-HDL-2). The Writer keeps its own transactional
	// projections for the commit critical section.
	projections := session.NewProjectionReader(store, registry, cache)
	content := sessionstore.NewContent(fz)
	return &Kernel{
		Store: store, Writers: writers, Registry: registry, Admission: admission, Runs: runs,
		Turns: &Coordinator{Projections: projections, Runs: runs, Now: now},
		Bus:   bus, Frozen: fz, Projections: projections, Content: content,
		Chatlog: &chatlog.Commands{Now: now},
		History: history.History{Store: store, Registry: registry, Projections: projections},
		Clock:   now,
	}, nil
}

// NewRegistry builds the module registry of a Kernel: the first-party
// modules as trusted core, extensions after them. Extensions cannot declare
// authoritative projections.
func NewRegistry(extensions []module.ModuleDescriptor) (*module.Registry, error) {
	return module.BuildRegistryWithExtensions(
		[]module.ModuleDescriptor{chatlog.Module, sessionstore.Module, turn.Module}, extensions)
}

// Close ends the Writers: the lease-free readers of other processes keep
// working; the execution side of a host closes before this.
func (a *Kernel) Close(ctx context.Context) error {
	return writer.CloseWriters(ctx, a.Writers)
}

// CreateSession creates the Session; ext are the segment's module extension
// slots (nil for none), carried opaquely by the kernel.
func (a *Kernel) CreateSession(ctx context.Context, sid session.SessionID, ext module.Extensions) error {
	_, err := a.Store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: a.Clock().UnixMilli(), Ext: ext})
	return err
}

// EnsureSession makes sure the Session exists, whatever record created it:
// a root made here, a fork or a spawned child all count. Create alone would
// refuse a Session whose segment fields differ, so existence is
// probed first.
func (a *Kernel) EnsureSession(ctx context.Context, sid session.SessionID) error {
	if _, err := a.Store.Header(ctx, sid); err == nil {
		return nil
	} else if !session.IsCode(err, session.ErrNotFound) {
		return err
	}
	if err := a.CreateSession(ctx, sid, nil); err != nil {
		// A concurrent creator winning the race is still "exists".
		if _, herr := a.Store.Header(ctx, sid); herr == nil {
			return nil
		}
		return err
	}
	return nil
}

// ForkRequest forks a Session at one commit of its ledger (OWN-FRK-1): the
// child inherits every commit of Parent up to and including At and continues
// from there under its own identity.
type ForkRequest struct {
	Parent session.SessionID
	At     ledger.CommitSeq
	Child  session.SessionID
	// Ext are the child segment's module extension slots.
	Ext module.Extensions
}

// Fork creates the child Session  and claims the artifacts its
// inherited prefix references (EXT-WRT-8). The child is not opened.
func (a *Kernel) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("sessionkernel: fork requires parent and child session ids")
	}
	if req.Parent == req.Child {
		return session.SegmentHeader{}, errors.New("sessionkernel: a session cannot fork itself")
	}
	// A fork point inside a Turn would hand the child a Turn whose Run is
	// the parent's execution: semantic history branches only at
	// quiescent points (OWN-FRK-1).
	if active, ok, err := a.History.ActiveAt(ctx, req.Parent, req.At); err != nil {
		return session.SegmentHeader{}, err
	} else if ok {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: req.Child,
			Detail: fmt.Sprintf("turn %s of %s is active at commit %d; fork at a quiescent point", active, req.Parent, req.At)}
	}
	return writer.Fork(ctx, a.Store, a.Registry, writer.ForkRequest{
		Parent: req.Parent, At: req.At, Child: req.Child, CreatedAtUnixMilli: a.Clock().UnixMilli(), Ext: req.Ext,
	})
}

// ForkBeforeTurn forks Parent at the commit just before turnID started
// (OWN-FRK-2): the child holds the conversation as it was when that Turn's
// inputs were still submitted and undelivered.
func (a *Kernel) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
	seq, err := a.History.StartCommit(ctx, parent, turnID)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	if seq == 0 {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: child,
			Detail: fmt.Sprintf("turn %s started in the first commit of %s; there is no prefix to fork", turnID, parent)}
	}
	return a.Fork(ctx, ForkRequest{Parent: parent, At: seq - 1, Child: child})
}

// Collect reclaims the storage of deleted Sessions no live Session reaches
//
//	and releases the claims of the commits it reclaimed.
func (a *Kernel) Collect(ctx context.Context) (session.CollectReport, error) {
	return writer.Collect(ctx, a.Store, a.Admission)
}

// Projection reads any registered projection through the Session's Writer
// (APP-MEM-1). Reading a Session needs no ownership.
func (a *Kernel) Projection(ctx context.Context, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion) (any, ledger.Head, error) {
	return a.Projections.Load(ctx, sid, id, v)
}
