package app

import (
	"context"
	"errors"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/lifecycle"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Artifacts groups the artifact ports: the binding index the Writers
// resolve against and the retention ledger their claims live in. Both are
// required and durable; the facts of a Session name frozen bodies through
// them, so they must survive every restart the facts survive.
type Artifacts struct {
	Bindings artifact.BindingStore
	Ledger   artifact.RetentionLedger
}

// SessionPorts are the durable stores and the policies the application
// composes its Session-side services from. The Store and the Content store
// are required; the other nil fields take the documented defaults.
type SessionPorts struct {
	// Store is the Session store (required).
	Store session.Stores
	// Content is the cas ContentStore the frozen bodies live in under
	// sessionstore.FrozenAuthority. The run store writes them; the
	// materializer reads them for prompts, replies and transcripts.
	// Required.
	Content artifact.ContentStore
	// Artifacts are the binding store and retention ledger (required).
	Artifacts Artifacts
	// Observers are notified of every group the Writers apply besides the
	// application's own event stream.
	Observers []writer.CommitObserver
	// Registry is the module registry every Writer, projection and event
	// stream decodes through. When nil, New builds one from the first-party
	// modules and Modules; a host that needs the Registry before the
	// application exists builds it with NewRegistry and passes it here.
	Registry *module.Registry
	// Modules are application modules registered after the first-party
	// ones; ignored when Registry is given.
	Modules []module.ModuleDescriptor
	// Clock stamps event times; nil selects time.Now.
	Clock func() time.Time
	// Cache stores folded projection states; nil asks the Store for a durable
	// cache and falls back to an in-memory one.
	Cache session.ProjectionCache
	// CacheEvery bounds how far a cached projection may fall behind the head;
	// zero takes module.DefaultCacheEvery.
	CacheEvery ledger.CommitSeq
	// Ownership configures how Writers open Sessions.
	Ownership session.OpenOptions
}

// NewRegistry builds the module registry: the first-party modules as
// trusted core, extensions after them. Extensions cannot declare
// authoritative projections.
func NewRegistry(extensions []module.ModuleDescriptor) (*module.Registry, error) {
	return module.BuildRegistryWithExtensions(
		[]module.ModuleDescriptor{chatlog.Module, sessionstore.Module, turn.Module}, extensions)
}

// sessionServices are the Session-side services composed over one Store:
// what every command commits through and every read folds through.
type sessionServices struct {
	Store    session.Stores
	Writers  writer.Writers
	Registry *module.Registry
	// Runs is the Run module's Session adapter: the Run core's store bound
	// per Writer, Run reads by SessionID and the Run Parts of Turn units.
	Runs *sessionstore.SessionRunStore
	// Turns commits the Turn protocol and reads Turn status.
	Turns *turn.Coordinator
	// Bus is the committed event stream, decoded, in commit order. It
	// carries facts only; transient observations are not part of it.
	Bus    *observe.Bus
	Frozen frozen.Store
	// Projections reads every projection without ownership.
	Projections session.ProjectionReader
	// Content materializes the frozen bodies projections name.
	Content chatlog.ContentResolver
	// Chatlog commits the chatlog's own facts.
	Chatlog *chatlog.Commands
	// Lifecycle creates, forks and reclaims Sessions over the Store.
	Lifecycle lifecycle.Lifecycle
	Clock     func() time.Time
}

// composeSessions fills the Session-side services from their ports.
func (app *Application) composeSessions(p SessionPorts) error { //nolint:gocritic // hugeParam: SessionPorts is a by-value options struct read once
	if p.Store == nil {
		return errors.New("app: a session Store is required")
	}
	if p.Content == nil {
		return errors.New("app: a content Store is required")
	}
	if p.Artifacts.Bindings == nil || p.Artifacts.Ledger == nil {
		return errors.New("app: a binding store and a retention ledger are required")
	}
	store := p.Store
	registry := p.Registry
	if registry == nil {
		var err error
		if registry, err = NewRegistry(p.Modules); err != nil {
			return err
		}
	}
	// The event stream observes every group the Writers apply and decodes
	// through the Registry, so both exist before the Writers do.
	bus := observe.NewBus(registry, store)
	observers := append([]writer.CommitObserver{bus}, p.Observers...)
	bindings, retention := p.Artifacts.Bindings, p.Artifacts.Ledger
	// A projection cache lets a reopened Session start folding instead of
	// refolding the whole log. An adapter that can store entries durably
	// provides its own; otherwise they live as long as the process.
	cache := p.Cache
	if cache == nil {
		if provider, ok := store.(session.ProjectionCacheProvider); ok {
			cache = provider.ProjectionCache()
		} else {
			cache = session.NewMemoryProjectionCache()
		}
	}
	// Frozen bodies live in the content store and are admitted through the
	// same binding store the Writers resolve against.
	fz, err := sessionstore.FrozenValues(p.Content, bindings)
	if err != nil {
		return err
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
		return err
	}
	// The read model folds from the Store through the cache: reading a
	// Session takes no ownership. The Writer keeps its own transactional
	// projections for the commit critical section.
	projections := session.NewProjectionReader(store, registry, cache)
	app.svc = sessionServices{
		Store: store, Writers: writers, Registry: registry, Runs: runs,
		Turns:       &turn.Coordinator{Projections: projections, Runs: runs, Now: now},
		Bus:         bus,
		Frozen:      fz,
		Projections: projections,
		Content:     sessionstore.NewContent(fz),
		Chatlog:     &chatlog.Commands{Now: now},
		Lifecycle:   lifecycle.Lifecycle{Store: store, Registry: registry, Admission: admission, Clock: now},
		Clock:       now,
	}
	app.history = turn.History{Store: store, Registry: registry, Projections: projections}
	return nil
}

// closeSessions ends the Writers: the lease-free readers of other processes
// keep working; the execution side closes before this.
func (app *Application) closeSessions(ctx context.Context) error {
	if app.svc.Writers == nil {
		return nil
	}
	return writer.CloseWriters(ctx, app.svc.Writers)
}
