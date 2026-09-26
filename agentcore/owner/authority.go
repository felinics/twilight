// Package authority composes the agent core -- the fact layer (Store,
// Writers, SessionRunStore, Coordinator), the decision layer (preset registry and
// prompt builder catalog) and the effect layer (an Executor port) -- into
// one Owner process (AUTH). Its exported fields are the core services a
// caller drives a Session with; Open hands out the ownership capability
// those services act under. It is deployment-neutral and carries no product
// policy: what to send, when to drain a backlog, whether to drive in the
// background and when to compact are the application's decisions.
package owner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
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

// Ports are the roles an Owner is composed from (OWN-PRT-1). Every field
// is an interface or a core value. The Store, the Executor, the Content
// store and both Artifacts are required and durable (OWN-PRT-3); the other
// nil fields take the in-process defaults documented on each.
type Ports struct {
	// Store is the Session kernel (required).
	Store session.Store
	// Content is the cas ContentStore the frozen bodies live in under
	// runmod.FrozenAuthority (RUN-WIR-4). The run store writes them; the
	// materializer reads them for prompts, replies and transcripts.
	// Required.
	Content artifact.ContentStore
	// Artifacts are the binding store and retention ledger (required).
	Artifacts Artifacts
	// Presets is the registry of decision identities; nil selects an
	// in-memory registry.
	Presets preset.Registry
	// Decisions resolve each preset's PromptBuilderRef (DEC-CAT): required.
	// The core ships no builder; the agent built on it supplies its catalog
	// (agent/prompt.DefaultPromptBuilders for the reference agent).
	Decisions *decision.PromptBuilders
	// MissingEffects is the takeover policy for an Executing effect the
	// Executor holds nothing for (RUN-CMT-7): the zero value disposes it,
	// reconcile.RedispatchMissing hands it to the Executor again within a
	// budget (RUN-EXE-15) and requires Processes.
	MissingEffects reconcile.MissingPolicy
	// Processes is the dispatch ledger RedispatchMissing writes; durable
	// like every store (OWN-PRT-3). Unused under DisposeMissing.
	Processes process.Store
	// Executor is the effect layer port (RUN-EXE-3): required.
	Executor effect.ExecutionPort
	// TargetResolver supplies the opaque resource target of each effect
	// (RUN-LOP-9). It belongs to the application's resource layer; nil
	// gives every effect no target (APP-TGT-1).
	TargetResolver loop.TargetResolver
	// Observers are notified of every group the Writers apply (EXT-WRT-7).
	Observers []writer.CommitObserver
	// Modules are application modules registered after the first-party four
	// (EXT-APP).
	Modules []extension.ModuleDescriptor
	// Clock stamps event times; nil selects time.Now.
	Clock func() time.Time
	// Cache stores folded projection states; nil asks the Store for a durable
	// cache and falls back to an in-memory one (APP-MEM-2).
	Cache extension.ProjectionCache
	// CacheEvery bounds how far a cached projection may fall behind the head;
	// zero takes extension.DefaultCacheEvery (EXT-PRJ-7).
	CacheEvery session.CommitSeq
	// Ownership configures how Writers open Sessions.
	Ownership session.OpenOptions
	// Fail receives failures of work the Owner does outside any caller's
	// call, such as settling a reattached Outcome; nil discards them.
	Fail func(session.SessionID, error)
}

// Owner is the composed core (OWN-PRT-2). Exported fields are the ports
// and core services; none is a product facade.
type Owner struct {
	Store     session.Store
	Writers   writer.Writers
	Registry  *extension.Registry
	Admission writer.Admission
	// Runs is the Run module's Session adapter: the Run core's store bound
	// per Writer, Run reads by SessionID and the Run Parts of Turn units.
	Runs *runmod.SessionRunStore
	// Turns commits the Turn protocol and reads Turn status.
	Turns    *turn.Coordinator
	Driver   *driver.Driver
	Presets  preset.Registry
	Executor effect.ExecutionPort
	Frozen   frozen.Store
	// Projections reads every projection through the Session's Writer.
	Projections extension.ProjectionReader
	// Content materializes the frozen bodies projections name (CHT-MAT-1).
	Content chatlog.ContentResolver
	// Chatlog commits the chatlog's own facts (APP-INP-1, APP-CKP-1).
	Chatlog *chatlog.Commands
	// History answers fork-boundary questions (OWN-FRK-2, SPN-5).
	History turn.History
	Clock   func() time.Time

	mu   sync.Mutex
	open map[session.SessionID]*openSession
}

// New composes an Owner from its ports (OWN-PRT-1).
func New(p Ports) (*Owner, error) { //nolint:gocritic // hugeParam: Ports is a by-value options struct read once
	if p.Executor == nil {
		return nil, errors.New("owner: an Executor port is required")
	}
	if p.Store == nil {
		return nil, errors.New("owner: a session Store is required")
	}
	if p.Content == nil {
		return nil, errors.New("owner: a content Store is required (OWN-PRT-3)")
	}
	if p.Artifacts.Bindings == nil || p.Artifacts.Ledger == nil {
		return nil, errors.New("owner: a binding store and a retention ledger are required (OWN-PRT-3)")
	}
	if p.MissingEffects == reconcile.RedispatchMissing && p.Processes == nil {
		return nil, errors.New("owner: MissingEffects=redispatch requires a dispatch ledger (Ports.Processes, RUN-EXE-15)")
	}
	store := p.Store
	// The first-party four are trusted core; Ports.Modules are extensions
	// and cannot declare authoritative projections (EXT-PRJ-9).
	registry, err := extension.BuildRegistryWithExtensions(
		[]extension.ModuleDescriptor{chatlog.Module, runmod.Module, attempt.Module, turn.Module}, p.Modules)
	if err != nil {
		return nil, err
	}
	bindings, ledger := p.Artifacts.Bindings, p.Artifacts.Ledger
	// A projection cache lets a reopened Session start folding instead of
	// refolding the whole log (EXT-PRJ-3). An adapter that can store entries
	// durably provides its own; otherwise they live as long as the process.
	cache := p.Cache
	if cache == nil {
		if provider, ok := store.(extension.ProjectionCacheProvider); ok {
			cache = provider.ProjectionCache()
		} else {
			cache = extension.NewMemoryProjectionCache()
		}
	}
	// Frozen bodies live in the content store and are admitted through the
	// same binding store the Writers resolve against (RUN-WIR-4).
	fz, err := runmod.FrozenValues(p.Content, bindings)
	if err != nil {
		return nil, err
	}
	now := p.Clock
	if now == nil {
		now = time.Now
	}
	admission := writer.Admission{Bindings: bindings, Ledger: ledger}
	writers := writer.NewWriters(store, registry, admission, p.Ownership,
		writer.WritersConfig{Cache: cache, CachePolicy: runmod.WriterCachePolicy(p.CacheEvery), Observers: p.Observers})
	runs, err := runmod.NewSessionRunStore(runmod.Config{Registry: registry, Store: store, Frozen: fz, Cache: cache, Now: now})
	if err != nil {
		return nil, err
	}
	presets := p.Presets
	if presets == nil {
		presets = preset.NewMemory()
	}
	decisions := p.Decisions
	if decisions == nil {
		return nil, errors.New("owner: a prompt builder catalog is required (Ports.Decisions); the core ships no default")
	}
	// The read model folds from the Store through the cache: reading a Session
	// takes no ownership (OWN-HDL-2). The Writer keeps its own transactional
	// projections for the commit critical section.
	projections := extension.NewProjectionReader(store, registry, cache)
	content := runmod.NewContent(fz)
	a := &Owner{
		Store: store, Writers: writers, Registry: registry, Admission: admission, Runs: runs,
		Turns:   &turn.Coordinator{Projections: projections, Runs: runs, Now: now},
		Presets: presets, Executor: p.Executor, Frozen: fz, Projections: projections, Content: content,
		Chatlog: &chatlog.Commands{Now: now},
		History: turn.History{Store: store, Registry: registry, Projections: projections},
		Clock:   now,
		open:    make(map[session.SessionID]*openSession),
	}
	a.Driver = driver.New()
	a.Driver.Runs, a.Driver.Turns, a.Driver.Executor = runs, a.Turns, p.Executor
	// A nil resolver gives every effect no target (APP-TGT-1).
	a.Driver.Presets, a.Driver.Decisions, a.Driver.Targets = presets, decisions, p.TargetResolver
	a.Driver.Sources = decision.Sources{Projections: projections, Content: content}
	a.Driver.Fail = p.Fail
	a.Driver.MissingEffects, a.Driver.Processes = p.MissingEffects, p.Processes
	return a, nil
}

// Close releases every generation this Owner holds -- recovery
// listeners, then Writers -- and every outstanding Handle is stale
// afterwards. Generations still opening or closing on another goroutine
// finish their own release.
func (a *Owner) Close(ctx context.Context) error {
	a.mu.Lock()
	var owned []session.SessionID
	for sid, gen := range a.open {
		if gen.state == open {
			gen.state = closing
			owned = append(owned, sid)
		}
	}
	a.mu.Unlock()
	a.Driver.Close()
	err := writer.CloseWriters(ctx, a.Writers)
	a.mu.Lock()
	for _, sid := range owned {
		delete(a.open, sid)
	}
	a.mu.Unlock()
	return err
}

// --- session lifecycle -------------------------------------------------------------

// CreateSession creates the Session; ext are the segment's module extension
// slots (nil for none), carried opaquely by the kernel (SES-WIR-5).
func (a *Owner) CreateSession(ctx context.Context, sid session.SessionID, ext session.Extensions) error {
	_, err := a.Store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: a.Clock().UnixMilli(), Ext: ext})
	return err
}

// EnsureSession makes sure the Session exists, whatever record created it:
// a root made here, a fork or a spawned child all count. Create alone would
// refuse a Session whose segment fields differ (SES-CRT-1), so existence is
// probed first.
func (a *Owner) EnsureSession(ctx context.Context, sid session.SessionID) error {
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
	At     session.CommitSeq
	Child  session.SessionID
	// Ext are the child segment's module extension slots (SES-WIR-5).
	Ext session.Extensions
}

// Fork creates the child Session (SES-FRK-1) and claims the artifacts its
// inherited prefix references (EXT-WRT-8). The child is not opened.
func (a *Owner) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("owner: fork requires parent and child session ids")
	}
	if req.Parent == req.Child {
		return session.SegmentHeader{}, errors.New("owner: a session cannot fork itself")
	}
	// A fork point inside a Turn would hand the child a Turn whose Run is
	// the parent's execution (SES-FRK-5): semantic history branches only at
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
func (a *Owner) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
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

// DeleteSession drops a Session's root (OWN-FRK-3, SES-GC-1); the claims of
// its commits go with their segments at Collect. A Session this Owner
// holds open is closed first; one owned by another process is ErrOwned.
func (a *Owner) DeleteSession(ctx context.Context, sid session.SessionID) error {
	if gen := a.beginClose(sid, nil); gen != nil {
		if err := a.release(ctx, sid, gen, true); err != nil {
			return err
		}
	} else {
		a.mu.Lock()
		_, transition := a.open[sid]
		a.mu.Unlock()
		if transition {
			return fmt.Errorf("%w: %s is opening or closing", ErrSessionOpen, sid)
		}
		if err := writer.CloseWriter(ctx, a.Writers, sid); err != nil {
			return err
		}
	}
	return writer.Delete(ctx, a.Store, sid)
}

// Collect reclaims the storage of deleted Sessions no live Session reaches
// (SES-GC-2) and releases the claims of the commits it reclaimed (SES-GC-3).
func (a *Owner) Collect(ctx context.Context) (session.CollectReport, error) {
	return writer.Collect(ctx, a.Store, a.Admission)
}

// --- reads by SessionID ----------------------------------------------------------------

// Reading a Session needs no ownership: projections are queried by identity.
// Commands take the Writer of an open Handle (APP-SES-1).

// Projection reads any registered projection through the Session's Writer
// (APP-MEM-1).
func (a *Owner) Projection(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	return a.Projections.Load(ctx, sid, id, v)
}
