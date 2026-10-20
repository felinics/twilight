// Package writer is the single in-process write entry of one Session
// A commit is a pipeline of stages, one per file: encoding and
// stream affinity (encode.go), artifact admission and retention claims
// (admission.go), projection folding and cache (projector.go), observer
// fan-out (observers.go). The Writer itself reads the head, evaluates the
// caller's decision against one transactional View, and appends under the
// kernel's ownership fence and CommitID index.
package writer

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"sync"
)

// TypedEvent is a module value plus its event metadata. The payload is
// encoded and validated against the Registry at commit time.
type TypedEvent struct {
	Type                ledger.EventType
	RecordedAtUnixMilli int64
	Value               any
}

// TypedBatch is the caller's view of one EventBatch: the events of one
// logical stream inside one commit. A commit carries at most one batch per
// stream and may span several streams.
type TypedBatch struct {
	Domain ledger.Domain
	Events []TypedEvent
}

// SemanticGroup is what a CommitFn decides: the commit identity and the
// per-stream batches it carries.
type SemanticGroup struct {
	CommitID ledger.CommitID
	Batches  []TypedBatch
}

// View is what a CommitFn may read: head, idempotency index and projections
// folded to the current head.
type View interface {
	Head() ledger.Head
	Epoch() ledger.Epoch
	// Header is the tip segment's header: the segment this Writer appends to.
	Header() session.SegmentHeader
	// Committed reports whether a commit is already in the ledger, from an
	// index the kernel keeps. An index read failure returns the error; it is
	// not reported as "not committed".
	Committed(ledger.CommitID) (bool, error)
	// LookupCommit returns the stored commit, reading it when the kernel
	// handle does not hold it.
	LookupCommit(ledger.CommitID) (ledger.Commit, bool, error)
	// StreamHead reports whether this Session has written to a logical
	// stream and the StreamSeq its next event takes.
	StreamHead(ledger.Domain) (ledger.StreamSeq, bool)
	// Projection returns a detached state that the caller owns.
	Projection(module.ProjectionID, module.ProjectionVersion) (any, error)
}

// CommitFn decides the commit to write; nil means write nothing.
type CommitFn func(View) (*SemanticGroup, error)

type CommitOutcome string

const (
	CommitApplied        CommitOutcome = "applied"
	CommitAlreadyApplied CommitOutcome = "already_applied" // the CommitID is in the ledger
	CommitConflict       CommitOutcome = "conflict"        // the kernel refused the commit as a conflict
	CommitInvalid        CommitOutcome = "invalid"
	CommitNoop           CommitOutcome = "noop"
)

// CommitResult is the outcome of a Commit. Outcome carries the semantic
// result and error is reserved for an infrastructure failure: CommitInvalid
// and CommitConflict are answers, reported with a nil error. Commit is the
// stored commit for applied and already_applied, zero otherwise.
type CommitResult struct {
	Outcome CommitOutcome
	Commit  ledger.Commit
	Claim   *artifact.RetentionClaim
	Detail  string
}

// Writer is the single in-process write entry of one Session.
type Writer interface {
	SessionID() session.SessionID
	Epoch() ledger.Epoch
	Header() session.SegmentHeader
	Commit(context.Context, CommitFn) (CommitResult, error)
	Projections() session.ProjectionReader
	// OwnerExists reports whether the owner names a commit of this ledger.
	OwnerExists(context.Context, artifact.ClaimOwner) (bool, error)
	Close(context.Context) error
}

// WritersConfig carries the projection cache and observers. Every field is
// optional: with no cache the Writer folds every registered projection from
// the beginning of the log and stores nothing; with no observers nothing is
// notified.
type WritersConfig struct {
	// Cache holds folded projection states, so a reopening Writer starts from
	// one instead of refolding the whole log.
	Cache session.ProjectionCache
	// CachePolicy decides which projections the Writer refreshes and when; nil
	// means module.CacheEvery(module.DefaultCacheEvery). It never affects reading: an entry
	// the cache already holds is used whoever wrote it.
	CachePolicy module.CachePolicy
	// Observers are notified of every applied commit.
	Observers []CommitObserver
}

// sessionWriter is the Writer: the kernel handle (head, fence, CommitID and
// stream index) plus the stages composed onto its commit pipeline.
type sessionWriter struct {
	mu       sync.Mutex
	kernel   session.Handle
	store    session.Store
	registry *module.Registry
	lost     error

	projections *projector
	admission   *admitter
	observers   *observers
}

// Bind builds local write state over an already acquired handle. The caller
// owns its lifetime, including renewal and release, even if Bind fails.
func (f *Factory) Bind(ctx context.Context, kernel session.Handle) (Writer, error) {
	if f == nil || f.Store == nil || f.Registry == nil || kernel == nil {
		return nil, errors.New("writer: nil factory store, registry or handle")
	}
	store, registry, admission, cfg := f.Store, f.Registry, f.Admission, f.Config
	sid := kernel.SessionID()
	header := kernel.Header()
	w := &sessionWriter{kernel: kernel, store: store, registry: registry,
		projections: newProjector(registry, sid, cfg.Cache, cfg.CachePolicy),
		admission:   &admitter{Admission: admission, segment: header.ID},
		observers:   &observers{list: cfg.Observers}}
	// Projections commonly share a checkpoint boundary after Close. Read each
	// distinct position once during this Bind; every entry still validates its
	// own boundary and codec. Never retain these reads across acquisitions.
	type boundaryRead struct {
		commit ledger.Commit
		ok     bool
	}
	boundaries := make(map[ledger.CommitSeq]boundaryRead)
	at := func(seq ledger.CommitSeq) (ledger.Commit, bool) {
		if cached, ok := boundaries[seq]; ok {
			return cached.commit, cached.ok
		}
		one, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: seq, Limit: 1})
		if err != nil {
			return ledger.Commit{}, false
		}
		var result boundaryRead
		if len(one.Commits) == 1 {
			result = boundaryRead{commit: one.Commits[0], ok: true}
		}
		boundaries[seq] = result
		return result.commit, result.ok
	}
	from, err := w.projections.prepare(ctx, header, at)
	if err != nil {
		return nil, err
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: from})
	if err != nil {
		return nil, err
	}
	if err := w.projections.resume(&page, from); err != nil {
		return nil, err
	}
	if err := w.admission.reconcile(ctx, w); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *sessionWriter) SessionID() session.SessionID { return w.kernel.SessionID() }
func (w *sessionWriter) Epoch() ledger.Epoch          { return w.kernel.Epoch() }

// Header is the tip segment's creation record, read from the kernel handle.
// Inside a CommitFn the View answers the same value.
func (w *sessionWriter) Header() session.SegmentHeader { return w.kernel.Header() }

func (w *sessionWriter) OwnerExists(_ context.Context, owner artifact.ClaimOwner) (bool, error) {
	if owner.Kind != ClaimOwnerKind || owner.Authority != string(w.kernel.Header().ID) {
		return false, &artifact.Error{Code: artifact.ErrInvalid, Operation: "owner_exists", Detail: "owner is not a commit of this writer's tip segment"}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lost != nil {
		return false, w.lost
	}
	return w.kernel.Committed(ledger.CommitID(owner.Identity))
}

// LookupCommit reads authoritative stored facts through the acquired handle's
// commit index. It is an optional read capability; it never evaluates a
// command, appends, or returns a retained local CommitResult.
func (w *sessionWriter) LookupCommit(id ledger.CommitID) (ledger.Commit, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lost != nil {
		return ledger.Commit{}, false, w.lost
	}
	return w.kernel.LookupCommit(id)
}

// errWriterClosed is the failure a closed Writer keeps returning.
var errWriterClosed = &ledger.Error{Code: ledger.CodeInvalid, Detail: "writer closed"}

// Close flushes local caches without renewing or releasing the caller's handle.
func (w *sessionWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	if errors.Is(w.lost, errWriterClosed) {
		w.mu.Unlock()
		return nil
	}
	var writes []cacheWrite
	// An entry at an inherited boundary is never started.
	head := w.kernel.Head()
	if ledger.OwnBoundary(w.kernel.Header().Seed(), head) {
		writes = w.projections.planRefresh(head, true)
	}
	w.lost = errWriterClosed
	w.mu.Unlock()
	w.projections.saveRefresh(ctx, writes)
	return nil
}

// --- view --------------------------------------------------------------------------

type view struct{ w *sessionWriter }

// Head and Header come from the kernel handle. The view does not lock the
// Writer: Commit already holds that lock, and locking it again would deadlock.
func (v view) Head() ledger.Head             { return v.w.kernel.Head() }
func (v view) Epoch() ledger.Epoch           { return v.w.kernel.Epoch() }
func (v view) Header() session.SegmentHeader { return v.w.kernel.Header() }

// Committed and LookupCommit are answered by the kernel, which holds the
// CommitID index Append needs.
func (v view) Committed(id ledger.CommitID) (bool, error) { return v.w.kernel.Committed(id) }

func (v view) LookupCommit(id ledger.CommitID) (ledger.Commit, bool, error) {
	return v.w.kernel.LookupCommit(id)
}

func (v view) StreamHead(stream ledger.Domain) (ledger.StreamSeq, bool) {
	return v.w.kernel.StreamHead(stream)
}

func (v view) Projection(id module.ProjectionID, ver module.ProjectionVersion) (any, error) {
	return v.w.projections.detached(id, ver)
}

func (w *sessionWriter) Projections() session.ProjectionReader { return memoryReader{w} }

// --- commit --------------------------------------------------------------------------

// Commit is the pipeline: read (View) -> decide (fn) -> encode -> CommitID
// replay check -> projection pre-fold -> retention claim -> Append -> advance
// projections -> refresh cache / notify observers. Cache writes
// and observer notifications run after the Writer's lock is released
func (w *sessionWriter) Commit(ctx context.Context, fn CommitFn) (CommitResult, error) {
	if fn == nil {
		return CommitResult{}, errors.New("writer: nil fn")
	}
	w.mu.Lock()
	var writes []cacheWrite
	var applied *ledger.Commit
	defer func() {
		// notifyMu is taken before mu is released, so notifications keep the
		// commit order while a later Commit already runs.
		if applied != nil && w.observers.any() {
			w.observers.hold()
			w.mu.Unlock()
			w.projections.saveRefresh(ctx, writes)
			w.observers.notify(ctx, w.kernel.SessionID(), *applied)
			w.observers.release()
			return
		}
		w.mu.Unlock()
		w.projections.saveRefresh(ctx, writes)
	}()
	if w.lost != nil {
		return CommitResult{}, w.lost
	}
	group, err := fn(view{w})
	if err != nil {
		return CommitResult{}, err
	}
	if group == nil {
		return CommitResult{Outcome: CommitNoop}, nil
	}
	if group.CommitID == "" {
		return CommitResult{Outcome: CommitInvalid, Detail: "empty CommitID"}, nil
	}
	batches, refs, invalid := encode(w.registry, group)
	if invalid != "" {
		return CommitResult{Outcome: CommitInvalid, Detail: invalid}, nil
	}
	if err := (ledger.Proposal{CommitID: group.CommitID, Batches: batches}).Validate(); err != nil {
		return CommitResult{Outcome: CommitInvalid, Detail: err.Error()}, nil
	}
	for _, ref := range refs {
		if invalid, err := w.admission.admit(ctx, ref.id, ref.decl); err != nil {
			return CommitResult{}, err
		} else if invalid != "" {
			return CommitResult{Outcome: CommitInvalid, Detail: fmt.Sprintf("%s: %s", ref.where, invalid)}, nil
		}
	}
	if existing, committed, err := w.kernel.LookupCommit(group.CommitID); err != nil {
		return CommitResult{}, err
	} else if committed {
		// The CommitID names the operation: a second commit under it is the
		// same operation and is not written again.
		return CommitResult{Outcome: CommitAlreadyApplied, Commit: existing}, nil
	}
	// Required projections must accept the commit before anything is persisted. The
	// provisional commit is what a reader folds: the kernel assigns only Seq
	// inside Append.
	provisional := ledger.Proposal{CommitID: group.CommitID, Batches: batches}.At(w.kernel.Head().Next)
	// Only a required-for-commit projection's fold refuses the commit; a derived
	// one that cannot fold is marked unhealthy once the commit lands.
	next, err := w.projections.fold(provisional)
	if err != nil {
		return CommitResult{Outcome: CommitInvalid, Detail: err.Error()}, nil
	}
	claim, invalid, err := w.admission.claim(ctx, group.CommitID, bindingIDs(refs))
	if err != nil {
		return CommitResult{}, err
	}
	if invalid != "" {
		return CommitResult{Outcome: CommitInvalid, Detail: invalid}, nil
	}
	stored, err := w.kernel.Append(ctx, ledger.Proposal{CommitID: group.CommitID, Batches: batches})
	if err != nil {
		if claim != nil && appendOutcomeKnown(err) {
			w.admission.release(ctx, claim) // best effort; Bind reconciles any orphan
		}
		if session.IsCode(err, session.ErrOwnershipLost) {
			w.lost = &ledger.Error{Code: ledger.CodeOwnershipLost, Detail: err.Error()}
			return CommitResult{}, w.lost
		}
		if session.IsCode(err, session.ErrConflict) {
			return CommitResult{Outcome: CommitConflict, Detail: err.Error()}, nil
		}
		if appendOutcomeKnown(err) {
			return CommitResult{}, err // rejected before any write; the Writer's state still matches the log
		}
		// The claim stays active until reopening can verify the owner
		// commit. Anything else leaves the log's content unknown to this
		// Writer: fail closed; a replay of the same commit is answered by
		// the kernel's index.
		w.lost = &ledger.Error{Code: ledger.CodeUnknownOutcome, Detail: err.Error()}
		return CommitResult{}, w.lost
	}
	w.projections.advance(next)
	writes = w.projections.planRefresh(w.kernel.Head(), false)
	applied = &stored
	return CommitResult{Outcome: CommitApplied, Commit: stored, Claim: claim}, nil
}

// appendOutcomeKnown reports the Append errors that guarantee nothing was
// written: the kernel's validation rejections, and a context error, which an
// adapter may only return before it starts writing.
func appendOutcomeKnown(err error) bool {
	if session.IsCode(err, session.ErrInvalid) || session.IsCode(err, session.ErrNotFound) {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
