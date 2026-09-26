// Package notice is the one bounded notice log every settlement source in
// the executor tier keeps (RUN-EXE-17): a Worker announcing that an
// AssignmentKey settled, a Backend announcing that a Ref settled. A notice
// says when to read; the read says what. The log is per incarnation
// (Epoch), ordered by Sequence, bounded by a window, and a subscriber that
// asks for a Sequence the window no longer holds is told so and re-reads
// what it waits on.
package notice

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agentcore/run/effect"
)

// DefaultWindow is the number of notices a Ring keeps for late subscribers.
const DefaultWindow = 4096

// Ring is the bounded, epoch-scoped notice log. T is the notice the ring
// hands subscribers; Record builds it from the ring's epoch and the next
// sequence so the two are never stamped by the caller.
type Ring[T any] struct {
	epoch  string
	window int

	mu      sync.Mutex
	next    uint64
	log     []entry[T]
	changed chan struct{} // closed and replaced on every record; waiters select on it
	closed  bool
}

type entry[T any] struct {
	sequence uint64
	value    T
}

// NewRing starts an empty ring for one incarnation; window <= 0 selects
// DefaultWindow.
func NewRing[T any](epoch string, window int) *Ring[T] {
	if window <= 0 {
		window = DefaultWindow
	}
	return &Ring[T]{epoch: epoch, window: window, changed: make(chan struct{})}
}

// Epoch names the incarnation the ring's sequences belong to.
func (r *Ring[T]) Epoch() string { return r.epoch }

// Record appends the notice build returns for the next sequence and wakes
// every subscriber. A closed ring records nothing.
func (r *Ring[T]) Record(build func(epoch string, sequence uint64) T) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.next++
	r.log = append(r.log, entry[T]{sequence: r.next, value: build(r.epoch, r.next)})
	if len(r.log) > r.window {
		r.log = r.log[len(r.log)-r.window:]
	}
	close(r.changed)
	r.changed = make(chan struct{})
}

// Close ends every subscription once it has delivered what the ring holds;
// later Records are dropped.
func (r *Ring[T]) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	close(r.changed)
	r.changed = make(chan struct{})
}

// Subscribe delivers to fn, in order, every notice with Sequence greater
// than after in the incarnation epoch names, then every new one, until fn
// returns false, the ring closes or ctx ends. An unknown epoch subscribes
// from the head; a sequence the window evicted is
// effect.ErrSettlementsEvicted, and the subscriber re-reads as for an
// unknown epoch.
func (r *Ring[T]) Subscribe(ctx context.Context, epoch string, after uint64, fn func(T) bool) error {
	return r.SubscribeAnnounced(ctx, epoch, after, nil, fn)
}

// SubscribeAnnounced is Subscribe that first hands fn one announcement,
// built by announce from the ring's epoch and head (the sequence of the
// last notice recorded): a frame without a subject that tells the
// subscriber which incarnation it is connected to and where its log
// stands. A subscriber that presented another epoch learns at once that
// notices recorded before this subscription are not replayed and re-reads
// what it waits for; one that resumes in the same epoch learns nothing new.
// nil announce announces nothing.
func (r *Ring[T]) SubscribeAnnounced(ctx context.Context, epoch string, after uint64, announce func(epoch string, head uint64) T, fn func(T) bool) error {
	r.mu.Lock()
	switch {
	case epoch != r.epoch:
		after = r.next
	case len(r.log) > 0 && after < r.log[0].sequence-1, len(r.log) == 0 && after < r.next:
		r.mu.Unlock()
		return effect.ErrSettlementsEvicted
	}
	head := r.next
	r.mu.Unlock()
	if announce != nil {
		if !fn(announce(r.epoch, head)) {
			return nil
		}
	}
	for {
		r.mu.Lock()
		var pending []T
		for i := range r.log {
			if r.log[i].sequence > after {
				pending = append(pending, r.log[i].value)
				after = r.log[i].sequence
			}
		}
		wait, closed := r.changed, r.closed
		r.mu.Unlock()
		for _, v := range pending {
			if !fn(v) {
				return nil
			}
		}
		if closed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

// Ref is a Backend's notice that the execution behind Ref reached its
// Outcome, readable through ExecutionBackend.Outcome (RUN-EXE-9). It is the
// Backend-tier twin of effect.Settlement, keyed by the Backend's Ref rather
// than the Worker's AssignmentKey.
type Ref struct {
	Ref      string `json:"ref"`
	Epoch    string `json:"epoch"`
	Sequence uint64 `json:"sequence"`
}

// Source is the optional notification capability of an ExecutionBackend,
// the way SettlementPort is of an ExecutionPort: Settled streams the Refs
// that settled, so the Worker waits on the stream instead of holding a
// blocking read open for the life of the execution.
type Source interface {
	Settled(ctx context.Context, epoch string, after uint64, fn func(Ref) bool) error
}

// RefHub is the Ring a Backend records its settled Refs on; it is a Source.
type RefHub struct{ ring *Ring[Ref] }

// NewRefHub starts a RefHub for one Backend incarnation.
func NewRefHub(epoch string, window int) *RefHub { return &RefHub{ring: NewRing[Ref](epoch, window)} }

// Epoch names the incarnation.
func (h *RefHub) Epoch() string { return h.ring.Epoch() }

// Settled is notice.Source over the hub: an announcement of the epoch and
// head first (an empty Ref), then every settled Ref.
func (h *RefHub) Settled(ctx context.Context, epoch string, after uint64, fn func(Ref) bool) error {
	return h.ring.SubscribeAnnounced(ctx, epoch, after, func(e string, head uint64) Ref { return Ref{Epoch: e, Sequence: head} }, fn)
}

// Record announces that ref settled.
func (h *RefHub) Record(ref string) {
	if h == nil {
		return
	}
	h.ring.Record(func(epoch string, sequence uint64) Ref { return Ref{Ref: ref, Epoch: epoch, Sequence: sequence} })
}

// Close ends the hub's subscriptions.
func (h *RefHub) Close() {
	if h != nil {
		h.ring.Close()
	}
}

var _ Source = (*RefHub)(nil)
