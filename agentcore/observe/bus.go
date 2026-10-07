// Package observe carries a Session's observations (OBS-1) in two streams.
// The Bus is the committed stream: a writer.CommitObserver that decodes
// every applied commit through the Registry and fans it out, in commit
// order, to the subscribers of each Session. Its subscription is a catch-up
// subscription: it may start from a CommitSeq, reading the ledger up to its
// head and continuing live, so a consumer that keeps its own checkpoint
// resumes after a crash without a gap. The Progresses are the transient
// stream: what the executor reported of an effect while it ran, and the
// failures of background work -- none of it a fact, all of it expendable. A
// host that serves one audience from both merges the two.
package observe

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
)

// Event is one item of a Session's observation stream, in either of two
// shapes. A committed Event carries one event decoded from the applied
// commits through the Registry: Module, Version and Value are the decoded
// payload, Unknown reports a type or version this process has no codec for
// (the event is still delivered) and Err reports the decode or history-read
// failure. A transient Event has a zero Row: Progress carries what the
// executor reported of an effect while it ran, or Err alone carries a
// failure of background work; neither is a fact, either may be lost, and
// the committed result that follows replaces it.
type Event struct {
	Session session.SessionID
	// Position is the event's place in the Session's ledger: the commit's
	// Seq and the event's index within the commit. It is what a consumer
	// checkpoints; zero on a transient Event.
	Position ledger.Position
	Row      ledger.Event
	Module   module.ModuleKey
	Version  module.PayloadVersion
	Value    any
	Unknown  bool
	Err      error
	// Progress is set for a transient observation of an effect in flight
	// (RUN-EXE-12, OBS-1): a model or tool delta relayed from the executor,
	// or a reset that voids the deltas received so far. Such an Event has a
	// zero Row: it is not a fact, may be lost, and is replaced by the
	// committed result that follows.
	Progress *Progress
}

// Progress is the transient part of an Event: what the executor reported of
// an effect while it ran.
type Progress struct {
	RunID      run.RunID
	Effect     run.EffectID
	Generation int
	Sequence   uint64
	Kind       string
	Payload    json.RawMessage
}

// History reads a Session's committed history: the Store, or anything that
// serves its ReadCommits. It is the source of truth for a catch-up
// subscription, including commits written through another Bus or owner.
type History interface {
	ReadCommits(context.Context, session.CommitReadRequest) (session.CommitPage, error)
}

// ErrNoHistory reports SubscribeFrom on a Bus built without a History.
var ErrNoHistory = errors.New("observe: the bus has no history to catch up from")

const (
	subscriberBuffer   = 64
	historyPageCommits = 64
	historyPoll        = 100 * time.Millisecond
)

// Bus decodes applied commits and fans them out per Session. It carries
// committed facts only; the transient observations of the running effects
// are the Progresses' stream.
type Bus struct {
	registry *module.Registry
	history  History
	mu       sync.Mutex
	subs     map[session.SessionID]map[*subscriber]struct{}
}

// NewBus returns a Bus decoding through registry. history, when not nil,
// lets SubscribeFrom catch up from and continuously tail the ledger; a Bus
// without it serves live subscriptions only.
func NewBus(registry *module.Registry, history History) *Bus {
	return &Bus{registry: registry, history: history, subs: make(map[session.SessionID]map[*subscriber]struct{})}
}

// Committed is writer.CommitObserver: one Event per committed event, in
// commit order. For durable subscriptions it is only a low-latency wake-up;
// those subscriptions always obtain the event itself from History.
func (b *Bus) Committed(_ context.Context, sid session.SessionID, commit ledger.Commit) {
	b.publish(sid, b.decode(sid, &commit)...)
}

// decode renders one commit as its Events, each at its Position.
func (b *Bus) decode(sid session.SessionID, commit *ledger.Commit) []Event {
	var events []Event
	index := uint32(0)
	for _, batch := range commit.Batches {
		for _, row := range batch.Events {
			e := Event{Session: sid, Position: ledger.Position{Commit: commit.Seq, Index: index}, Row: row}
			index++
			decoded, err := b.registry.Decode(row)
			if err != nil {
				e.Unknown, e.Err = true, err
			} else {
				e.Module, e.Version, e.Value, e.Unknown = decoded.Module, decoded.Version, decoded.Value, decoded.Unknown
			}
			events = append(events, e)
		}
	}
	return events
}

func (b *Bus) publish(sid session.SessionID, events ...Event) {
	b.mu.Lock()
	subs := make([]*subscriber, 0, len(b.subs[sid]))
	for s := range b.subs[sid] {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	for _, s := range subs {
		if !s.push(events) {
			b.detach(sid, s)
		}
	}
}

// Subscribe registers a subscriber to one Session's stream from this moment
// on; the channel closes when ctx is done. Delivery has a hard bound: a
// subscriber which does not keep up is detached and its channel is closed,
// rather than retaining an unbounded queue or blocking a committer.
func (b *Bus) Subscribe(ctx context.Context, sid session.SessionID) <-chan Event {
	s := b.attach(sid, false)
	go func() {
		select {
		case <-ctx.Done():
			s.closeLive()
		case <-s.done:
		}
		b.detach(sid, s)
	}()
	return s.out
}

// SubscribeFrom reads every committed event of the Session from CommitSeq
// from, in order, and then continuously tails History. History remains the
// source of truth after catch-up, so writes made through another Bus or
// owner are discovered by bounded polling. Local Committed calls only wake
// the tailer early. Reads are paged and the channel closes when ctx is done;
// a read failure closes it after an Event with Err.
func (b *Bus) SubscribeFrom(ctx context.Context, sid session.SessionID, from ledger.CommitSeq) (<-chan Event, error) {
	if b.history == nil {
		return nil, ErrNoHistory
	}
	s := b.attach(sid, true)
	page, err := b.readPage(ctx, sid, from)
	if err != nil {
		b.detach(sid, s)
		return nil, err
	}
	go b.tail(ctx, sid, from, page, s)
	return s.out, nil
}

func (b *Bus) readPage(ctx context.Context, sid session.SessionID, from ledger.CommitSeq) (session.CommitPage, error) {
	return b.history.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: from, Limit: historyPageCommits})
}

func (b *Bus) tail(ctx context.Context, sid session.SessionID, from ledger.CommitSeq, page session.CommitPage, s *subscriber) {
	defer func() {
		b.detach(sid, s)
		close(s.out)
	}()
	cursor := from
	for {
		for i := range page.Commits {
			commit := &page.Commits[i]
			for _, event := range b.decode(sid, commit) {
				select {
				case s.out <- event:
				case <-ctx.Done():
					return
				}
			}
			cursor = commit.Seq + 1
		}
		if !page.HasMore {
			timer := time.NewTimer(historyPoll)
			select {
			case <-s.wake:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
		var err error
		page, err = b.readPage(ctx, sid, cursor)
		if err != nil {
			if ctx.Err() == nil {
				select {
				case s.out <- Event{Session: sid, Err: err}:
				case <-ctx.Done():
				}
			}
			return
		}
	}
}

func (b *Bus) attach(sid session.SessionID, durable bool) *subscriber {
	s := &subscriber{out: make(chan Event, subscriberBuffer), durable: durable}
	if durable {
		s.wake = make(chan struct{}, 1)
	} else {
		s.done = make(chan struct{})
	}
	b.mu.Lock()
	if b.subs[sid] == nil {
		b.subs[sid] = make(map[*subscriber]struct{})
	}
	b.subs[sid][s] = struct{}{}
	b.mu.Unlock()
	return s
}

func (b *Bus) detach(sid session.SessionID, s *subscriber) {
	b.mu.Lock()
	delete(b.subs[sid], s)
	if len(b.subs[sid]) == 0 {
		delete(b.subs, sid)
	}
	b.mu.Unlock()
}

// subscriber is either a bounded live delivery channel or a durable
// tailer's wake-up. Durable subscribers never accept event data from the
// publisher; the tailer reads it from History.
type subscriber struct {
	mu      sync.Mutex
	out     chan Event
	wake    chan struct{}
	done    chan struct{}
	durable bool
	closed  bool
}

// push never blocks. A full live subscriber is explicitly ended; a durable
// subscriber merely receives a coalesced hint to poll History immediately.
func (s *subscriber) push(events []Event) bool {
	if s.durable {
		select {
		case s.wake <- struct{}{}:
		default:
		}
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	for i := range events {
		select {
		case s.out <- events[i]:
		default:
			close(s.out)
			close(s.done)
			s.closed = true
			return false
		}
	}
	return true
}

func (s *subscriber) closeLive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		close(s.out)
		close(s.done)
		s.closed = true
	}
}
