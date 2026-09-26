// Package observe is the Session event stream (OBS-1): a
// writer.CommitObserver that decodes every applied commit through the
// Registry and fans it out, in commit order, to the subscribers of each
// Session. UI, SSE and CLI observation all derive from this one source. A
// subscription is a catch-up subscription: it may start from a CommitSeq,
// reading the ledger up to its head and continuing live, so a consumer that
// keeps its own checkpoint resumes after a crash without a gap.
package observe

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/felinics/twilight/agentcore/run"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// Event is one item of a Session's event stream. Every observation of a
// Session derives from applied commits, so an Event is normally one
// committed event decoded through the Registry: Module, Version and Value
// are the decoded payload, Unknown reports a type or version this process
// has no codec for (the event is still delivered). An Event with Err set and
// a zero Row is a failure of background work (a drive that errored); it is
// reported here for the same audience but never enters the stream.
type Event struct {
	Session session.SessionID
	// Position is the event's place in the Session's ledger: the commit's
	// Seq and the event's index within the commit. It is what a consumer
	// checkpoints; zero on a failure or progress Event.
	Position session.Position
	Row      session.Event
	Module   extension.ModuleKey
	Version  extension.PayloadVersion
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
// serves its ReadCommits. It is what a catch-up subscription reads before
// it goes live.
type History interface {
	ReadCommits(context.Context, session.CommitReadRequest) (session.CommitPage, error)
}

// ErrNoHistory reports SubscribeFrom on a Bus built without a History.
var ErrNoHistory = errors.New("observe: the bus has no history to catch up from")

// Bus decodes applied commits and fans them out per Session.
type Bus struct {
	registry *extension.Registry
	history  History
	mu       sync.Mutex
	subs     map[session.SessionID]map[*subscriber]struct{}
}

// NewBus returns a Bus decoding through registry. history, when not nil,
// lets SubscribeFrom catch up from the ledger; a Bus without it serves live
// subscriptions only.
func NewBus(registry *extension.Registry, history History) *Bus {
	return &Bus{registry: registry, history: history, subs: make(map[session.SessionID]map[*subscriber]struct{})}
}

// Committed is writer.CommitObserver: one Event per committed event, in
// commit order.
func (b *Bus) Committed(_ context.Context, sid session.SessionID, commit session.Commit) {
	b.publish(sid, b.decode(sid, &commit)...)
}

// decode renders one commit as its Events, each at its Position.
func (b *Bus) decode(sid session.SessionID, commit *session.Commit) []Event {
	var events []Event
	index := uint32(0)
	for _, batch := range commit.Batches {
		for _, row := range batch.Events {
			e := Event{Session: sid, Position: session.Position{Commit: commit.Seq, Index: index}, Row: row}
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

// Failed reports a background failure to the Session's subscribers.
func (b *Bus) Failed(sid session.SessionID, err error) {
	b.publish(sid, Event{Session: sid, Err: err})
}

// Publish delivers a transient progress observation to the Session's
// subscribers (RUN-EXE-12).
func (b *Bus) Publish(sid session.SessionID, p Progress) {
	b.publish(sid, Event{Session: sid, Progress: &p})
}

func (b *Bus) publish(sid session.SessionID, events ...Event) {
	b.mu.Lock()
	subs := make([]*subscriber, 0, len(b.subs[sid]))
	for s := range b.subs[sid] {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	for _, s := range subs {
		s.push(events)
	}
}

// Subscribe registers a subscriber to one Session's stream from this moment
// on; the channel closes when ctx is done.
func (b *Bus) Subscribe(ctx context.Context, sid session.SessionID) <-chan Event {
	s := b.attach(sid)
	go s.drain(ctx, func() { b.detach(sid, s) })
	return s.out
}

// SubscribeFrom is the catch-up form of Subscribe: every committed event of
// the Session from CommitSeq from, in order, then the live stream. The
// subscriber is attached before the history is read, so no commit made
// meanwhile is lost, and a live commit the history already covered is
// dropped, so none is delivered twice. Failure and progress Events, which
// have no Position, are delivered as they come. The channel closes when ctx
// is done; a read failure closes it after an Event with Err.
func (b *Bus) SubscribeFrom(ctx context.Context, sid session.SessionID, from session.CommitSeq) (<-chan Event, error) {
	if b.history == nil {
		return nil, ErrNoHistory
	}
	s := b.attach(sid)
	page, err := b.history.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: from})
	if err != nil {
		b.detach(sid, s)
		return nil, err
	}
	var history []Event
	for i := range page.Commits {
		history = append(history, b.decode(sid, &page.Commits[i])...)
	}
	s.mu.Lock()
	s.history, s.skipBelow = history, page.Head.Next
	s.mu.Unlock()
	go s.drain(ctx, func() { b.detach(sid, s) })
	return s.out, nil
}

func (b *Bus) attach(sid session.SessionID) *subscriber {
	s := &subscriber{out: make(chan Event, 64), wake: make(chan struct{}, 1)}
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

// subscriber decouples the committer from the consumer: push appends to an
// unbounded queue and never blocks, so a slow reader delays only its own
// delivery, never a Commit; drain moves the history, then the queue, into
// the channel in order and closes it when the subscription ends. skipBelow
// is the ledger head the history covered: a live committed Event below it
// was delivered from the history already.
type subscriber struct {
	mu        sync.Mutex
	history   []Event
	skipBelow session.CommitSeq
	queue     []Event
	out       chan Event
	wake      chan struct{}
}

func (s *subscriber) push(events []Event) {
	s.mu.Lock()
	s.queue = append(s.queue, events...)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *subscriber) drain(ctx context.Context, unsubscribe func()) {
	defer func() {
		unsubscribe()
		close(s.out)
	}()
	for {
		s.mu.Lock()
		history, live, skipBelow := s.history, s.queue, s.skipBelow
		s.history, s.queue = nil, nil
		s.mu.Unlock()
		for i := range history {
			select {
			case s.out <- history[i]:
			case <-ctx.Done():
				return
			}
		}
		for i := range live {
			e := &live[i]
			if e.Err == nil && e.Progress == nil && e.Position.Commit < skipBelow {
				// Delivered from the history already.
				continue
			}
			select {
			case s.out <- *e:
			case <-ctx.Done():
				return
			}
		}
		select {
		case <-s.wake:
		case <-ctx.Done():
			return
		}
	}
}
