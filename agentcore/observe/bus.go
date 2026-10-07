// Package observe carries a Session's observations (OBS-1) in two streams.
// Bus and Progresses are process-local fan-outs for committed and transient
// observations. SessionLedgerTailer is the durable stream: it follows the
// shared ledger and uses CommitObserver notifications only as polling hints.
package observe

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
)

// Event is one item of a Session's observation stream, in either of two
// shapes. A committed Event carries one event decoded from an applied commit:
// Module, Version and Value are its decoded payload, while Unknown reports a
// type or version this process has no codec for. A transient Event has a zero
// Row and carries Progress or Err.
type Event struct {
	Session session.SessionID
	// Position is the event's place in the Session's ledger: the commit's
	// sequence and the event's index in that commit. It is zero for transient
	// observations.
	Position ledger.Position
	Row      ledger.Event
	Module   module.ModuleKey
	Version  module.PayloadVersion
	Value    any
	Unknown  bool
	Err      error
	// Progress is expendable executor output. The durable committed result
	// which follows replaces it.
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

const subscriberBuffer = 64

// Bus is the process-local committed fan-out. It neither reads nor retains a
// ledger: Committed decodes the supplied commit and immediately publishes it
// to the subscribers that are present in this process.
type Bus struct {
	registry *module.Registry
	mu       sync.Mutex
	subs     map[session.SessionID]map[*localSubscriber]struct{}
}

// NewBus returns an empty process-local committed fan-out.
func NewBus(registry *module.Registry) *Bus {
	return &Bus{registry: registry, subs: make(map[session.SessionID]map[*localSubscriber]struct{})}
}

// Committed implements writer.CommitObserver.
func (b *Bus) Committed(_ context.Context, sid session.SessionID, commit ledger.Commit) {
	b.publish(sid, decodeCommit(b.registry, sid, &commit))
}

func decodeCommit(registry *module.Registry, sid session.SessionID, commit *ledger.Commit) []Event {
	events := make([]Event, 0)
	index := uint32(0)
	for _, batch := range commit.Batches {
		for _, row := range batch.Events {
			e := Event{Session: sid, Position: ledger.Position{Commit: commit.Seq, Index: index}, Row: row}
			index++
			decoded, err := registry.Decode(row)
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

func (b *Bus) publish(sid session.SessionID, events []Event) {
	b.mu.Lock()
	subs := make([]*localSubscriber, 0, len(b.subs[sid]))
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

// Subscribe registers a live-only subscriber. A subscriber that does not
// keep up is detached and closed rather than blocking a committer.
func (b *Bus) Subscribe(ctx context.Context, sid session.SessionID) <-chan Event {
	s := newLocalSubscriber()
	b.mu.Lock()
	if b.subs[sid] == nil {
		b.subs[sid] = make(map[*localSubscriber]struct{})
	}
	b.subs[sid][s] = struct{}{}
	b.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			s.close()
		case <-s.done:
		}
		b.detach(sid, s)
	}()
	return s.out
}

func (b *Bus) detach(sid session.SessionID, s *localSubscriber) {
	b.mu.Lock()
	delete(b.subs[sid], s)
	if len(b.subs[sid]) == 0 {
		delete(b.subs, sid)
	}
	b.mu.Unlock()
}

// localSubscriber owns closure of its bounded output. push and close may race,
// so both are serialized by mu.
type localSubscriber struct {
	mu     sync.Mutex
	out    chan Event
	done   chan struct{}
	closed bool
}

func newLocalSubscriber() *localSubscriber {
	return &localSubscriber{out: make(chan Event, subscriberBuffer), done: make(chan struct{})}
}

func (s *localSubscriber) push(events []Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	for i := range events {
		select {
		case s.out <- events[i]:
		default:
			s.closeLocked()
			return false
		}
	}
	return true
}

func (s *localSubscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
}

func (s *localSubscriber) closeLocked() {
	if !s.closed {
		close(s.out)
		close(s.done)
		s.closed = true
	}
}
