package observe

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agentcore/session"
)

// Progresses fans a Session's transient observations out to its
// subscribers: the progress the executor reported of an effect while it ran
// (RUN-EXE-12) and the failures of background work, both reported for the
// same audience as the committed stream but never entering it. None of it
// is a fact: an item has a zero Position, may be lost, and is replaced by
// the committed result that follows.
type Progresses struct {
	mu   sync.Mutex
	subs map[session.SessionID]map[*subscriber]struct{}
}

// NewProgresses returns an empty transient stream.
func NewProgresses() *Progresses {
	return &Progresses{subs: make(map[session.SessionID]map[*subscriber]struct{})}
}

// Publish delivers one transient progress observation to the Session's
// subscribers.
func (p *Progresses) Publish(sid session.SessionID, pr Progress) {
	p.publish(sid, Event{Session: sid, Progress: &pr})
}

// Failed reports a failure of background work (a drive that errored) to the
// Session's subscribers.
func (p *Progresses) Failed(sid session.SessionID, err error) {
	p.publish(sid, Event{Session: sid, Err: err})
}

func (p *Progresses) publish(sid session.SessionID, events ...Event) {
	p.mu.Lock()
	subs := make([]*subscriber, 0, len(p.subs[sid]))
	for s := range p.subs[sid] {
		subs = append(subs, s)
	}
	p.mu.Unlock()
	for _, s := range subs {
		s.push(events)
	}
}

// Subscribe registers a subscriber to one Session's transient stream from
// this moment on; the channel closes when ctx is done.
func (p *Progresses) Subscribe(ctx context.Context, sid session.SessionID) <-chan Event {
	s := &subscriber{out: make(chan Event, 64), wake: make(chan struct{}, 1)}
	p.mu.Lock()
	if p.subs[sid] == nil {
		p.subs[sid] = make(map[*subscriber]struct{})
	}
	p.subs[sid][s] = struct{}{}
	p.mu.Unlock()
	go s.drain(ctx, func() {
		p.mu.Lock()
		delete(p.subs[sid], s)
		if len(p.subs[sid]) == 0 {
			delete(p.subs, sid)
		}
		p.mu.Unlock()
	})
	return s.out
}
