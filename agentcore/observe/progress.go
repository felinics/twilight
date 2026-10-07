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
	subs map[session.SessionID]map[*localSubscriber]struct{}
}

// NewProgresses returns an empty transient stream.
func NewProgresses() *Progresses {
	return &Progresses{subs: make(map[session.SessionID]map[*localSubscriber]struct{})}
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
	subs := make([]*localSubscriber, 0, len(p.subs[sid]))
	for s := range p.subs[sid] {
		subs = append(subs, s)
	}
	p.mu.Unlock()
	for _, s := range subs {
		if !s.push(events) {
			p.detach(sid, s)
		}
	}
}

// Subscribe registers a subscriber to one Session's transient stream from
// this moment on; the channel closes when ctx is done. A subscriber which
// does not keep up is detached and closed at the same hard bound as a live
// committed subscription.
func (p *Progresses) Subscribe(ctx context.Context, sid session.SessionID) <-chan Event {
	s := newLocalSubscriber()
	p.mu.Lock()
	if p.subs[sid] == nil {
		p.subs[sid] = make(map[*localSubscriber]struct{})
	}
	p.subs[sid][s] = struct{}{}
	p.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			s.close()
		case <-s.done:
		}
		p.detach(sid, s)
	}()
	return s.out
}

func (p *Progresses) detach(sid session.SessionID, s *localSubscriber) {
	p.mu.Lock()
	delete(p.subs[sid], s)
	if len(p.subs[sid]) == 0 {
		delete(p.subs, sid)
	}
	p.mu.Unlock()
}
