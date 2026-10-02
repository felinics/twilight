// Package owner is the ownership table over the Sessions one process holds:
// Open acquires a Session's Writer, Close releases it.
package owner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// ErrSessionOpen reports an Open of a Session this Owner already holds, or
// is still opening or closing.
var ErrSessionOpen = errors.New("owner: session is already open")

// ErrClosed reports an Open after the Owner began closing.
var ErrClosed = errors.New("owner: closed")

type openState uint8

const (
	opening openState = iota // Writer being acquired
	open                     // owned; commands run through the Writer
	closing                  // release in progress; Open still refuses
)

// openSession is one generation of ownership of a Session.
type openSession struct {
	state openState
	w     writer.Writer
}

// Owner holds one generation of ownership per Session over the Writers
// that open and close them.
type Owner struct {
	// Writers opens and closes the Sessions' Writers.
	Writers writer.Writers

	mu   sync.Mutex
	open map[session.SessionID]*openSession
	// closing begins with Close: Open refuses from here on and Close waits
	// for the Opens in flight before releasing, so none outlives what the
	// caller closes behind it.
	closing  bool
	inflight sync.WaitGroup
}

// New returns an Owner over writers with no Session open.
func New(writers writer.Writers) *Owner {
	return &Owner{Writers: writers, open: make(map[session.SessionID]*openSession)}
}

// Handle is the ownership capability of one open generation: its Writer is
// what every command of the Session commits through.
type Handle struct {
	gen *openSession
	a   *Owner
}

// Open acquires the Session's Writer. A Session already held, opening or
// closing is ErrSessionOpen; an Open after the Owner began closing is
// ErrClosed.
func (a *Owner) Open(ctx context.Context, sid session.SessionID) (*Handle, error) {
	a.inflight.Add(1)
	defer a.inflight.Done()
	gen := &openSession{state: opening}
	a.mu.Lock()
	if a.closing {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrClosed, sid)
	}
	if _, held := a.open[sid]; held {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrSessionOpen, sid)
	}
	a.open[sid] = gen
	a.mu.Unlock()
	w, err := a.Writers.Writer(ctx, sid)
	if err != nil {
		_ = a.release(context.WithoutCancel(ctx), sid, gen, false)
		return nil, err
	}
	gen.w = w
	a.mu.Lock()
	gen.state = open
	a.mu.Unlock()
	return &Handle{gen: gen, a: a}, nil
}

// beginClose marks the Session's current generation closing when it is gen
// and open; otherwise nil.
func (a *Owner) beginClose(sid session.SessionID, gen *openSession) *openSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.open[sid]
	if current == nil || current != gen || current.state != open {
		return nil
	}
	current.state = closing
	return current
}

// release ends the generation: the Writer when closeWriter, then the table
// entry, if it is still this generation.
func (a *Owner) release(ctx context.Context, sid session.SessionID, gen *openSession, closeWriter bool) error {
	var err error
	if closeWriter {
		err = writer.CloseWriter(ctx, a.Writers, sid)
	}
	a.mu.Lock()
	if a.open[sid] == gen {
		delete(a.open, sid)
	}
	a.mu.Unlock()
	return err
}

// Close refuses new Opens, waits for the ones in flight and releases the
// Writer of every generation this Owner then holds. Every outstanding
// Handle is stale afterwards.
func (a *Owner) Close(ctx context.Context) error {
	a.mu.Lock()
	a.closing = true
	a.mu.Unlock()
	a.inflight.Wait()
	a.mu.Lock()
	var owned []*openSession
	var sids []session.SessionID
	for sid, gen := range a.open {
		if gen.state == open {
			gen.state = closing
			owned = append(owned, gen)
			sids = append(sids, sid)
		}
	}
	a.mu.Unlock()
	var err error
	for i, gen := range owned {
		if rerr := a.release(ctx, sids[i], gen, true); rerr != nil && err == nil {
			err = rerr
		}
	}
	return err
}

// ID is the Session the Handle owns.
func (h *Handle) ID() session.SessionID { return h.gen.w.SessionID() }

// Writer is the ownership capability every command of the Session commits
// through.
func (h *Handle) Writer() writer.Writer { return h.gen.w }

// Close releases this generation; a Handle whose generation was already
// released or replaced releases nothing.
func (h *Handle) Close(ctx context.Context) error {
	sid := h.ID()
	gen := h.a.beginClose(sid, h.gen)
	if gen == nil {
		return nil
	}
	return h.a.release(ctx, sid, gen, true)
}
