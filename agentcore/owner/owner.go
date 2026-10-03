// Package owner is the ownership authority over the Sessions one process
// holds: Open acquires a Session's Writer, runs the takeover disposition
// and hands out the Handle every command runs through; one generation of
// ownership exists at a time, and Close releases it. The services a Handle
// is driven with are the Kernel's and the Execution's; this package adds
// nothing but the ownership table.
package owner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
)

// ErrSessionOpen reports an Open of a Session this Owner already holds, or
// is still opening or closing.
var ErrSessionOpen = errors.New("owner: session is already open")

// ErrClosed reports an Open after the Owner began closing: the Kernel and
// the Execution it serves are closing behind it.
var ErrClosed = errors.New("owner: closed")

type openState uint8

const (
	opening openState = iota // Writer being acquired, recovery being installed
	open                     // owned; commands run through the Writer
	closing                  // release in progress; Open still refuses
)

// openSession is one generation of ownership of a Session.
type openSession struct {
	state openState
	w     writer.Writer
}

// Owner holds the Sessions this process owns over the Session-side ports
// they commit through; the Execution advances them.
type Owner struct {
	// Writers opens and closes the Sessions' Writers.
	Writers writer.Writers
	// Maintenance deletes Sessions; Admission releases their artifacts.
	Maintenance session.Maintenance
	Admission   writer.Admission
	Execution   *runtime.Execution

	mu   sync.Mutex
	open map[session.SessionID]*openSession
	// closing begins with Close: Open refuses from here on and Close waits
	// for the Opens in flight before releasing, so none outlives the
	// Execution and the Kernel closing behind this call.
	closing  bool
	inflight sync.WaitGroup
}

// New returns an Owner over the Kernel's Session ports and the Execution x,
// with no Session open.
func New(k *sessionkernel.Kernel, x *runtime.Execution) *Owner {
	return &Owner{Writers: k.Writers, Maintenance: k.Store, Admission: k.Admission, Execution: x,
		open: make(map[session.SessionID]*openSession)}
}

// Handle is the ownership capability of one open generation: its Writer is
// what every command of the Session commits through. Recovered is the number
// of recovery commands the takeover disposition issued when it opened.
type Handle struct {
	Recovered int
	gen       *openSession
	a         *Owner
}

// Open acquires the Session's Writer, runs the takeover disposition and
// resumes the waits a Responder answers. A Session already held, opening
// or closing is ErrSessionOpen; an Open after the Owner began closing is
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
	n, err := a.Execution.Open(ctx, w)
	if err != nil {
		_ = a.release(context.WithoutCancel(ctx), sid, gen, true)
		return nil, err
	}
	// Waits a previous owner left with a Responder are answered by this
	// one: the Responder continues from its durable state.
	a.Execution.ResumeWaiting(ctx, w)
	a.mu.Lock()
	gen.state = open
	a.mu.Unlock()
	return &Handle{Recovered: n, gen: gen, a: a}, nil
}

// beginClose marks the Session's current generation closing when it is gen
// (or any generation when gen is nil) and open; otherwise nil.
func (a *Owner) beginClose(sid session.SessionID, gen *openSession) *openSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.open[sid]
	if current == nil || (gen != nil && current != gen) || current.state != open {
		return nil
	}
	current.state = closing
	return current
}

// release ends the generation: the recovery listeners and, when closeWriter,
// the Writer; then the table entry, if it is still this generation.
func (a *Owner) release(ctx context.Context, sid session.SessionID, gen *openSession, closeWriter bool) error {
	var err error
	if closeWriter {
		a.Execution.Stop(sid)
		err = writer.CloseWriter(ctx, a.Writers, sid)
	}
	a.mu.Lock()
	if a.open[sid] == gen {
		delete(a.open, sid)
	}
	a.mu.Unlock()
	return err
}

// DeleteSession tombstones a Session and reclaims along its path. A Session
// this Owner holds open is closed first; one opening or closing is
// ErrSessionOpen; one owned by another process is the store's ErrOwned.
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
	return writer.Delete(ctx, a.Maintenance, a.Admission, sid)
}

// Close refuses new Opens, waits for the ones in flight and releases every
// generation this Owner then holds: the recovery listeners and the Writer
// of each. Every outstanding Handle is stale afterwards. The Kernel and the
// Execution stay open: they are the host's to close, after this.
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
