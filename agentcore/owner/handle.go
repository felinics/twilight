package owner

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// ErrSessionOpen reports an Open of a Session this Owner already holds
// open, or is still opening or closing: one generation of ownership at a
// time, and a generation's release completes before the next begins
// (OWN-HDL-1).
var ErrSessionOpen = errors.New("owner: session is already open")

type openState uint8

const (
	opening openState = iota // Writer being acquired, recovery being installed
	open                     // owned; commands run through the Writer
	closing                  // release in progress; Open still refuses
)

// openSession is one generation of ownership over a Session: the Writer
// taken at Open and the recovery lifetime installed with it. Handles point
// at a generation, so a Close releases only the generation it belongs to,
// and the table keeps the generation until its release has completed, so an
// Open racing a Close cannot take a Writer the Close is about to shut.
type openSession struct {
	state openState
	w     writer.Writer
}

// Handle is this Owner's current execution capability over one Session
// (OWN-HDL-1, DRV-3): its Writer is open under this process's epoch, the
// takeover disposition has run, and the recovery lifetime is installed. A
// SessionID is a durable identity; a Handle says this process owns it now.
//
// The Handle carries no operations of its own. Every command of the core --
// turn.Commands, chatlog.Commands, driver.Drive, run.Runtime.Commit -- takes
// its Writer, so all authoritative writes of the Session land on one Writer,
// one epoch and one projection view, and the Writer fences a stale owner.
// Reads take a SessionID and need no Handle.
type Handle struct {
	// Recovered is the takeover disposition count from opening (RUN-CMT-7).
	Recovered int

	gen *openSession
	a   *Owner
}

// Open takes ownership of the Session -- its Writer and its recovery
// listeners -- and runs the takeover disposition (DRV-3). The stream must
// exist. A Session this Owner holds open, is opening, or is still
// closing is ErrSessionOpen. A failed takeover disposition releases the
// Writer again, so a failed Open leaves nothing owned.
func (a *Owner) Open(ctx context.Context, sid session.SessionID) (*Handle, error) {
	gen := &openSession{state: opening}
	a.mu.Lock()
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
	n, err := a.Driver.Open(ctx, w)
	if err != nil {
		_ = a.release(context.WithoutCancel(ctx), sid, gen, true)
		return nil, err
	}
	a.mu.Lock()
	gen.state = open
	a.mu.Unlock()
	return &Handle{Recovered: n, gen: gen, a: a}, nil
}

// beginClose moves the Session's current generation to closing when it is
// gen (or any generation when gen is nil) and in the open state; it returns
// the generation to release, or nil when there is nothing this caller owns.
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

// release shuts a generation down -- recovery listeners, then the Writer --
// and drops it from the table. It is the one release path: Close,
// DeleteSession and a failed Open all end here, so no Writer is closed while
// another generation may be using it.
func (a *Owner) release(ctx context.Context, sid session.SessionID, gen *openSession, closeWriter bool) error {
	var err error
	if closeWriter {
		a.Driver.Stop(sid)
		err = writer.CloseWriter(ctx, a.Writers, sid)
	}
	a.mu.Lock()
	if a.open[sid] == gen {
		delete(a.open, sid)
	}
	a.mu.Unlock()
	return err
}

// ID is the Session this Handle owns.
func (h *Handle) ID() session.SessionID { return h.gen.w.SessionID() }

// Writer is the write capability itself: the Session's Writer under this
// process's epoch. Core commands take it; a Writer that has lost ownership
// refuses to commit, which is what makes the Handle a capability rather
// than a name.
func (h *Handle) Writer() writer.Writer { return h.gen.w }

// Close releases this Handle's generation of ownership: it stops the
// Session's recovery listeners and releases its Writer. A Handle whose
// generation was already released or is being released (by Close,
// DeleteSession or Owner.Close) does nothing; a later generation is
// never touched. A Writer that failed (EXT-WRT-4) does not block the close;
// the next Open reopens from the log.
func (h *Handle) Close(ctx context.Context) error {
	sid := h.ID()
	gen := h.a.beginClose(sid, h.gen)
	if gen == nil {
		return nil
	}
	return h.a.release(ctx, sid, gen, true)
}
