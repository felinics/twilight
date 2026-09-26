package writer

import (
	"context"
	"errors"
	"sync"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

type writerSet struct {
	store     session.Store
	registry  *extension.Registry
	admission Admission
	opts      session.OpenOptions
	cfg       WritersConfig
	mu        sync.Mutex
	open      map[session.SessionID]Writer
}

// NewWriters returns a Writers that opens each Session once and hands out the
// same Writer afterwards (EXT-WRT-6).
func NewWriters(store session.Store, registry *extension.Registry, admission Admission, opts session.OpenOptions, cfg WritersConfig) Writers {
	return &writerSet{store: store, registry: registry, admission: admission, opts: opts, cfg: cfg, open: make(map[session.SessionID]Writer)}
}

func (ws *writerSet) Writer(ctx context.Context, sid session.SessionID) (Writer, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if w, ok := ws.open[sid]; ok {
		lost := failure(w)
		if lost == nil {
			return w, nil
		}
		// Ownership loss is not confined to this instance: reopening would take
		// the Session back from the process that owns it now, and that is the
		// host's decision (CloseWriter forgets the failed Writer first). Every
		// other failure is: a fresh Writer rebuilds from the log and a replay
		// of the same CommitID is answered by the kernel's index (EXT-WRT-4).
		if errors.Is(lost, &extension.Error{Code: extension.ErrOwnershipLost}) {
			return nil, lost
		}
		if !errors.Is(lost, errWriterClosed) {
			_ = w.Close(ctx)
		}
		delete(ws.open, sid)
	}
	w, err := openWriter(ctx, ws.store, ws.registry, ws.admission, sid, ws.opts, ws.cfg)
	if err != nil {
		return nil, err
	}
	ws.open[sid] = w
	return w, nil
}

// failure returns the error a failed Writer keeps returning, or nil.
func failure(w Writer) error {
	lw, ok := w.(*sessionWriter)
	if !ok {
		return nil
	}
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.lost
}

// CloseWriter closes and forgets the Writer of one Session, so the next
// Writer(sid) reopens it from the log; a Session without an open Writer is a
// no-op.
func (ws *writerSet) CloseWriter(ctx context.Context, sid session.SessionID) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.open[sid]
	if !ok {
		return nil
	}
	delete(ws.open, sid)
	if errors.Is(failure(w), errWriterClosed) {
		return nil
	}
	return w.Close(ctx)
}

// Close closes every open Writer and forgets it; a later Writer(sid) reopens.
func (ws *writerSet) Close(ctx context.Context) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	var first error
	for sid, w := range ws.open {
		delete(ws.open, sid)
		if errors.Is(failure(w), errWriterClosed) {
			continue
		}
		if err := w.Close(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// CloseWriters closes and forgets every Writer of a NewWriters value.
func CloseWriters(ctx context.Context, ws Writers) error {
	if c, ok := ws.(interface{ Close(context.Context) error }); ok {
		return c.Close(ctx)
	}
	return nil
}

// CloseWriter closes and forgets one Session's Writer of a NewWriters value
// (EXT-WRT-6). Another Writers implementation has its Writer closed in place.
func CloseWriter(ctx context.Context, ws Writers, sid session.SessionID) error {
	if c, ok := ws.(interface {
		CloseWriter(context.Context, session.SessionID) error
	}); ok {
		return c.CloseWriter(ctx, sid)
	}
	w, err := ws.Writer(ctx, sid)
	if err != nil {
		return err
	}
	return w.Close(ctx)
}
