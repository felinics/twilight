package executor

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agentcore/run/effect"
)

// DefaultProgressWindow is the frames a ProgressHub keeps per key when
// WorkerOptions.Progress is built with a zero window.
const DefaultProgressWindow = 256

// ProgressHub is the Worker's in-memory progress buffer (RUN-EXE-12): the
// effect.ProgressSink its backends publish into and the effect.ProgressPort
// the Owner reads from. Each key keeps a bounded ring of frames stamped
// with the key's generation and a running sequence; a Reset opens the next
// generation and an End closes the key. Nothing here is persisted: a
// subscriber that misses frames sees a gap, a Worker restart starts over.
type ProgressHub struct {
	window int

	mu      sync.Mutex
	keys    map[effect.AssignmentKey]*progressLog
	changed chan struct{} // closed and replaced on every change; waiters select on it
	// ended lists closed keys oldest first so their logs can be dropped
	// once more than window of them are held.
	ended []effect.AssignmentKey
}

type progressLog struct {
	generation int
	next       uint64
	frames     []effect.ProgressFrame
	ended      bool
}

// NewProgressHub returns a hub keeping window frames per key (zero takes
// DefaultProgressWindow).
func NewProgressHub(window int) *ProgressHub {
	if window <= 0 {
		window = DefaultProgressWindow
	}
	return &ProgressHub{window: window, keys: make(map[effect.AssignmentKey]*progressLog), changed: make(chan struct{})}
}

// Publish is effect.ProgressSink: the frame joins its key's current
// generation with the next sequence. A frame for an ended key is dropped.
func (h *ProgressHub) Publish(_ context.Context, f effect.ProgressFrame) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.append(f)
}

// Reset opens the next generation of key: earlier frames are void for the
// receiver, which learns so from the reset frame.
func (h *ProgressHub) Reset(key effect.AssignmentKey) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	l := h.log(key)
	if l.ended {
		return
	}
	l.generation++
	h.append(effect.ProgressFrame{Key: key, Kind: effect.ProgressReset})
}

// End closes key: an end frame is the last one, later publishes are
// dropped, and the log is released once enough other keys have ended.
func (h *ProgressHub) End(key effect.AssignmentKey) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	l := h.log(key)
	if l.ended {
		return
	}
	h.append(effect.ProgressFrame{Key: key, Kind: effect.ProgressEnd})
	l.ended = true
	h.ended = append(h.ended, key)
	for len(h.ended) > h.window {
		delete(h.keys, h.ended[0])
		h.ended = h.ended[1:]
	}
}

// log returns key's log, creating it; the caller holds mu.
func (h *ProgressHub) log(key effect.AssignmentKey) *progressLog {
	l, ok := h.keys[key]
	if !ok {
		l = &progressLog{generation: 1}
		h.keys[key] = l
	}
	return l
}

// append stamps and stores f and wakes every waiter; the caller holds mu.
func (h *ProgressHub) append(f effect.ProgressFrame) {
	l := h.log(f.Key)
	if l.ended {
		return
	}
	l.next++
	f.Generation, f.Sequence = l.generation, l.next
	l.frames = append(l.frames, f)
	if len(l.frames) > h.window {
		l.frames = l.frames[len(l.frames)-h.window:]
	}
	close(h.changed)
	h.changed = make(chan struct{})
}

// Progress is effect.ProgressPort. A key the hub has not seen yet is waited
// for as long as ctx allows, because the Owner subscribes right after
// Dispatch and the backend's first frame may still be on its way; the
// Worker resolves a key it knows to be absent or terminal before calling.
func (h *ProgressHub) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	for {
		h.mu.Lock()
		var pending []effect.ProgressFrame
		ended := false
		if l, ok := h.keys[key]; ok {
			for i := range l.frames {
				if l.frames[i].Sequence > after {
					pending = append(pending, l.frames[i])
				}
			}
			ended = l.ended
		}
		wait := h.changed
		h.mu.Unlock()
		for _, f := range pending {
			after = f.Sequence
			if !fn(f) {
				return nil
			}
		}
		if ended && len(pending) == 0 {
			return nil
		}
		if ended {
			continue // deliver whatever landed with the end before returning
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

// Known reports whether the hub holds key.
func (h *ProgressHub) Known(key effect.AssignmentKey) bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.keys[key]
	return ok
}

var (
	_ effect.ProgressSink = (*ProgressHub)(nil)
	_ effect.ProgressPort = (*ProgressHub)(nil)
)
