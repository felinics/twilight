// Package inboxtest holds the reference implementation of the inbox.Store
// contract and its conformance suite.
package inboxtest

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/session"
)

// Map is inbox.Store over Go maps. The zero value is ready; Now stamps
// entries, nil selecting time.Now.
type Map struct {
	Now func() time.Time

	mu      sync.Mutex
	entries map[session.SessionID][]inbox.Entry
}

var _ inbox.Store = (*Map)(nil)

func (m *Map) now() int64 {
	if m.Now == nil {
		return time.Now().UnixMilli()
	}
	return m.Now().UnixMilli()
}

func (m *Map) Enqueue(_ context.Context, sid session.SessionID, c inbox.Command) (inbox.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[session.SessionID][]inbox.Entry)
	}
	for i := range m.entries[sid] {
		e := &m.entries[sid][i]
		if e.Command.ID != c.ID {
			continue
		}
		if e.Command.Kind != c.Kind || !e.Command.Payload.Equal(c.Payload) {
			return inbox.Entry{}, inbox.ErrCommandConflict
		}
		return clone(e), nil
	}
	e := inbox.Entry{Seq: uint64(len(m.entries[sid])), Command: c, EnqueuedAtUnixMilli: m.now()}
	m.entries[sid] = append(m.entries[sid], e)
	return clone(&e), nil
}

func (m *Map) Lookup(_ context.Context, sid session.SessionID, id inbox.CommandID) (inbox.Entry, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.entries[sid] {
		if m.entries[sid][i].Command.ID == id {
			return clone(&m.entries[sid][i]), true, nil
		}
	}
	return inbox.Entry{}, false, nil
}

func (m *Map) Pending(_ context.Context, sid session.SessionID) ([]inbox.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []inbox.Entry
	for i := range m.entries[sid] {
		if m.entries[sid][i].Pending() {
			out = append(out, clone(&m.entries[sid][i]))
		}
	}
	return out, nil
}

func (m *Map) Resolve(_ context.Context, sid session.SessionID, seq uint64, r inbox.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.entries[sid]
	if seq >= uint64(len(entries)) || !entries[seq].Pending() {
		return inbox.ErrNotPending
	}
	r.ResolvedAtUnixMilli = m.now()
	entries[seq].Result = &r
	return nil
}

func (m *Map) Sessions(_ context.Context, limit int) ([]session.SessionID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []session.SessionID
	for sid, entries := range m.entries {
		for i := range entries {
			if entries[i].Pending() {
				out = append(out, sid)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func clone(e *inbox.Entry) inbox.Entry {
	out := *e
	if e.Result != nil {
		r := *e.Result
		out.Result = &r
	}
	return out
}
