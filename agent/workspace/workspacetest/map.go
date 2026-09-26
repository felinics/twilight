// Package workspacetest holds the reference implementation of the
// workspace.Store contract and its conformance suite.
package workspacetest

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agent/workspace"
)

// Map is workspace.Store over Go maps; the zero value is ready.
type Map struct {
	mu         sync.Mutex
	workspaces map[workspace.ID]workspace.Workspace
	snapshots  map[workspace.SnapshotRef]workspace.Snapshot
}

var _ workspace.Store = (*Map)(nil)

func (m *Map) init() {
	if m.workspaces == nil {
		m.workspaces = make(map[workspace.ID]workspace.Workspace)
		m.snapshots = make(map[workspace.SnapshotRef]workspace.Snapshot)
	}
}

func clone(w workspace.Workspace) workspace.Workspace { //nolint:gocritic // hugeParam: copies on purpose
	if w.Snapshot != nil {
		s := *w.Snapshot
		w.Snapshot = &s
	}
	if w.Runtime != nil {
		r := *w.Runtime
		w.Runtime = &r
	}
	return w
}

func (m *Map) Create(_ context.Context, w workspace.Workspace) error { //nolint:gocritic // hugeParam: Store contract takes the value
	m.mu.Lock()
	defer m.mu.Unlock()
	m.init()
	if _, ok := m.workspaces[w.ID]; ok {
		return workspace.ErrExists
	}
	m.workspaces[w.ID] = clone(w)
	return nil
}

func (m *Map) Get(_ context.Context, id workspace.ID) (workspace.Workspace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workspaces[id]
	if !ok {
		return workspace.Workspace{}, workspace.ErrNotFound
	}
	return clone(w), nil
}

func (m *Map) Put(_ context.Context, w workspace.Workspace) error { //nolint:gocritic // hugeParam: Store contract takes the value
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.workspaces[w.ID]; !ok {
		return workspace.ErrNotFound
	}
	m.workspaces[w.ID] = clone(w)
	return nil
}

func (m *Map) UpdateRuntime(_ context.Context, id workspace.ID, expected uint64, binding workspace.RuntimeBinding) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workspaces[id]
	if !ok {
		return workspace.ErrNotFound
	}
	var current uint64
	if w.Runtime != nil {
		current = w.Runtime.Generation
	}
	if current != expected {
		return workspace.ErrGenerationConflict
	}
	w.Runtime = &binding
	m.workspaces[id] = w
	return nil
}

func (m *Map) UpdateSnapshot(_ context.Context, id workspace.ID, ref workspace.SnapshotRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, ok := m.workspaces[id]
	if !ok {
		return workspace.ErrNotFound
	}
	if snap, ok := m.snapshots[ref]; !ok || snap.Workspace != id {
		return workspace.ErrNotFound
	}
	w.Snapshot = &ref
	m.workspaces[id] = w
	return nil
}

func (m *Map) PutSnapshot(_ context.Context, s workspace.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.init()
	if _, ok := m.workspaces[s.Workspace]; !ok {
		return workspace.ErrNotFound
	}
	m.snapshots[s.Ref] = s
	return nil
}

func (m *Map) GetSnapshot(_ context.Context, ref workspace.SnapshotRef) (workspace.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.snapshots[ref]
	if !ok {
		return workspace.Snapshot{}, workspace.ErrNotFound
	}
	return s, nil
}

func (m *Map) Fork(_ context.Context, f workspace.Fork) (workspace.Workspace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	src, ok := m.workspaces[f.Source]
	if !ok {
		return workspace.Workspace{}, workspace.ErrNotFound
	}
	snap, ok := m.snapshots[f.Snapshot]
	if !ok || snap.Workspace != f.Source {
		return workspace.Workspace{}, workspace.ErrNotFound
	}
	if _, ok := m.workspaces[f.Destination]; ok {
		return workspace.Workspace{}, workspace.ErrExists
	}
	base := f.Base
	if base == "" {
		base = src.Base
	}
	ref := f.Snapshot
	dst := workspace.Workspace{ID: f.Destination, Project: src.Project, Base: base, Snapshot: &ref}
	m.workspaces[f.Destination] = dst
	return clone(dst), nil
}
