// Package processtest holds the reference implementation of the dispatch
// ledger contract (process.Store) and its conformance suite.
package processtest

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// Map is process.Store over Go maps; the zero value is ready.
type Map struct {
	mu      sync.Mutex
	ledgers map[effect.AssignmentKey]*mapLedger
}

type mapLedger struct {
	commits []ledger.Commit
	// epoch is the highest Epoch that has written this ledger.
	epoch ledger.Epoch
}

var _ process.Store = (*Map)(nil)

func (l *mapLedger) head() ledger.Head { return ledger.Head{Next: ledger.CommitSeq(len(l.commits))} }

func (l *mapLedger) fold() (process.State, error) {
	var state process.State
	for i := range l.commits {
		var err error
		if state, err = process.Fold(state, &l.commits[i]); err != nil {
			return process.State{}, err
		}
	}
	return state, nil
}

// Load folds the key's ledger (process.Store).
func (m *Map) Load(_ context.Context, key effect.AssignmentKey) (process.State, ledger.Head, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.ledgers[key]
	if !ok || len(l.commits) == 0 {
		return process.State{}, ledger.Head{}, false, nil
	}
	state, err := l.fold()
	if err != nil {
		return process.State{}, ledger.Head{}, false, err
	}
	state.Key = key
	return state, l.head(), true, nil
}

// Read returns the key's commits from Seq from (process.Store).
func (m *Map) Read(_ context.Context, key effect.AssignmentKey, from ledger.CommitSeq) ([]ledger.Commit, ledger.Head, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.ledgers[key]
	if !ok {
		return nil, ledger.Head{}, nil
	}
	if int(from) >= len(l.commits) {
		return nil, l.head(), nil
	}
	out := make([]ledger.Commit, len(l.commits)-int(from))
	copy(out, l.commits[from:])
	return out, l.head(), nil
}

// Append commits c under the kernel's rules and the owner's epoch fence
// (process.Store).
func (m *Map) Append(_ context.Context, epoch ledger.Epoch, key effect.AssignmentKey, c ledger.Commit) error { //nolint:gocritic // hugeParam: the Store contract takes the commit by value
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ledgers == nil {
		m.ledgers = make(map[effect.AssignmentKey]*mapLedger)
	}
	l, ok := m.ledgers[key]
	if !ok {
		l = &mapLedger{}
		m.ledgers[key] = l
	}
	for i := range l.commits {
		if l.commits[i].CommitID == c.CommitID {
			return ledger.ErrAlreadyApplied
		}
	}
	if epoch < l.epoch {
		return ledger.ErrFenced
	}
	if c.Seq != l.head().Next {
		return ledger.ErrConflict
	}
	state, err := l.fold()
	if err != nil {
		return err
	}
	if _, err := process.Fold(state, &c); err != nil {
		return err
	}
	l.commits = append(l.commits, c)
	l.epoch = epoch
	return nil
}
