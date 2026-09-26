// Package storetest holds the reference implementation of the execution
// store contract and the conformance suite every adapter runs. Map keeps
// the ledgers in memory under one mutex: it is the oracle the suite is
// written against and the store the kernel's own tests run over, so no
// test of the executor depends on a deployment's adapter.
package storetest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// Map is executionstore.Store over Go maps. The zero value is ready; Now is
// the lease clock, nil selecting time.Now. Every method holds one mutex for
// its whole read-fold-judge-write, which is the transaction the contract
// requires (RUN-EXE-6).
type Map struct {
	Now func() time.Time

	mu      sync.Mutex
	ledgers map[effect.AssignmentKey]*mapLedger
}

type mapLedger struct {
	commits []executionstore.Commit
	lease   executionstore.Lease
	held    bool
}

var _ executionstore.Store = (*Map)(nil)

// NewMap returns a Map whose leases are judged by now.
func NewMap(now func() time.Time) *Map { return &Map{Now: now} }

func (m *Map) now() time.Time {
	if m.Now == nil {
		return time.Now()
	}
	return m.Now()
}

func (m *Map) ledger(key effect.AssignmentKey) *mapLedger {
	if m.ledgers == nil {
		m.ledgers = make(map[effect.AssignmentKey]*mapLedger)
	}
	l, ok := m.ledgers[key]
	if !ok {
		l = &mapLedger{}
		m.ledgers[key] = l
	}
	return l
}

func (l *mapLedger) head() executionstore.Head {
	return executionstore.Head{Next: executionstore.CommitSeq(len(l.commits))}
}

func (l *mapLedger) fold() (executionstore.ExecutionState, error) {
	var state executionstore.ExecutionState
	for i := range l.commits {
		var err error
		if state, err = executionstore.Fold(state, &l.commits[i]); err != nil {
			return executionstore.ExecutionState{}, err
		}
	}
	return state, nil
}

func (l *mapLedger) applied(id executionstore.CommitID) bool {
	for i := range l.commits {
		if l.commits[i].CommitID == id {
			return true
		}
	}
	return false
}

func (l *mapLedger) execution() (executionstore.Execution, error) {
	state, err := l.fold()
	if err != nil {
		return executionstore.Execution{}, err
	}
	exec := executionstore.Execution{ExecutionState: state}
	if l.held {
		exec.Lease = l.lease
	}
	return exec, nil
}

// Load folds the key's ledger and joins its lease (executionstore.Store).
func (m *Map) Load(_ context.Context, key effect.AssignmentKey) (executionstore.Execution, executionstore.Head, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.ledgers[key]
	if !ok || len(l.commits) == 0 {
		return executionstore.Execution{}, executionstore.Head{}, false, nil
	}
	exec, err := l.execution()
	if err != nil {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	}
	return exec, l.head(), true, nil
}

// Read returns the key's commits from Seq from (executionstore.Store).
func (m *Map) Read(_ context.Context, key effect.AssignmentKey, from executionstore.CommitSeq) ([]executionstore.Commit, executionstore.Head, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.ledgers[key]
	if !ok {
		return nil, executionstore.Head{}, nil
	}
	if int(from) >= len(l.commits) {
		return nil, l.head(), nil
	}
	out := make([]executionstore.Commit, len(l.commits)-int(from))
	copy(out, l.commits[from:])
	return out, l.head(), nil
}

// Append commits c to the key's ledger (executionstore.Store).
func (m *Map) Append(_ context.Context, lease executionstore.Lease, key effect.AssignmentKey, c executionstore.Commit) error { //nolint:gocritic // hugeParam: the Store contract takes the commit by value
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.ledger(key)
	if l.applied(c.CommitID) {
		return executionstore.ErrAlreadyApplied
	}
	if c.Seq != l.head().Next {
		return executionstore.ErrConflict
	}
	state, err := l.fold()
	if err != nil {
		return err
	}
	if lease.IsZero() {
		for i := range c.Events {
			if executionstore.Fenced(c.Events[i].Type) {
				return fmt.Errorf("%w: %s requires the key's lease", executionstore.ErrLeaseLost, c.Events[i].Type)
			}
		}
	} else if !l.held || l.lease.Owner != lease.Owner || l.lease.Epoch != lease.Epoch || l.lease.UntilUnixMilli <= m.now().UnixMilli() {
		return executionstore.ErrLeaseLost
	}
	if _, err := executionstore.Fold(state, &c); err != nil {
		return err
	}
	l.commits = append(l.commits, c)
	return nil
}

// Acquire takes the key's lease under a new Epoch (executionstore.Store).
func (m *Map) Acquire(_ context.Context, key effect.AssignmentKey, owner string, ttl time.Duration) (executionstore.Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.ledgers[key]
	if !ok || len(l.commits) == 0 {
		return executionstore.Lease{}, false, effect.ErrExecutionNotFound
	}
	state, err := l.fold()
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	if state.Terminal() {
		return executionstore.Lease{}, false, nil
	}
	now := m.now()
	expired := !l.held || l.lease.UntilUnixMilli <= now.UnixMilli()
	if l.held && l.lease.Owner != owner && !expired {
		return executionstore.Lease{}, false, nil
	}
	epoch := l.lease.Epoch
	if !l.held || l.lease.Owner != owner || expired {
		epoch++
	}
	out := executionstore.Lease{Key: key, Owner: owner, Epoch: epoch, UntilUnixMilli: now.Add(ttl).UnixMilli()}
	if epoch != l.lease.Epoch {
		ev, err := executionstore.NewEvent(executionstore.EventExecutionClaimed, now.UnixMilli(), executionstore.Claimed{Owner: owner, Epoch: epoch})
		if err != nil {
			return executionstore.Lease{}, false, err
		}
		c := executionstore.Commit{Seq: l.head().Next, CommitID: executionstore.DeriveCommitID(key, "claim", fmt.Sprint(uint64(epoch))), Events: []executionstore.Event{ev}}
		if _, err := executionstore.Fold(state, &c); err != nil {
			return executionstore.Lease{}, false, err
		}
		l.commits = append(l.commits, c)
	}
	l.lease, l.held = out, true
	return out, true, nil
}

// Renew extends the lease when it is still the key's (executionstore.Store).
func (m *Map) Renew(_ context.Context, lease executionstore.Lease, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.ledgers[lease.Key]
	if !ok || len(l.commits) == 0 {
		return effect.ErrExecutionNotFound
	}
	state, err := l.fold()
	if err != nil {
		return err
	}
	if state.Terminal() || !l.held || l.lease.Owner != lease.Owner || l.lease.Epoch != lease.Epoch {
		return executionstore.ErrLeaseLost
	}
	l.lease.UntilUnixMilli = m.now().Add(ttl).UnixMilli()
	return nil
}

// LeaseOf returns the key's lease (executionstore.Store).
func (m *Map) LeaseOf(_ context.Context, key effect.AssignmentKey) (executionstore.Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.ledgers[key]
	if !ok || len(l.commits) == 0 {
		return executionstore.Lease{}, false, effect.ErrExecutionNotFound
	}
	if !l.held {
		return executionstore.Lease{}, false, nil
	}
	return l.lease, true, nil
}

// ListOwned returns the keys whose lease names owner (executionstore.Store),
// in a stable order.
func (m *Map) ListOwned(_ context.Context, owner string) ([]effect.AssignmentKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []effect.AssignmentKey
	for key, l := range m.ledgers {
		if l.held && l.lease.Owner == owner {
			out = append(out, key)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Session != b.Session {
			return a.Session < b.Session
		}
		if a.RunID != b.RunID {
			return a.RunID < b.RunID
		}
		return a.Effect < b.Effect
	})
	return out, nil
}

// Seed writes the ledger executionstore.SeedCommits plans for state, with
// its lease, for tests; it refuses a key that already has a ledger. Seed is
// not part of executionstore.Store.
func (m *Map) Seed(_ context.Context, state executionstore.Execution) error { //nolint:gocritic // hugeParam: seeds the value
	commits, err := executionstore.SeedCommits(state, m.now().UnixMilli())
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := state.Assignment.Key()
	l := m.ledger(key)
	if len(l.commits) != 0 {
		return fmt.Errorf("storetest: seed: %v already has a ledger", key)
	}
	l.commits = commits
	if state.State != effect.ExecutionAborted && state.Lease.Owner != "" {
		l.lease, l.held = state.Lease, true
		l.lease.Key = key
	}
	return nil
}
