// Package checkpointtest holds the reference implementation of the
// checkpoint.Store contract and its conformance suite.
package checkpointtest

import (
	"context"
	"sync"

	"github.com/felinics/twilight/agentcore/checkpoint"
)

// Map is checkpoint.Store over a Go map; the zero value is ready.
type Map struct {
	mu   sync.Mutex
	next map[[2]string]uint64
}

var _ checkpoint.Store = (*Map)(nil)

// Load returns the consumer's position in ledger (checkpoint.Store).
func (m *Map) Load(_ context.Context, consumer, ledger string) (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	next, ok := m.next[[2]string{consumer, ledger}]
	return next, ok, nil
}

// Save moves the consumer's position forward (checkpoint.Store).
func (m *Map) Save(_ context.Context, consumer, ledger string, next uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.next == nil {
		m.next = make(map[[2]string]uint64)
	}
	k := [2]string{consumer, ledger}
	if current, ok := m.next[k]; ok && current > next {
		return checkpoint.ErrRewind
	}
	m.next[k] = next
	return nil
}
