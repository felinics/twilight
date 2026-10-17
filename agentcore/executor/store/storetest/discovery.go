package storetest

import (
	"context"
	"sort"

	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

var _ executionstore.Directory = (*Map)(nil)

func keyLess(a, b effect.AssignmentKey) bool {
	if a.Session != b.Session {
		return a.Session < b.Session
	}
	if a.OwnerKind != b.OwnerKind {
		return a.OwnerKind < b.OwnerKind
	}
	if a.OwnerID != b.OwnerID {
		return a.OwnerID < b.OwnerID
	}
	return a.Effect < b.Effect
}

// ListOutstanding joins each durable ledger fold/head with its lease under
// the same mutex as Append/Acquire. The lookahead row only sets More: it is
// not the cursor, so an exclusive resume cannot lose a page-boundary row.
func (m *Map) ListOutstanding(ctx context.Context, after effect.AssignmentKey, limit int) (effect.OutstandingPage, error) {
	if err := ctx.Err(); err != nil {
		return effect.OutstandingPage{}, err
	}
	if limit <= 0 {
		return effect.OutstandingPage{}, effect.ErrInvalidOutstandingLimit
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []effect.AssignmentKey
	for key, l := range m.ledgers {
		if len(l.commits) != 0 && keyLess(after, key) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keyLess(keys[i], keys[j]) })
	page := effect.OutstandingPage{Next: after}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return effect.OutstandingPage{}, err
		}
		l := m.ledgers[key]
		state, err := l.fold()
		if err != nil {
			return effect.OutstandingPage{}, err
		}
		if state.Aborted() || state.Acknowledged {
			continue
		}
		if len(page.Items) == limit {
			page.More = true
			break
		}
		summary := effect.ExecutionSummary{Key: key, Status: state.State, Revision: uint64(l.head().Next)}
		if l.held {
			summary.Owner, summary.LeaseUntilUnixMilli = l.lease.Owner, l.lease.UntilUnixMilli
		}
		page.Items = append(page.Items, summary)
		page.Next = key
	}
	return page, nil
}
