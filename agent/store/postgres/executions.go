package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agentcore/es"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// ExecutionStore is executor/store.Store over the executions tables: every
// write holds the key's advisory lock, so any number of Workers over the
// database serialize per key (RUN-EXE-6).
type ExecutionStore struct{ d *DB }

var _ executionstore.Store = (*ExecutionStore)(nil)

// Executions is the Worker's execution record store over this database.
func (d *DB) Executions() *ExecutionStore { return &ExecutionStore{d: d} }

func ledgerKey(key effect.AssignmentKey) (string, error) {
	digest, err := es.DigestCanonical(key)
	if err != nil {
		return "", err
	}
	return string(digest), nil
}

// executionLedger reads the key's commits from Seq from and the ledger head.
func executionLedger(ctx context.Context, q *db.Queries, k string, from executionstore.CommitSeq) (commits []executionstore.Commit, head executionstore.Head, err error) {
	rows, err := q.ExecutionCommitsFrom(ctx, db.ExecutionCommitsFromParams{Key: k, Seq: int64(from)}) //nolint:gosec // G115: seq values fit int64
	if err != nil {
		return nil, executionstore.Head{}, err
	}
	commits = make([]executionstore.Commit, 0, len(rows))
	for _, r := range rows {
		var c executionstore.Commit
		if err := json.Unmarshal([]byte(r.Body), &c); err != nil {
			return nil, executionstore.Head{}, fmt.Errorf("postgres: execution commit %d: %w", r.Seq, err)
		}
		commits = append(commits, c)
	}
	last, err := q.ExecutionHead(ctx, k)
	if err != nil {
		return nil, executionstore.Head{}, err
	}
	return commits, executionstore.Head{Next: executionstore.CommitSeq(last + 1)}, nil //nolint:gosec // G115: -1 for an empty ledger yields 0
}

func foldExecution(commits []executionstore.Commit) (executionstore.ExecutionState, error) {
	var state executionstore.ExecutionState
	for i := range commits {
		var err error
		if state, err = executionstore.Fold(state, &commits[i]); err != nil {
			return executionstore.ExecutionState{}, err
		}
	}
	return state, nil
}

func executionLease(ctx context.Context, q *db.Queries, k string, key effect.AssignmentKey) (executionstore.Lease, bool, error) {
	row, err := q.ExecutionLease(ctx, k)
	if noRows(err) {
		return executionstore.Lease{}, false, nil
	}
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	return executionstore.Lease{Key: key, Owner: row.Owner, Epoch: executionstore.Epoch(row.Epoch), UntilUnixMilli: row.LeaseUntil}, true, nil //nolint:gosec // G115: epochs stored from a uint64
}

func loadExecution(ctx context.Context, q *db.Queries, k string, key effect.AssignmentKey) (exec executionstore.Execution, head executionstore.Head, ok bool, err error) {
	commits, head, err := executionLedger(ctx, q, k, 0)
	if err != nil || len(commits) == 0 {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	}
	state, err := foldExecution(commits)
	if err != nil {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	}
	exec = executionstore.Execution{ExecutionState: state}
	if lease, held, err := executionLease(ctx, q, k, key); err != nil {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	} else if held {
		exec.Lease = lease
	}
	return exec, head, true, nil
}

func (s *ExecutionStore) Load(ctx context.Context, key effect.AssignmentKey) (exec executionstore.Execution, head executionstore.Head, ok bool, err error) {
	k, err := ledgerKey(key)
	if err != nil {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	}
	return loadExecution(ctx, s.d.q, k, key)
}

func (s *ExecutionStore) Read(ctx context.Context, key effect.AssignmentKey, from executionstore.CommitSeq) (commits []executionstore.Commit, head executionstore.Head, err error) {
	k, err := ledgerKey(key)
	if err != nil {
		return nil, executionstore.Head{}, err
	}
	return executionLedger(ctx, s.d.q, k, from)
}

func appendExecutionCommit(ctx context.Context, q *db.Queries, k string, key effect.AssignmentKey, head executionstore.Head, c *executionstore.Commit) error {
	body, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if head.Next == 0 {
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return err
		}
		if err := q.InsertExecution(ctx, db.InsertExecutionParams{Key: k, AssignmentKey: string(keyJSON)}); err != nil {
			return err
		}
	}
	err = q.InsertExecutionCommit(ctx, db.InsertExecutionCommitParams{Key: k, Seq: int64(c.Seq), CommitID: string(c.CommitID), Body: string(body)}) //nolint:gosec // G115: seq values fit int64
	if isUniqueViolation(err) {
		return executionstore.ErrConflict
	}
	return err
}

func (s *ExecutionStore) Append(ctx context.Context, lease executionstore.Lease, key effect.AssignmentKey, c executionstore.Commit) error { //nolint:gocritic // hugeParam: the Store contract takes the commit by value
	k, err := ledgerKey(key)
	if err != nil {
		return err
	}
	return s.d.tx(ctx, "execution:"+k, func(q *db.Queries) error {
		if _, err := q.ExecutionCommitSeq(ctx, db.ExecutionCommitSeqParams{Key: k, CommitID: string(c.CommitID)}); err == nil {
			return executionstore.ErrAlreadyApplied
		} else if !noRows(err) {
			return err
		}
		commits, head, err := executionLedger(ctx, q, k, 0)
		if err != nil {
			return err
		}
		if c.Seq != head.Next {
			return executionstore.ErrConflict
		}
		state, err := foldExecution(commits)
		if err != nil {
			return err
		}
		if lease.IsZero() {
			for i := range c.Events {
				if executionstore.Fenced(c.Events[i].Type) {
					return fmt.Errorf("%w: %s requires the key's lease", executionstore.ErrLeaseLost, c.Events[i].Type)
				}
			}
		} else {
			current, held, err := executionLease(ctx, q, k, key)
			if err != nil {
				return err
			}
			if !held || current.Owner != lease.Owner || current.Epoch != lease.Epoch || current.UntilUnixMilli <= s.d.now().UnixMilli() {
				return executionstore.ErrLeaseLost
			}
		}
		if _, err := executionstore.Fold(state, &c); err != nil {
			return err
		}
		return appendExecutionCommit(ctx, q, k, key, head, &c)
	})
}

func (s *ExecutionStore) Acquire(ctx context.Context, key effect.AssignmentKey, owner string, ttl time.Duration) (executionstore.Lease, bool, error) {
	k, err := ledgerKey(key)
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	now := s.d.now()
	var out executionstore.Lease
	acquired := false
	err = s.d.tx(ctx, "execution:"+k, func(q *db.Queries) error {
		commits, head, err := executionLedger(ctx, q, k, 0)
		if err != nil {
			return err
		}
		if len(commits) == 0 {
			return effect.ErrExecutionNotFound
		}
		state, err := foldExecution(commits)
		if err != nil {
			return err
		}
		if state.Terminal() {
			return nil
		}
		current, held, err := executionLease(ctx, q, k, key)
		if err != nil {
			return err
		}
		expired := !held || current.UntilUnixMilli <= now.UnixMilli()
		if held && current.Owner != owner && !expired {
			return nil
		}
		epoch := current.Epoch
		if !held || current.Owner != owner || expired {
			epoch++
		}
		out = executionstore.Lease{Key: key, Owner: owner, Epoch: epoch, UntilUnixMilli: now.Add(ttl).UnixMilli()}
		if err := q.UpsertExecutionLease(ctx, db.UpsertExecutionLeaseParams{Key: k, Owner: owner, Epoch: int64(epoch), LeaseUntil: out.UntilUnixMilli}); err != nil { //nolint:gosec // G115: epochs fit int64
			return err
		}
		acquired = true
		if epoch == current.Epoch {
			return nil
		}
		ev, err := executionstore.NewEvent(executionstore.EventExecutionClaimed, now.UnixMilli(), executionstore.Claimed{Owner: owner, Epoch: epoch})
		if err != nil {
			return err
		}
		c := executionstore.Commit{Seq: head.Next, CommitID: executionstore.DeriveCommitID(key, "claim", fmt.Sprint(uint64(epoch))), Events: []executionstore.Event{ev}}
		if _, err := executionstore.Fold(state, &c); err != nil {
			return err
		}
		return appendExecutionCommit(ctx, q, k, key, head, &c)
	})
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	return out, acquired, nil
}

func (s *ExecutionStore) Renew(ctx context.Context, lease executionstore.Lease, ttl time.Duration) error {
	k, err := ledgerKey(lease.Key)
	if err != nil {
		return err
	}
	now := s.d.now()
	return s.d.tx(ctx, "execution:"+k, func(q *db.Queries) error {
		state, _, ok, err := loadExecution(ctx, q, k, lease.Key)
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		if state.Terminal() || state.Lease.Owner != lease.Owner || state.Lease.Epoch != lease.Epoch {
			return executionstore.ErrLeaseLost
		}
		return q.RenewExecutionLease(ctx, db.RenewExecutionLeaseParams{LeaseUntil: now.Add(ttl).UnixMilli(), Key: k})
	})
}

func (s *ExecutionStore) LeaseOf(ctx context.Context, key effect.AssignmentKey) (executionstore.Lease, bool, error) {
	k, err := ledgerKey(key)
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	present, err := s.d.q.ExecutionExists(ctx, k)
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	if !present {
		return executionstore.Lease{}, false, effect.ErrExecutionNotFound
	}
	return executionLease(ctx, s.d.q, k, key)
}

func (s *ExecutionStore) ListOwned(ctx context.Context, owner string) ([]effect.AssignmentKey, error) {
	raws, err := s.d.q.ExecutionsOwnedBy(ctx, owner)
	if err != nil {
		return nil, err
	}
	out := make([]effect.AssignmentKey, 0, len(raws))
	for _, raw := range raws {
		var key effect.AssignmentKey
		if err := json.Unmarshal([]byte(raw), &key); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, nil
}

// Seed writes the ledger executionstore.SeedCommits plans for state, with
// its lease row, for tests and operators; it is not part of the contract.
func (s *ExecutionStore) Seed(ctx context.Context, state executionstore.Execution) error { //nolint:gocritic // hugeParam: seeds the value
	key := state.Assignment.Key()
	k, err := ledgerKey(key)
	if err != nil {
		return err
	}
	commits, err := executionstore.SeedCommits(state, s.d.now().UnixMilli())
	if err != nil {
		return err
	}
	lease := state.Lease
	if state.State == effect.ExecutionAborted {
		lease = executionstore.Lease{}
	}
	return s.d.tx(ctx, "execution:"+k, func(q *db.Queries) error {
		if _, head, err := executionLedger(ctx, q, k, 0); err != nil {
			return err
		} else if head.Next != 0 {
			return fmt.Errorf("postgres: seed: %v already has a ledger", key)
		}
		head := executionstore.Head{}
		for i := range commits {
			if err := appendExecutionCommit(ctx, q, k, key, head, &commits[i]); err != nil {
				return err
			}
			head = executionstore.Head{Next: commits[i].Seq + 1}
		}
		if lease.Owner == "" {
			return nil
		}
		return q.InsertExecutionLease(ctx, db.InsertExecutionLeaseParams{Key: k, Owner: lease.Owner, Epoch: int64(lease.Epoch), LeaseUntil: lease.UntilUnixMilli}) //nolint:gosec // G115: epochs fit int64
	})
}
