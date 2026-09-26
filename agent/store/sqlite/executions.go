package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/es"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// ExecutionStore is executor/store.Store over three tables: executions
// names each ledger by the digest of its AssignmentKey, execution_commits
// holds the commits in Seq order, execution_leases holds the one lease row
// per key. Every write is one immediate transaction that reads the ledger,
// folds it, judges the commit and appends, so the store is the fence and
// the state machine's guard for any number of Workers over the same file
// (RUN-EXE-6). Nothing is ever deleted: an acknowledged ledger keeps
// answering for its key (RUN-EXE-13).
type ExecutionStore struct {
	db  *sql.DB
	now func() time.Time
}

var _ executionstore.Store = (*ExecutionStore)(nil)

func ledgerKey(key effect.AssignmentKey) (string, error) {
	d, err := es.DigestCanonical(key)
	if err != nil {
		return "", err
	}
	return string(d), nil
}

type querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readCommits(ctx context.Context, q querier, k string, from executionstore.CommitSeq) (commits []executionstore.Commit, head executionstore.Head, err error) {
	rows, err := q.QueryContext(ctx, `SELECT seq, body FROM execution_commits WHERE key = ? AND seq >= ? ORDER BY seq`, k, uint64(from))
	if err != nil {
		return nil, executionstore.Head{}, err
	}
	defer rows.Close()
	var out []executionstore.Commit
	for rows.Next() {
		var seq uint64
		var body string
		if err := rows.Scan(&seq, &body); err != nil {
			return nil, executionstore.Head{}, err
		}
		var c executionstore.Commit
		if err := json.Unmarshal([]byte(body), &c); err != nil {
			return nil, executionstore.Head{}, fmt.Errorf("sqlite: execution commit %d: %w", seq, err)
		}
		out = append(out, c)
		head = executionstore.Head{Next: executionstore.CommitSeq(seq) + 1}
	}
	if err := rows.Err(); err != nil {
		return nil, executionstore.Head{}, err
	}
	if from > 0 {
		// The head is the ledger's, not the slice's.
		var n sql.NullInt64
		if err := q.QueryRowContext(ctx, `SELECT MAX(seq) FROM execution_commits WHERE key = ?`, k).Scan(&n); err != nil {
			return nil, executionstore.Head{}, err
		}
		if n.Valid && n.Int64 >= 0 {
			head = executionstore.Head{Next: executionstore.CommitSeq(n.Int64) + 1} //nolint:gosec // G115: seq is stored from a uint64 and checked non-negative
		}
	}
	return out, head, nil
}

func fold(commits []executionstore.Commit) (executionstore.ExecutionState, error) {
	var state executionstore.ExecutionState
	for i := range commits {
		var err error
		state, err = executionstore.Fold(state, &commits[i])
		if err != nil {
			return executionstore.ExecutionState{}, err
		}
	}
	return state, nil
}

func readLease(ctx context.Context, q querier, k string, key effect.AssignmentKey) (executionstore.Lease, bool, error) {
	var owner string
	var epoch uint64
	var until int64
	err := q.QueryRowContext(ctx, `SELECT owner, epoch, lease_until FROM execution_leases WHERE key = ?`, k).Scan(&owner, &epoch, &until)
	if errors.Is(err, sql.ErrNoRows) {
		return executionstore.Lease{}, false, nil
	}
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	return executionstore.Lease{Key: key, Owner: owner, Epoch: executionstore.Epoch(epoch), UntilUnixMilli: until}, true, nil
}

func (s *ExecutionStore) loadIn(ctx context.Context, q querier, k string, key effect.AssignmentKey) (exec executionstore.Execution, head executionstore.Head, ok bool, err error) {
	commits, head, err := readCommits(ctx, q, k, 0)
	if err != nil {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	}
	if len(commits) == 0 {
		return executionstore.Execution{}, executionstore.Head{}, false, nil
	}
	folded, err := fold(commits)
	if err != nil {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	}
	exec = executionstore.Execution{ExecutionState: folded}
	if lease, ok, err := readLease(ctx, q, k, key); err != nil {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	} else if ok {
		exec.Lease = lease
	}
	return exec, head, true, nil
}

// Load folds the key's ledger and joins its lease (executionstore.Store).
func (s *ExecutionStore) Load(ctx context.Context, key effect.AssignmentKey) (exec executionstore.Execution, head executionstore.Head, ok bool, err error) {
	k, err := ledgerKey(key)
	if err != nil {
		return executionstore.Execution{}, executionstore.Head{}, false, err
	}
	return s.loadIn(ctx, s.db, k, key)
}

// Read returns the key's commits from Seq from (executionstore.Store).
func (s *ExecutionStore) Read(ctx context.Context, key effect.AssignmentKey, from executionstore.CommitSeq) (commits []executionstore.Commit, head executionstore.Head, err error) {
	k, err := ledgerKey(key)
	if err != nil {
		return nil, executionstore.Head{}, err
	}
	return readCommits(ctx, s.db, k, from)
}

func appendCommit(ctx context.Context, t *sql.Tx, k string, key effect.AssignmentKey, head executionstore.Head, c *executionstore.Commit) (executionstore.Head, error) {
	body, err := json.Marshal(c)
	if err != nil {
		return head, err
	}
	if head.Next == 0 {
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return head, err
		}
		if _, err := t.ExecContext(ctx, `INSERT INTO executions (key, assignment_key) VALUES (?, ?)`, k, string(keyJSON)); err != nil {
			return head, err
		}
	}
	_, err = t.ExecContext(ctx, `INSERT INTO execution_commits (key, seq, commit_id, body) VALUES (?, ?, ?, ?)`,
		k, uint64(c.Seq), string(c.CommitID), string(body))
	if err != nil {
		return head, err
	}
	return executionstore.Head{Next: c.Seq + 1}, nil
}

// Append commits c to the key's ledger (executionstore.Store).
func (s *ExecutionStore) Append(ctx context.Context, lease executionstore.Lease, key effect.AssignmentKey, c executionstore.Commit) error { //nolint:gocritic // hugeParam: the Store contract takes the commit by value; it is persisted, never shared
	k, err := ledgerKey(key)
	if err != nil {
		return err
	}
	return tx(ctx, s.db, func(t *sql.Tx) error {
		// Identity first: a replayed command is answered from the ledger
		// whatever its Seq says.
		var existingSeq uint64
		err := t.QueryRowContext(ctx, `SELECT seq FROM execution_commits WHERE key = ? AND commit_id = ?`, k, string(c.CommitID)).Scan(&existingSeq)
		switch {
		case err == nil:
			return executionstore.ErrAlreadyApplied
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		commits, head, err := readCommits(ctx, t, k, 0)
		if err != nil {
			return err
		}
		if c.Seq != head.Next {
			return executionstore.ErrConflict
		}
		state, err := fold(commits)
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
			current, ok, err := readLease(ctx, t, k, key)
			if err != nil {
				return err
			}
			if !ok || current.Owner != lease.Owner || current.Epoch != lease.Epoch || current.UntilUnixMilli <= s.now().UnixMilli() {
				return executionstore.ErrLeaseLost
			}
		}
		if _, err := executionstore.Fold(state, &c); err != nil {
			return err
		}
		_, err = appendCommit(ctx, t, k, key, head, &c)
		return err
	})
}

// Acquire takes the key's lease under a new Epoch (executionstore.Store).
func (s *ExecutionStore) Acquire(ctx context.Context, key effect.AssignmentKey, owner string, ttl time.Duration) (executionstore.Lease, bool, error) {
	k, err := ledgerKey(key)
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	now := s.now()
	var out executionstore.Lease
	acquired := false
	err = tx(ctx, s.db, func(t *sql.Tx) error {
		commits, head, err := readCommits(ctx, t, k, 0)
		if err != nil {
			return err
		}
		if len(commits) == 0 {
			return effect.ErrExecutionNotFound
		}
		state, err := fold(commits)
		if err != nil {
			return err
		}
		if state.Terminal() {
			return nil
		}
		current, held, err := readLease(ctx, t, k, key)
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
		if _, err := t.ExecContext(ctx, `INSERT INTO execution_leases (key, owner, epoch, lease_until) VALUES (?, ?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET owner = excluded.owner, epoch = excluded.epoch, lease_until = excluded.lease_until`,
			k, owner, uint64(epoch), out.UntilUnixMilli); err != nil {
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
		_, err = appendCommit(ctx, t, k, key, head, &c)
		return err
	})
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	return out, acquired, nil
}

// Renew extends the lease when it is still the key's (executionstore.Store).
func (s *ExecutionStore) Renew(ctx context.Context, lease executionstore.Lease, ttl time.Duration) error {
	k, err := ledgerKey(lease.Key)
	if err != nil {
		return err
	}
	now := s.now()
	return tx(ctx, s.db, func(t *sql.Tx) error {
		state, _, ok, err := s.loadIn(ctx, t, k, lease.Key)
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		if state.Terminal() || state.Lease.Owner != lease.Owner || state.Lease.Epoch != lease.Epoch {
			return executionstore.ErrLeaseLost
		}
		_, err = t.ExecContext(ctx, `UPDATE execution_leases SET lease_until = ? WHERE key = ?`, now.Add(ttl).UnixMilli(), k)
		return err
	})
}

// LeaseOf returns the key's lease row (executionstore.Store).
func (s *ExecutionStore) LeaseOf(ctx context.Context, key effect.AssignmentKey) (executionstore.Lease, bool, error) {
	k, err := ledgerKey(key)
	if err != nil {
		return executionstore.Lease{}, false, err
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM executions WHERE key = ?`, k).Scan(&exists); err != nil {
		return executionstore.Lease{}, false, err
	}
	if exists == 0 {
		return executionstore.Lease{}, false, effect.ErrExecutionNotFound
	}
	return readLease(ctx, s.db, k, key)
}

// ListOwned returns the keys whose lease row names owner (executionstore.Store).
func (s *ExecutionStore) ListOwned(ctx context.Context, owner string) ([]effect.AssignmentKey, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT e.assignment_key FROM execution_leases l JOIN executions e ON e.key = l.key WHERE l.owner = ? ORDER BY l.key`, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []effect.AssignmentKey
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var key effect.AssignmentKey
		if err := json.Unmarshal([]byte(raw), &key); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}

// List folds every ledger, for operators and tests; it is not part of
// executionstore.Store. A ledger that no longer folds is skipped, so one
// corrupt ledger does not hide the others.
func (s *ExecutionStore) List(ctx context.Context) ([]executionstore.Execution, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, assignment_key FROM executions ORDER BY key`)
	if err != nil {
		return nil, err
	}
	var keys []struct {
		k   string
		key effect.AssignmentKey
	}
	for rows.Next() {
		var k, raw string
		if err := rows.Scan(&k, &raw); err != nil {
			rows.Close()
			return nil, err
		}
		var key effect.AssignmentKey
		if err := json.Unmarshal([]byte(raw), &key); err != nil {
			continue
		}
		keys = append(keys, struct {
			k   string
			key effect.AssignmentKey
		}{k, key})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	var out []executionstore.Execution
	for _, e := range keys {
		state, _, ok, err := s.loadIn(ctx, s.db, e.k, e.key)
		if err != nil || !ok {
			continue
		}
		out = append(out, state)
	}
	return out, nil
}

// Seed writes the ledger executionstore.SeedCommits plans for state, with
// its lease row, for tests and operators. It is not part of
// executionstore.Store and refuses a key that already has a ledger.
func (s *ExecutionStore) Seed(ctx context.Context, state executionstore.Execution) error { //nolint:gocritic // hugeParam: seeds the value
	key := state.Assignment.Key()
	k, err := ledgerKey(key)
	if err != nil {
		return err
	}
	commits, err := executionstore.SeedCommits(state, s.now().UnixMilli())
	if err != nil {
		return err
	}
	lease := state.Lease
	if state.State == effect.ExecutionAborted {
		lease = executionstore.Lease{}
	}
	return tx(ctx, s.db, func(t *sql.Tx) error {
		if _, head, err := readCommits(ctx, t, k, 0); err != nil {
			return err
		} else if head.Next != 0 {
			return fmt.Errorf("sqlite: seed: %v already has a ledger", key)
		}
		head := executionstore.Head{}
		for i := range commits {
			var err error
			if head, err = appendCommit(ctx, t, k, key, head, &commits[i]); err != nil {
				return err
			}
		}
		if lease.Owner == "" {
			return nil
		}
		_, err := t.ExecContext(ctx, `INSERT INTO execution_leases (key, owner, epoch, lease_until) VALUES (?, ?, ?, ?)`, k, lease.Owner, uint64(lease.Epoch), lease.UntilUnixMilli)
		return err
	})
}
