package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// ProcessStore is process.Store over the processes and process_commits
// tables: one ledger per effect, the same commit rules as the execution
// ledger, and an epoch column that fences a superseded owner's reconciler.
type ProcessStore struct{ db *sql.DB }

var _ process.Store = (*ProcessStore)(nil)

// Processes is the process.Store over this database.
func (d *DB) Processes() *ProcessStore { return &ProcessStore{db: d.db} }

func readProcessCommits(ctx context.Context, q querier, k string, from ledger.CommitSeq) (commits []ledger.Commit, head ledger.Head, err error) {
	rows, err := q.QueryContext(ctx, `SELECT seq, body FROM process_commits WHERE key = ? AND seq >= ? ORDER BY seq`, k, uint64(from))
	if err != nil {
		return nil, ledger.Head{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var seq uint64
		var body string
		if err := rows.Scan(&seq, &body); err != nil {
			return nil, ledger.Head{}, err
		}
		var c ledger.Commit
		if err := json.Unmarshal([]byte(body), &c); err != nil {
			return nil, ledger.Head{}, fmt.Errorf("sqlite: process commit %d: %w", seq, err)
		}
		commits = append(commits, c)
		head = ledger.Head{Next: ledger.CommitSeq(seq) + 1}
	}
	if err := rows.Err(); err != nil {
		return nil, ledger.Head{}, err
	}
	if from > 0 {
		var n sql.NullInt64
		if err := q.QueryRowContext(ctx, `SELECT MAX(seq) FROM process_commits WHERE key = ?`, k).Scan(&n); err != nil {
			return nil, ledger.Head{}, err
		}
		if n.Valid && n.Int64 >= 0 {
			head = ledger.Head{Next: ledger.CommitSeq(n.Int64) + 1} //nolint:gosec // G115: seq is stored from a uint64 and checked non-negative
		}
	}
	return commits, head, nil
}

func foldProcess(commits []ledger.Commit) (process.State, error) {
	var state process.State
	for i := range commits {
		var err error
		state, err = process.Fold(state, &commits[i])
		if err != nil {
			return process.State{}, err
		}
	}
	return state, nil
}

// Load folds the key's process ledger (process.Store).
func (s *ProcessStore) Load(ctx context.Context, key effect.AssignmentKey) (state process.State, head ledger.Head, ok bool, err error) {
	k, err := ledgerKey(key)
	if err != nil {
		return process.State{}, ledger.Head{}, false, err
	}
	commits, head, err := readProcessCommits(ctx, s.db, k, 0)
	if err != nil || len(commits) == 0 {
		return process.State{}, ledger.Head{}, false, err
	}
	state, err = foldProcess(commits)
	if err != nil {
		return process.State{}, ledger.Head{}, false, err
	}
	state.Key = key
	return state, head, true, nil
}

// Read returns the key's process commits from Seq from (process.Store).
func (s *ProcessStore) Read(ctx context.Context, key effect.AssignmentKey, from ledger.CommitSeq) (commits []ledger.Commit, head ledger.Head, err error) {
	k, err := ledgerKey(key)
	if err != nil {
		return nil, ledger.Head{}, err
	}
	return readProcessCommits(ctx, s.db, k, from)
}

// Append commits c under the kernel's rules and the owner's epoch fence
// (process.Store).
func (s *ProcessStore) Append(ctx context.Context, epoch ledger.Epoch, key effect.AssignmentKey, c ledger.Commit) error { //nolint:gocritic // hugeParam: the Store contract takes the commit by value; it is persisted, never shared
	k, err := ledgerKey(key)
	if err != nil {
		return err
	}
	return tx(ctx, s.db, func(t *sql.Tx) error {
		var existingSeq uint64
		err := t.QueryRowContext(ctx, `SELECT seq FROM process_commits WHERE key = ? AND commit_id = ?`, k, string(c.CommitID)).Scan(&existingSeq)
		switch {
		case err == nil:
			return ledger.ErrAlreadyApplied
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		var seen uint64
		if err := t.QueryRowContext(ctx, `SELECT epoch FROM processes WHERE key = ?`, k).Scan(&seen); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if uint64(epoch) < seen {
			return ledger.ErrFenced
		}
		commits, head, err := readProcessCommits(ctx, t, k, 0)
		if err != nil {
			return err
		}
		if c.Seq != head.Next {
			return ledger.ErrConflict
		}
		state, err := foldProcess(commits)
		if err != nil {
			return err
		}
		if _, err := process.Fold(state, &c); err != nil {
			return err
		}
		body, err := json.Marshal(c)
		if err != nil {
			return err
		}
		if head.Next == 0 {
			keyJSON, err := json.Marshal(key)
			if err != nil {
				return err
			}
			if _, err := t.ExecContext(ctx, `INSERT INTO processes (key, assignment_key, epoch) VALUES (?, ?, ?)`, k, string(keyJSON), uint64(epoch)); err != nil {
				return err
			}
		}
		if _, err := t.ExecContext(ctx, `UPDATE processes SET epoch = MAX(epoch, ?) WHERE key = ?`, uint64(epoch), k); err != nil {
			return err
		}
		_, err = t.ExecContext(ctx, `INSERT INTO process_commits (key, seq, commit_id, body) VALUES (?, ?, ?, ?)`,
			k, uint64(c.Seq), string(c.CommitID), string(body))
		return err
	})
}
