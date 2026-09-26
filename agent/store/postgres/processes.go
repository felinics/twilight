package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// ProcessStore is process.Store over the process tables (RUN-EXE-15).
type ProcessStore struct{ d *DB }

var _ process.Store = (*ProcessStore)(nil)

// Processes is the dispatch ledger over this database.
func (d *DB) Processes() *ProcessStore { return &ProcessStore{d: d} }

func processLedger(ctx context.Context, q *db.Queries, k string, from ledger.CommitSeq) ([]ledger.Commit, ledger.Head, error) {
	rows, err := q.ProcessCommitsFrom(ctx, db.ProcessCommitsFromParams{Key: k, Seq: int64(from)}) //nolint:gosec // G115: seq values fit int64
	if err != nil {
		return nil, ledger.Head{}, err
	}
	commits := make([]ledger.Commit, 0, len(rows))
	for _, r := range rows {
		var c ledger.Commit
		if err := json.Unmarshal([]byte(r.Body), &c); err != nil {
			return nil, ledger.Head{}, fmt.Errorf("postgres: process commit %d: %w", r.Seq, err)
		}
		commits = append(commits, c)
	}
	last, err := q.ProcessHead(ctx, k)
	if err != nil {
		return nil, ledger.Head{}, err
	}
	return commits, ledger.Head{Next: ledger.CommitSeq(last + 1)}, nil //nolint:gosec // G115: -1 for an empty ledger yields 0
}

func foldProcess(commits []ledger.Commit) (process.State, error) {
	var state process.State
	for i := range commits {
		var err error
		if state, err = process.Fold(state, &commits[i]); err != nil {
			return process.State{}, err
		}
	}
	return state, nil
}

func (s *ProcessStore) Load(ctx context.Context, key effect.AssignmentKey) (process.State, ledger.Head, bool, error) {
	k, err := ledgerKey(key)
	if err != nil {
		return process.State{}, ledger.Head{}, false, err
	}
	commits, head, err := processLedger(ctx, s.d.q, k, 0)
	if err != nil || len(commits) == 0 {
		return process.State{}, ledger.Head{}, false, err
	}
	state, err := foldProcess(commits)
	if err != nil {
		return process.State{}, ledger.Head{}, false, err
	}
	state.Key = key
	return state, head, true, nil
}

func (s *ProcessStore) Read(ctx context.Context, key effect.AssignmentKey, from ledger.CommitSeq) ([]ledger.Commit, ledger.Head, error) {
	k, err := ledgerKey(key)
	if err != nil {
		return nil, ledger.Head{}, err
	}
	return processLedger(ctx, s.d.q, k, from)
}

func (s *ProcessStore) Append(ctx context.Context, epoch ledger.Epoch, key effect.AssignmentKey, c ledger.Commit) error { //nolint:gocritic // hugeParam: the Store contract takes the commit by value
	k, err := ledgerKey(key)
	if err != nil {
		return err
	}
	return s.d.tx(ctx, "process:"+k, func(q *db.Queries) error {
		if _, err := q.ProcessCommitSeq(ctx, db.ProcessCommitSeqParams{Key: k, CommitID: string(c.CommitID)}); err == nil {
			return ledger.ErrAlreadyApplied
		} else if !noRows(err) {
			return err
		}
		seen, err := q.ProcessEpoch(ctx, k)
		if err != nil && !noRows(err) {
			return err
		}
		if int64(epoch) < seen { //nolint:gosec // G115: epochs fit int64
			return ledger.ErrFenced
		}
		commits, head, err := processLedger(ctx, q, k, 0)
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
			if err := q.InsertProcess(ctx, db.InsertProcessParams{Key: k, AssignmentKey: string(keyJSON), Epoch: int64(epoch)}); err != nil { //nolint:gosec // G115: epochs fit int64
				return err
			}
		} else if err := q.RaiseProcessEpoch(ctx, db.RaiseProcessEpochParams{Epoch: int64(epoch), Key: k}); err != nil { //nolint:gosec // G115: epochs fit int64
			return err
		}
		err = q.InsertProcessCommit(ctx, db.InsertProcessCommitParams{Key: k, Seq: int64(c.Seq), CommitID: string(c.CommitID), Body: string(body)}) //nolint:gosec // G115: seq values fit int64
		if isUniqueViolation(err) {
			return ledger.ErrConflict
		}
		return err
	})
}
