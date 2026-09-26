package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/felinics/twilight/agentcore/checkpoint"
)

// CheckpointStore is checkpoint.Store over the checkpoints table: one row
// per (consumer, ledger), moved forward in an immediate transaction so two
// instances of one consumer cannot move it backwards past each other.
type CheckpointStore struct{ db *sql.DB }

var _ checkpoint.Store = (*CheckpointStore)(nil)

// Checkpoints is the checkpoint.Store over this database.
func (d *DB) Checkpoints() *CheckpointStore { return &CheckpointStore{db: d.db} }

// Load returns the consumer's position in ledger (checkpoint.Store).
func (s *CheckpointStore) Load(ctx context.Context, consumer, ledger string) (next uint64, ok bool, err error) {
	var stored int64
	err = s.db.QueryRowContext(ctx, `SELECT next FROM checkpoints WHERE consumer = ? AND ledger = ?`, consumer, ledger).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint64(stored), true, nil //nolint:gosec // G115: stored from a uint64
}

// Save moves the consumer's position forward (checkpoint.Store).
func (s *CheckpointStore) Save(ctx context.Context, consumer, ledger string, next uint64) error {
	return tx(ctx, s.db, func(t *sql.Tx) error {
		var current int64
		err := t.QueryRowContext(ctx, `SELECT next FROM checkpoints WHERE consumer = ? AND ledger = ?`, consumer, ledger).Scan(&current)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return err
		case uint64(current) > next: //nolint:gosec // G115: stored from a uint64
			return checkpoint.ErrRewind
		}
		_, err = t.ExecContext(ctx, `INSERT INTO checkpoints (consumer, ledger, next) VALUES (?, ?, ?)
			ON CONFLICT(consumer, ledger) DO UPDATE SET next = excluded.next`, consumer, ledger, int64(next)) //nolint:gosec // G115: Seq values fit int64
		return err
	})
}
