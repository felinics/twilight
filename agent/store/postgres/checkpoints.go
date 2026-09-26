package postgres

import (
	"context"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agentcore/checkpoint"
)

// CheckpointStore is checkpoint.Store over the checkpoints table.
type CheckpointStore struct{ d *DB }

var _ checkpoint.Store = (*CheckpointStore)(nil)

// Checkpoints is the checkpoint.Store over this database.
func (d *DB) Checkpoints() *CheckpointStore { return &CheckpointStore{d: d} }

func (s *CheckpointStore) Load(ctx context.Context, consumer, ledger string) (next uint64, ok bool, err error) {
	stored, err := s.d.q.Checkpoint(ctx, db.CheckpointParams{Consumer: consumer, Ledger: ledger})
	if noRows(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return uint64(stored), true, nil //nolint:gosec // G115: stored from a uint64
}

func (s *CheckpointStore) Save(ctx context.Context, consumer, ledger string, next uint64) error {
	return s.d.tx(ctx, "checkpoint:"+consumer+"/"+ledger, func(q *db.Queries) error {
		current, err := q.Checkpoint(ctx, db.CheckpointParams{Consumer: consumer, Ledger: ledger})
		switch {
		case noRows(err):
		case err != nil:
			return err
		case uint64(current) > next: //nolint:gosec // G115: stored from a uint64
			return checkpoint.ErrRewind
		}
		return q.UpsertCheckpoint(ctx, db.UpsertCheckpointParams{Consumer: consumer, Ledger: ledger, Next: int64(next)}) //nolint:gosec // G115: Seq values fit int64
	})
}
