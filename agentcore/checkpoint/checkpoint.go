// Package checkpoint is the consumer's position in a ledger: the durable
// cursor of a catch-up subscription. A consumer reads a ledger from its
// checkpoint, acts, and saves the position it has processed through; after
// a crash it resumes from the saved position and sees every commit at least
// once. A consumer whose own state lives in the same store as its
// checkpoint may save both in one transaction and see each commit exactly
// once; every other consumer must be idempotent, which the ledgers' CommitID
// rule gives it for free.
package checkpoint

import "context"

// Store persists checkpoints by consumer and ledger. consumer names the
// reader (a projection, a relay, an observer); ledger names what it reads
// ("session/<id>", "execution/<key digest>"). Next is the first Seq the
// consumer has not processed.
type Store interface {
	// Load returns the consumer's Next for ledger; ok is false when the
	// consumer has never saved one, and the consumer starts at 0.
	Load(ctx context.Context, consumer, ledger string) (next uint64, ok bool, err error)
	// Save records Next. A Save that moves the checkpoint backwards is
	// refused with ErrRewind: a consumer never un-processes a commit.
	Save(ctx context.Context, consumer, ledger string, next uint64) error
}

// ErrRewind reports a Save below the stored checkpoint.
type rewindError struct{}

func (rewindError) Error() string { return "checkpoint: save below the stored position" }

// ErrRewind is returned by Save when next is below the stored Next.
var ErrRewind error = rewindError{}
