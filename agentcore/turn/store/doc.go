// Package store is the transactional boundary of a Turn: the
// TurnStore port a Loop drives, the Snapshot a command sees, the CommitRequest
// and CommitResult of one command, the pure EvaluateCommit every adapter runs
// inside its own commit, and FoldTurn, the rebuild of a MachineState from a
// Turn's complete fact sequence.
package store
