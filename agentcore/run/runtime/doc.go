// Package runtime is the transactional boundary of a Run (RUN-CMT): the
// RunStore port a Loop drives, the Snapshot a command sees, the CommitRequest
// and CommitResult of one command, the pure EvaluateCommit every adapter runs
// inside its own commit, and FoldRun, the rebuild of a MachineState from a
// Run's complete fact sequence.
package runtime
