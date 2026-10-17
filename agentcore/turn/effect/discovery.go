package effect

import (
	"context"
	"errors"
)

var (
	// ErrOutstandingUnsupported means the execution store has no durable
	// outstanding directory. It is a capability error, never an empty scan.
	ErrOutstandingUnsupported = errors.New("agent: effect: outstanding discovery unsupported")
	// ErrInvalidOutstandingLimit reports a non-positive page size.
	ErrInvalidOutstandingLimit = errors.New("agent: effect: outstanding limit must be positive")
)

// ExecutionSummary is a read-only management view of one durable execution.
// It contains no provider binding or physical backend handle. Revision is the
// execution ledger's durable head (the next commit sequence), not a lease
// epoch, wall clock, or status counter. Owner and LeaseUntilUnixMilli are zero
// when the execution has never been claimed; expired leases remain visible.
// Lease expiry is only a reason to probe/reconcile: it proves neither that an
// effect did not happen nor that replay is safe.
type ExecutionSummary struct {
	Key                 AssignmentKey
	Status              ExecutionStatus
	Revision            uint64
	Owner               string
	LeaseUntilUnixMilli int64
}

// OutstandingPage is a keyset page. Next is the last returned key, or after
// for an empty page. More means another outstanding key existed at read time;
// the lookahead key is not consumed. Resume with Next, exclusively.
type OutstandingPage struct {
	Items []ExecutionSummary
	Next  AssignmentKey
	More  bool
}

// OutstandingPort is the optional execution management plane, deliberately
// separate from the per-key ExecutionPort. ListOutstanding returns every
// nonterminal acceptance (including never-claimed ones), and every terminal
// execution whose Outcome has not been acknowledged. Aborted tombstones and
// acknowledged executions are excluded. Outcomes must remain readable until
// safe acknowledgement; time or an expired lease is not settlement proof.
//
// Keys are sorted lexicographically by the tuple (Session, OwnerKind, OwnerID, Effect),
// not by a concatenated encoding. after is exclusive; its zero value starts
// a scan. limit must be positive. Each page reflects durable state at its
// read, not a snapshot spanning calls: concurrent new keys behind the cursor
// are found on the next full scan. Discovery never authorizes blind replay
// of an external effect with unknown outcome and promises no exactly-once
// execution.
type OutstandingPort interface {
	ListOutstanding(ctx context.Context, after AssignmentKey, limit int) (OutstandingPage, error)
}
