package runtime

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/wire"
)

var (
	// ErrRunNotFound reports an operation addressed a RunID not in the Session.
	ErrRunNotFound = errors.New("agent: run not found")
)

// CheckContext avoids locking when cancellation already makes an operation
// inapplicable. Context is intentionally not retained by the Runtime.
func CheckContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("agent: runtime: nil context")
	}
	return ctx.Err()
}

// ErrOwnershipLost reports that the write capability behind a RunStore was
// superseded (RUN-CMT-6). It is terminal for the caller: no further command of
// this process can reach the stream.
var ErrOwnershipLost = errors.New("agent: run store ownership lost")

// RunStore is the transactional port of the Run core (RUN-CMT-1): the store
// the Runs of one Scope live in, already bound to the caller's write
// capability. It speaks only Run types; how a command reaches durable
// storage, how facts are encoded on the wire and how the bound capability
// fences a stale owner are the adapter's business (agent/session/run). Runs
// are created by the owning module's creation commit; there is no Create.
type RunStore interface {
	// Scope is the store this port is bound to.
	Scope() run.Scope
	// Load is the owner's view of a Run for the next command. A port whose
	// capability was superseded still answers from its own epoch's state and
	// is fenced at Commit (RUN-LOP-5).
	Load(context.Context, run.RunID) (Snapshot, error)
	// Commit evaluates one command (RUN-CMT-3) and appends its facts as one
	// atomic group. Replays are answered from the store's command index
	// without re-deciding (RUN-CMT-5).
	Commit(context.Context, CommitRequest) (CommitResult, error)
	// FrozenRequest returns the request body a Prepared or Executing ModelStep
	// names by RequestDigest (RUN-WIR-4); a missing body is frozen.ErrMissing.
	FrozenRequest(context.Context, run.Digest) (model.ModelRequest, error)
}

// Snapshot is one Run as a command sees it.
type Snapshot struct {
	// State is a detached in-process view.
	State run.MachineState
	// Position is the Run's last fact position at read time.
	Position run.RunPosition
}

type CommitRequest struct {
	// Base is the Position the caller loaded. PrepareModelRequest treats it
	// as a hard CAS; other commands rebase call-locally and may pass zero
	// (RUN-CMT-4).
	Base    run.RunPosition
	Command wire.CommandEnvelope
}

type CommitStatus uint8

const (
	CommitAccepted CommitStatus = iota
	CommitAlreadyApplied
)

type CommitResult struct {
	Status   CommitStatus
	Snapshot Snapshot
	// Facts are the Run facts the command produced, in stream order. On a
	// replay they are the facts of the original commit.
	Facts []run.Fact
}
