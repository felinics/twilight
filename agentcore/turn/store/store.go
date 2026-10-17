package store

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/wire"
)

var (
	// ErrTurnNotFound reports an operation addressed a TurnID not in the Session.
	ErrTurnNotFound = errors.New("agent: turn not found")
)

// CheckContext avoids locking when cancellation already makes an operation
// inapplicable. Context is intentionally not retained by the Runtime.
func CheckContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("agent: runtime: nil context")
	}
	return ctx.Err()
}

// ErrOwnershipLost reports that the write capability behind a TurnStore was
// superseded. It is terminal for the caller: no further command of
// this process can reach the stream.
var ErrOwnershipLost = errors.New("agent: turn store ownership lost")

// TurnStore is the transactional port of the Turn core: the store
// the TurnStore of one Scope live in, already bound to the caller's write
// capability. It speaks only Turn types; how a command reaches durable
// storage, how facts are encoded on the wire and how the bound capability
// fences a stale owner are the adapter's business (turn/sessionstore). TurnStore
// are created by the owning module's creation commit; there is no Create.
type TurnStore interface {
	// Scope is the store this port is bound to.
	Scope() turn.Scope
	// Load is the owner's view of a Turn for the next command. A port whose
	// capability was superseded still answers from its own epoch's state and
	// is fenced at Commit.
	Load(context.Context, turn.TurnID) (Snapshot, error)
	// Commit evaluates one command and appends its facts as one
	// atomic group. Replays are answered from the store's command index
	// without re-deciding.
	Commit(context.Context, CommitRequest) (CommitResult, error)
	// FrozenRequest returns the request body a Prepared or Executing ModelStep
	// names by RequestDigest; a missing body is frozen.ErrMissing.
	FrozenRequest(context.Context, turn.Digest) (model.ModelRequest, error)
}

// Snapshot is one Turn as a command sees it.
type Snapshot struct {
	// State is a detached in-process view.
	State turn.MachineState
	// Position is the Turn's last fact position at read time.
	Position turn.TurnPosition
}

type CommitRequest struct {
	// Base is the Position the caller loaded. PrepareModelRequest treats it
	// as a hard CAS; other commands rebase call-locally and may pass zero
	Base    turn.TurnPosition
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
	// Facts are the Turn facts the command produced, in stream order. On a
	// replay they are the facts of the original commit.
	Facts []turn.Fact
}
