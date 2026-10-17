package effect

import (
	"context"
	"errors"
)

// Settlement is the executor's notice that the execution of Key reached a
// terminal state and its Outcome is readable through GetOutcome. It carries
// no Outcome: the notice says when to read, the read says what. A caller
// that missed a notice (it was not subscribed, its stream dropped) loses
// nothing it cannot recover by reading, so a subscriber and a reader agree
// without any coordination between them.
// Sequence orders notices within the Epoch. A process-local port may change
// Epoch on restart and keep only a bounded window; a shared durable port may
// keep one stable Epoch and replay from its cursor across any replica or
// restart.
type Settlement struct {
	Key      AssignmentKey `json:"key"`
	Epoch    string        `json:"epoch"`
	Sequence uint64        `json:"sequence"`
}

// SettlementPort is the notification side of an ExecutionPort, an optional
// capability like ProgressPort. Settlements delivers to fn, in order, every
// Settlement with Sequence greater than after in the stream named by epoch,
// then every new one as it happens, until fn returns false or ctx ends. A
// durable implementation replays from its shared cursor across replicas and
// restarts; an in-process implementation may start a new Epoch and ask the
// caller to re-read. One subscription per port covers every key that port
// settles, whoever dispatched them: the cost of waiting is per executor, not
// per effect.
type SettlementPort interface {
	Settlements(ctx context.Context, epoch string, after uint64, fn func(Settlement) bool) error
}

// ErrSettlementsEvicted reports a Settlements subscription that asked for a
// Sequence the port no longer holds: the subscriber must re-read the keys
// it waits on and subscribe again from the head.
var ErrSettlementsEvicted = errors.New("agent: effect: settlements before the requested sequence were evicted")
