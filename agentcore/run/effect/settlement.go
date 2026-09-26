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
//
// Sequence orders notices within one port incarnation, so a subscriber that
// reconnects can ask for the ones after the last it saw. It is not durable:
// a port that restarts starts a new incarnation (Epoch changes) and the
// subscriber treats the change as a gap, re-reading every key it waits on.
type Settlement struct {
	Key      AssignmentKey `json:"key"`
	Epoch    string        `json:"epoch"`
	Sequence uint64        `json:"sequence"`
}

// SettlementPort is the notification side of an ExecutionPort, an optional
// capability like ProgressPort. Settlements delivers to fn, in order, every
// Settlement the port records with Sequence greater than after in the
// incarnation named by epoch, then every new one as it happens, until fn
// returns false or ctx ends. An empty epoch, or one the port does not
// recognise, subscribes from the head of the current incarnation: the
// caller re-reads what it waits on (GetOutcome is a plain read) and relies
// on the stream only for what settles afterwards. The port keeps a bounded
// window of past Settlements; asking for a Sequence it has evicted is
// ErrSettlementsEvicted, and the caller re-reads as for an unknown epoch.
//
// One subscription per port covers every key that port settles, whoever
// dispatched them: the cost of waiting is per executor, not per effect.
type SettlementPort interface {
	Settlements(ctx context.Context, epoch string, after uint64, fn func(Settlement) bool) error
}

// ErrSettlementsEvicted reports a Settlements subscription that asked for a
// Sequence the port no longer holds: the subscriber must re-read the keys
// it waits on and subscribe again from the head.
var ErrSettlementsEvicted = errors.New("agent: effect: settlements before the requested sequence were evicted")
