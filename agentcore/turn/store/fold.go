package store

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
)

// FoldTurn rebuilds a MachineState from the complete fact sequence of one Turn
// in stream order: the first fact must be TurnStarted. No Decide,
// no effects, no replay.
func FoldTurn(facts []turn.Fact) (turn.MachineState, error) {
	if len(facts) == 0 {
		return turn.MachineState{}, errors.New("agent: fold: no facts")
	}
	if _, ok := facts[0].(turn.TurnStarted); !ok {
		return turn.MachineState{}, fmt.Errorf("agent: fold: first fact is %T, want TurnStarted", facts[0])
	}
	var state turn.MachineState
	for i, f := range facts {
		f, err := turn.SnapshotFact(f)
		if err != nil {
			return turn.MachineState{}, err
		}
		state, err = schema.Machine().Evolve(state, f)
		if err != nil {
			return turn.MachineState{}, fmt.Errorf("agent: fold: fact %d (%s): %w", i, schema.Wire().FactType(f), err)
		}
	}
	return state, nil
}
