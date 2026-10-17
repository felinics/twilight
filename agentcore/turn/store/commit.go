package store

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/wire"
)

type DecisionKind uint8

const (
	DecisionApply DecisionKind = iota
	DecisionConflict
	DecisionStale
	DecisionTerminal
)

// CommitDecision is EvaluateCommit's verdict. The TurnStore maps rejections
// onto the sentinel errors: Conflict -> ErrCommandConflict, Stale ->
// ErrStaleRuntime, Terminal -> ErrTurnTerminal.
type CommitDecision struct {
	Kind     DecisionKind
	NewState turn.MachineState
	Facts    []turn.Fact
	// Reject carries the precondition failure for Conflict/Stale/Terminal.
	Reject error
}

// ValidateEnvelope checks command identity. Envelopes are only
// built by wire.Codec.Envelope, so there is no per-commit
// self-verification of the command bytes.
func ValidateEnvelope(env *wire.CommandEnvelope) error {
	if env.TurnID == "" || env.ID == "" {
		return errors.New("agent: commit: empty TurnID or CommandID")
	}
	return nil
}

// EvaluateCommit is the pure evaluation every TurnStore runs inside its
// store's critical section after the replay lookup (steps 4-8). Execution ownership is
// Session-level, so there is no per-target authorization: a
// command against a target whose state does not admit it is Stale.
//
//nolint:gocritic // hugeParam: public pure commit evaluator keeps state/request as value protocol inputs.
func EvaluateCommit(cur turn.MachineState, position turn.TurnPosition, req CommitRequest) (CommitDecision, error) {
	env := req.Command
	if env.TurnID != cur.TurnID {
		return CommitDecision{}, fmt.Errorf("agent: commit: command turn %q does not match authority turn %q", env.TurnID, cur.TurnID)
	}
	if err := ValidateEnvelope(&env); err != nil {
		return CommitDecision{}, err
	}
	// The effect a start, settlement or recovery names is part of the
	// command identity: without it no CommandID derives.
	if op, effect, named := namedEffect(env.Command); named && effect == "" {
		return CommitDecision{Kind: DecisionConflict, Reject: fmt.Errorf("agent: commit: %s requires its effect identity", op)}, nil
	}
	// Derived-identity families must use their derived CommandID:
	// the derivation is the idempotency index, so a caller-minted random ID
	// cannot bypass duplicate detection.
	if err := checkDerivedCommandID(&env, req.Base, schema.Identity()); err != nil {
		return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
	}

	// Terminal absorbs non-duplicate commands (replay was handled before).
	if cur.Status.Terminal() {
		return CommitDecision{Kind: DecisionTerminal, Reject: turn.ErrTurnTerminal}, nil
	}

	// Base: PrepareModelRequest is the only hard-CAS command.
	if _, plan := env.Command.(turn.PrepareModelRequest); plan && req.Base != position {
		return CommitDecision{Kind: DecisionStale, Reject: turn.ErrStaleRuntime}, nil
	}

	// Step 7: Decide once, fold with Evolve.
	facts, err := schema.Machine().Decide(cur, env.Command)
	if err != nil {
		switch {
		case errors.Is(err, turn.ErrTurnTerminal):
			return CommitDecision{Kind: DecisionTerminal, Reject: err}, nil
		case errors.Is(err, turn.ErrStaleRuntime):
			return CommitDecision{Kind: DecisionStale, Reject: err}, nil
		case errors.Is(err, turn.ErrCommandConflict):
			return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
		default:
			// Precondition failures against the current state are stale from
			// the caller's perspective: reload and rederive.
			return CommitDecision{Kind: DecisionStale, Reject: err}, nil
		}
	}
	if cmd, ok := env.Command.(turn.PrepareModelRequest); ok {
		if len(facts) == 0 {
			return CommitDecision{}, errors.New("agent: commit: prepare produced no facts")
		}
		if _, ok := facts[0].(turn.ModelStepPrepared); !ok {
			return CommitDecision{}, errors.New("agent: commit: prepare did not produce ModelStepPrepared")
		}
		wantStep := schema.Identity().DeriveModelStepID(env.TurnID, env.ID)
		if cmd.StepID != wantStep {
			return CommitDecision{Kind: DecisionStale, Reject: fmt.Errorf("prepare: StepID %q does not match derived StepID %q", cmd.StepID, wantStep)}, nil
		}
	}

	state := cur
	detached := make([]turn.Fact, len(facts))
	for i, f := range facts {
		// Detach every fact before it is folded: Decide forwards fields from
		// the caller's command and the decision must not carry caller-owned
		// mutable objects across the Runtime boundary.
		f, err = turn.SnapshotFact(f)
		if err != nil {
			return CommitDecision{}, err
		}
		state, err = schema.Machine().Evolve(state, f)
		if err != nil {
			return CommitDecision{}, err
		}
		detached[i] = f
	}
	return CommitDecision{Kind: DecisionApply, NewState: state, Facts: detached}, nil
}

// namedEffect reports the effect a command names and the operation a
// rejection reports it under; named is false for commands whose identity
// does not derive from an effect.
func namedEffect(cmd turn.AgentCommand) (op string, effect turn.EffectID, named bool) {
	switch c := cmd.(type) {
	case turn.StartModelExecution:
		return "model start", c.Effect, true
	case turn.StartToolCall:
		return "tool start", c.Effect, true
	case turn.RecoverModelExecution:
		return "model recovery", c.Effect, true
	case turn.SubmitModelResult:
		return "model result", c.Effect, true
	case turn.SubmitModelFailure:
		return "model failure", c.Effect, true
	case turn.RejectModelResult:
		return "model reject", c.Effect, true
	case turn.SubmitToolResult:
		return "tool result", c.Effect, true
	case turn.SubmitToolFailure:
		return "tool failure", c.Effect, true
	default:
		return "", "", false
	}
}

// checkDerivedCommandID enforces the derived-identity rules.
func checkDerivedCommandID(env *wire.CommandEnvelope, base turn.TurnPosition, id turn.Identity) error {
	var want turn.CommandID
	switch cmd := env.Command.(type) {
	case turn.PrepareModelRequest:
		want = id.DeriveModelRequestCommandID(env.TurnID, base)
	case turn.AcceptInput:
		want = id.DeriveInputCommandID(env.TurnID, cmd.InputIDs()...)
	case turn.WithdrawPreparedStep:
		want = id.DeriveWithdrawCommandID(env.TurnID, cmd.StepID)
	case turn.ApproveToolCall:
		want = id.DeriveResponseCommandID(env.TurnID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case turn.RejectToolCall:
		want = id.DeriveResponseCommandID(env.TurnID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case turn.SubmitToolResponse:
		want = id.DeriveResponseCommandID(env.TurnID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case turn.StartModelExecution:
		want = id.DeriveStartCommandID(cmd.Effect)
	case turn.StartToolCall:
		want = id.DeriveStartCommandID(cmd.Effect)
	case turn.RecoverModelExecution:
		want = id.DeriveRecoveryCommandID(cmd.Effect)
	case turn.SubmitModelResult:
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case turn.SubmitModelFailure:
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case turn.RejectModelResult:
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case turn.SubmitToolResult:
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case turn.SubmitToolFailure:
		// An Unknown tool failure is either the owner's settlement of a
		// delivered Outcome or a takeover disposition. The two
		// are distinct operations with their own identities; either closes
		// the effect once.
		if cmd.Outcome == turn.ToolOutcomeUnknown && env.ID == id.DeriveRecoveryCommandID(cmd.Effect) {
			return nil
		}
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case turn.DeclineToolCall:
		want = id.DeriveDeclineCommandID(env.TurnID, cmd.StepID, cmd.CallID)
	default:
		return nil
	}
	if want != "" && env.ID != want {
		return fmt.Errorf("agent: commit: %s requires its derived CommandID", env.Type)
	}
	return nil
}
