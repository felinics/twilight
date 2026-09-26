package runtime

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/wire"
)

type DecisionKind uint8

const (
	DecisionApply DecisionKind = iota
	DecisionConflict
	DecisionStale
	DecisionTerminal
)

// CommitDecision is EvaluateCommit's verdict. The RunStore maps rejections
// onto the sentinel errors: Conflict -> ErrCommandConflict, Stale ->
// ErrStaleRuntime, Terminal -> ErrRunTerminal.
type CommitDecision struct {
	Kind     DecisionKind
	NewState run.MachineState
	Facts    []run.Fact
	// Reject carries the precondition failure for Conflict/Stale/Terminal.
	Reject error
}

// ValidateEnvelope is step 1 of RUN-CMT-3: identity. Envelopes are only
// built by wire.Codec.Envelope (RUN-WIR-3), so there is no per-commit
// self-verification of the command bytes.
func ValidateEnvelope(env *wire.CommandEnvelope) error {
	if env.RunID == "" || env.ID == "" {
		return errors.New("agent: commit: empty RunID or CommandID")
	}
	return nil
}

// EvaluateCommit is the pure evaluation every RunStore runs inside its
// store's critical section after the replay lookup (RUN-CMT-3 steps 4-8). Execution ownership is
// Session-level (RUN-CMT-6), so there is no per-target authorization: a
// command against a target whose state does not admit it is Stale.
//
//nolint:gocritic // hugeParam: public pure commit evaluator keeps state/request as value protocol inputs.
func EvaluateCommit(cur run.MachineState, position run.RunPosition, req CommitRequest) (CommitDecision, error) {
	env := req.Command
	if env.RunID != cur.RunID {
		return CommitDecision{}, fmt.Errorf("agent: commit: command run %q does not match authority run %q", env.RunID, cur.RunID)
	}
	if err := ValidateEnvelope(&env); err != nil {
		return CommitDecision{}, err
	}
	// The effect a start, settlement or recovery names is part of the
	// command identity (RUN-WIR-1): without it no CommandID derives.
	if op, effect, named := namedEffect(env.Command); named && effect == "" {
		return CommitDecision{Kind: DecisionConflict, Reject: fmt.Errorf("agent: commit: %s requires its effect identity", op)}, nil
	}
	// Derived-identity families must use their derived CommandID (RUN-WIR-3):
	// the derivation is the idempotency index, so a caller-minted random ID
	// cannot bypass duplicate detection.
	if err := checkDerivedCommandID(&env, req.Base, schema.Identity()); err != nil {
		return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
	}

	// Terminal absorbs non-duplicate commands (replay was handled before).
	if cur.Status.Terminal() {
		return CommitDecision{Kind: DecisionTerminal, Reject: run.ErrRunTerminal}, nil
	}

	// Base: PrepareModelRequest is the only hard-CAS command (RUN-CMT-4).
	if _, plan := env.Command.(run.PrepareModelRequest); plan && req.Base != position {
		return CommitDecision{Kind: DecisionStale, Reject: run.ErrStaleRuntime}, nil
	}

	// Step 7: Decide once, fold with Evolve.
	facts, err := schema.Machine().Decide(cur, env.Command)
	if err != nil {
		switch {
		case errors.Is(err, run.ErrRunTerminal):
			return CommitDecision{Kind: DecisionTerminal, Reject: err}, nil
		case errors.Is(err, run.ErrStaleRuntime):
			return CommitDecision{Kind: DecisionStale, Reject: err}, nil
		case errors.Is(err, run.ErrCommandConflict):
			return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
		default:
			// Precondition failures against the current state are stale from
			// the caller's perspective: reload and rederive.
			return CommitDecision{Kind: DecisionStale, Reject: err}, nil
		}
	}
	if cmd, ok := env.Command.(run.PrepareModelRequest); ok {
		if len(facts) == 0 {
			return CommitDecision{}, errors.New("agent: commit: prepare produced no facts")
		}
		if _, ok := facts[0].(run.ModelStepPrepared); !ok {
			return CommitDecision{}, errors.New("agent: commit: prepare did not produce ModelStepPrepared")
		}
		wantStep := schema.Identity().DeriveModelStepID(env.RunID, env.ID)
		if cmd.StepID != wantStep {
			return CommitDecision{Kind: DecisionStale, Reject: fmt.Errorf("prepare: StepID %q does not match derived StepID %q", cmd.StepID, wantStep)}, nil
		}
	}

	state := cur
	detached := make([]run.Fact, len(facts))
	for i, f := range facts {
		// Detach every fact before it is folded: Decide forwards fields from
		// the caller's command and the decision must not carry caller-owned
		// mutable objects across the Runtime boundary.
		f, err = run.SnapshotFact(f)
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
func namedEffect(cmd run.AgentCommand) (op string, effect run.EffectID, named bool) {
	switch c := cmd.(type) {
	case run.StartModelExecution:
		return "model start", c.Effect, true
	case run.StartToolCall:
		return "tool start", c.Effect, true
	case run.RecoverModelExecution:
		return "model recovery", c.Effect, true
	case run.SubmitModelResult:
		return "model result", c.Effect, true
	case run.SubmitModelFailure:
		return "model failure", c.Effect, true
	case run.RejectModelResult:
		return "model reject", c.Effect, true
	case run.SubmitToolResult:
		return "tool result", c.Effect, true
	case run.SubmitToolFailure:
		return "tool failure", c.Effect, true
	default:
		return "", "", false
	}
}

// checkDerivedCommandID enforces the derived-identity rules of RUN-WIR-3.
func checkDerivedCommandID(env *wire.CommandEnvelope, base run.RunPosition, id run.Identity) error {
	var want run.CommandID
	switch cmd := env.Command.(type) {
	case run.PrepareModelRequest:
		want = id.DeriveModelRequestCommandID(env.RunID, base)
	case run.AcceptInput:
		want = id.DeriveInputCommandID(env.RunID, cmd.InputIDs()...)
	case run.WithdrawPreparedStep:
		want = id.DeriveWithdrawCommandID(env.RunID, cmd.StepID)
	case run.ApproveToolCall:
		want = id.DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case run.RejectToolCall:
		want = id.DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case run.SubmitToolResponse:
		want = id.DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case run.StartModelExecution:
		want = id.DeriveStartCommandID(cmd.Effect)
	case run.StartToolCall:
		want = id.DeriveStartCommandID(cmd.Effect)
	case run.RecoverModelExecution:
		want = id.DeriveRecoveryCommandID(cmd.Effect)
	case run.SubmitModelResult:
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case run.SubmitModelFailure:
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case run.RejectModelResult:
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case run.SubmitToolResult:
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case run.SubmitToolFailure:
		// An Unknown tool failure is either the owner's settlement of a
		// delivered Outcome or a takeover disposition (RUN-CMT-7). The two
		// are distinct operations with their own identities; either closes
		// the effect once.
		if cmd.Outcome == run.ToolOutcomeUnknown && env.ID == id.DeriveRecoveryCommandID(cmd.Effect) {
			return nil
		}
		want = id.DeriveSettlementCommandID(cmd.Effect)
	case run.DeclineToolCall:
		want = id.DeriveDeclineCommandID(env.RunID, cmd.StepID, cmd.CallID)
	default:
		return nil
	}
	if want != "" && env.ID != want {
		return fmt.Errorf("agent: commit: %s requires its derived CommandID", env.Type)
	}
	return nil
}
