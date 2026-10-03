package run

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run/model"
)

// Canonical and Identity are the two rule sets the Machine interprets under.
// Decide and Evolve never compute a digest or derive an identity themselves:
// every persisted digest and every RunID-scoped identity comes from the bound
// rules, so a Run replays under the rules of the schema version it was written
// under whatever the current version is.

// Canonical is the digest rules the state machine is composed with; the
// implementation lives in agentcore/run/canonical, which imports this
// package, so the seam is an interface (composed in agentcore/run/schema).
// It is the digest rules for every body a fact names.
type Canonical interface {
	DigestRequest(model.ModelRequest) (Digest, error)
	DigestToolDefinition(model.ToolDefinition) (Digest, error)
	DigestToolResponseDecision(ResponseKind, ResponseDecision, string) (Digest, error)
	DigestToolResponsePayload(CanonicalJSON) (Digest, error)
	// DigestModelResult names a frozen model result (ModelStepCompleted.ResultDigest).
	DigestModelResult(model.ModelResult) (Digest, error)
	// DigestToolOutput names one tool output (ToolCallCompleted.OutputDigest).
	DigestToolOutput(CanonicalJSON) (Digest, error)
}

// Identity is the identity derivation. Everything a Run persists that names
// a step, call, response, effect or command is derived here; the
// implementation lives in agentcore/run/canonical.
type Identity interface {
	DeriveModelRequestCommandID(run RunID, position RunPosition) CommandID
	DeriveModelStepID(run RunID, cmd CommandID) StepID
	DeriveCallID(source StepID, index int) CallID
	DeriveToolStepID(source StepID) StepID
	DeriveResponseID(run RunID, step StepID, call CallID, kind ResponseKind) ResponseID
	DeriveResponseCommandID(run RunID, step StepID, call CallID, resp ResponseID) CommandID
	DeriveInputCommandID(run RunID, inputs ...InputID) CommandID
	DeriveWithdrawCommandID(run RunID, step StepID) CommandID
	// DeriveEffectID names one request for an external effect: the model
	// call of a ModelStep (empty call; sequence counts the results the step
	// rejected before this request) or one tool call of a ToolStep
	// (sequence 0: a call starts at most once).
	DeriveEffectID(run RunID, step StepID, call CallID, sequence int) EffectID
	// The start, settlement and recovery of an effect are identified by the
	// effect alone: each happens at most once per effect.
	DeriveStartCommandID(effect EffectID) CommandID
	DeriveSettlementCommandID(effect EffectID) CommandID
	DeriveRecoveryCommandID(effect EffectID) CommandID
	// DeriveDeclineCommandID identifies the decline of one Pending call
	// (DeclineToolCall). The call has no effect yet and is declined at most
	// once, so the identity is the call's coordinates.
	DeriveDeclineCommandID(run RunID, step StepID, call CallID) CommandID
}

// StateMachine is the state machine: the Decide (decide.go) and
// Evolve (evolve.go) transitions bound to the digest and identity rules of the
// schema that selects it. The zero StateMachine is unbound and refuses every
// command and fact.
type StateMachine struct {
	Canonical Canonical
	Identity  Identity
}

var errMachineUnbound = errors.New("agent: machine has no canonical or identity rules bound")

func (m StateMachine) bound() error {
	if m.Canonical == nil || m.Identity == nil {
		return errMachineUnbound
	}
	return nil
}

// CreateGroup produces the facts that establish a Run and queue its initial
// inputs (RUN-NEW-1). It is pure: the owning module places these facts in
// its creation commit, the RunStore never sees a Create command.
func (m StateMachine) CreateGroup(run NewRun, inputs []AgentInput) ([]Fact, error) {
	if err := m.bound(); err != nil {
		return nil, err
	}
	if err := ValidateNewRun(run); err != nil {
		return nil, err
	}
	facts := make([]Fact, 0, 1+len(inputs))
	facts = append(facts, RunCreated(run))
	seen := make(map[InputID]struct{}, len(inputs))
	for _, in := range inputs {
		if in.ID == "" {
			return nil, errors.New("agent: create group: input with empty InputID")
		}
		if in.Digest == "" {
			return nil, fmt.Errorf("agent: create group: input %s has no content digest", in.ID)
		}
		if _, dup := seen[in.ID]; dup {
			return nil, fmt.Errorf("agent: create group: duplicate InputID %q", in.ID)
		}
		seen[in.ID] = struct{}{}
		facts = append(facts, InputAccepted{Input: cloneAgentInput(in)})
	}
	return facts, nil
}
