package canonical

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/felinics/twilight/agentcore/turn"
)

// namespacedHash derives a stable identifier from a namespace and ordered
// parts. Parts are length-prefixed so no two distinct part lists collide.
func namespacedHash(namespace string, parts ...string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d:%s", len(namespace), namespace)
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Identity is the identity derivation: every TurnID-scoped identity (steps,
// calls, responses, effects, command ids) is a namespaced hash of its
// preimage. The preimages are persisted semantics and never change: a
// different derivation would give a replayed command another CommitID
type Identity struct{}

// DeriveModelRequestCommandID derives the CommandID for PrepareModelRequest
// from the Turn and the TurnPosition the prompt builder loaded: concurrent
// prompt builders on the same position converge on one command identity.
func (Identity) DeriveModelRequestCommandID(turnID turn.TurnID, position turn.TurnPosition) turn.CommandID {
	return turn.CommandID(namespacedHash("twilight/model-request", string(turnID), fmt.Sprintf("%d", position)))
}

// DeriveModelStepID derives the ModelStep identity from the Turn and the
// preparing command: one accepted Prepare opens one step.
func (Identity) DeriveModelStepID(turnID turn.TurnID, cmd turn.CommandID) turn.StepID {
	return turn.StepID(namespacedHash("twilight/model-step", string(turnID), string(cmd)))
}

// DeriveCallID derives the Turn-owned identity of one tool call from the
// ModelStep that produced it and the call's position in that step's result.
// The provider's own tool_call_id is kept beside it as ProviderCallID for the
// request round trip; it is not trusted to be unique or non-empty.
func (Identity) DeriveCallID(source turn.StepID, index int) turn.CallID {
	return turn.CallID(namespacedHash("twilight/tool-call", string(source), fmt.Sprintf("%d", index)))
}

// DeriveToolStepID derives the ToolStep identity from its source ModelStep:
// a ModelStep completes once and opens at most one ToolStep.
func (Identity) DeriveToolStepID(source turn.StepID) turn.StepID {
	return turn.StepID(namespacedHash("twilight/tool-step", string(source)))
}

// DeriveResponseID derives the stable ResponseID the Machine assigns when it
// creates a Waiting request. One call has at most one outstanding request, so
// (turn, step, call, kind) identifies it.
func (Identity) DeriveResponseID(turnID turn.TurnID, step turn.StepID, call turn.CallID, kind turn.ResponseKind) turn.ResponseID {
	return turn.ResponseID(namespacedHash("twilight/response", string(turnID), string(step), string(call), string(kind)))
}

// DeriveResponseCommandID derives the CommandID for approval/rejection/answer
// commands: independent ingress processes converge on one command identity
// without coordination.
func (Identity) DeriveResponseCommandID(turnID turn.TurnID, step turn.StepID, call turn.CallID, resp turn.ResponseID) turn.CommandID {
	return turn.CommandID(namespacedHash("twilight/response-command", string(turnID), string(step), string(call), string(resp)))
}

// DeriveInputCommandID derives the CommandID for AcceptInput from the Turn and
// the ordered InputIDs of the batch: the same inputs in the same order replay
// to the same command, a different batch is a different command. Queue-claim
// references stay private to the host.
func (Identity) DeriveInputCommandID(turnID turn.TurnID, inputs ...turn.InputID) turn.CommandID {
	parts := make([]string, 0, len(inputs)+1)
	parts = append(parts, string(turnID))
	for _, in := range inputs {
		parts = append(parts, string(in))
	}
	return turn.CommandID(namespacedHash("twilight/input-command", parts...))
}

// DeriveWithdrawCommandID derives the CommandID of WithdrawPreparedStep: one
// Prepared step is withdrawn at most once, so the identity needs no content.
func (Identity) DeriveWithdrawCommandID(turnID turn.TurnID, step turn.StepID) turn.CommandID {
	return turn.CommandID(namespacedHash("twilight/withdraw-command", string(turnID), string(step)))
}

// DeriveEffectID derives the identity of one request for an external effect
// the model call of a ModelStep (empty call) or one tool call of
// a ToolStep. sequence distinguishes the requests one step makes: a ModelStep
// whose result was rejected requests its model call again, so its sequence
// is the count of rejections before the request; a tool call starts at most
// once, so its sequence is 0. The EffectID is the one handle the Turn and the
// execution plane share: the start, settlement and recovery commands of the
// effect derive from it, and the executor keys its attempts by it.
func (Identity) DeriveEffectID(turnID turn.TurnID, step turn.StepID, call turn.CallID, sequence int) turn.EffectID {
	return turn.EffectID(namespacedHash("twilight/effect", string(turnID), string(step), string(call), fmt.Sprintf("%d", sequence)))
}

// DeriveStartCommandID derives the CommandID of StartModelExecution or
// StartToolCall from the effect it requests. Commit enforces this derivation
// so a caller-minted ID cannot bypass the idempotency index.
func (Identity) DeriveStartCommandID(effect turn.EffectID) turn.CommandID {
	return turn.CommandID(namespacedHash("twilight/start-command", string(effect)))
}

// DeriveSettlementCommandID derives the CommandID of the settlement of one
// effect (model result/failure/reject, tool result/failure). One effect
// settles once, so the identity needs no content: a replay with the same
// outcome is idempotent, a different outcome is a conflict.
func (Identity) DeriveSettlementCommandID(effect turn.EffectID) turn.CommandID {
	return turn.CommandID(namespacedHash("twilight/settlement-command", string(effect)))
}

// DeriveRecoveryCommandID derives the CommandID of the recovery of one effect
// whose outcome cannot be reached: RecoverModelExecution for a
// model effect, the Unknown SubmitToolFailure for a tool effect. The identity
// is a function of the effect alone, so whichever owner repeats the recovery
// replays the same command.
func (Identity) DeriveRecoveryCommandID(effect turn.EffectID) turn.CommandID {
	return turn.CommandID(namespacedHash("twilight/recovery-command", string(effect)))
}

// DeriveDeclineCommandID derives the CommandID of DeclineToolCall: the
// pre-start failure of one Pending call. The call has no effect yet and is
// declined at most once, so the identity is the call's coordinates alone; a
// replay of the same decline is idempotent, a different failure is a
// conflict.
func (Identity) DeriveDeclineCommandID(turnID turn.TurnID, step turn.StepID, call turn.CallID) turn.CommandID {
	return turn.CommandID(namespacedHash("twilight/decline-command", string(turnID), string(step), string(call)))
}
