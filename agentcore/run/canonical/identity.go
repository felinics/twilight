package canonical

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
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

// Identity is the identity derivation: every RunID-scoped identity (steps,
// calls, responses, effects, command ids) is a namespaced hash of its
// preimage. The preimages are persisted semantics and never change: a
// different derivation would give a replayed command another CommitID
// (RUN-CMT-8).
type Identity struct{}

// DeriveModelRequestCommandID derives the CommandID for PrepareModelRequest
// from the Run and the RunPosition the prompt builder loaded (RUN-WIR-3): concurrent
// prompt builders on the same position converge on one command identity.
func (Identity) DeriveModelRequestCommandID(runID run.RunID, position run.RunPosition) run.CommandID {
	return run.CommandID(namespacedHash("twilight/model-request", string(runID), fmt.Sprintf("%d", position)))
}

// DeriveModelStepID derives the ModelStep identity from the Run and the
// preparing command: one accepted Prepare opens one step.
func (Identity) DeriveModelStepID(runID run.RunID, cmd run.CommandID) run.StepID {
	return run.StepID(namespacedHash("twilight/model-step", string(runID), string(cmd)))
}

// DeriveCallID derives the Run-owned identity of one tool call from the
// ModelStep that produced it and the call's position in that step's result.
// The provider's own tool_call_id is kept beside it as ProviderCallID for the
// request round trip; it is not trusted to be unique or non-empty.
func (Identity) DeriveCallID(source run.StepID, index int) run.CallID {
	return run.CallID(namespacedHash("twilight/tool-call", string(source), fmt.Sprintf("%d", index)))
}

// DeriveToolStepID derives the ToolStep identity from its source ModelStep:
// a ModelStep completes once and opens at most one ToolStep.
func (Identity) DeriveToolStepID(source run.StepID) run.StepID {
	return run.StepID(namespacedHash("twilight/tool-step", string(source)))
}

// DeriveResponseID derives the stable ResponseID the Machine assigns when it
// creates a Waiting request. One call has at most one outstanding request, so
// (run, step, call, kind) identifies it.
func (Identity) DeriveResponseID(runID run.RunID, step run.StepID, call run.CallID, kind run.ResponseKind) run.ResponseID {
	return run.ResponseID(namespacedHash("twilight/response", string(runID), string(step), string(call), string(kind)))
}

// DeriveResponseCommandID derives the CommandID for approval/rejection/answer
// commands: independent ingress processes converge on one command identity
// without coordination.
func (Identity) DeriveResponseCommandID(runID run.RunID, step run.StepID, call run.CallID, resp run.ResponseID) run.CommandID {
	return run.CommandID(namespacedHash("twilight/response-command", string(runID), string(step), string(call), string(resp)))
}

// DeriveInputCommandID derives the CommandID for AcceptInput from the Run and
// the ordered InputIDs of the batch: the same inputs in the same order replay
// to the same command, a different batch is a different command. Queue-claim
// references stay private to the host.
func (Identity) DeriveInputCommandID(runID run.RunID, inputs ...run.InputID) run.CommandID {
	parts := make([]string, 0, len(inputs)+1)
	parts = append(parts, string(runID))
	for _, in := range inputs {
		parts = append(parts, string(in))
	}
	return run.CommandID(namespacedHash("twilight/input-command", parts...))
}

// DeriveWithdrawCommandID derives the CommandID of WithdrawPreparedStep: one
// Prepared step is withdrawn at most once, so the identity needs no content.
func (Identity) DeriveWithdrawCommandID(runID run.RunID, step run.StepID) run.CommandID {
	return run.CommandID(namespacedHash("twilight/withdraw-command", string(runID), string(step)))
}

// DeriveEffectID derives the identity of one request for an external effect
// (RUN-WIR-1): the model call of a ModelStep (empty call) or one tool call of
// a ToolStep. sequence distinguishes the requests one step makes: a ModelStep
// whose result was rejected requests its model call again, so its sequence
// is the count of rejections before the request; a tool call starts at most
// once, so its sequence is 0. The EffectID is the one handle the Run and the
// execution plane share: the start, settlement and recovery commands of the
// effect derive from it, and the executor keys its attempts by it.
func (Identity) DeriveEffectID(runID run.RunID, step run.StepID, call run.CallID, sequence int) run.EffectID {
	return run.EffectID(namespacedHash("twilight/effect", string(runID), string(step), string(call), fmt.Sprintf("%d", sequence)))
}

// DeriveStartCommandID derives the CommandID of StartModelExecution or
// StartToolCall from the effect it requests. Commit enforces this derivation
// so a caller-minted ID cannot bypass the idempotency index (RUN-WIR-3).
func (Identity) DeriveStartCommandID(effect run.EffectID) run.CommandID {
	return run.CommandID(namespacedHash("twilight/start-command", string(effect)))
}

// DeriveSettlementCommandID derives the CommandID of the settlement of one
// effect (model result/failure/reject, tool result/failure). One effect
// settles once, so the identity needs no content: a replay with the same
// outcome is idempotent, a different outcome is a conflict.
func (Identity) DeriveSettlementCommandID(effect run.EffectID) run.CommandID {
	return run.CommandID(namespacedHash("twilight/settlement-command", string(effect)))
}

// DeriveRecoveryCommandID derives the CommandID of the recovery of one effect
// whose outcome cannot be reached (RUN-CMT-7): RecoverModelExecution for a
// model effect, the Unknown SubmitToolFailure for a tool effect. The identity
// is a function of the effect alone, so whichever owner repeats the recovery
// replays the same command.
func (Identity) DeriveRecoveryCommandID(effect run.EffectID) run.CommandID {
	return run.CommandID(namespacedHash("twilight/recovery-command", string(effect)))
}

// DeriveDeclineCommandID derives the CommandID of DeclineToolCall: the
// pre-start failure of one Pending call. The call has no effect yet and is
// declined at most once, so the identity is the call's coordinates alone; a
// replay of the same decline is idempotent, a different failure is a
// conflict.
func (Identity) DeriveDeclineCommandID(runID run.RunID, step run.StepID, call run.CallID) run.CommandID {
	return run.CommandID(namespacedHash("twilight/decline-command", string(runID), string(step), string(call)))
}
