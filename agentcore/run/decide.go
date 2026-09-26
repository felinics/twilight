package run

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run/model"
)

// Machine errors. EvaluateCommit maps rejection reasons onto these; Decide
// returns them directly when a command's preconditions fail against the
// current state.
var (
	ErrCommandConflict = errors.New("agent: command identity conflict")
	ErrStaleRuntime    = errors.New("agent: stale runtime revision or grant")
	ErrRunTerminal     = errors.New("agent: run is terminal")
)

// rejectionf wraps a precondition failure that is not one of the sentinel
// errors; EvaluateCommit surfaces it as-is.
func rejectionf(format string, args ...any) error {
	return fmt.Errorf("agent: reject: "+format, args...)
}

// Decide produces the facts a command yields against s, or the rejection
// (RUN-MCH-1).
//
//nolint:gocritic // hugeParam: v1 Decide is value-based.
func (m StateMachine) Decide(s MachineState, c AgentCommand) ([]Fact, error) {
	if err := m.bound(); err != nil {
		return nil, err
	}
	if s.Status.Terminal() {
		return nil, ErrRunTerminal
	}
	switch cmd := c.(type) {
	case PrepareModelRequest:
		return m.decidePrepareModelRequest(&s, &cmd)
	case WithdrawPreparedStep:
		return decideWithdrawPreparedStep(&s, cmd)
	case StartModelExecution:
		return m.decideStartModelExecution(&s, cmd)
	case RecoverModelExecution:
		return m.decideRecoverModelExecution(&s, cmd)
	case SubmitModelResult:
		return m.decideSubmitModelResult(&s, &cmd)
	case SubmitModelFailure:
		return decideSubmitModelFailure(&s, cmd)
	case RejectModelResult:
		return decideRejectModelResult(&s, &cmd)
	case StartToolCall:
		return m.decideStartToolCall(&s, cmd)
	case SubmitToolResult:
		return m.decideSubmitToolResult(&s, cmd)
	case SubmitToolFailure:
		return decideSubmitToolFailure(&s, cmd)
	case DeclineToolCall:
		return decideDeclineToolCall(&s, cmd)
	case ApproveToolCall:
		return m.decideApproveToolCall(&s, cmd)
	case RejectToolCall:
		return m.decideRejectToolCall(&s, &cmd)
	case SubmitToolResponse:
		return m.decideSubmitToolResponse(&s, &cmd)
	case CancelRun:
		return decideCancelRun(&s, cmd)
	case AcceptInput:
		return decideAcceptInput(&s, cmd)
	default:
		return nil, rejectionf("unknown command variant %T", c)
	}
}

// --- rule 1: PrepareModelRequest ---

func (m StateMachine) decidePrepareModelRequest(s *MachineState, cmd *PrepareModelRequest) ([]Fact, error) {
	if !atOpen(s.Current) {
		return nil, rejectionf("prepare: run is not at Open")
	}
	if cmd.StepID == "" {
		return nil, rejectionf("prepare: empty StepID")
	}
	if cmd.Model == "" {
		return nil, rejectionf("prepare: empty model")
	}
	if ModelRef(cmd.Request.Model) != cmd.Model {
		return nil, rejectionf("prepare: request model %q does not match command model %q", cmd.Request.Model, cmd.Model)
	}
	// InputIDs must match PendingInputs completely and in current order.
	if len(cmd.InputIDs) != len(s.PendingInputs) {
		return nil, rejectionf("prepare: InputIDs must consume all %d pending inputs, got %d", len(s.PendingInputs), len(cmd.InputIDs))
	}
	for i, id := range cmd.InputIDs {
		if s.PendingInputs[i].ID != id {
			return nil, rejectionf("prepare: InputIDs[%d]=%q does not match pending input %q", i, id, s.PendingInputs[i].ID)
		}
	}
	// Tools must correspond one-to-one, in order, with the provider tool
	// definitions inside the frozen request. The spec keeps only the digest;
	// the body stays in the request.
	if len(cmd.Tools) != len(cmd.Request.Tools) {
		return nil, rejectionf("prepare: %d ToolSpecs for %d request tools", len(cmd.Tools), len(cmd.Request.Tools))
	}
	for i, spec := range cmd.Tools {
		if spec.Name == "" || spec.Name != cmd.Request.Tools[i].Name {
			return nil, rejectionf("prepare: ToolSpec[%d] %q does not match request tool %q", i, spec.Name, cmd.Request.Tools[i].Name)
		}
		wantDigest, err := m.Canonical.DigestToolDefinition(cmd.Request.Tools[i])
		if err != nil {
			return nil, err
		}
		if spec.DefinitionDigest != wantDigest {
			return nil, rejectionf("prepare: ToolSpec[%d] definition digest mismatch", i)
		}
	}
	wantReq, err := m.Canonical.DigestRequest(cmd.Request)
	if err != nil {
		return nil, err
	}
	if cmd.RequestDigest != wantReq {
		return nil, rejectionf("prepare: request digest mismatch")
	}
	return []Fact{ModelStepPrepared{
		StepID:        cmd.StepID,
		Model:         cmd.Model,
		RequestDigest: cmd.RequestDigest,
		InputIDs:      cmd.InputIDs,
		Tools:         cmd.Tools,
	}}, nil
}

// --- rule 1b: WithdrawPreparedStep ---

func decideWithdrawPreparedStep(s *MachineState, cmd WithdrawPreparedStep) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelPrepared {
		return nil, rejectionf("withdraw: step is not Prepared")
	}
	if len(s.PendingInputs) == 0 {
		return nil, rejectionf("withdraw: no pending inputs; the prepared request is still complete")
	}
	return []Fact{ModelStepWithdrawn(cmd)}, nil
}

// --- rule 2: StartModelExecution / RecoverModelExecution ---

func currentModelStep(s *MachineState, step StepID) (*ModelStep, error) {
	ms, ok := s.Current.(ModelStep)
	if !ok {
		return nil, rejectionf("no current ModelStep")
	}
	if ms.RefValue.ID != step {
		return nil, rejectionf("step %q is not the current ModelStep %q", step, ms.RefValue.ID)
	}
	return &ms, nil
}

// decideStartModelExecution admits the start of the step's next model
// effect. The effect identity is derived, so a start that names any other
// effect -- one derived against a different rejection count, or minted by
// the caller -- is stale rather than a new grant (RUN-WIR-1).
func (m StateMachine) decideStartModelExecution(s *MachineState, cmd StartModelExecution) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelPrepared {
		return nil, rejectionf("start model: step is not Prepared")
	}
	if cmd.Effect == "" {
		return nil, rejectionf("start model: missing effect identity")
	}
	if want := m.Identity.DeriveEffectID(s.RunID, cmd.StepID, "", ms.Rejects); cmd.Effect != want {
		return nil, rejectionf("start model: effect %q is not the step's next model effect", cmd.Effect)
	}
	return []Fact{ModelStepStarted(cmd)}, nil
}

// decideRecoverModelExecution withdraws the Executing step when the recovery
// names the effect it is executing; a recovery of an earlier effect of the
// same step is stale.
func (m StateMachine) decideRecoverModelExecution(s *MachineState, cmd RecoverModelExecution) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("recover model: step is not Executing")
	}
	if cmd.Effect != ms.Effect {
		return nil, rejectionf("recover model: effect %q is not the step's executing effect", cmd.Effect)
	}
	return []Fact{ModelStepRecovered(cmd)}, nil
}

// settlesModelEffect checks that a settlement names the effect the step is
// executing (RUN-WIR-1): a settlement of an earlier effect of the same step
// is stale.
func settlesModelEffect(ms *ModelStep, effect EffectID, op string) error {
	if effect == "" {
		return rejectionf("%s: missing effect identity", op)
	}
	if effect != ms.Effect {
		return rejectionf("%s: effect %q is not the step's executing effect", op, effect)
	}
	return nil
}

// --- rule 3: SubmitModelResult ---

func (m StateMachine) decideSubmitModelResult(s *MachineState, cmd *SubmitModelResult) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("model result: step is not Executing")
	}
	if err := settlesModelEffect(ms, cmd.Effect, "model result"); err != nil {
		return nil, err
	}
	resultDigest, err := m.Canonical.DigestModelResult(cmd.Result)
	if err != nil {
		return nil, err
	}
	completed := ModelStepCompleted{StepID: cmd.StepID, Effect: cmd.Effect, Usage: cmd.Result.Usage, FinishReason: cmd.Result.FinishReason, ResultDigest: resultDigest}

	// The result's own tool calls decide whether a ToolStep opens; gating on
	// the caller-supplied bindings would let zero bindings silently complete
	// a run whose model asked for tools.
	if len(cmd.Result.ToolCalls) == 0 {
		if len(cmd.Calls) != 0 {
			return nil, rejectionf("model result: %d bindings for a result with no tool calls", len(cmd.Calls))
		}
		// Inputs that arrived during this step keep the Run alive: the next
		// Prepare consumes them. Only an empty queue ends the Run.
		if len(s.PendingInputs) > 0 {
			return []Fact{completed}, nil
		}
		return []Fact{completed, RunEnded{End: RunCompletedEnd{}}}, nil
	}
	bindings, err := m.checkToolCallBindings(ms, cmd)
	if err != nil {
		return nil, err
	}
	opened, err := m.openToolStep(s.RunID, cmd.StepID, bindings, cmd.Scheduling)
	if err != nil {
		return nil, err
	}
	return []Fact{completed, opened}, nil
}

// checkToolCallBindings validates the caller's bindings one-to-one against
// the model result and the frozen ToolSpecs (RUN-MCH-2) and returns them with
// Response cleared, ready for openToolStep to derive.
func (m StateMachine) checkToolCallBindings(ms *ModelStep, cmd *SubmitModelResult) ([]ToolCallBinding, error) {
	if len(cmd.Calls) != len(cmd.Result.ToolCalls) {
		return nil, rejectionf("model result: %d bindings for %d tool calls", len(cmd.Calls), len(cmd.Result.ToolCalls))
	}
	specByName := make(map[string]ToolSpec, len(ms.Tools))
	for _, spec := range ms.Tools {
		specByName[spec.Name] = spec
	}
	seen := make(map[CallID]bool, len(cmd.Calls))
	bindings := make([]ToolCallBinding, len(cmd.Calls))
	for i := range cmd.Calls {
		b := cmd.Calls[i]
		rc := &cmd.Result.ToolCalls[i]
		if want := m.Identity.DeriveCallID(cmd.StepID, i); b.CallID != want {
			return nil, rejectionf("model result: binding %d CallID %q is not the derived id %q", i, b.CallID, want)
		}
		if b.ProviderCallID != rc.ToolCallID {
			return nil, rejectionf("model result: binding %d ProviderCallID %q does not match result call %q", i, b.ProviderCallID, rc.ToolCallID)
		}
		if seen[b.CallID] {
			return nil, rejectionf("model result: duplicate CallID %q", b.CallID)
		}
		seen[b.CallID] = true
		if err := m.checkBindingAgainstResult(&b, rc, specByName); err != nil {
			return nil, err
		}
		b.Response = nil // derived by openToolStep; callers leave it empty
		bindings[i] = b
	}
	return bindings, nil
}

// checkBindingAgainstResult accepts only a binding for the tool the model
// actually named, with the arguments the model actually produced. A known
// tool must match its frozen ToolSpec; an unknown one stays an unresolved
// DirectExecution binding that StartToolCalls records as a lookup failure.
func (m StateMachine) checkBindingAgainstResult(b *ToolCallBinding, rc *model.ModelToolCall, specByName map[string]ToolSpec) error {
	if spec, known := specByName[rc.ToolName]; known {
		if b.ToolRef != spec.Ref {
			return rejectionf("model result: binding %q ToolRef %q does not match frozen spec ref %q for tool %q", b.CallID, b.ToolRef, spec.Ref, rc.ToolName)
		}
		if b.DefinitionDigest != spec.DefinitionDigest {
			return rejectionf("model result: binding %q definition digest does not match frozen ToolSpec", b.CallID)
		}
		if b.Policy != spec.Policy {
			return rejectionf("model result: binding %q policy does not match frozen ToolSpec", b.CallID)
		}
	} else {
		if string(b.ToolRef) != rc.ToolName {
			return rejectionf("model result: binding %q ToolRef %q does not match result tool %q", b.CallID, b.ToolRef, rc.ToolName)
		}
		if b.Policy != DirectExecution || b.DefinitionDigest != "" {
			return rejectionf("model result: unresolved binding %q must be DirectExecution with empty digest", b.CallID)
		}
	}
	if !b.Arguments.Equal(rc.Input.Canonical()) {
		return rejectionf("model result: binding %q arguments do not match the model result", b.CallID)
	}
	return nil
}

// openToolStep derives the ToolStep identity from its source, attaches a
// ResponseRequest to every call whose policy waits, and freezes the
// scheduling (RUN-LOP-1).
func (m StateMachine) openToolStep(runID RunID, source StepID, bindings []ToolCallBinding, scheduling ToolScheduling) (ToolStepOpened, error) {
	toolStepID := m.Identity.DeriveToolStepID(source)
	for i := range bindings {
		kind, waits := responseKindForPolicy(bindings[i].Policy)
		if !waits {
			continue
		}
		bindings[i].Response = &ResponseRequest{
			RunID:   runID,
			StepID:  toolStepID,
			CallID:  bindings[i].CallID,
			ID:      m.Identity.DeriveResponseID(runID, toolStepID, bindings[i].CallID, kind),
			Kind:    kind,
			Payload: bindings[i].Arguments,
		}
	}
	normalized, err := normalizeToolScheduling(scheduling)
	if err != nil {
		return ToolStepOpened{}, rejectionf("model result: %v", err)
	}
	return ToolStepOpened{
		StepID:     toolStepID,
		Source:     source,
		Calls:      bindings,
		Scheduling: normalized,
	}, nil
}

// --- rule 4: SubmitModelFailure ---

func decideSubmitModelFailure(s *MachineState, cmd SubmitModelFailure) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("model failure: step is not Executing")
	}
	if err := settlesModelEffect(ms, cmd.Effect, "model failure"); err != nil {
		return nil, err
	}
	if cmd.Failure.Class == "" {
		return nil, rejectionf("model failure: empty failure class")
	}
	reason := ReasonProviderFailure
	if cmd.Failure.Class == FailureEffectUnknown {
		reason = ReasonEffectUnknown
	}
	return []Fact{
		ModelStepFailed(cmd),
		RunEnded{End: RunFailedEnd{
			Reason:  reason,
			Failure: RunFailure{Class: cmd.Failure.Class, Message: cmd.Failure.Message},
		}},
	}, nil
}

// --- rule 5: RejectModelResult ---

func decideRejectModelResult(s *MachineState, cmd *RejectModelResult) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("reject model result: step is not Executing")
	}
	if err := settlesModelEffect(ms, cmd.Effect, "reject model result"); err != nil {
		return nil, err
	}
	rejected := ModelStepRejected{StepID: cmd.StepID, Effect: cmd.Effect, Usage: cmd.Usage, Failure: cmd.Failure}
	switch cmd.Disposition {
	case ModelRejectRetry:
		return []Fact{rejected}, nil
	case ModelRejectFailRun:
		return []Fact{rejected, RunEnded{End: RunFailedEnd{
			Reason:  ReasonMalformedModel,
			Failure: RunFailure{Class: FailureMalformedModel, Message: cmd.Failure.Message},
		}}}, nil
	default:
		return nil, rejectionf("reject model result: unknown disposition %d", cmd.Disposition)
	}
}

// --- rules 6-8: tool call lifecycle ---

func currentToolStep(s *MachineState, step StepID) (*ToolStep, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok {
		return nil, rejectionf("no current ToolStep")
	}
	if ts.RefValue.ID != step {
		return nil, rejectionf("step %q is not the current ToolStep %q", step, ts.RefValue.ID)
	}
	return &ts, nil
}

// decideStartToolCall admits the start of one Pending call's tool effect; a
// call starts at most once, so its effect is the call's only one.
func (m StateMachine) decideStartToolCall(s *MachineState, cmd StartToolCall) ([]Fact, error) {
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("start tool: unknown call %q", cmd.CallID)
	}
	if ts.Calls[i].Status != ToolPending {
		return nil, rejectionf("start tool: call %q is not Pending", cmd.CallID)
	}
	if cmd.Effect == "" {
		return nil, rejectionf("start tool: missing effect identity")
	}
	if want := m.Identity.DeriveEffectID(s.RunID, cmd.StepID, cmd.CallID, 0); cmd.Effect != want {
		return nil, rejectionf("start tool: effect %q is not the call's tool effect", cmd.Effect)
	}
	return []Fact{ToolCallStarted(cmd)}, nil
}

func (m StateMachine) decideSubmitToolResult(s *MachineState, cmd SubmitToolResult) ([]Fact, error) {
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("tool result: unknown call %q", cmd.CallID)
	}
	if ts.Calls[i].Status != ToolExecuting {
		return nil, rejectionf("tool result: call %q is not Executing", cmd.CallID)
	}
	if err := settlesToolEffect(&ts.Calls[i], cmd.Effect, "tool result"); err != nil {
		return nil, err
	}
	outputDigest, err := m.Canonical.DigestToolOutput(cmd.Result.Output)
	if err != nil {
		return nil, err
	}
	return []Fact{ToolCallCompleted{StepID: cmd.StepID, CallID: cmd.CallID, Effect: cmd.Effect, OutputDigest: outputDigest}}, nil
}

func decideSubmitToolFailure(s *MachineState, cmd SubmitToolFailure) ([]Fact, error) {
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("tool failure: unknown call %q", cmd.CallID)
	}
	call := ts.Calls[i]
	if call.Status != ToolExecuting {
		return nil, rejectionf("tool failure: call %q is not Executing", cmd.CallID)
	}
	if err := settlesToolEffect(&call, cmd.Effect, "tool failure"); err != nil {
		return nil, err
	}
	if cmd.Failure.Class == "" {
		if cmd.Outcome == ToolOutcomeUnknown {
			cmd.Failure.Class = FailureEffectUnknown
		} else {
			return nil, rejectionf("tool failure: empty failure class")
		}
	}
	switch cmd.Outcome {
	case ToolOutcomeKnown:
		return []Fact{ToolCallFailed{StepID: cmd.StepID, CallID: cmd.CallID, Effect: cmd.Effect, Failure: cmd.Failure, Outcome: ToolOutcomeKnown}}, nil
	case ToolOutcomeUnknown:
		failure := cmd.Failure
		if failure.Class == "" {
			failure.Class = FailureEffectUnknown
		}
		if failure.Class != FailureEffectUnknown {
			return nil, rejectionf("tool failure: unknown outcome must use %s", FailureEffectUnknown)
		}
		return []Fact{ToolCallFailed{StepID: cmd.StepID, CallID: cmd.CallID, Effect: cmd.Effect, Failure: failure, Outcome: ToolOutcomeUnknown}}, nil
	default:
		return nil, rejectionf("tool failure: unknown outcome value %d", cmd.Outcome)
	}
}

// settlesToolEffect checks that a settlement names the effect the call is
// executing (RUN-WIR-1): the call's one tool effect, recorded by its
// ToolCallStarted.
func settlesToolEffect(call *ToolCallState, effect EffectID, op string) error {
	if effect == "" {
		return rejectionf("%s: missing effect identity", op)
	}
	if effect != call.Effect {
		return rejectionf("%s: effect %q is not the executing effect of call %q", op, effect, call.CallID)
	}
	return nil
}

// decideDeclineToolCall fails a Pending call before its tool effect is
// requested (RUN-EXE-5): no ToolCallStarted, no effect, no start barrier.
func decideDeclineToolCall(s *MachineState, cmd DeclineToolCall) ([]Fact, error) {
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("decline tool: unknown call %q", cmd.CallID)
	}
	if ts.Calls[i].Status != ToolPending {
		return nil, rejectionf("decline tool: call %q is not Pending", cmd.CallID)
	}
	if cmd.Failure.Class == "" {
		return nil, rejectionf("decline tool: empty failure class")
	}
	return []Fact{ToolCallFailed{StepID: cmd.StepID, CallID: cmd.CallID, Failure: cmd.Failure, Outcome: ToolOutcomeKnown}}, nil
}

// --- rules 9-10: responses ---

// waitingCall checks that call is Waiting on the current ToolStep for the
// given response kind and ID.
func waitingCall(s *MachineState, step StepID, call CallID, kind ResponseKind, resp ResponseID) error {
	ts, err := currentToolStep(s, step)
	if err != nil {
		return err
	}
	i := ts.callIndex(call)
	if i < 0 {
		return rejectionf("response: unknown call %q", call)
	}
	c := ts.Calls[i]
	if c.Status != ToolWaiting || c.Waiting == nil {
		return rejectionf("response: call %q is not Waiting", call)
	}
	if c.Waiting.Kind != kind {
		return rejectionf("response: call %q expects kind %q, got %q", call, c.Waiting.Kind, kind)
	}
	if c.Waiting.ID != resp {
		return rejectionf("response: call %q expects ResponseID %q, got %q", call, c.Waiting.ID, resp)
	}
	return nil
}

func (m StateMachine) decideApproveToolCall(s *MachineState, cmd ApproveToolCall) ([]Fact, error) {
	if err := waitingCall(s, cmd.StepID, cmd.CallID, ResponseApproval, cmd.ResponseID); err != nil {
		return nil, err
	}
	wantDigest, err := m.Canonical.DigestToolResponseDecision(ResponseApproval, ResponseDecisionApproved, "")
	if err != nil {
		return nil, err
	}
	if cmd.ResponseDigest != wantDigest {
		return nil, rejectionf("response: approval digest mismatch")
	}
	return []Fact{ToolCallApproved(cmd)}, nil
}

func (m StateMachine) decideRejectToolCall(s *MachineState, cmd *RejectToolCall) ([]Fact, error) {
	// Reject closes a Waiting call of either kind as a Known failure:
	// approval rejection and external-response abandonment ("the answer is
	// never coming") share one exit. Waiting -> Failed(Known) is legal;
	// without this, an abandoned ask-user call would strand the run with
	// CancelRun as the only escape.
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("response: unknown call %q", cmd.CallID)
	}
	c := ts.Calls[i]
	if c.Status != ToolWaiting || c.Waiting == nil {
		return nil, rejectionf("response: call %q is not Waiting", cmd.CallID)
	}
	if c.Waiting.ID != cmd.ResponseID {
		return nil, rejectionf("response: call %q expects ResponseID %q, got %q", cmd.CallID, c.Waiting.ID, cmd.ResponseID)
	}
	wantDigest, err := m.Canonical.DigestToolResponseDecision(c.Waiting.Kind, ResponseDecisionRejected, cmd.Reason)
	if err != nil {
		return nil, err
	}
	if cmd.ResponseDigest != wantDigest {
		return nil, rejectionf("response: rejection digest mismatch")
	}
	class := FailurePermissionDenied
	if c.Waiting.Kind == ResponseExternal {
		class = FailureResponseRejected
	}
	facts := []Fact{ToolCallFailed{
		StepID:  cmd.StepID,
		CallID:  cmd.CallID,
		Failure: ToolFailure{Class: class, Message: cmd.Reason},
		Outcome: ToolOutcomeKnown,
	}}
	return facts, nil
}

func (m StateMachine) decideSubmitToolResponse(s *MachineState, cmd *SubmitToolResponse) ([]Fact, error) {
	if err := waitingCall(s, cmd.StepID, cmd.CallID, ResponseExternal, cmd.ResponseID); err != nil {
		return nil, err
	}
	wantDigest, err := m.Canonical.DigestToolResponsePayload(cmd.Payload)
	if err != nil {
		return nil, err
	}
	if cmd.ResponseDigest != wantDigest {
		return nil, rejectionf("response: answer payload digest mismatch")
	}
	return []Fact{ToolCallAnswered{StepID: cmd.StepID, CallID: cmd.CallID, ResponseID: cmd.ResponseID, ResponseDigest: cmd.ResponseDigest}}, nil
}

// --- rules 12-13: cancel and input ---

func decideCancelRun(s *MachineState, cmd CancelRun) ([]Fact, error) {
	// CancelRun always records RunStopped(cancelled).
	if cmd.Reason != "" && cmd.Reason != ReasonCancelled {
		return nil, rejectionf("cancel: reason must be empty or %q", ReasonCancelled)
	}
	facts := cancelledToolCalls(s)
	uncertain := make([]CallID, 0, len(facts))
	for _, f := range facts {
		if failed, ok := f.(ToolCallFailed); ok && failed.Outcome == ToolOutcomeUnknown {
			uncertain = append(uncertain, failed.CallID)
		}
	}
	// A model call still executing is settled as unknown under its effect
	// before the Run stops: RunEnded closes no effect by itself (RUN-WIR-1).
	var uncertainModel StepID
	if ms, ok := s.Current.(ModelStep); ok && ms.Status == ModelExecuting {
		uncertainModel = ms.RefValue.ID
		facts = append(facts, ModelStepFailed{StepID: ms.RefValue.ID, Effect: ms.Effect,
			Failure: StepFailure{Class: FailureEffectUnknown, Message: "cancelled while the model call was executing"}})
	}
	facts = append(facts, RunEnded{End: RunStoppedEnd{
		Reason:         ReasonCancelled,
		UncertainCalls: uncertain,
		UncertainModel: uncertainModel,
	}})
	return facts, nil
}

// cancelledToolCalls settles every unfinished call before the Run stops.
func cancelledToolCalls(s *MachineState) []Fact {
	ts, ok := s.Current.(ToolStep)
	if !ok {
		return nil
	}
	facts := make([]Fact, 0, len(ts.Calls))
	for i := range ts.Calls {
		failure := ToolFailure{Class: FailureCancelled, Message: "run cancelled before tool execution"}
		outcome := ToolOutcomeKnown
		switch ts.Calls[i].Status {
		case ToolPending, ToolWaiting:
		case ToolExecuting:
			failure = ToolFailure{Class: FailureEffectUnknown, Message: "execution cancelled before settlement"}
			outcome = ToolOutcomeUnknown
		default:
			continue
		}
		facts = append(facts, ToolCallFailed{StepID: ts.RefValue.ID, CallID: ts.Calls[i].CallID, Effect: ts.Calls[i].Effect, Failure: failure, Outcome: outcome})
	}
	return facts
}

// decideAcceptInput queues a batch of inputs in any non-terminal state
// (RUN-MCH-4): PendingInputs is the durable mid-run input queue, consumed by
// the next Prepare. A Prepared step with a non-empty queue is withdrawn by
// Next. The batch is all-or-nothing: an empty list or an empty InputID is a
// rejection, and an input already pending, or repeated inside the batch, is
// ErrCommandConflict; in every failure no fact is produced.
func decideAcceptInput(s *MachineState, cmd AcceptInput) ([]Fact, error) {
	if len(cmd.Inputs) == 0 {
		return nil, rejectionf("accept input: empty batch")
	}
	seen := make(map[InputID]struct{}, len(cmd.Inputs)+len(s.PendingInputs))
	for _, in := range s.PendingInputs {
		seen[in.ID] = struct{}{}
	}
	facts := make([]Fact, 0, len(cmd.Inputs))
	for _, in := range cmd.Inputs {
		if in.ID == "" {
			return nil, rejectionf("accept input: empty InputID")
		}
		if in.Digest == "" {
			return nil, rejectionf("accept input: %s has no content digest", in.ID)
		}
		if _, dup := seen[in.ID]; dup {
			return nil, ErrCommandConflict
		}
		seen[in.ID] = struct{}{}
		facts = append(facts, InputAccepted{Input: in})
	}
	return facts, nil
}
