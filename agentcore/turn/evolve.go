package turn

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/turn/model"
)

// Evolve is the fold semantics. It first
// checks that the fact is a legal transition from s (fold and recovery must
// defend themselves without access to commands), then applies it.
//
//nolint:gocritic // hugeParam: v1 fold body intentionally preserves value-state semantics.
func (m StateMachine) Evolve(s MachineState, f Fact) (MachineState, error) {
	if err := m.bound(); err != nil {
		return s, err
	}
	if err := m.guardFact(&s, f); err != nil {
		return s, err
	}
	switch fact := f.(type) {
	case TurnStarted:
		return applyTurnStarted(&fact), nil
	case ModelStepPrepared:
		return applyModelStepPrepared(s, &fact), nil
	case ModelStepWithdrawn:
		return applyModelStepWithdrawn(s), nil
	case ModelStepStarted:
		return applyModelEffect(applyModelStatus(s, ModelExecuting, model.Usage{}, false), fact.Effect), nil
	case ModelStepRecovered:
		return applyModelStepWithdrawn(s), nil
	case ModelStepRejected:
		return applyModelStatus(s, ModelPrepared, fact.Usage, true), nil
	case ModelStepFailed:
		return applyModelStepFailed(s), nil
	case ModelStepCompleted:
		return applyModelStepCompleted(s, &fact), nil
	case ToolStepOpened:
		return applyToolStepOpened(s, &fact), nil
	case ToolCallStarted:
		return applyCall(s, fact.CallID, func(c *ToolCallState) { c.Status, c.Effect = ToolExecuting, fact.Effect }), nil
	case ToolCallApproved:
		return applyCall(s, fact.CallID, func(c *ToolCallState) { c.Status, c.Waiting = ToolPending, nil }), nil
	case ToolCallCompleted:
		return applyCall(s, fact.CallID, func(c *ToolCallState) {
			c.Status, c.Result, c.Waiting = ToolCompleted, &ToolCallResult{OutputDigest: fact.OutputDigest}, nil
		}), nil
	case ToolCallAnswered:
		return applyCall(s, fact.CallID, func(c *ToolCallState) {
			c.Status, c.Result, c.Waiting = ToolCompleted, &ToolCallResult{OutputDigest: fact.ResponseDigest}, nil
		}), nil
	case ToolCallFailed:
		return applyCall(s, fact.CallID, func(c *ToolCallState) {
			c.Status, c.Failure, c.Waiting = ToolFailed, &ToolCallFailure{Failure: fact.Failure, Outcome: fact.Outcome}, nil
		}), nil
	case InputAccepted:
		return applyInputAccepted(s, &fact), nil
	case TurnEnded:
		return applyTurnEnded(s, &fact), nil
	default:
		return s, fmt.Errorf("agent: evolve: unknown fact variant %T", f)
	}
}

// --- apply: mechanical folds; guardFact has established every precondition ---

func applyTurnStarted(fact *TurnStarted) MachineState {
	return MachineState{TurnID: fact.TurnID, Preset: fact.Preset, Status: TurnActive, Current: Open{}}
}

func applyModelStepPrepared(s MachineState, fact *ModelStepPrepared) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	s.Current = ModelStep{
		RefValue:      StepRef{TurnID: s.TurnID, ID: fact.StepID},
		RequestDigest: fact.RequestDigest,
		Model:         fact.Model,
		Tools:         fact.Tools,
		Status:        ModelPrepared,
	}
	s.ModelSteps++
	s.PendingInputs = nil
	return s
}

// applyModelStepWithdrawn discards the current step: Withdrawn (Prepared,
// never sent) and Recovered (Executing, attempt lost) both return the Turn to
// Open without counting a model step. PendingInputs are untouched, so the
// next Prepare consumes them.
func applyModelStepWithdrawn(s MachineState) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	s.Current = Open{}
	s.ModelSteps--
	return s
}

// applyModelStatus moves the current ModelStep to status, adding usage and
// counting a reject when the fact was a rejection.
func applyModelStatus(s MachineState, status ModelStepStatus, usage model.Usage, rejected bool) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	ms := s.Current.(ModelStep) //nolint:errcheck // guard established Current is this ModelStep
	ms.Status = status
	if rejected {
		ms.Rejects++
	}
	s.Current = ms
	s.Usage = s.Usage.Add(usage)
	return s
}

// applyModelEffect records the effect the current ModelStep requested.
func applyModelEffect(s MachineState, effect EffectID) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	ms := s.Current.(ModelStep) //nolint:errcheck // caller established Current is a ModelStep
	ms.Effect = effect
	s.Current = ms
	return s
}

// applyModelStepFailed settles the step's effect and leaves the Turn Open for
// the TurnEnded that follows in the same group. The step ran, so ModelSteps
// keeps counting it.
func applyModelStepFailed(s MachineState) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	s.Current = Open{}
	return s
}

func applyModelStepCompleted(s MachineState, fact *ModelStepCompleted) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	s.Usage = s.Usage.Add(fact.Usage)
	s.Current = Open{}
	return s
}

func applyToolStepOpened(s MachineState, fact *ToolStepOpened) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	calls := make([]ToolCallState, len(fact.Calls))
	for i, b := range fact.Calls {
		calls[i] = ToolCallState{
			CallID:           b.CallID,
			ProviderCallID:   b.ProviderCallID,
			ToolRef:          b.ToolRef,
			DefinitionDigest: b.DefinitionDigest,
			Arguments:        b.Arguments,
			Policy:           b.Policy,
			Replay:           b.Replay,
			Placement:        b.Placement,
			Status:           ToolPending,
		}
		if b.Response != nil {
			w := *b.Response
			calls[i].Status, calls[i].Waiting = ToolWaiting, &w
		}
	}
	s.Current = ToolStep{
		RefValue:   StepRef{TurnID: s.TurnID, ID: fact.StepID},
		Source:     fact.Source,
		Calls:      calls,
		Scheduling: fact.Scheduling,
	}
	return s
}

// applyCall mutates one call of the current ToolStep and closes the step when
// every call has reached Completed or Failed.
func applyCall(s MachineState, callID CallID, mutate func(*ToolCallState)) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	ts := s.Current.(ToolStep) //nolint:errcheck // guard established Current is this ToolStep
	calls := append([]ToolCallState(nil), ts.Calls...)
	mutate(&calls[ts.callIndex(callID)])
	ts.Calls = calls
	if allToolCallsTerminal(calls) {
		s.LastToolStep = &ts
		s.Current = Open{}
	} else {
		s.Current = ts
	}
	return s
}

func applyInputAccepted(s MachineState, fact *InputAccepted) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	s.PendingInputs = append(append([]AgentInput(nil), s.PendingInputs...), fact.Input)
	return s
}

func applyTurnEnded(s MachineState, fact *TurnEnded) MachineState { //nolint:gocritic // hugeParam: Evolve is a pure value transition; the caller keeps its state
	status, reason, failure := endProjection(fact.End)
	s.Status = status
	s.Current = nil
	result := &TurnResult{Status: status, Reason: reason, Failure: failure, Usage: s.Usage}
	if stopped, ok := fact.End.(TurnStoppedEnd); ok {
		result.Message = stopped.Message
		result.UncertainCalls = append([]CallID(nil), stopped.UncertainCalls...)
		result.UncertainModel = stopped.UncertainModel
	}
	s.Result = result
	return s
}

// --- guards: one per fact. Each names the legal source state and the
// self-consistency the fact must carry. ---

func (m StateMachine) guardFact(s *MachineState, f Fact) error {
	if created, ok := f.(TurnStarted); ok {
		return guardTurnStarted(s, &created)
	}
	if s.TurnID == "" {
		return errors.New("agent: evolve: fact before TurnStarted")
	}
	if s.Status.Terminal() {
		return errors.New("agent: evolve: fact after terminal state")
	}
	switch fact := f.(type) {
	case ModelStepPrepared:
		return m.guardModelStepPrepared(s, &fact)
	case ModelStepWithdrawn:
		if err := requireModelStep(s, fact.StepID, ModelPrepared); err != nil {
			return err
		}
		if len(s.PendingInputs) == 0 {
			return errors.New("agent: evolve: model step withdrawn without pending inputs")
		}
		return nil
	case ModelStepStarted:
		if fact.Effect == "" {
			return errors.New("agent: evolve: model step started without an effect")
		}
		return requireModelStep(s, fact.StepID, ModelPrepared)
	case ModelStepRecovered:
		return requireModelEffect(s, fact.StepID, fact.Effect)
	case ModelStepRejected:
		return requireModelEffect(s, fact.StepID, fact.Effect)
	case ModelStepFailed:
		if fact.Effect == "" || fact.Failure.Class == "" {
			return errors.New("agent: evolve: model step failed without effect or failure class")
		}
		return requireModelEffect(s, fact.StepID, fact.Effect)
	case ModelStepCompleted:
		if fact.ResultDigest == "" {
			return errors.New("agent: evolve: model step completed without result digest")
		}
		return requireModelEffect(s, fact.StepID, fact.Effect)
	case ToolStepOpened:
		return m.guardToolStepOpened(s, &fact)
	case ToolCallStarted:
		if fact.Effect == "" {
			return errors.New("agent: evolve: tool call started without an effect")
		}
		_, err := requireCall(s, fact.StepID, fact.CallID, ToolPending)
		return err
	case ToolCallApproved:
		return m.guardToolCallApproved(s, &fact)
	case ToolCallCompleted:
		if fact.OutputDigest == "" {
			return errors.New("agent: evolve: tool call completed without output digest")
		}
		call, err := requireCall(s, fact.StepID, fact.CallID, ToolExecuting)
		if err != nil {
			return err
		}
		return requireCallEffect(&call, fact.Effect)
	case ToolCallAnswered:
		return m.guardToolCallAnswered(s, &fact)
	case ToolCallFailed:
		return guardToolCallFailed(s, &fact)
	case InputAccepted:
		return guardInputAccepted(s, &fact)
	case TurnEnded:
		if err := validateTurnEnd(fact.End); err != nil {
			return err
		}
		return requireNoExecutingEffect(s)
	default:
		return fmt.Errorf("agent: evolve: unknown fact variant %T", f)
	}
}

// requireNoExecutingEffect: a Turn ends only after every effect it requested
// was settled by a fact naming it. TurnEnded closes no effect, so
// an Executing model step or tool call at that point would be an effect no
// settlement ever names.
func requireNoExecutingEffect(s *MachineState) error {
	switch cur := s.Current.(type) {
	case ModelStep:
		if cur.Status == ModelExecuting {
			return fmt.Errorf("agent: evolve: turn ended while model step %q executes effect %q", cur.RefValue.ID, cur.Effect)
		}
	case ToolStep:
		for i := range cur.Calls {
			if cur.Calls[i].Status == ToolExecuting {
				return fmt.Errorf("agent: evolve: turn ended while tool call %q executes effect %q", cur.Calls[i].CallID, cur.Calls[i].Effect)
			}
		}
	}
	return nil
}

func requireOpen(s *MachineState, what string) error {
	if !atOpen(s.Current) {
		return fmt.Errorf("agent: evolve: %s while turn is not at Open", what)
	}
	return nil
}

// requireModelStep checks that Current is ModelStep stepID in status.
func requireModelStep(s *MachineState, stepID StepID, status ModelStepStatus) error {
	ms, ok := s.Current.(ModelStep)
	if !ok || ms.RefValue.ID != stepID || ms.Status != status {
		return fmt.Errorf("agent: evolve: model step %q is not %s", stepID, status)
	}
	return nil
}

// requireModelEffect is requireModelStep for an Executing step plus the
// check that a settlement names the effect the step is executing: a
// settlement fact carries the EffectID its start fact recorded.
func requireModelEffect(s *MachineState, stepID StepID, effect EffectID) error {
	if err := requireModelStep(s, stepID, ModelExecuting); err != nil {
		return err
	}
	ms, _ := s.Current.(ModelStep)
	if effect != "" && effect != ms.Effect {
		return fmt.Errorf("agent: evolve: model step %q settles effect %q, executing %q", stepID, effect, ms.Effect)
	}
	return nil
}

// requireCallEffect checks that a settlement names the effect the call is
// executing; a call that never started settles with no effect.
func requireCallEffect(call *ToolCallState, effect EffectID) error {
	if effect != "" && effect != call.Effect {
		return fmt.Errorf("agent: evolve: call %q settles effect %q, executing %q", call.CallID, effect, call.Effect)
	}
	return nil
}

// requireCall returns the call when Current is ToolStep stepID and the call
// is in one of statuses.
func requireCall(s *MachineState, stepID StepID, callID CallID, statuses ...ToolCallStatus) (ToolCallState, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok || ts.RefValue.ID != stepID {
		return ToolCallState{}, fmt.Errorf("agent: evolve: tool step %q is not current", stepID)
	}
	i := ts.callIndex(callID)
	if i < 0 {
		return ToolCallState{}, fmt.Errorf("agent: evolve: unknown call %q", callID)
	}
	call := ts.Calls[i]
	for _, want := range statuses {
		if call.Status == want {
			return call, nil
		}
	}
	return ToolCallState{}, fmt.Errorf("agent: evolve: tool call %q is %s", callID, call.Status)
}

func guardTurnStarted(s *MachineState, fact *TurnStarted) error {
	if s.TurnID != "" || s.Current != nil || s.Status != TurnActive {
		return errors.New("agent: evolve: turn started on a non-zero state")
	}
	if err := ValidateNewTurn(NewTurn(*fact)); err != nil {
		return err
	}
	if fact.TurnID == "" {
		return errors.New("agent: evolve: turn started with empty TurnID")
	}
	return nil
}

// guardInputAccepted admits an input in any non-terminal state; only a
// duplicate pending InputID is illegal.
func guardInputAccepted(s *MachineState, fact *InputAccepted) error {
	if fact.Input.ID == "" {
		return errors.New("agent: evolve: input accepted with empty InputID")
	}
	for _, in := range s.PendingInputs {
		if in.ID == fact.Input.ID {
			// Decide rejects this and an exact replay never reaches Evolve, so
			// a persisted duplicate is a corrupt log, not an idempotent append.
			return fmt.Errorf("agent: evolve: input %q already pending", fact.Input.ID)
		}
	}
	return nil
}

func (m StateMachine) guardModelStepPrepared(s *MachineState, fact *ModelStepPrepared) error {
	if err := requireOpen(s, "model step prepared"); err != nil {
		return err
	}
	if fact.StepID == "" || fact.Model == "" || fact.RequestDigest == "" {
		return errors.New("agent: evolve: model step prepared is missing identity or digest")
	}
	// v1 preparation is the atomic consumption boundary for pending inputs.
	// A persisted fact must name every pending input exactly once, in queue
	// order; accepting a subset or an invented ID would make replay diverge
	// from the command that created this frozen request.
	if len(fact.InputIDs) != len(s.PendingInputs) {
		return fmt.Errorf("agent: evolve: model step prepared input IDs do not completely consume pending inputs: got %d, want %d", len(fact.InputIDs), len(s.PendingInputs))
	}
	for i, input := range s.PendingInputs {
		if fact.InputIDs[i] != input.ID {
			return fmt.Errorf("agent: evolve: model step prepared input ID at position %d = %q, want pending input %q", i, fact.InputIDs[i], input.ID)
		}
	}
	// The request body is not in the fact; its digest is checked against the
	// body by Decide and by the frozen.Store on read.
	return nil
}

func (m StateMachine) guardToolStepOpened(s *MachineState, fact *ToolStepOpened) error {
	if err := requireOpen(s, "tool step opened"); err != nil {
		return err
	}
	if len(fact.Calls) == 0 {
		return errors.New("agent: evolve: tool step opened with no calls")
	}
	if fact.StepID == "" || fact.Source == "" {
		return errors.New("agent: evolve: tool step is missing identity")
	}
	if _, err := normalizeToolScheduling(fact.Scheduling); err != nil {
		return fmt.Errorf("agent: evolve: tool step scheduling: %w", err)
	}
	if m.Identity.DeriveToolStepID(fact.Source) != fact.StepID {
		return errors.New("agent: evolve: tool step id does not derive from its source")
	}
	seen := make(map[CallID]struct{}, len(fact.Calls))
	for i := range fact.Calls {
		if err := m.guardToolCallBinding(s.TurnID, fact.StepID, &fact.Calls[i], seen); err != nil {
			return err
		}
	}
	return nil
}

func (m StateMachine) guardToolCallBinding(turnID TurnID, stepID StepID, call *ToolCallBinding, seen map[CallID]struct{}) error {
	if call.CallID == "" {
		return errors.New("agent: evolve: tool step contains empty CallID")
	}
	if _, dup := seen[call.CallID]; dup {
		return fmt.Errorf("agent: evolve: duplicate CallID %q", call.CallID)
	}
	seen[call.CallID] = struct{}{}
	if call.ToolRef == "" || call.Arguments.IsZero() {
		return fmt.Errorf("agent: evolve: tool call %q is missing binding data", call.CallID)
	}
	if call.Response == nil {
		return nil
	}
	kind, ok := responseKindForPolicy(call.Policy)
	if !ok {
		return fmt.Errorf("agent: evolve: direct call %q cannot carry a response request", call.CallID)
	}
	return m.validateResponseRequest(call.Response, turnID, stepID, call.CallID, kind, call.Arguments)
}

func (m StateMachine) guardToolCallApproved(s *MachineState, fact *ToolCallApproved) error {
	call, err := requireCall(s, fact.StepID, fact.CallID, ToolWaiting)
	if err != nil {
		return err
	}
	if err := requireWaitingFor(&call, ResponseApproval, fact.ResponseID); err != nil {
		return err
	}
	if d, err := m.Canonical.DigestToolResponseDecision(ResponseApproval, ResponseDecisionApproved, ""); err != nil || d != fact.ResponseDigest {
		return fmt.Errorf("agent: evolve: tool call %q approval digest mismatch", fact.CallID)
	}
	return nil
}

func (m StateMachine) guardToolCallAnswered(s *MachineState, fact *ToolCallAnswered) error {
	call, err := requireCall(s, fact.StepID, fact.CallID, ToolWaiting)
	if err != nil {
		return err
	}
	if err := requireWaitingFor(&call, ResponseExternal, fact.ResponseID); err != nil {
		return err
	}
	if fact.ResponseDigest == "" {
		return fmt.Errorf("agent: evolve: tool call %q answered without response digest", fact.CallID)
	}
	return nil
}

func requireWaitingFor(call *ToolCallState, kind ResponseKind, responseID ResponseID) error {
	if call.Waiting == nil || call.Waiting.Kind != kind {
		return fmt.Errorf("agent: evolve: tool call %q is not waiting for %s", call.CallID, kind)
	}
	if call.Waiting.ID != responseID {
		return fmt.Errorf("agent: evolve: tool call %q response ID mismatch", call.CallID)
	}
	return nil
}

func guardToolCallFailed(s *MachineState, fact *ToolCallFailed) error {
	call, err := requireCall(s, fact.StepID, fact.CallID, ToolPending, ToolExecuting, ToolWaiting)
	if err != nil {
		return err
	}
	if err := requireCallEffect(&call, fact.Effect); err != nil {
		return err
	}
	if fact.Outcome == ToolOutcomeUnknown && call.Status != ToolExecuting {
		return fmt.Errorf("agent: evolve: unknown outcome requires Executing call, %q is %s", fact.CallID, call.Status)
	}
	// Class/outcome agreement is the rule ValidateToolCallState applies to
	// the folded state; check it here so the error names the fact.
	return ValidateToolCallState(ToolCallState{CallID: fact.CallID, Status: ToolFailed,
		Failure: &ToolCallFailure{Failure: fact.Failure, Outcome: fact.Outcome}})
}

func responseKindForPolicy(p ResponsePolicy) (ResponseKind, bool) {
	switch p {
	case ApprovalRequired:
		return ResponseApproval, true
	case ExternalResponse:
		return ResponseExternal, true
	default:
		return "", false
	}
}

func (m StateMachine) validateResponseRequest(req *ResponseRequest, turnID TurnID, stepID StepID, callID CallID, kind ResponseKind, payload CanonicalJSON) error {
	if req.TurnID != turnID || req.StepID != stepID || req.CallID != callID || req.Kind != kind {
		return fmt.Errorf("agent: evolve: response request identity mismatch for call %q", callID)
	}
	if req.ID == "" || req.ID != m.Identity.DeriveResponseID(turnID, stepID, callID, kind) {
		return fmt.Errorf("agent: evolve: response request ID mismatch for call %q", callID)
	}
	if !req.Payload.Equal(payload) {
		return fmt.Errorf("agent: evolve: response request payload mismatch for call %q", callID)
	}
	return nil
}

func allToolCallsTerminal(calls []ToolCallState) bool {
	if len(calls) == 0 {
		return false
	}
	for i := range calls {
		if !calls[i].Status.Terminal() {
			return false
		}
	}
	return true
}
