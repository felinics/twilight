package loop

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/prompt"
	turn "github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/plan"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
)

// Loop is the decision interpreter of one Turn. It holds no
// authoritative state: every step starts from TurnStore.Load, derives the next
// action with plan.Next, records the protocol transition and, for a start,
// hands the requested effect to the Executor as an Assignment keyed by its
// EffectID. Outcomes are read by that key through the Executor port and
// settled under the effect's derived CommandIDs; the Loop never waits on an
// effect inside Advance and never learns which attempt the Executor made
// for it.
type Loop struct {
	Executor Executor
	Builder  prompt.Builder
	Settings Settings

	mu       sync.Mutex
	slots    map[turn.TurnID]*turnSlot
	eventsMu sync.Mutex
}

// turnSlot serializes one Turn: step guards a single Advance or Deliver at a
// time; driving marks a blocking Turn drive in progress so a second driver is
// reported instead of interleaved. refs counts the callers holding
// the slot; the entry is dropped with the last release, so the map is bounded
// by the Turns being driven now rather than by every Turn the Loop has seen.
type turnSlot struct {
	step    sync.Mutex
	driving bool
	refs    int
}

// New validates the settings and binds the executor and the
// prompt builder.
func New(exec Executor, builder prompt.Builder, settings Settings) (*Loop, error) {
	if exec == nil {
		return nil, errors.New("agent: loop: nil executor")
	}
	if builder == nil {
		return nil, errors.New("agent: loop: nil builder")
	}
	if m := settings.Scheduling.Mode; m != "" && m != turn.ToolScheduleParallel && m != turn.ToolScheduleSequential {
		return nil, fmt.Errorf("agent: loop: unknown scheduling mode %q", m)
	}
	if settings.Scheduling.MaxParallel < 0 {
		return nil, errors.New("agent: loop: negative MaxParallel")
	}
	if settings.MaxCandidateRebuilds < 0 {
		return nil, errors.New("agent: loop: negative MaxCandidateRebuilds")
	}
	return &Loop{Executor: exec, Builder: builder, Settings: settings, slots: make(map[turn.TurnID]*turnSlot)}, nil
}

func (l *Loop) toolScheduling() turn.ToolScheduling {
	s := l.Settings.Scheduling
	if s.Mode == "" {
		s.Mode = turn.ToolScheduleParallel
	}
	return s
}

// targetFor resolves the opaque target of the effect ec describes
// A nil resolver or a nil answer leaves the Assignment without a
// target; an incomplete answer is an error and the effect does not start.
// dispatchRetries and dispatchBackoff bound the Loop's answer to
// effect.ErrDispatchRetryable: the executor refused before
// anything started, so the same Assignment is offered again a few times
// inside this Advance, per Settings.Dispatch, before the refusal is treated
// like any other Known dispatch failure.

// dispatch hands an Assignment to the Executor, repeating a retryable refusal
// within the dispatch policy; every other answer is returned as is.
func (l *Loop) dispatch(ctx context.Context, a Assignment) error {
	var err error
	policy := l.Settings.Dispatch
	for attempt := 1; ; attempt++ {
		err = l.Executor.Dispatch(ctx, a)
		if err == nil || !errors.Is(err, effect.ErrDispatchRetryable) || attempt >= policy.retries() {
			return err
		}
		timer := time.NewTimer(policy.backoff() * time.Duration(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}

// A temporary refusal may still follow durable acceptance. Cancellation also
// proves nothing about the far side of an RPC. Keep the effect identity until
// reconciliation can distinguish acceptance from a fenced absence.
func recoverableDispatch(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, effect.ErrDispatchUnknown) ||
		errors.Is(err, effect.ErrDispatchRetryable) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (l *Loop) targetFor(ctx context.Context, ec EffectContext) (*turn.TargetRef, error) {
	if l.Settings.TargetResolver == nil {
		return nil, nil
	}
	target, err := l.Settings.TargetResolver.ResolveTarget(ctx, ec)
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, nil
	}
	if target.Kind == "" || target.ID == "" {
		return nil, errors.New("agent: loop: target resolver returned an incomplete target")
	}
	cloned := *target
	return &cloned, nil
}

// slotLocked returns the slot of turnID, creating it; l.mu must be held.
func (l *Loop) slotLocked(turnID turn.TurnID) *turnSlot {
	s, ok := l.slots[turnID]
	if !ok {
		s = &turnSlot{}
		l.slots[turnID] = s
	}
	return s
}

// releaseLocked drops one reference to the slot of turnID and forgets the slot
// with the last one; l.mu must be held.
func (l *Loop) releaseLocked(turnID turn.TurnID) {
	if s, ok := l.slots[turnID]; ok {
		s.refs--
		if s.refs <= 0 {
			delete(l.slots, turnID)
		}
	}
}

// acquire returns the slot of turnID and holds a reference until release.
func (l *Loop) acquire(turnID turn.TurnID) *turnSlot {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.slotLocked(turnID)
	s.refs++
	return s
}

func (l *Loop) release(turnID turn.TurnID) {
	l.mu.Lock()
	l.releaseLocked(turnID)
	l.mu.Unlock()
}

// startDriving marks turnID as driven by a blocking Run and holds its slot for
// the drive; stopDriving releases both.
func (l *Loop) startDriving(turnID turn.TurnID) (*turnSlot, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.slotLocked(turnID)
	if s.driving {
		return nil, ErrTurnAlreadyRunning
	}
	s.driving = true
	s.refs++
	return s, nil
}

func (l *Loop) stopDriving(turnID turn.TurnID) {
	l.mu.Lock()
	if s, ok := l.slots[turnID]; ok {
		s.driving = false
	}
	l.releaseLocked(turnID)
	l.mu.Unlock()
}

func (l *Loop) isDriving(turnID turn.TurnID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.slots[turnID]
	return ok && s.driving
}

func (l *Loop) checkArgs(ctx context.Context, st store.TurnStore, turnID turn.TurnID) error {
	if ctx == nil {
		return errors.New("agent: loop: nil context")
	}
	if st == nil {
		return errors.New("agent: loop: nil TurnStore")
	}
	if st.Scope() == "" || turnID == "" {
		return errors.New("agent: loop: empty Scope or TurnID")
	}
	return nil
}

func (l *Loop) wrapSink(events EventSink) EventSink {
	if events == nil {
		return nil
	}
	return &serializedEventSink{sink: events, mu: &l.eventsMu}
}

// Advance moves the Turn to its next quiescent point without waiting on any
// effect: it records protocol transitions (prepare, withdraw,
// start barriers) and dispatches Assignments, then returns LoopDispatched,
// LoopWaiting or LoopFinished. The caller reads each returned key through
// Executor.GetOutcome and passes it to Deliver. A concurrent Advance or a
// blocking drive of the same Turn is reported as ErrTurnAlreadyRunning. store is
// the TurnStore bound to the caller's write capability: every commit of the
// step goes through it.
func (l *Loop) Advance(ctx context.Context, st store.TurnStore, turnID turn.TurnID, events EventSink) (LoopResult, error) {
	if err := l.checkArgs(ctx, st, turnID); err != nil {
		return LoopResult{}, err
	}
	if l.isDriving(turnID) {
		return LoopResult{}, ErrTurnAlreadyRunning
	}
	s := l.acquire(turnID)
	defer l.release(turnID)
	if !s.step.TryLock() {
		return LoopResult{}, ErrTurnAlreadyRunning
	}
	defer s.step.Unlock()
	return l.advance(ctx, st, turnID, l.wrapSink(events))
}

// advance is the body of Advance. It only dispatches assignments and returns
// their keys; outcome retrieval is a separate message-shaped operation through
// Executor.GetOutcome. This keeps the Executor boundary usable across process
// boundaries.
func (l *Loop) advance(ctx context.Context, rt store.TurnStore, turnID turn.TurnID, events EventSink) (LoopResult, error) {
	maxRebuilds := l.Settings.MaxCandidateRebuilds
	if maxRebuilds == 0 {
		maxRebuilds = 4
	}
	rebuilds := 0
	candidateContended := false
	var lastPosition turn.TurnPosition
	for {
		if err := ctx.Err(); err != nil {
			return LoopResult{}, err
		}
		snapshot, err := rt.Load(ctx, turnID)
		if err != nil {
			return LoopResult{}, err
		}
		if snapshot.State.TurnID != turnID {
			return LoopResult{}, fmt.Errorf("agent: loop: store returned TurnID %q for %q", snapshot.State.TurnID, turnID)
		}
		if snapshot.State.Status.Terminal() {
			return l.finish(ctx, events, rt.Scope(), turnID, snapshot.State.Result), nil
		}
		// Only authoritative Turn progress resets the contention bound, not
		// another Build or a context-only change made by a hook.
		if snapshot.Position != lastPosition {
			rebuilds = 0
			candidateContended = false
			lastPosition = snapshot.Position
		}
		if candidateContended {
			return LoopResult{}, ErrCandidateContended
		}

		action, err := plan.Next(snapshot.State)
		if err != nil {
			return LoopResult{}, err
		}

		switch act := action.(type) {
		case plan.NeedModelRequest:
			// The hook runs while the Turn is Open, before the plan reads
			// the context; what it commits moves no Turn fact,
			// so the snapshot's Position stays valid for the Prepare.
			if hook := l.Settings.BeforePrepare; hook != nil {
				if err := hook(ctx, rt, act.Hint); err != nil {
					return LoopResult{}, err
				}
			}
			decision, err := l.planAndPrepare(ctx, rt, events, &snapshot, act.Hint)
			if err != nil {
				return LoopResult{}, err
			}
			switch decision {
			case CandidateRebuild:
				if rebuilds >= maxRebuilds {
					candidateContended = true
				} else {
					rebuilds++
				}
				// Reload before enforcing the bound: the hook may itself
				// have advanced Turn authority. Replan the hint and CAS too.
			case CandidateMaintenance:
				return LoopResult{Disposition: LoopWaiting, Reason: ContextMaintenance}, nil
			}
		case plan.WithdrawPrepared:
			// Inputs arrived after this step was frozen: discard the unsent
			// request and replan with them. A retriable rejection
			// means another actor moved the Turn; the reload decides.
			res, err := l.commit(ctx, rt, turnID, schema.Identity().DeriveWithdrawCommandID(turnID, act.StepID), snapshot.Position,
				turn.WithdrawPreparedStep(act))
			if err != nil && !retriable(err) {
				return LoopResult{}, err
			}
			if err == nil {
				l.emitCommitted(ctx, events, rt.Scope(), turnID, res.Facts)
			}
		case plan.StartModelCall:
			dispatched, err := l.startModelStep(ctx, rt, events, &snapshot, act.StepID)
			if err != nil {
				return LoopResult{}, err
			}
			if dispatched != nil {
				return LoopResult{Disposition: LoopDispatched, Dispatched: []AssignmentKey{*dispatched}}, nil
			}
		case plan.StartToolCalls:
			dispatched, err := l.startToolCalls(ctx, rt, events, &snapshot, act)
			if err != nil {
				return LoopResult{}, err
			}
			if len(dispatched) > 0 {
				return LoopResult{Disposition: LoopDispatched, Dispatched: dispatched}, nil
			}
		case plan.Idle:
			recovery := turn.NeedsRecovery(snapshot.State)
			reason := WaitReason("")
			if recovery {
				reason = ExecutionRecovery
			}
			return LoopResult{Disposition: LoopWaiting, Reason: reason, ExecutionRecovery: recovery}, nil
		default:
			return LoopResult{}, fmt.Errorf("agent: loop: unknown action %T", action)
		}
	}
}

// finish is the single exit for a terminal Turn, whether the terminal state
// was read by Load or returned by the settlement that produced it.
func (l *Loop) finish(ctx context.Context, events EventSink, scope turn.Scope, turnID turn.TurnID, result *turn.TurnResult) LoopResult {
	if events != nil {
		_ = events.Emit(ctx, Event{Session: scope, TurnID: turnID, Kind: EventTurnFinished, Durability: EventCommitted})
	}
	return LoopResult{Disposition: LoopFinished, Result: result}
}

// Deliver settles one Outcome. It finds the model step or tool
// call Executing under the effect the Outcome's key names and commits the
// settlement under the effect's derived CommandID; an Outcome whose effect
// is no longer Executing is dropped and nothing is written. It returns
// LoopFinished when the settlement terminated the Turn, LoopDelivered when the
// host should Advance next, LoopDropped for a stale Outcome. Ownership loss
// is returned as is.
func (l *Loop) Deliver(ctx context.Context, st store.TurnStore, out Outcome, events EventSink) (LoopResult, error) {
	if out.Key.OwnerKind != OwnerTurn {
		return LoopResult{}, errors.New("loop: outcome requires a Turn owner")
	}
	if err := l.checkArgs(ctx, st, turn.TurnID(out.Key.OwnerID)); err != nil {
		return LoopResult{}, err
	}
	s := l.acquire(turn.TurnID(out.Key.OwnerID))
	defer l.release(turn.TurnID(out.Key.OwnerID))
	s.step.Lock()
	defer s.step.Unlock()
	return l.deliver(ctx, st, out, l.wrapSink(events))
}

func (l *Loop) deliver(ctx context.Context, rt store.TurnStore, out Outcome, events EventSink) (LoopResult, error) {
	if out.Key.Session != "" && out.Key.Session != rt.Scope() {
		return LoopResult{Disposition: LoopDropped}, nil
	}
	turnID := turn.TurnID(out.Key.OwnerID)
	snapshot, err := rt.Load(ctx, turnID)
	if err != nil {
		if errors.Is(err, store.ErrTurnNotFound) {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		return LoopResult{}, err
	}
	if snapshot.State.Status.Terminal() {
		return LoopResult{Disposition: LoopDropped}, nil
	}
	ref := effectRef{turnID: turnID, id: out.Key.Effect}

	// The key names an effect; the machine state says which step or call is
	// Executing under it. An effect nothing is Executing under is stale.
	var cmd turn.AgentCommand
	var settleErr error
	var stepID turn.StepID
	var callID turn.CallID
	switch cur := snapshot.State.Current.(type) {
	case turn.ModelStep:
		if cur.Status != turn.ModelExecuting || out.Key.Effect == "" || cur.Effect != out.Key.Effect {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		stepID = cur.RefValue.ID
		cmd, settleErr = l.modelCompletion(&cur, out)
	case turn.ToolStep:
		call, ok := executingCall(&cur, out.Key.Effect)
		if !ok {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		stepID, callID = cur.RefValue.ID, call.CallID
		cmd = toolCompletion(stepID, callID, call.Effect, out)
	default:
		return LoopResult{Disposition: LoopDropped}, nil
	}

	// The Outcome remains durable until ACK. Keep the caller's deadline on
	// settlement RPCs: cancellation leaves a recoverable delivery obligation,
	// rather than an unbounded commit pinning this write capability.
	finished, err := l.settle(ctx, rt, events, &ref, snapshot.Position, cmd)
	if err != nil {
		return LoopResult{}, err
	}
	if callID != "" && events != nil {
		_ = events.Emit(ctx, Event{Session: rt.Scope(), TurnID: turnID, StepID: stepID, CallID: callID,
			Kind: EventToolCompleted, Durability: EventCommitted})
	}
	if settleErr != nil {
		// The settlement landed (the step is withdrawn to Open) but the
		// condition -- a frozen body the executor cannot read -- would recur
		// on the next Advance, so the drive stops here and the host decides
		// whether to try again.
		return LoopResult{Disposition: LoopDelivered}, settleErr
	}
	if finished != nil {
		return l.finish(ctx, events, rt.Scope(), turnID, finished), nil
	}
	return LoopResult{Disposition: LoopDelivered}, nil
}

// Run drives the Turn until it finishes, has no executable action, or the
// context is cancelled: Advance, wait for the Outcomes of what it
// dispatched, GetOutcome, Deliver, repeat. This synchronous convenience uses
// the same Advance/Deliver protocol; Activation hosts use those directly.
// Context cancellation detaches local observation. Only a committed domain
// cancellation authorizes cancelling an accepted external execution.
func (l *Loop) Run(ctx context.Context, rt store.TurnStore, turnID turn.TurnID, events EventSink) (LoopResult, error) {
	if err := l.checkArgs(ctx, rt, turnID); err != nil {
		return LoopResult{}, err
	}
	s, err := l.startDriving(turnID)
	if err != nil {
		return LoopResult{}, err
	}
	defer l.stopDriving(turnID)
	events = l.wrapSink(events)

	outcomes := make(chan outcomeRead, 64)
	pending := map[AssignmentKey]func(){}
	watcher := l.Settings.Watcher
	if watcher == nil {
		watcher = &effect.Watcher{Port: l.Executor}
		defer watcher.Close()
	}
	settleCtx := ctx
	readCtx, stopReads := context.WithCancel(settleCtx)
	defer stopReads()
	// Registrations with the Watcher outlive this drive unless dropped: a
	// key still pending when the drive returns (ownership lost, a read error)
	// belongs to whoever drives next.
	defer func() {
		for _, cancel := range pending {
			cancel()
		}
	}()

	for {
		if len(pending) == 0 {
			s.step.Lock()
			res, err := l.advance(ctx, rt, turnID, events)
			s.step.Unlock()
			if err != nil {
				return LoopResult{}, err
			}
			if res.Disposition != LoopDispatched {
				return res, nil
			}
			for _, k := range res.Dispatched {
				pending[k] = awaitOutcome(readCtx, watcher, k, outcomes)
				if port, ok := l.Executor.(effect.ProgressPort); ok && events != nil {
					go l.forwardProgress(readCtx, port, k, events)
				}
			}
		}

		var read outcomeRead
		select {
		case read = <-outcomes:
		case <-ctx.Done():
			return LoopResult{}, ctx.Err()
		}
		if _, ours := pending[read.key]; !ours {
			continue // an Outcome of an attempt this drive did not dispatch
		}
		if read.err != nil {
			return LoopResult{}, fmt.Errorf("agent: loop: read outcome: %w", read.err)
		}
		pending[read.key]()
		delete(pending, read.key)

		s.step.Lock()
		res, err := l.deliver(settleCtx, rt, read.outcome, events)
		s.step.Unlock()
		if err != nil {
			return res, err
		}
		if res.Disposition == LoopFinished {
			return res, nil
		}
	}
}

type outcomeRead struct {
	key     AssignmentKey
	outcome Outcome
	err     error
}

// awaitOutcome registers key with the Watcher and forwards what it finds to
// outcomes: the Outcome once the executor's settlement notice (or the
// Watcher's periodic read) makes it readable, or the definitive error of a
// key the executor will never answer for. Nothing is held open for the
// length of the execution; the returned cancel drops the registration.
func awaitOutcome(ctx context.Context, watcher *effect.Watcher, key AssignmentKey, outcomes chan<- outcomeRead) (cancel func()) {
	send := func(read outcomeRead) {
		select {
		case outcomes <- read:
		case <-ctx.Done():
		}
	}
	return watcher.Watch(ctx, key,
		func(out Outcome) { send(outcomeRead{key: key, outcome: out}) },
		func(err error) { send(outcomeRead{key: key, err: err}) })
}

// commit builds the envelope via the sanctioned constructor and submits it.
// A non-sentinel commit failure is replayed once with the same CommandID
// if the first attempt actually committed and only the response
// was lost, the replay returns AlreadyApplied instead of re-executing an
// expensive step. Ownership loss is never retried.
func (l *Loop) commit(ctx context.Context, rt store.TurnStore, turnID turn.TurnID, id turn.CommandID, base turn.TurnPosition, cmd turn.AgentCommand) (store.CommitResult, error) {
	env, err := schema.Wire().Envelope(turnID, id, cmd)
	if err != nil {
		return store.CommitResult{}, err
	}
	req := store.CommitRequest{Base: base, Command: env}
	res, err := rt.Commit(ctx, req)
	if err != nil && !retriable(err) && !ownershipLost(err) {
		res, err = rt.Commit(ctx, req)
	}
	return res, err
}

// retriable reports the commit errors that mean "reload and rederive".
func retriable(err error) bool {
	return errors.Is(err, turn.ErrStaleRuntime) || errors.Is(err, turn.ErrTurnTerminal) || errors.Is(err, turn.ErrCommandConflict)
}
