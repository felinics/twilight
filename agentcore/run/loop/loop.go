package loop

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// Loop is the decision interpreter of one Run (RUN-LOP-2). It holds no
// authoritative state: every step starts from RunStore.Load, derives the next
// action with plan.Next, records the protocol transition and, for a start,
// hands the requested effect to the Executor as an Assignment keyed by its
// EffectID. Outcomes are read by that key through the Executor port and
// settled under the effect's derived CommandIDs; the Loop never waits on an
// effect inside Advance and never learns which attempt the Executor made
// for it.
type Loop struct {
	Executor Executor
	Builder  PromptBuilder
	Settings Settings

	mu       sync.Mutex
	slots    map[run.RunID]*runSlot
	eventsMu sync.Mutex
	// ownWatcher is the Watcher built when Settings names none; see watcher.
	ownWatcher *effect.Watcher
}

// runSlot serializes one Run: step guards a single Advance or Deliver at a
// time; driving marks a blocking Run in progress so a second driver is
// reported instead of interleaved (RUN-CMT-6). refs counts the callers holding
// the slot; the entry is dropped with the last release, so the map is bounded
// by the Runs being driven now rather than by every Run the Loop has seen.
type runSlot struct {
	step    sync.Mutex
	driving bool
	refs    int
}

// New validates the settings (RUN-LOP-1) and binds the executor and the
// prompt builder.
func New(exec Executor, builder PromptBuilder, settings Settings) (*Loop, error) {
	if exec == nil {
		return nil, errors.New("agent: loop: nil executor")
	}
	if builder == nil {
		return nil, errors.New("agent: loop: nil builder")
	}
	if m := settings.Scheduling.Mode; m != "" && m != run.ToolScheduleParallel && m != run.ToolScheduleSequential {
		return nil, fmt.Errorf("agent: loop: unknown scheduling mode %q", m)
	}
	if settings.Scheduling.MaxParallel < 0 {
		return nil, errors.New("agent: loop: negative MaxParallel")
	}
	return &Loop{Executor: exec, Builder: builder, Settings: settings, slots: make(map[run.RunID]*runSlot)}, nil
}

func (l *Loop) toolScheduling() run.ToolScheduling {
	s := l.Settings.Scheduling
	if s.Mode == "" {
		s.Mode = run.ToolScheduleParallel
	}
	return s
}

// targetFor resolves the opaque target of the effect ec describes
// (RUN-LOP-9). A nil resolver or a nil answer leaves the Assignment without a
// target; an incomplete answer is an error and the effect does not start.
// dispatchRetries and dispatchBackoff bound the Loop's answer to
// effect.ErrDispatchRetryable (RUN-EXE-3): the executor refused before
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

func (l *Loop) targetFor(ctx context.Context, ec EffectContext) (*run.TargetRef, error) {
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

// slotLocked returns the slot of runID, creating it; l.mu must be held.
func (l *Loop) slotLocked(runID run.RunID) *runSlot {
	s, ok := l.slots[runID]
	if !ok {
		s = &runSlot{}
		l.slots[runID] = s
	}
	return s
}

// releaseLocked drops one reference to the slot of runID and forgets the slot
// with the last one; l.mu must be held.
func (l *Loop) releaseLocked(runID run.RunID) {
	if s, ok := l.slots[runID]; ok {
		s.refs--
		if s.refs <= 0 {
			delete(l.slots, runID)
		}
	}
}

// acquire returns the slot of runID and holds a reference until release.
func (l *Loop) acquire(runID run.RunID) *runSlot {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.slotLocked(runID)
	s.refs++
	return s
}

func (l *Loop) release(runID run.RunID) {
	l.mu.Lock()
	l.releaseLocked(runID)
	l.mu.Unlock()
}

// startDriving marks runID as driven by a blocking Run and holds its slot for
// the drive; stopDriving releases both.
func (l *Loop) startDriving(runID run.RunID) (*runSlot, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.slotLocked(runID)
	if s.driving {
		return nil, ErrRunAlreadyRunning
	}
	s.driving = true
	s.refs++
	return s, nil
}

func (l *Loop) stopDriving(runID run.RunID) {
	l.mu.Lock()
	if s, ok := l.slots[runID]; ok {
		s.driving = false
	}
	l.releaseLocked(runID)
	l.mu.Unlock()
}

func (l *Loop) isDriving(runID run.RunID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.slots[runID]
	return ok && s.driving
}

func (l *Loop) checkArgs(ctx context.Context, store runtime.RunStore, runID run.RunID) error {
	if ctx == nil {
		return errors.New("agent: loop: nil context")
	}
	if store == nil {
		return errors.New("agent: loop: nil run store")
	}
	if store.Scope() == "" || runID == "" {
		return errors.New("agent: loop: empty Scope or RunID")
	}
	return nil
}

func (l *Loop) wrapSink(events EventSink) EventSink {
	if events == nil {
		return nil
	}
	return &serializedEventSink{sink: events, mu: &l.eventsMu}
}

// Advance moves the Run to its next quiescent point without waiting on any
// effect (RUN-LOP-2): it records protocol transitions (prepare, withdraw,
// start barriers) and dispatches Assignments, then returns LoopDispatched,
// LoopWaiting or LoopFinished. The caller reads each returned key through
// Executor.GetOutcome and passes it to Deliver. A concurrent Advance or a
// blocking Run of the same Run is reported as ErrRunAlreadyRunning. store is
// the RunStore bound to the caller's write capability: every commit of the
// step goes through it (OWN-HDL-2).
func (l *Loop) Advance(ctx context.Context, store runtime.RunStore, runID run.RunID, events EventSink) (LoopResult, error) {
	if err := l.checkArgs(ctx, store, runID); err != nil {
		return LoopResult{}, err
	}
	if l.isDriving(runID) {
		return LoopResult{}, ErrRunAlreadyRunning
	}
	s := l.acquire(runID)
	defer l.release(runID)
	if !s.step.TryLock() {
		return LoopResult{}, ErrRunAlreadyRunning
	}
	defer s.step.Unlock()
	return l.advance(ctx, store, runID, l.wrapSink(events))
}

// advance is the body of Advance. It only dispatches assignments and returns
// their keys; outcome retrieval is a separate message-shaped operation through
// Executor.GetOutcome. This keeps the Executor boundary usable across process
// boundaries.
func (l *Loop) advance(ctx context.Context, rt runtime.RunStore, runID run.RunID, events EventSink) (LoopResult, error) {
	for {
		if err := ctx.Err(); err != nil {
			return LoopResult{}, err
		}
		snapshot, err := rt.Load(ctx, runID)
		if err != nil {
			return LoopResult{}, err
		}
		if snapshot.State.RunID != runID {
			return LoopResult{}, fmt.Errorf("agent: loop: runtime returned RunID %q for %q", snapshot.State.RunID, runID)
		}
		if snapshot.State.Status.Terminal() {
			return l.finish(ctx, events, rt.Scope(), runID, snapshot.State.Result), nil
		}

		action, err := plan.Next(snapshot.State)
		if err != nil {
			return LoopResult{}, err
		}

		switch act := action.(type) {
		case plan.NeedModelRequest:
			// The hook runs while the Run is Open, before the plan reads
			// the context (RUN-LOP-10); what it commits moves no Run fact,
			// so the snapshot's Position stays valid for the Prepare.
			if hook := l.Settings.BeforePrepare; hook != nil {
				if err := hook(ctx, rt, act.Hint); err != nil {
					return LoopResult{}, err
				}
			}
			if err := l.planAndPrepare(ctx, rt, events, &snapshot, act.Hint); err != nil {
				return LoopResult{}, err
			}
		case plan.WithdrawPrepared:
			// Inputs arrived after this step was frozen: discard the unsent
			// request and replan with them (RUN-LOP-8). A retriable rejection
			// means another actor moved the Run; the reload decides.
			res, err := l.commit(ctx, rt, runID, schema.Identity().DeriveWithdrawCommandID(runID, act.StepID), snapshot.Position,
				run.WithdrawPreparedStep(act))
			if err != nil && !retriable(err) {
				return LoopResult{}, err
			}
			if err == nil {
				l.emitCommitted(ctx, events, rt.Scope(), runID, res.Facts)
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
			recovery := plan.NeedsRecovery(snapshot.State)
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

// finish is the single exit for a terminal Run, whether the terminal state
// was read by Load or returned by the settlement that produced it.
func (l *Loop) finish(ctx context.Context, events EventSink, scope run.Scope, runID run.RunID, result *run.RunResult) LoopResult {
	if events != nil {
		_ = events.Emit(ctx, Event{Session: scope, RunID: runID, Kind: EventRunFinished, Durability: EventCommitted})
	}
	return LoopResult{Disposition: LoopFinished, Result: result}
}

// Deliver settles one Outcome (RUN-EXE-4). It finds the model step or tool
// call Executing under the effect the Outcome's key names and commits the
// settlement under the effect's derived CommandID; an Outcome whose effect
// is no longer Executing is dropped and nothing is written. It returns
// LoopFinished when the settlement terminated the Run, LoopDelivered when the
// host should Advance next, LoopDropped for a stale Outcome. Ownership loss
// is returned as is (RUN-LOP-5).
func (l *Loop) Deliver(ctx context.Context, store runtime.RunStore, out Outcome, events EventSink) (LoopResult, error) {
	if err := l.checkArgs(ctx, store, out.Key.RunID); err != nil {
		return LoopResult{}, err
	}
	s := l.acquire(out.Key.RunID)
	defer l.release(out.Key.RunID)
	s.step.Lock()
	defer s.step.Unlock()
	return l.deliver(ctx, store, out, l.wrapSink(events))
}

func (l *Loop) deliver(ctx context.Context, rt runtime.RunStore, out Outcome, events EventSink) (LoopResult, error) {
	if out.Key.Session != "" && out.Key.Session != rt.Scope() {
		return LoopResult{Disposition: LoopDropped}, nil
	}
	runID := out.Key.RunID
	snapshot, err := rt.Load(ctx, runID)
	if err != nil {
		if errors.Is(err, runtime.ErrRunNotFound) {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		return LoopResult{}, err
	}
	if snapshot.State.Status.Terminal() {
		return LoopResult{Disposition: LoopDropped}, nil
	}
	ref := effectRef{runID: runID, id: out.Key.Effect}

	// The key names an effect; the machine state says which step or call is
	// Executing under it. An effect nothing is Executing under is stale.
	var cmd run.AgentCommand
	var settleErr error
	var stepID run.StepID
	var callID run.CallID
	switch cur := snapshot.State.Current.(type) {
	case run.ModelStep:
		if cur.Status != run.ModelExecuting || out.Key.Effect == "" || cur.Effect != out.Key.Effect {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		stepID = cur.RefValue.ID
		cmd, settleErr = l.modelCompletion(&cur, out)
	case run.ToolStep:
		call, ok := executingCall(&cur, out.Key.Effect)
		if !ok {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		stepID, callID = cur.RefValue.ID, call.CallID
		cmd = toolCompletion(stepID, callID, call.Effect, out)
	default:
		return LoopResult{Disposition: LoopDropped}, nil
	}

	// Settlement uses a detached control context: a cancelled host request must
	// not discard an accepted effect's outcome (RUN-LOP-5).
	finished, err := l.settle(context.WithoutCancel(ctx), rt, events, &ref, snapshot.Position, cmd)
	if err != nil {
		return LoopResult{}, err
	}
	if callID != "" && events != nil {
		_ = events.Emit(ctx, Event{Session: rt.Scope(), RunID: runID, StepID: stepID, CallID: callID,
			Kind: EventToolCompleted, Durability: EventCommitted})
	}
	if settleErr != nil {
		// The settlement landed (the step is withdrawn to Open) but the
		// condition -- a frozen body the executor cannot read -- would recur
		// on the next Advance, so the drive stops here and the host decides
		// whether to try again (RUN-LOP-3).
		return LoopResult{Disposition: LoopDelivered}, settleErr
	}
	if finished != nil {
		return l.finish(ctx, events, rt.Scope(), runID, finished), nil
	}
	return LoopResult{Disposition: LoopDelivered}, nil
}

// Run drives the Run until it finishes, has no executable action, or the
// context is cancelled (RUN-LOP-2): Advance, wait for the Outcomes of what it
// dispatched, GetOutcome, Deliver, repeat. It is the blocking form every
// host uses; hosts that receive Outcomes from elsewhere call Advance and
// Deliver themselves. The caller context bounds the drive: on cancellation the
// in-flight assignments of the Run are cancelled and their Outcomes are still
// settled (RUN-LOP-5) before ctx.Err() is returned.
func (l *Loop) Run(ctx context.Context, rt runtime.RunStore, runID run.RunID, events EventSink) (LoopResult, error) {
	if err := l.checkArgs(ctx, rt, runID); err != nil {
		return LoopResult{}, err
	}
	s, err := l.startDriving(runID)
	if err != nil {
		return LoopResult{}, err
	}
	defer l.stopDriving(runID)
	events = l.wrapSink(events)

	outcomes := make(chan outcomeRead, 64)
	pending := map[AssignmentKey]func(){}
	cancelled := false
	settleCtx := context.WithoutCancel(ctx)
	readCtx, stopReads := context.WithCancel(settleCtx)
	defer stopReads()
	// Registrations with the Watcher outlive this drive unless dropped: a
	// key still pending when Run returns (ownership lost, a read error)
	// belongs to whoever drives next.
	defer func() {
		for _, cancel := range pending {
			cancel()
		}
	}()

	// onOwnershipLost stops every in-flight effect: their Outcomes are not ours
	// to write any more (RUN-LOP-5). The cancelled effects still report, so the
	// pending Outcomes are drained -- never settled -- before returning; a tool
	// that ignores its context blocks here as it always would.
	onOwnershipLost := func(err error) error {
		for key := range pending {
			_ = l.Executor.Cancel(settleCtx, key)
		}
		for len(pending) > 0 {
			read := <-outcomes
			delete(pending, read.key)
		}
		return err
	}

	for {
		if len(pending) == 0 {
			if cancelled {
				return LoopResult{}, ctx.Err()
			}
			s.step.Lock()
			res, err := l.advance(ctx, rt, runID, events)
			s.step.Unlock()
			if err != nil {
				if ownershipLost(err) {
					return LoopResult{}, onOwnershipLost(err)
				}
				return LoopResult{}, err
			}
			if res.Disposition != LoopDispatched {
				return res, nil
			}
			for _, k := range res.Dispatched {
				pending[k] = l.awaitOutcome(readCtx, k, outcomes)
				if port, ok := l.Executor.(effect.ProgressPort); ok && events != nil {
					go l.forwardProgress(readCtx, port, k, events)
				}
			}
		}

		var read outcomeRead
		if cancelled {
			read = <-outcomes
		} else {
			select {
			case read = <-outcomes:
			case <-ctx.Done():
				// Stop what we started; each cancelled effect still reports an
				// Outcome, settled below under the detached context.
				cancelled = true
				for key := range pending {
					_ = l.Executor.Cancel(settleCtx, key)
				}
				continue
			}
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
			if ownershipLost(err) {
				return LoopResult{}, onOwnershipLost(err)
			}
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

// watcher is where this Loop's blocking Runs wait for Outcomes: the one
// Settings names, or a private one over the Executor built on first use.
func (l *Loop) watcher() *effect.Watcher {
	if l.Settings.Watcher != nil {
		return l.Settings.Watcher
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ownWatcher == nil {
		l.ownWatcher = &effect.Watcher{Port: l.Executor}
	}
	return l.ownWatcher
}

// awaitOutcome registers key with the Watcher and forwards what it finds to
// outcomes: the Outcome once the executor's settlement notice (or the
// Watcher's periodic read) makes it readable, or the definitive error of a
// key the executor will never answer for. Nothing is held open for the
// length of the execution; the returned cancel drops the registration.
func (l *Loop) awaitOutcome(ctx context.Context, key AssignmentKey, outcomes chan<- outcomeRead) (cancel func()) {
	send := func(read outcomeRead) {
		select {
		case outcomes <- read:
		case <-ctx.Done():
		}
	}
	return l.watcher().Watch(ctx, key,
		func(out Outcome) { send(outcomeRead{key: key, outcome: out}) },
		func(err error) { send(outcomeRead{key: key, err: err}) })
}

// commit builds the envelope via the sanctioned constructor and submits it.
// A non-sentinel commit failure is replayed once with the same CommandID
// (RUN-LOP-5): if the first attempt actually committed and only the response
// was lost, the replay returns AlreadyApplied instead of re-executing an
// expensive step. Ownership loss is never retried.
func (l *Loop) commit(ctx context.Context, rt runtime.RunStore, runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand) (runtime.CommitResult, error) {
	env, err := schema.Wire().Envelope(runID, id, cmd)
	if err != nil {
		return runtime.CommitResult{}, err
	}
	req := runtime.CommitRequest{Base: base, Command: env}
	res, err := rt.Commit(ctx, req)
	if err != nil && !retriable(err) && !ownershipLost(err) {
		res, err = rt.Commit(ctx, req)
	}
	return res, err
}

// retriable reports the commit errors that mean "reload and rederive".
func retriable(err error) bool {
	return errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrCommandConflict)
}
