package loop

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/prompt"
	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/store"
)

// Loop is the interpreter of Runs (RUN-LOP-2). It holds no
// authoritative state: every step starts from RunStore.Load, derives the next
// action with plan.Next, records the protocol transition and, for a start,
// hands the requested effect to the Executor as an Assignment keyed by its
// EffectID. An Outcome comes back by that key through Deliver and is settled
// under the effect's derived CommandIDs. The Loop never waits on an effect:
// Advance returns with the keys it dispatched, and whoever hosts the Run
// reads their Outcomes and delivers them. It never learns which attempt the
// Executor made for an effect.
//
// One Loop serves every Run of a deployment: what differs per Run, the
// prompt Builder of its preset, is Advance's argument, and everything a
// settlement needs is read from the step the Run froze. Deliver and
// Redispatch therefore need no preset at all.
type Loop struct {
	// Ports is the effect layer the Loop dispatches through; of the optional
	// capabilities it uses only Ack, to acknowledge a settled effect.
	Ports    effect.Ports
	Settings Settings

	mu    sync.Mutex
	slots map[run.RunID]*runSlot
}

// runSlot serializes one Run: step guards a single Advance or Deliver at a
// time (RUN-CMT-6). refs counts the callers holding the slot; the entry is
// dropped with the last release, so the map is bounded by the Runs being
// stepped now rather than by every Run the Loop has seen.
type runSlot struct {
	step sync.Mutex
	refs int
}

// New binds the effect ports and the deployment's settings.
func New(ports effect.Ports, settings Settings) (*Loop, error) {
	if ports.Execution == nil {
		return nil, errors.New("agent: loop: nil executor")
	}
	return &Loop{Ports: ports, Settings: settings, slots: make(map[run.RunID]*runSlot)}, nil
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
func (l *Loop) dispatch(ctx context.Context, a effect.Assignment) error {
	var err error
	policy := l.Settings.Dispatch
	for attempt := 1; ; attempt++ {
		err = l.Ports.Execution.Dispatch(ctx, a)
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

func (l *Loop) checkArgs(ctx context.Context, st store.RunStore, runID run.RunID) error {
	if ctx == nil {
		return errors.New("agent: loop: nil context")
	}
	if st == nil {
		return errors.New("agent: loop: nil run store")
	}
	if st.Scope() == "" || runID == "" {
		return errors.New("agent: loop: empty Scope or RunID")
	}
	return nil
}

// Advance moves the Run to its next quiescent point without waiting on any
// effect (RUN-LOP-2): it records protocol transitions (prepare, withdraw,
// start barriers) and dispatches Assignments, then returns LoopDispatched,
// LoopWaiting or LoopFinished. builder is the Run's preset's prompt Builder:
// it is asked for the next model request when the Run is Open, and the
// policy it returns is frozen onto the step. The caller reads each returned
// key through the executor and passes its Outcome to Deliver. One Run takes
// one step at a time: a concurrent Advance or Deliver of the same Run waits
// for the step in progress and then takes its own, from the state that step
// left. store is the RunStore bound to the caller's write capability:
// every commit of the step goes through it (OWN-HDL-2).
func (l *Loop) Advance(ctx context.Context, st store.RunStore, builder prompt.Builder, runID run.RunID) (LoopResult, error) {
	if err := l.checkArgs(ctx, st, runID); err != nil {
		return LoopResult{}, err
	}
	if builder == nil {
		return LoopResult{}, errors.New("agent: loop: nil builder")
	}
	s := l.acquire(runID)
	defer l.release(runID)
	s.step.Lock()
	defer s.step.Unlock()
	return l.advance(ctx, st, builder, runID)
}

// advance is the body of Advance. It only dispatches assignments and returns
// their keys; outcome retrieval is a separate message-shaped operation through
// Executor.GetOutcome. This keeps the Executor boundary usable across process
// boundaries.
func (l *Loop) advance(ctx context.Context, rt store.RunStore, builder prompt.Builder, runID run.RunID) (LoopResult, error) {
	for {
		if err := ctx.Err(); err != nil {
			return LoopResult{}, err
		}
		snapshot, err := rt.Load(ctx, runID)
		if err != nil {
			return LoopResult{}, err
		}
		if snapshot.State.RunID != runID {
			return LoopResult{}, fmt.Errorf("agent: loop: store returned RunID %q for %q", snapshot.State.RunID, runID)
		}
		if snapshot.State.Status.Terminal() {
			return finish(snapshot.State.Result), nil
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
				if err := hook(ctx, rt.Scope(), act.Hint); err != nil {
					return LoopResult{}, err
				}
			}
			if err := l.planAndPrepare(ctx, rt, builder, &snapshot, act.Hint); err != nil {
				return LoopResult{}, err
			}
		case plan.WithdrawPrepared:
			// Inputs arrived after this step was frozen: discard the unsent
			// request and replan with them (RUN-LOP-8). A retriable rejection
			// means another actor moved the Run; the reload decides.
			_, err := l.commit(ctx, rt, runID, schema.Identity().DeriveWithdrawCommandID(runID, act.StepID), snapshot.Position,
				run.WithdrawPreparedStep(act))
			if err != nil && !retriable(err) {
				return LoopResult{}, err
			}
		case plan.StartModelCall:
			dispatched, err := l.startModelStep(ctx, rt, &snapshot, act.StepID)
			if err != nil {
				return LoopResult{}, err
			}
			if dispatched != nil {
				return LoopResult{Disposition: LoopDispatched, Dispatched: []effect.AssignmentKey{*dispatched}}, nil
			}
		case plan.StartToolCalls:
			dispatched, held, err := l.startToolCalls(ctx, rt, &snapshot, act)
			if err != nil {
				return LoopResult{}, err
			}
			if len(dispatched) > 0 {
				return LoopResult{Disposition: LoopDispatched, Dispatched: dispatched}, nil
			}
			if held {
				// The scheduling window is full of calls in flight: the
				// Pending ones start when one of them settles.
				return LoopResult{Disposition: LoopWaiting, Executing: executingKeys(rt.Scope(), &snapshot.State)}, nil
			}
		case plan.Idle:
			res := LoopResult{Disposition: LoopWaiting}
			if run.NeedsRecovery(snapshot.State) {
				res.Executing = executingKeys(rt.Scope(), &snapshot.State)
			}
			return res, nil
		default:
			return LoopResult{}, fmt.Errorf("agent: loop: unknown action %T", action)
		}
	}
}

// executingKeys are the keys of the effects state is Executing under: the
// model step's, or one per Executing tool call.
func executingKeys(scope run.Scope, state *run.MachineState) []effect.AssignmentKey {
	var keys []effect.AssignmentKey
	switch cur := state.Current.(type) {
	case run.ModelStep:
		if cur.Status == run.ModelExecuting && cur.Effect != "" {
			keys = append(keys, effect.AssignmentKey{Session: scope, RunID: state.RunID, Effect: cur.Effect})
		}
	case run.ToolStep:
		for i := range cur.Calls {
			if c := &cur.Calls[i]; c.Status == run.ToolExecuting && c.Effect != "" {
				keys = append(keys, effect.AssignmentKey{Session: scope, RunID: state.RunID, Effect: c.Effect})
			}
		}
	}
	return keys
}

// finish is the single exit for a terminal Run, whether the terminal state
// was read by Load or returned by the settlement that produced it.
func finish(result *run.RunResult) LoopResult {
	return LoopResult{Disposition: LoopFinished, Result: result}
}

// Deliver settles one Outcome (RUN-EXE-4). It finds the model step or tool
// call Executing under the effect the Outcome's key names and commits the
// settlement under the effect's derived CommandID; an Outcome whose effect
// is no longer Executing is dropped and nothing is written. It returns
// LoopFinished when the settlement terminated the Run, LoopDelivered when the
// host should Advance next, LoopDropped for a stale Outcome. Ownership loss
// is returned as is (RUN-LOP-5).
func (l *Loop) Deliver(ctx context.Context, st store.RunStore, out effect.Outcome) (LoopResult, error) {
	if err := l.checkArgs(ctx, st, out.Key.RunID); err != nil {
		return LoopResult{}, err
	}
	s := l.acquire(out.Key.RunID)
	defer l.release(out.Key.RunID)
	s.step.Lock()
	defer s.step.Unlock()
	return l.deliver(ctx, st, out)
}

func (l *Loop) deliver(ctx context.Context, rt store.RunStore, out effect.Outcome) (LoopResult, error) {
	if out.Key.Session != "" && out.Key.Session != rt.Scope() {
		return LoopResult{Disposition: LoopDropped}, nil
	}
	runID := out.Key.RunID
	snapshot, err := rt.Load(ctx, runID)
	if err != nil {
		if errors.Is(err, store.ErrRunNotFound) {
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
	switch cur := snapshot.State.Current.(type) {
	case run.ModelStep:
		if cur.Status != run.ModelExecuting || out.Key.Effect == "" || cur.Effect != out.Key.Effect {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		cmd, settleErr = l.modelCompletion(&cur, out)
	case run.ToolStep:
		call, ok := executingCall(&cur, out.Key.Effect)
		if !ok {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		cmd = toolCompletion(cur.RefValue.ID, call.CallID, call.Effect, out)
	default:
		return LoopResult{Disposition: LoopDropped}, nil
	}

	// Settlement uses a detached control context: a cancelled host request must
	// not discard an accepted effect's outcome (RUN-LOP-5).
	finished, err := l.settle(context.WithoutCancel(ctx), rt, &ref, snapshot.Position, cmd)
	if err != nil {
		return LoopResult{}, err
	}
	if settleErr != nil {
		// The settlement landed (the step is withdrawn to Open) but the
		// condition -- a frozen body the executor cannot read -- would recur
		// on the next Advance, so the drive stops here and the host decides
		// whether to try again (RUN-LOP-3).
		return LoopResult{Disposition: LoopDelivered}, settleErr
	}
	if finished != nil {
		return finish(finished), nil
	}
	return LoopResult{Disposition: LoopDelivered}, nil
}

// commit builds the envelope via the sanctioned constructor and submits it.
// A non-sentinel commit failure is replayed once with the same CommandID
// (RUN-LOP-5): if the first attempt actually committed and only the response
// was lost, the replay returns AlreadyApplied instead of re-executing an
// expensive step. Ownership loss is never retried.
func (l *Loop) commit(ctx context.Context, rt store.RunStore, runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand) (store.CommitResult, error) {
	env, err := schema.Wire().Envelope(runID, id, cmd)
	if err != nil {
		return store.CommitResult{}, err
	}
	req := store.CommitRequest{Base: base, Command: env}
	res, err := rt.Commit(ctx, req)
	if err != nil && !retriable(err) && !errors.Is(err, store.ErrOwnershipLost) {
		res, err = rt.Commit(ctx, req)
	}
	return res, err
}

// retriable reports the commit errors that mean "reload and rederive".
func retriable(err error) bool {
	return errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrCommandConflict)
}
