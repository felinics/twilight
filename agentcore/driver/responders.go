package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Responder answers an ExternalResponse wait on the system's behalf: a tool
// whose ResponsePolicy is ExternalResponse settles not through the executor
// but through whoever answers its ResponseRequest, and a Responder is such
// an answerer living in the owner process (the subagent tool). A drive that
// leaves such a call waiting asks the Responder and drives on with its
// answer, and a Session that opens with such calls waiting asks again, so a
// Responder must be idempotent against its own durable state: a crash
// mid-answer is continued, not repeated. The payload it returns settles the
// call as a success (SubmitToolResponse); an error rejects it
// (RejectToolCall, response_rejected) with the error as reason.
type Responder interface {
	Respond(ctx context.Context, w writer.Writer, call *WaitingCall) (run.CanonicalJSON, error)
}

// WaitingCall is what a Responder answers: the Run's ResponseRequest and
// the call's frozen binding.
type WaitingCall struct {
	Request   run.ResponseRequest
	ToolRef   run.ToolRef
	Arguments run.CanonicalJSON
}

// Responders answers the ExternalResponse waits of the Runs this process
// drives, one Responder per ToolRef. Each wait is answered by one goroutine
// at a time across every drive and every resume of this process.
type Responders struct {
	// Runs is the Run module's Session adapter the answers commit through.
	Runs *sessionstore.SessionRunStore
	// Tools are the Responders by the ToolRef whose waits they answer.
	Tools map[run.ToolRef]Responder
	// Fail receives an answer that could not be settled; nil discards it.
	Fail func(session.SessionID, error)

	mu sync.Mutex
	// answering are the ResponseIDs a Responder is working on.
	answering map[run.ResponseID]struct{}
}

// answer is one wait claimed for answering.
type answer struct {
	responder Responder
	call      WaitingCall
}

func (rs *Responders) fail(sid session.SessionID, err error) {
	if rs.Fail != nil {
		rs.Fail(sid, err)
	}
}

// claim returns the ExternalResponse waits of the Run that have a Responder
// and are not being answered, marking them as being answered; the caller
// releases each with release.
func (rs *Responders) claim(ctx context.Context, w writer.Writer, runID run.RunID) []answer {
	if len(rs.Tools) == 0 {
		return nil
	}
	snap, err := rs.Runs.Bind(w).Load(ctx, runID)
	if err != nil || snap.State.Status.Terminal() {
		return nil
	}
	step, ok := snap.State.Current.(run.ToolStep)
	if !ok {
		return nil
	}
	var out []answer
	for i := range step.Calls {
		c := &step.Calls[i]
		if c.Status != run.ToolWaiting || c.Waiting == nil || c.Waiting.Kind != run.ResponseExternal {
			continue
		}
		responder, ok := rs.Tools[c.ToolRef]
		if !ok {
			continue
		}
		rs.mu.Lock()
		if _, busy := rs.answering[c.Waiting.ID]; busy {
			rs.mu.Unlock()
			continue
		}
		if rs.answering == nil {
			rs.answering = make(map[run.ResponseID]struct{})
		}
		rs.answering[c.Waiting.ID] = struct{}{}
		rs.mu.Unlock()
		out = append(out, answer{responder: responder,
			call: WaitingCall{Request: *run.CloneResponseRequest(c.Waiting), ToolRef: c.ToolRef, Arguments: c.Arguments}})
	}
	return out
}

func (rs *Responders) release(a *answer) {
	rs.mu.Lock()
	delete(rs.answering, a.call.Request.ID)
	rs.mu.Unlock()
}

// answerWaiting asks the Responder of every claimable wait of the Run and
// commits the answers, concurrently, before returning. settled reports that
// at least one answer was committed, so the drive continues. A lost
// ownership ends the drive with its error; other failures reach Fail and
// leave the wait for the next drive.
func (rs *Responders) answerWaiting(ctx context.Context, w writer.Writer, runID run.RunID) (settled bool, err error) {
	answers := rs.claim(ctx, w, runID)
	if len(answers) == 0 {
		return false, nil
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var lost error
	for i := range answers {
		wg.Add(1)
		go func(a *answer) {
			defer wg.Done()
			defer rs.release(a)
			ok, err := rs.respond(ctx, w, a)
			mu.Lock()
			defer mu.Unlock()
			settled = settled || ok
			if err != nil && lost == nil {
				lost = err
			}
		}(&answers[i])
	}
	wg.Wait()
	return settled, lost
}

// answerAll asks the Responder of every claimable wait of every active Run
// of the Session, each in its own goroutine under lifetime, and calls then
// for each committed answer. It returns at once. Reads of the Runs go by
// ctx; the answers and their settlement go by lifetime.
func (rs *Responders) answerAll(ctx context.Context, w writer.Writer, lifetime context.Context, then func(*answer)) {
	if len(rs.Tools) == 0 {
		return
	}
	state, _, err := w.Projections().Load(ctx, w.SessionID(), sessionstore.MachineProjectionID, sessionstore.MachineProjection.Version)
	if err != nil {
		return
	}
	machine, ok := state.(sessionstore.Machine)
	if !ok {
		return
	}
	for runID := range machine.Active {
		answers := rs.claim(ctx, w, runID)
		for i := range answers {
			go func(a answer) {
				defer rs.release(&a)
				ok, err := rs.respond(lifetime, w, &a)
				if err != nil {
					rs.fail(w.SessionID(), err)
				}
				if ok {
					then(&a)
				}
			}(answers[i])
		}
	}
}

// respond asks the Responder and settles its answer; ok reports a committed
// settlement. A cancelled ctx leaves the wait for the next drive.
func (rs *Responders) respond(ctx context.Context, w writer.Writer, a *answer) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	payload, rerr := a.responder.Respond(ctx, w, &a.call)
	if ctx.Err() != nil {
		return false, nil // the drive was cancelled: the wait stays for the next one
	}
	if err := rs.settleResponse(ctx, w, a, payload, rerr); err != nil {
		if lostOwnership(err) {
			return false, err
		}
		rs.fail(w.SessionID(), fmt.Errorf("driver: settle response of run %s call %s: %w", a.call.Request.RunID, a.call.Request.CallID, err))
		return false, nil
	}
	return true, nil
}

func lostOwnership(err error) bool {
	return errors.Is(err, store.ErrOwnershipLost) || errors.Is(err, &ledger.Error{Code: ledger.CodeOwnershipLost}) || session.IsCode(err, session.ErrOwnershipLost)
}

// settleResponse commits the answer as SubmitToolResponse, or the error as
// RejectToolCall; a Run already terminal is settled by another actor.
func (rs *Responders) settleResponse(ctx context.Context, w writer.Writer, a *answer, payload run.CanonicalJSON, rerr error) error {
	req := a.call.Request
	var cmd run.AgentCommand
	if rerr != nil {
		digest, err := schema.Canonical().DigestToolResponseDecision(req.Kind, run.ResponseDecisionRejected, rerr.Error())
		if err != nil {
			return err
		}
		cmd = run.RejectToolCall{StepID: req.StepID, CallID: req.CallID, ResponseID: req.ID, ResponseDigest: digest, Reason: rerr.Error()}
	} else {
		digest, err := schema.Canonical().DigestToolResponsePayload(payload)
		if err != nil {
			return err
		}
		cmd = run.SubmitToolResponse{StepID: req.StepID, CallID: req.CallID, ResponseID: req.ID, ResponseDigest: digest, Payload: payload}
	}
	env, err := schema.Wire().Envelope(req.RunID, schema.Identity().DeriveResponseCommandID(req.RunID, req.StepID, req.CallID, req.ID), cmd)
	if err != nil {
		return err
	}
	_, err = rs.Runs.Bind(w).Commit(ctx, store.CommitRequest{Command: env})
	if errors.Is(err, run.ErrRunTerminal) {
		return nil // settled by another actor already: nothing to do here
	}
	return err
}
