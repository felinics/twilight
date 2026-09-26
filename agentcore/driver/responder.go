package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Responder answers an ExternalResponse wait on the system's behalf (DRV-4):
// a tool whose ResponsePolicy is ExternalResponse settles not through the
// executor but through whoever answers its ResponseRequest, and a
// Responder is such an answerer living in the owner process (the subagent
// tool, SPN-1). A drive that leaves such a call waiting asks the Responder
// and drives on with its answer, and a Session that opens with such calls
// waiting asks again, so a Responder must be idempotent against its own
// durable state: a crash mid-answer is continued, not repeated. The payload
// it returns settles the call as a success (SubmitToolResponse); an error
// rejects it (RejectToolCall, response_rejected) with the error as reason.
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

// answer is one wait claimed for answering.
type answer struct {
	responder Responder
	call      WaitingCall
}

// claimAnswers returns the ExternalResponse waits of the Run that have a
// Responder and are not being answered, marking them as being answered; the
// caller releases each with release.
func (d *Driver) claimAnswers(ctx context.Context, w writer.Writer, runID run.RunID) []answer {
	if len(d.Responders) == 0 {
		return nil
	}
	snap, err := d.Runs.Bind(w).Load(ctx, runID)
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
		responder, ok := d.Responders[c.ToolRef]
		if !ok {
			continue
		}
		d.mu.Lock()
		if _, busy := d.answering[c.Waiting.ID]; busy {
			d.mu.Unlock()
			continue
		}
		d.answering[c.Waiting.ID] = struct{}{}
		d.mu.Unlock()
		out = append(out, answer{responder: responder,
			call: WaitingCall{Request: *run.CloneResponseRequest(c.Waiting), ToolRef: c.ToolRef, Arguments: c.Arguments}})
	}
	return out
}

func (d *Driver) release(a *answer) {
	d.mu.Lock()
	delete(d.answering, a.call.Request.ID)
	d.mu.Unlock()
}

// answerWaiting answers, concurrently and to completion, every claimable
// wait of the Run and reports whether it settled any: the caller then
// drives on. It runs under the caller's context like the rest of the drive;
// a cancelled answer leaves the wait for the next drive or Open. A lost
// ownership is returned like the Loop returns it (RUN-LOP-5): the drive
// ends with it.
func (d *Driver) answerWaiting(ctx context.Context, w writer.Writer, runID run.RunID) (bool, error) {
	answers := d.claimAnswers(ctx, w, runID)
	if len(answers) == 0 {
		return false, nil
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	settled := false
	var lost error
	for i := range answers {
		wg.Add(1)
		go func(a *answer) {
			defer wg.Done()
			defer d.release(a)
			ok, err := d.respond(ctx, w, a)
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

// answerAllWaiting is the Open-time pass (DRV-4, SPN-4): every claimable
// wait of every live Run is answered under the Session's recovery lifetime,
// each followed by a drive of its Turn. Open does not wait for them.
func (d *Driver) answerAllWaiting(ctx context.Context, w writer.Writer) {
	if len(d.Responders) == 0 {
		return
	}
	state, _, err := w.Projections().Load(ctx, w.SessionID(), runmod.MachineProjectionID, runmod.MachineProjection.Version)
	if err != nil {
		return
	}
	machine, ok := state.(runmod.Machine)
	if !ok {
		return
	}
	lt := d.ensureRecoveryLifetime(w)
	for runID := range machine.Active {
		answers := d.claimAnswers(ctx, w, runID)
		for i := range answers {
			go func(a answer) {
				defer d.release(&a)
				ok, err := d.respond(lt.ctx, lt.w, &a)
				if err != nil {
					d.fail(lt.w.SessionID(), err)
				}
				if !ok {
					return
				}
				surface, err := turn.ReadSurface(lt.ctx, lt.w.Projections(), lt.w.SessionID())
				if err != nil {
					d.fail(lt.w.SessionID(), err)
					return
				}
				if turnID, ok := surface.OwnerOf(a.call.Request.RunID); ok {
					if _, err := d.Drive(lt.ctx, lt.w, turnID); err != nil {
						d.fail(lt.w.SessionID(), fmt.Errorf("driver: drive after response of run %s: %w", a.call.Request.RunID, err))
					}
				}
			}(answers[i])
		}
	}
}

// respond runs one answer to its commit; it reports false when the answer
// was not settled: the context ended, or the commit failed. A commit fenced
// by a new owner is returned as the error (RUN-LOP-5); any other failure is
// reported through Fail and the wait stays for the next drive.
func (d *Driver) respond(ctx context.Context, w writer.Writer, a *answer) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	payload, rerr := a.responder.Respond(ctx, w, &a.call)
	if ctx.Err() != nil {
		return false, nil // the drive was cancelled: the wait stays for the next one
	}
	if err := d.settleResponse(ctx, w, a, payload, rerr); err != nil {
		if lostOwnership(err) {
			return false, err
		}
		d.fail(w.SessionID(), fmt.Errorf("driver: settle response of run %s call %s: %w", a.call.Request.RunID, a.call.Request.CallID, err))
		return false, nil
	}
	return true, nil
}

// lostOwnership reports a commit fenced by a new owner, as the Writer
// (EXT-WRT-4) or the kernel (SES-OWN-2) reports it.
func lostOwnership(err error) bool {
	return errors.Is(err, runtime.ErrOwnershipLost) || errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) || session.IsCode(err, session.ErrOwnershipLost)
}

// settleResponse commits the answer: SubmitToolResponse with the payload,
// or RejectToolCall with the responder's error as reason. Both are keyed
// by the derived response CommandID, so a repeat is already applied.
func (d *Driver) settleResponse(ctx context.Context, w writer.Writer, a *answer, payload run.CanonicalJSON, rerr error) error {
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
	_, err = d.Runs.Bind(w).Commit(ctx, runtime.CommitRequest{Command: env})
	if errors.Is(err, run.ErrRunTerminal) {
		return nil // settled by another actor already: nothing to do here
	}
	return err
}
