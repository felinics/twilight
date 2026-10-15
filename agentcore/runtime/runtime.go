// Package runtime interprets a Session's durable conversation state. A Step
// performs finite work through an explicitly supplied Writer; it never owns
// that Writer, waits for an external Outcome, or keeps a background lifetime.
package runtime

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
)

type Turns interface {
	sessionkernel.Commands
	sessionkernel.Reader
}

type Config struct {
	Writer    writer.Writer
	Driver    *driver.Driver
	Turns     Turns
	Chatlog   *chatlog.Commands
	Preset    preset.PresetRef
	NewTurnID func() turn.TurnID
}

// SessionRuntime is scoped to one write capability. Hosts construct a new
// instance for each Activation rather than retaining it across ownership.
type SessionRuntime struct {
	w      writer.Writer
	driver *driver.Driver
	turns  Turns
	chat   *chatlog.Commands
	preset preset.PresetRef
	newID  func() turn.TurnID
	// advanceAfterRoute keeps a Turn-placement race in the same finite drive:
	// once delivery observes that the active Turn settled, the replacement Turn
	// may advance immediately after it is routed.
	advanceAfterRoute bool
}

func New(cfg Config) (*SessionRuntime, error) {
	if cfg.Writer == nil || cfg.Driver == nil || cfg.Turns == nil || cfg.Chatlog == nil {
		return nil, errors.New("runtime: writer, driver, turn and chatlog services are required")
	}
	if cfg.Preset.ID == "" || cfg.Preset.Digest == "" {
		return nil, errors.New("runtime: a preset ref is required")
	}
	newID := cfg.NewTurnID
	if newID == nil {
		newID = turn.NewTurnID
	}
	return &SessionRuntime{w: cfg.Writer, driver: cfg.Driver, turns: cfg.Turns,
		chat: cfg.Chatlog, preset: cfg.Preset, newID: newID}, nil
}

func (r *SessionRuntime) ref(id turn.TurnID) sessionstore.TurnRef {
	return sessionstore.TurnRef{SessionID: r.w.SessionID(), TurnID: id}
}

func (r *SessionRuntime) QueueInput(ctx context.Context, id turn.InputID, content turn.CanonicalJSON) (turn.AgentInput, error) {
	return r.QueueInputWithPlacement(ctx, id, content, chatlog.PlacementNextTurn)
}

func (r *SessionRuntime) QueueInputWithPlacement(ctx context.Context, id turn.InputID, content turn.CanonicalJSON, placement chatlog.Placement) (turn.AgentInput, error) {
	return r.chat.SubmitWithPlacement(ctx, r.w, id, content, placement)
}

// DeliverInput explicitly targets a Turn; it never changes the placement of
// a queued input implicitly or waits for the Turn to finish.
func (r *SessionRuntime) DeliverInput(ctx context.Context, id turn.TurnID, input turn.InputID, content turn.CanonicalJSON) (sessionkernel.TurnResult, error) {
	in, err := r.QueueInput(ctx, input, content)
	if err != nil {
		return sessionkernel.TurnResult{}, err
	}
	return r.turns.Deliver(ctx, r.w, sessionkernel.DeliverRequest{Ref: r.ref(id), Inputs: []turn.AgentInput{in}})
}

// Step advances an active Turn or routes submitted inputs into the next Turn.
// It stops at a durable boundary. Continue is also the host's opportunity to
// yield its time slice, provided a durable continuation remains discoverable.
func (r *SessionRuntime) Step(ctx context.Context) (driver.StepResult, error) {
	if err := ctx.Err(); err != nil {
		return driver.StepResult{}, err
	}
	surface, err := sessionstore.ReadSurface(ctx, r.w.Projections(), r.w.SessionID())
	if err != nil {
		return driver.StepResult{}, err
	}
	if active, ok := surface.Active(); ok {
		chat, err := chatlog.ReadSurface(ctx, r.w.Projections(), r.w.SessionID())
		if err != nil {
			return driver.StepResult{}, err
		}
		var deliver []turn.AgentInput
		for _, pending := range chat.SubmittedInputs() {
			if pending.Placement == chatlog.PlacementTurn {
				deliver = append(deliver, turn.AgentInput{ID: turn.InputID(pending.ID), Digest: pending.Digest})
			}
		}
		if len(deliver) > 0 {
			res, err := r.turns.Deliver(ctx, r.w, sessionkernel.DeliverRequest{Ref: r.ref(active.TurnID), Inputs: deliver})
			if err != nil {
				// The command may have durably stopped the Turn before rejecting
				// delivery. Preserve the fall-forward policy for the next Step.
				r.advanceAfterRoute = true
				return driver.StepResult{}, err
			}
			if res.Status != turn.TurnActive {
				r.advanceAfterRoute = true
			}
		}
		return r.driver.Step(ctx, r.w, active.TurnID)
	}
	// A Session can have outstanding maintenance without any active Turn.
	if r.driver.Compactions != nil {
		res, err := r.driver.Compactions.Advance(ctx, r.w)
		if err != nil {
			return driver.StepResult{}, err
		}
		if res.Pending {
			return driver.StepResult{Disposition: driver.Waiting}, nil
		}
	}
	chat, err := chatlog.ReadSurface(ctx, r.w.Projections(), r.w.SessionID())
	if err != nil {
		return driver.StepResult{}, err
	}
	pending := chat.SubmittedInputs()
	if len(pending) == 0 {
		return driver.StepResult{Disposition: driver.Finished}, nil
	}
	inputs := make([]turn.AgentInput, 0, len(pending))
	for _, in := range pending {
		inputs = append(inputs, turn.AgentInput{ID: turn.InputID(in.ID), Digest: in.Digest})
	}
	if len(inputs) == 0 {
		return driver.StepResult{Disposition: driver.Finished}, nil
	}
	ref := r.ref(r.newID())
	_, err = r.turns.Start(ctx, r.w, sessionkernel.StartRequest{Ref: ref, Inputs: inputs, Preset: r.preset})
	if err != nil {
		return driver.StepResult{}, err
	}
	if r.advanceAfterRoute {
		r.advanceAfterRoute = false
		return r.driver.Step(ctx, r.w, ref.TurnID)
	}
	// Starting a Turn is the routing boundary for this finite step. Leave model
	// preparation to the next Step so a host time slice can yield after routing
	// without unexpectedly dispatching external work.
	return driver.StepResult{Disposition: driver.Continue}, nil
}

// Stop records the domain cancellation. Physical cancellation is recovered
// from that durable disposition, never inferred from an Activation's context.
func (r *SessionRuntime) Stop(ctx context.Context, reason string) (sessionkernel.TurnResult, bool, error) {
	surface, err := sessionstore.ReadSurface(ctx, r.w.Projections(), r.w.SessionID())
	if err != nil {
		return sessionkernel.TurnResult{}, false, err
	}
	active, ok := surface.Active()
	if !ok {
		return sessionkernel.TurnResult{}, false, nil
	}
	res, err := r.turns.Stop(ctx, r.w, sessionkernel.StopRequest{Ref: r.ref(active.TurnID), Reason: reason})
	return res, true, err
}
