package app

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agent/spawn"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// children is spawn.Children over this application: a child Session is
// created, forked, read and driven exactly as any Session the application
// opens, so the subagent tool adds no second conversation protocol.
type children struct{ app *Application }

func (c children) Open(ctx context.Context, sid session.SessionID, pref preset.PresetRef) (spawn.Child, error) {
	s, err := c.app.OpenSession(ctx, sid, SessionOptions{Preset: pref})
	if err != nil {
		return nil, err
	}
	return child{s}, nil
}

func (c children) Create(ctx context.Context, sid session.SessionID, ext module.Extensions) error {
	return c.app.svc.Lifecycle.Create(ctx, sid, ext)
}

// ForkBeforeInputs forks parent at the last commit before turnID and its
// inputs: the conversation as it stood before that Turn was asked, under
// the same quiescence guard every fork passes.
func (c children) ForkBeforeInputs(ctx context.Context, parent session.SessionID, turnID turn.TurnID, sid session.SessionID, ext module.Extensions) error {
	at, err := c.app.history.BeforeInputs(ctx, parent, turnID)
	if err != nil {
		return err
	}
	_, err = c.app.Fork(ctx, ForkRequest{Parent: parent, At: at, Child: sid, Ext: ext})
	return err
}

func (c children) Header(ctx context.Context, sid session.SessionID) (session.SegmentHeader, error) {
	return c.app.svc.Store.Header(ctx, sid)
}

func (c children) TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	return c.app.TurnSurface(ctx, sid)
}

// AwaitingRecovery reads the Turn's disposition: a Run Executing with no
// Session of this host carrying it waits for the takeover disposition.
func (c children) AwaitingRecovery(ctx context.Context, ref turn.TurnRef) (bool, error) {
	res, err := c.app.svc.Turns.Status(ctx, ref)
	if err != nil {
		return false, err
	}
	return res.Status == turn.TurnActive && res.Disposition == turn.ResumeExecuting, nil
}

// InputText is the text of the Turn's first input as this agent shapes
// inputs; empty for a Turn with none or one whose body is not text.
func (c children) InputText(ctx context.Context, ref turn.TurnRef) (string, error) {
	surface, err := c.app.TurnSurface(ctx, ref.SessionID)
	if err != nil {
		return "", err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok || len(view.InputIDs) == 0 {
		return "", nil
	}
	chat, err := c.app.ChatlogSurface(ctx, ref.SessionID)
	if err != nil {
		return "", err
	}
	in, ok := chat.Inputs.Get(view.InputIDs[0])
	if !ok {
		return "", nil
	}
	text, err := input.TextOf(in.Input.Content)
	if err != nil {
		return "", nil //nolint:nilerr // a body that is not text is no text
	}
	return text, nil
}

func (c children) Reply(ctx context.Context, ref turn.TurnRef) (string, error) {
	return c.app.Reply(ctx, ref)
}

// child is spawn.Child over an open Session.
type child struct{ s *Session }

func (c child) Resume(ctx context.Context) ([]turn.TurnID, bool, error) {
	results, ok, err := c.s.Resume(ctx)
	if !ok {
		return nil, false, err
	}
	ids := make([]turn.TurnID, len(results))
	for i := range results {
		ids[i] = results[i].TurnID
	}
	return ids, true, err
}

func (c child) Settle(ctx context.Context, turnID turn.TurnID) error {
	_, err := c.s.Settle(ctx, turnID)
	return err
}

func (c child) Send(ctx context.Context, text string) (turn.TurnID, error) {
	results, err := c.s.Send(ctx, text)
	if len(results) == 0 {
		if err == nil {
			err = fmt.Errorf("app: send to %s settled no turn", c.s.sid)
		}
		return "", err
	}
	return results[0].TurnID, err
}

func (c child) Close(ctx context.Context) error { return c.s.Close(ctx) }
