package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/turn"
)

// The command Kinds this agent applies from a Session's inbox (APP-INB-2),
// with their payload types. A Kind outside this set is rejected.
const (
	// CommandSubmit submits text under a caller-chosen InputID and routes
	// it (Send without waiting); payload SubmitCommand.
	CommandSubmit inbox.Kind = "submit"
	// CommandStop stops the active Turn; payload StopCommand.
	CommandStop inbox.Kind = "stop"
	// CommandWithdraw withdraws a submitted, undelivered input; payload
	// WithdrawCommand.
	CommandWithdraw inbox.Kind = "withdraw"
	// CommandRetry retries the first Turn awaiting Retry; payload
	// RetryCommand.
	CommandRetry inbox.Kind = "retry"
	// CommandBindWorkspace binds the Session to a Workspace (APP-WSP-2);
	// payload BindWorkspaceCommand.
	CommandBindWorkspace inbox.Kind = "bind_workspace"
	// CommandUnbindWorkspace ends the Session's workspace binding; payload
	// UnbindWorkspaceCommand.
	CommandUnbindWorkspace inbox.Kind = "unbind_workspace"
)

type BindWorkspaceCommand struct {
	WorkspaceID workspace.ID `json:"workspaceId"`
}

type UnbindWorkspaceCommand struct {
	Reason string `json:"reason,omitempty"`
}

type SubmitCommand struct {
	InputID run.InputID `json:"inputId"`
	Text    string      `json:"text"`
}

type StopCommand struct {
	// TurnID, when set, must be the active Turn; the Stop is rejected
	// otherwise, so a stale client never stops a Turn it did not see.
	TurnID turn.TurnID `json:"turnId,omitempty"`
	Reason string      `json:"reason,omitempty"`
}

type WithdrawCommand struct {
	InputID run.InputID `json:"inputId"`
	Reason  string      `json:"reason,omitempty"`
}

type RetryCommand struct {
	Reason string `json:"reason,omitempty"`
}

// ErrNoInbox reports an inbox operation on an application built without
// Config.Inbox.
var ErrNoInbox = errors.New("app: no inbox store is configured")

// errRejected marks an application failure that is a committed-state
// answer, not a transient one: the command resolves rejected.
var errRejected = errors.New("app: command rejected")

// DefaultInboxPoll is how often an open Session reads its inbox when
// SessionOptions.InboxPoll is zero: the bound on the delay of a command
// whose wake-up did not reach this process.
const DefaultInboxPoll = time.Second

// NewCommand builds an inbox.Command with payload canonicalized.
func NewCommand(id inbox.CommandID, kind inbox.Kind, payload any) (inbox.Command, error) {
	raw, err := run.CanonicalJSONFromValue(payload)
	if err != nil {
		return inbox.Command{}, err
	}
	return inbox.Command{ID: id, Kind: kind, Payload: raw}, nil
}

// Enqueue is the caller side of APP-INB-1: the command is durable when
// Enqueue returns, whoever owns the Session and whether anyone does. When
// this process holds the Session open, its applier is woken; otherwise the
// command waits for the next owner's Open (APP-INB-3) or poll.
func (app *Application) Enqueue(ctx context.Context, sid session.SessionID, c inbox.Command) (inbox.Entry, error) {
	if app.inbox == nil {
		return inbox.Entry{}, ErrNoInbox
	}
	e, err := app.inbox.Enqueue(ctx, sid, c)
	if err != nil {
		return inbox.Entry{}, err
	}
	if e.Pending() {
		app.mu.RLock()
		s := app.sessions[sid]
		app.mu.RUnlock()
		switch {
		case s != nil:
			s.wakeInbox()
		case app.activation != nil:
			app.activateInBackground(sid)
		}
	}
	return e, nil
}

// AwaitCommand blocks until the command is resolved or ctx ends. A
// resolution by this process wakes it at once; one by another owner is
// seen at the next read, bounded by awaitPoll.
func (app *Application) AwaitCommand(ctx context.Context, sid session.SessionID, id inbox.CommandID) (inbox.Result, error) {
	if app.inbox == nil {
		return inbox.Result{}, ErrNoInbox
	}
	for {
		resolved := app.resolvedSignal()
		e, ok, err := app.inbox.Lookup(ctx, sid, id)
		if err != nil {
			return inbox.Result{}, err
		}
		if !ok {
			return inbox.Result{}, fmt.Errorf("app: command %s is not in the inbox of %s", id, sid)
		}
		if !e.Pending() {
			return *e.Result, nil
		}
		timer := time.NewTimer(awaitPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return inbox.Result{}, ctx.Err()
		case <-resolved:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// awaitPoll bounds how late a resolution by another process is seen.
const awaitPoll = time.Second

// resolvedSignal is a channel closed at the next resolution this process
// records; the caller takes it before reading, so a resolution between the
// read and the wait is not missed.
func (app *Application) resolvedSignal() <-chan struct{} {
	app.resolvedMu.Lock()
	defer app.resolvedMu.Unlock()
	if app.resolved == nil {
		app.resolved = make(chan struct{})
	}
	return app.resolved
}

// notifyResolved wakes every AwaitCommand of this process.
func (app *Application) notifyResolved() {
	app.resolvedMu.Lock()
	defer app.resolvedMu.Unlock()
	if app.resolved != nil {
		close(app.resolved)
		app.resolved = nil
	}
}

// LookupCommand returns the command's entry as stored; ok is false for an
// unknown command.
func (app *Application) LookupCommand(ctx context.Context, sid session.SessionID, id inbox.CommandID) (inbox.Entry, bool, error) {
	if app.inbox == nil {
		return inbox.Entry{}, false, ErrNoInbox
	}
	return app.inbox.Lookup(ctx, sid, id)
}

// PendingSessions returns Sessions with unapplied commands (CLD-CMD-4), at
// most limit of them (0 for all): what the activation scan opens next.
func (app *Application) PendingSessions(ctx context.Context, limit int) ([]session.SessionID, error) {
	if app.inbox == nil {
		return nil, ErrNoInbox
	}
	return app.inbox.Sessions(ctx, limit)
}

// --- owner side ---------------------------------------------------------------

// Wake asks the Session's applier to read its inbox now (CLD-GWY-2): the
// wake-up a gateway sends after enqueuing a command. It never blocks.
func (s *Session) Wake() { s.wakeInbox() }

func (s *Session) wakeInbox() {
	if s.inboxWake == nil {
		return
	}
	select {
	case s.inboxWake <- struct{}{}:
	default:
	}
}

// startInbox applies the pending commands once, then keeps applying them
// on every wake-up and every poll until Close (APP-INB-3).
func (s *Session) startInbox(ctx context.Context) {
	if s.app.inbox == nil {
		return
	}
	s.inboxWake = make(chan struct{}, 1)
	if _, err := s.ApplyPending(ctx); err != nil {
		s.app.warn(fmt.Errorf("app: applying the inbox of %s on open: %w", s.sid, err))
	}
	poll := s.opts.InboxPoll
	if poll <= 0 {
		poll = DefaultInboxPoll
	}
	s.loops.Add(1)
	go func() {
		defer s.loops.Done()
		ticker := time.NewTicker(poll)
		defer ticker.Stop()
		for {
			select {
			case <-s.bg.Done():
				return
			case <-s.inboxWake:
			case <-ticker.C:
			}
			if _, err := s.ApplyPending(s.bg); err != nil && s.bg.Err() == nil {
				s.app.warn(fmt.Errorf("app: applying the inbox of %s: %w", s.sid, err))
			}
		}
	}()
}

// ApplyPending applies the Session's pending commands in Seq order through
// this Session's Writer and resolves each (APP-INB-2). A command the
// committed state does not admit resolves rejected; a transient failure
// (store, fenced Writer) leaves the command pending, stops the pass so the
// order is kept, and is returned. It returns the number of commands
// resolved. Applying is idempotent: a command whose effect was committed
// before a crash prevented its resolution is found already applied.
func (s *Session) ApplyPending(ctx context.Context) (int, error) {
	if s.app.inbox == nil {
		return 0, ErrNoInbox
	}
	s.inboxMu.Lock()
	defer s.inboxMu.Unlock()
	pending, err := s.app.inbox.Pending(ctx, s.sid)
	if err != nil {
		return 0, err
	}
	if len(pending) > 0 {
		s.touch()
	}
	n := 0
	for i := range pending {
		e := &pending[i]
		result := inbox.Result{Status: inbox.StatusApplied}
		if err := s.apply(ctx, &e.Command); err != nil {
			if !rejected(err) {
				return n, fmt.Errorf("command %s (%s): %w", e.Command.ID, e.Command.Kind, err)
			}
			result = inbox.Result{Status: inbox.StatusRejected, Reason: err.Error()}
		}
		if err := s.app.inbox.Resolve(ctx, s.sid, e.Seq, result); err != nil && !errors.Is(err, inbox.ErrNotPending) {
			return n, fmt.Errorf("resolving command %s: %w", e.Command.ID, err)
		}
		s.app.notifyResolved()
		n++
	}
	return n, nil
}

// rejected reports the failures that are answers of the committed state.
func rejected(err error) bool {
	return errors.Is(err, errRejected) || errors.Is(err, turn.ErrConflict) || errors.Is(err, chatlog.ErrNotSubmitted)
}

func decode[T any](c *inbox.Command) (T, error) {
	var v T
	if c.Payload.IsZero() {
		return v, nil
	}
	if err := c.Payload.Decode(&v); err != nil {
		return v, fmt.Errorf("%w: malformed %s payload: %w", errRejected, c.Kind, err)
	}
	return v, nil
}

// apply runs one command against the Session.
func (s *Session) apply(ctx context.Context, c *inbox.Command) error {
	switch c.Kind {
	case CommandSubmit:
		cmd, err := decode[SubmitCommand](c)
		if err != nil {
			return err
		}
		if cmd.InputID == "" {
			return fmt.Errorf("%w: submit without an input id", errRejected)
		}
		_, err = s.SubmitInput(ctx, cmd.InputID, cmd.Text)
		return err
	case CommandStop:
		cmd, err := decode[StopCommand](c)
		if err != nil {
			return err
		}
		status, err := s.Status(ctx)
		if err != nil {
			return err
		}
		if status.Active == "" {
			return fmt.Errorf("%w: no active turn", errRejected)
		}
		if cmd.TurnID != "" && cmd.TurnID != status.Active {
			return fmt.Errorf("%w: turn %s is not the active turn", errRejected, cmd.TurnID)
		}
		_, err = s.a.Turns.Stop(ctx, s.h.Writer(), turn.StopRequest{Ref: s.ref(status.Active), Reason: cmd.Reason})
		return err
	case CommandWithdraw:
		cmd, err := decode[WithdrawCommand](c)
		if err != nil {
			return err
		}
		return s.a.Chatlog.Withdraw(ctx, s.h.Writer(), cmd.InputID, cmd.Reason)
	case CommandRetry:
		cmd, err := decode[RetryCommand](c)
		if err != nil {
			return err
		}
		ref, ok, err := s.retryCommit(ctx, cmd.Reason)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: no turn awaits retry", errRejected)
		}
		s.driveInBackground(ref)
		return nil
	case CommandBindWorkspace:
		cmd, err := decode[BindWorkspaceCommand](c)
		if err != nil {
			return err
		}
		if cmd.WorkspaceID == "" {
			return fmt.Errorf("%w: bind_workspace without a workspace id", errRejected)
		}
		err = s.BindWorkspace(ctx, cmd.WorkspaceID)
		if errors.Is(err, workspace.ErrNotFound) || errors.Is(err, ErrNoWorkspaces) {
			return fmt.Errorf("%w: %w", errRejected, err)
		}
		return err
	case CommandUnbindWorkspace:
		cmd, err := decode[UnbindWorkspaceCommand](c)
		if err != nil {
			return err
		}
		err = s.UnbindWorkspace(ctx, cmd.Reason)
		if errors.Is(err, ErrNoWorkspaces) {
			return fmt.Errorf("%w: %w", errRejected, err)
		}
		return err
	default:
		return fmt.Errorf("%w: unknown command kind %q", errRejected, c.Kind)
	}
}
