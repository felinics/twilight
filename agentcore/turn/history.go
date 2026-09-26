package turn

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// History answers boundary questions about a Session's ledger (OWN-FRK-2,
// SPN-5): it reads the turn and chatlog event types, so callers need
// not scan raw commits themselves.
type History struct {
	Store       session.Store
	Registry    *extension.Registry
	Projections extension.ProjectionReader
}

// StartCommit finds the commit of sid's ledger that carries turnID's
// started fact. It errors when the Turn is not found.
func (h History) StartCommit(ctx context.Context, sid session.SessionID, turnID TurnID) (session.CommitSeq, error) {
	return h.scanBoundary(ctx, sid, turnID, false)
}

// PrefixCommit finds the last commit of sid's history that precedes turnID
// and its inputs: the fork point that excludes the whole Turn, so a child
// rooted there sees the conversation as it stood before the Turn opened,
// without the Turn's submitted inputs (SPN-5). It errors when the Turn
// opens the history.
func (h History) PrefixCommit(ctx context.Context, sid session.SessionID, turnID TurnID) (session.CommitSeq, error) {
	at, err := h.scanBoundary(ctx, sid, turnID, true)
	if err != nil {
		return 0, err
	}
	if at == 0 {
		return 0, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: sid,
			Detail: fmt.Sprintf("turn %s opens the history of %s; there is no prefix to fork", turnID, sid)}
	}
	return at - 1, nil
}

// scanBoundary finds the fork boundary for turnID. Without inputs the
// boundary is the turn's started commit; with inputs it is the earliest
// commit that submitted one of the Turn's inputs, which always precedes the
// started commit.
func (h History) scanBoundary(ctx context.Context, sid session.SessionID, turnID TurnID, includeInputs bool) (session.CommitSeq, error) {
	var inputIDs map[chatlog.InputID]struct{}
	if includeInputs {
		surface, err := ReadSurface(ctx, h.Projections, sid)
		if err != nil {
			return 0, err
		}
		view, ok := surface.Turns[turnID]
		if !ok {
			return 0, fmt.Errorf("%w: turn %s not found in %s", ErrConflict, turnID, sid)
		}
		inputIDs = make(map[chatlog.InputID]struct{}, len(view.InputIDs))
		for _, id := range view.InputIDs {
			inputIDs[id] = struct{}{}
		}
	}
	var from session.CommitSeq
	var started, firstInput session.CommitSeq
	var hasStarted, hasFirstInput bool
	for {
		page, err := h.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: from, Limit: 256})
		if err != nil {
			return 0, err
		}
		for _, c := range page.Commits {
			for _, b := range c.Batches {
				for _, e := range b.Events {
					switch e.Type {
					case TypeStarted:
						if hasStarted {
							continue
						}
						decoded, err := h.Registry.Decode(e)
						if err != nil || decoded.Unknown {
							continue
						}
						if p, ok := decoded.Value.(StartedPayload); ok && p.TurnID == turnID {
							started, hasStarted = c.Seq, true
						}
					case chatlog.TypeInputSubmitted:
						if inputIDs == nil || hasFirstInput {
							continue
						}
						decoded, err := h.Registry.Decode(e)
						if err != nil || decoded.Unknown {
							continue
						}
						if p, ok := decoded.Value.(chatlog.InputSubmittedPayload); ok {
							if _, ours := inputIDs[p.InputID]; ours {
								firstInput, hasFirstInput = c.Seq, true
							}
						}
					}
				}
			}
		}
		if !page.HasMore || len(page.Commits) == 0 {
			if !hasStarted {
				return 0, fmt.Errorf("%w: turn %s not found in %s", ErrConflict, turnID, sid)
			}
			boundary := started
			if hasFirstInput && firstInput < boundary {
				boundary = firstInput
			}
			return boundary, nil
		}
		from = page.Commits[len(page.Commits)-1].Seq + 1
	}
}

// ActiveAt reports the Turn that is active in sid's history as of commit at
// (inclusive): the turn surface folded over commits [0, at]. A fork at such
// a point would hand the child a Turn whose execution belongs to the parent
// (OWN-FRK-1), so callers refuse it.
func (h History) ActiveAt(ctx context.Context, sid session.SessionID, at session.CommitSeq) (TurnID, bool, error) {
	scope, err := h.Registry.ScopeFor(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return "", false, err
	}
	state, err := scope.Def.Initial()
	if err != nil {
		return "", false, err
	}
	var from session.CommitSeq
	for {
		page, err := h.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: from, Limit: 256})
		if err != nil {
			return "", false, err
		}
		var chunk []session.Commit
		for _, c := range page.Commits {
			if c.Seq > at {
				break
			}
			chunk = append(chunk, c)
		}
		if state, err = h.Registry.FoldFrom(scope, state, chunk, page.Header); err != nil {
			return "", false, err
		}
		if !page.HasMore || len(page.Commits) == 0 || page.Commits[len(page.Commits)-1].Seq >= at {
			break
		}
		from = page.Commits[len(page.Commits)-1].Seq + 1
	}
	surface, ok := state.(TurnSurface)
	if !ok {
		return "", false, fmt.Errorf("turn: surface projection is %T", state)
	}
	if active, ok := surface.Active(); ok {
		return active.TurnID, true, nil
	}
	return "", false, nil
}
