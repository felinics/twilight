package chatlog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// ReadSurface loads the chatlog surface of one Session through r.
func ReadSurface(ctx context.Context, r extension.ProjectionReader, sid session.SessionID) (Surface, error) {
	state, _, err := r.Load(ctx, sid, SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return Surface{}, err
	}
	surface, ok := state.(Surface)
	if !ok {
		return Surface{}, fmt.Errorf("chatlog: surface projection is %T", state)
	}
	return surface, nil
}

// ReadContext loads the chatlog context of one Session through r.
func ReadContext(ctx context.Context, r extension.ProjectionReader, sid session.SessionID) (Context, error) {
	state, _, err := r.Load(ctx, sid, ContextProjectionID, ContextProjection.Version)
	if err != nil {
		return Context{}, err
	}
	c, ok := state.(Context)
	if !ok {
		return Context{}, fmt.Errorf("chatlog: context projection is %T", state)
	}
	return c, nil
}

// LastAssistantText is the text of the Turn's last assistant entry: the
// Surface names the frozen ModelResult by digest and content resolves it
// (CHT-MAT-1). It is empty when the Turn has no assistant entry.
func LastAssistantText(ctx context.Context, r extension.ProjectionReader, content ContentResolver, sid session.SessionID, turnID TurnID) (string, error) {
	surface, err := ReadSurface(ctx, r, sid)
	if err != nil {
		return "", err
	}
	return surface.LastAssistantText(ctx, content, turnID)
}

// LastAssistantText is LastAssistantText over an already loaded Surface.
func (s *Surface) LastAssistantText(ctx context.Context, content ContentResolver, turnID TurnID) (string, error) {
	for i := len(s.EntryOrder) - 1; i >= 0; i-- {
		e := s.EntryOrder[i]
		if e.Kind != EntryAssistant {
			continue
		}
		a, ok := s.Assistants.Get(AssistantID(e.ID))
		if !ok || a.TurnID != turnID {
			continue
		}
		entry := Entry{Kind: EntryAssistant, ID: e.ID, Digest: a.Digest, Assistant: &a}
		m, err := NewMaterializer(content).Entry(ctx, &entry)
		if err != nil {
			return "", err
		}
		return m.Text(), nil
	}
	return "", nil
}

// NewInputID mints a collision-free InputID; the chatlog requires
// session-global uniqueness across restarts (CHT-EVT-1).
func NewInputID() run.InputID { return run.InputID("in-" + randomSuffix(8)) }

func randomSuffix(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("chatlog: rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}
