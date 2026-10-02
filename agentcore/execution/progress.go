package execution

import (
	"context"

	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/session"
)

// forwardProgress subscribes to key's frames on port and publishes each to
// the transient stream: this is how deltas produced wherever the effect
// runs reach the host's observation stream. It ends with the stream, the
// context, or a port that has no progress for the key.
func forwardProgress(ctx context.Context, port effect.ProgressPort, key effect.AssignmentKey, progress *observe.Progresses) {
	sid := session.SessionID(key.Session)
	_ = port.Progress(ctx, key, 0, func(f effect.ProgressFrame) bool {
		if f.Kind == effect.ProgressEnd {
			return true
		}
		progress.Publish(sid, observe.Progress{RunID: key.RunID, Effect: key.Effect, Generation: f.Generation,
			Sequence: f.Sequence, Kind: string(f.Kind), Payload: f.Payload})
		return true
	})
}
