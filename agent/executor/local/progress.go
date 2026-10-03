package local

import (
	"context"
	"encoding/json"

	effect "github.com/felinics/twilight/agentcore/run/effect"
)

// progressSink is the ToolProgressSink of one tool execution: each payload
// becomes a tool_progress frame of the call's key.
type progressSink struct {
	sink effect.ProgressSink
	key  AssignmentKey
}

func (p *progressSink) Publish(ctx context.Context, progress ToolProgress) {
	if p.sink == nil {
		return
	}
	p.sink.Publish(ctx, effect.ProgressFrame{Key: p.key, Kind: effect.ProgressToolProgress, Payload: progress.Payload})
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}
