package sessionstore

import (
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

func digestOf(domain string, parts ...string) jsonstable.Digest {
	raw, _ := jsonstable.EncodeTypedPayload(1, domain, parts)
	return jsonstable.DigestBytes(raw)
}

// PlanDigest identifies the immutable Turn start intent.
func PlanDigest(id turn.TurnID, preset ref.Ref, inputs []turn.AgentInput) jsonstable.Digest {
	parts := []string{string(id), string(preset.ID), string(preset.Digest)}
	for _, input := range inputs {
		parts = append(parts, string(input.ID), string(input.Digest))
	}
	return digestOf("twilight/turn/plan", parts...)
}
func StartOperationDigest(sid session.SessionID, id turn.TurnID, plan jsonstable.Digest) jsonstable.Digest {
	return digestOf("twilight/turn/start-operation", string(sid), string(id), string(plan))
}
func CancelCommandID(sid session.SessionID, id turn.TurnID) turn.CommandID {
	return turn.CommandID(digestOf("twilight/turn/cancel", string(sid), string(id)))
}
