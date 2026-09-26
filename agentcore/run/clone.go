package run

import (
	"encoding/json"

	"github.com/felinics/twilight/agentcore/es"
)

// Deep-copy helpers: Runtime return values must be read-only snapshots
// (RUN-CMT-6) — a caller mutating a returned slice or map must never reach
// authoritative storage or committed event bytes.
//
// The agent Runtime is an Owner boundary. All persisted request/result
// shapes are agent-owned JSON-stable values, so cloning is mechanical: copy
// structs and copy slice/map containers. CanonicalJSON values are immutable.

func cloneRaw(v CanonicalJSON) CanonicalJSON { return v }

func cloneAgentInput(in AgentInput) AgentInput { return in }

func CloneResponseRequest(r *ResponseRequest) *ResponseRequest {
	if r == nil {
		return nil
	}
	c := *r
	c.Payload = cloneRaw(c.Payload)
	return &c
}

func cloneToolCallBinding(b *ToolCallBinding) ToolCallBinding {
	out := *b
	out.Arguments = cloneRaw(out.Arguments)
	out.Response = CloneResponseRequest(out.Response)
	return out
}

func cloneToolCallBindings(bs []ToolCallBinding) []ToolCallBinding {
	if bs == nil {
		return nil
	}
	out := make([]ToolCallBinding, len(bs))
	for i := range bs {
		out[i] = cloneToolCallBinding(&bs[i])
	}
	return out
}

func cloneToolSpecs(specs []ToolSpec) []ToolSpec {
	if specs == nil {
		return nil
	}
	return append([]ToolSpec(nil), specs...)
}

func snapshotJSONStable[T any](v T) (T, error) {
	var out T
	raw, err := es.MarshalCanonical(v)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

// SnapshotFact detaches the caller-owned containers a fact may still share
// with its command (tool specs, bindings, input payloads). Digest-only facts
// carry no such containers and are copied by value.
func SnapshotFact(f Fact) (Fact, error) {
	switch fact := f.(type) {
	case ModelStepPrepared:
		return snapshotJSONStable(fact)
	case ToolStepOpened:
		return snapshotJSONStable(fact)
	case InputAccepted:
		return snapshotJSONStable(fact)
	default:
		return cloneFact(f), nil
	}
}

func cloneFact(f Fact) Fact {
	switch fact := f.(type) {
	case ModelStepPrepared:
		fact.InputIDs = append([]InputID(nil), fact.InputIDs...)
		fact.Tools = cloneToolSpecs(fact.Tools)
		return fact
	case ToolStepOpened:
		fact.Calls = cloneToolCallBindings(fact.Calls)
		return fact
	case InputAccepted:
		fact.Input = cloneAgentInput(fact.Input)
		return fact
	case RunEnded:
		if stopped, ok := fact.End.(RunStoppedEnd); ok {
			stopped.UncertainCalls = append([]CallID(nil), stopped.UncertainCalls...)
			fact.End = stopped
		}
		return fact
	default:
		return f
	}
}
