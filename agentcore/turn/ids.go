package turn

import (
	"github.com/felinics/twilight/agentcore/jsonstable"
)

type TurnID string
type StepID string
type CallID string
type CommandID string
type ResponseID string
type InputID string
type ToolRef string
type ModelRef string

// TargetRef is an opaque resource identity an Effect may operate on. Agent
// Core preserves it for execution routing but never interprets its Kind or
// lifecycle; optional domains such as Workspace define those semantics.
type TargetRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Digest is "sha256:<64 lowercase hex>" over canonical protocol bytes.
// It remains an alias while Turn protocol types live in this package.
type Digest = jsonstable.Digest

// PromptToken is opaque to agent; the application uses it to identify the
// context revision from which a Prompt was built.
type PromptToken string

// EffectID is the derived identity of one request for an external effect:
// the model call of one ModelStep or one tool call of a ToolStep
// (Identity.DeriveEffectID). The requester fixes the effect's kind and
// binding -- a ModelStep's effect is its model call under RequestDigest, a
// tool call's effect is the call with its frozen arguments -- so the Turn carries
// no effect record beside the requester. It is the only handle the Turn and
// the execution plane share: the Turn records it in the start fact and
// settles or recovers the effect under CommandIDs derived from it; the
// executor keys the attempts it makes for the effect by it. Which worker
// runs an attempt, under what lease and with what backend handle never
// enters the Turn.
type EffectID string

// Scope is the opaque identity of the store one Turn lives in: a Session in
// Twilight. Turn never interprets it. It only scopes what has to stay distinct
// across stores -- execution keys handed to a shared executor, the prompt
// builder's read of the surrounding conversation -- and the adapter that
// realizes TurnStore converts it to and from its own identity type.
type Scope string

// TurnPosition is the index of a Turn's last fact in the Turn's own stream.
// Only the Turn's own facts move it; whatever else the surrounding store
// appends leaves it untouched, which is what makes Prepare's hard CAS
// insensitive to concurrent writes of other modules.
type TurnPosition uint64
